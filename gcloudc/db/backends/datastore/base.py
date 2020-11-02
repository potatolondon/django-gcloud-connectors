import datetime
import decimal
import logging
import os
import uuid
import warnings

import requests

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.base.client import BaseDatabaseClient
from django.db.backends.base.creation import BaseDatabaseCreation
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection,
    TableInfo,
)
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.backends.base.validation import BaseDatabaseValidation
from django.utils import (
    six,
    timezone,
)
from django.utils.encoding import smart_text
from google.cloud import (
    datastore,
    environment_vars,
)

from . import dbapi as Database
from .commands import (
    DeleteCommand,
    FlushCommand,
    InsertCommand,
    SelectCommand,
    UpdateCommand,
    coerce_unicode,
)
from .indexing import load_special_indexes
from .utils import (
    decimal_to_string,
    ensure_datetime,
    get_datastore_key,
    make_utc_compatible,
)

logger = logging.getLogger(__name__)


class Connection(object):
    """ Dummy connection class """

    def __init__(self, wrapper, params):
        self.creation = wrapper.creation
        self.ops = wrapper.ops
        self.queries = []
        self.settings_dict = params
        self.namespace = wrapper.namespace

        self.gclient = datastore.Client(
            namespace=wrapper.namespace,
            project=params["PROJECT"],
            # avoid a bug in the google client - it tries to authenticate even when the emulator is enabled
            # see https://github.com/googleapis/google-cloud-python/issues/5738
            _http=requests.Session if os.environ.get(environment_vars.GCD_HOST) else None,
        )

    def acquire_constraint_markers(self, markers):
        pass

    def release_constraint_markers(self, markers):
        pass

    def rollback(self):
        pass

    def commit(self):
        pass

    def close(self):
        pass


class Cursor(object):
    """ Dummy cursor class """

    def __init__(self, connection):
        self.connection = connection
        self.start_cursor = None
        self.returned_ids = []
        self.rowcount = -1
        self.last_select_command = None
        self.last_delete_command = None

    def execute(self, sql, *params):
        if isinstance(sql, SelectCommand):
            # Also catches subclasses of SelectCommand (e.g Update)
            self.last_select_command = sql
            self.rowcount = self.last_select_command.execute() or -1
        elif isinstance(sql, FlushCommand):
            sql.execute()
        elif isinstance(sql, UpdateCommand):
            self.rowcount = sql.execute()
        elif isinstance(sql, DeleteCommand):
            self.rowcount = sql.execute()
        elif isinstance(sql, InsertCommand):
            self.connection.queries.append(sql)
            self.returned_ids = sql.execute()
        else:
            raise Database.CouldBeSupportedError(
                "Can't execute traditional SQL: '%s' (although perhaps we could make GQL work)" % sql
            )

    def next(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def fetchone(self, delete_flag=False):
        try:
            result = next(self.last_select_command.results)

            if isinstance(result, int):
                return (result,)

            query = self.last_select_command.query

            row = []

            # Prepend extra select values to the resulting row
            for col, select in query.extra_selects:
                row.append(result.get(col))

            for col in self.last_select_command.query.init_list:
                row.append(result.get(col))

            self.returned_ids.append(result.key.id_or_name)
            return row
        except StopIteration:
            return None

    def fetchmany(self, size, delete_flag=False):
        if not self.last_select_command.results:
            return []

        result = []
        for i in range(size):
            entity = self.fetchone(delete_flag)
            if entity is None:
                break

            # Python DB API suggests a list of tuples, and returning
            # a list-of-lists breaks some tests
            result.append(tuple(entity))

        return result

    @property
    def lastrowid(self):
        return self.returned_ids[-1].id_or_name

    def __iter__(self):
        return self

    def close(self):
        pass


MAXINT = 9223372036854775808

_BULK_BATCH_SIZE_SETTING = "BULK_BATCH_SIZE"


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "gcloudc.db.backends.datastore.compiler"

    # Datastore will store all integers as 64bit long values
    integer_field_ranges = {
        "SmallIntegerField": (-MAXINT, MAXINT - 1),
        "IntegerField": (-MAXINT, MAXINT - 1),
        "BigIntegerField": (-MAXINT, MAXINT - 1),
        "PositiveSmallIntegerField": (0, MAXINT - 1),
        "PositiveIntegerField": (0, MAXINT - 1),
    }

    def bulk_batch_size(self, field, objs):
        # This value is used in cascade deletions, and also on bulk insertions
        # This is the limit of the number of entities that can be manipulated in
        # a single transaction

        settings_dict = self.connection.settings_dict
        if _BULK_BATCH_SIZE_SETTING in settings_dict['OPTIONS']:
            return int(settings_dict['OPTIONS'][_BULK_BATCH_SIZE_SETTING])
        return 500

    def quote_name(self, name):
        return name

    def date_trunc_sql(self, lookup_type, field_name):
        return ""

    def datetime_trunc_sql(self, lookup_type, field_name, tzname):
        return "", []

    def datetime_extract_sql(self, lookup_name, sql, tzname):
        return "", []

    def date_extract_sql(self, lookup_name, sql):
        return "", []

    def get_db_converters(self, expression):
        converters = super().get_db_converters(expression)

        db_type = expression.field.db_type(self.connection)
        internal_type = expression.field.get_internal_type()

        if internal_type == "TextField":
            converters.append(self.convert_textfield_value)
        elif internal_type == "DateTimeField":
            converters.append(self.convert_datetime_value)
        elif internal_type == "DateField":
            converters.append(self.convert_date_value)
        elif internal_type == "TimeField":
            converters.append(self.convert_time_value)
        elif internal_type == "DecimalField":
            converters.append(self.convert_decimal_value)
        elif internal_type == 'UUIDField':
            converters.append(self.convert_uuidfield_value)
        elif db_type == "list":
            converters.append(self.convert_list_value)
        elif db_type == "set":
            converters.append(self.convert_set_value)

        return converters

    def convert_uuidfield_value(self, value, expression, connection, context):
        if value is not None:
            value = uuid.UUID(value)
        return value

    def convert_textfield_value(self, value, expression, connection, context=None):
        if isinstance(value, bytes):
            # Should we log a warning here? It shouldn't have been stored as bytes
            value = value.decode("utf-8")
        return value

    def convert_datetime_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_datetime(value)

    def convert_date_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_date(value)

    def convert_time_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_time(value)

    def convert_decimal_value(self, value, expression, connection, context=None):
        return self.connection.ops.value_from_db_decimal(value)

    def convert_list_value(self, value, expression, connection, context=None):
        if expression.output_field.db_type(connection) != "list":
            return value

        if not value:
            value = []
        return value

    def convert_set_value(self, value, expression, connection, context=None):
        if expression.output_field.db_type(connection) != "set":
            return value

        if not value:
            value = set()
        else:
            value = set(value)
        return value

    def sql_flush(self, style, tables, seqs, allow_cascade=False):
        additional_djangaeidx_tables = [
            x
            for x in self.connection.introspection.table_names()
            if [y for y in tables if x.startswith("_djangae_idx_{}".format(y))]
        ]

        return [
            FlushCommand(table, self.connection)
            for table in tables + additional_djangaeidx_tables
        ]

    def prep_lookup_key(self, model, value, field):
        if isinstance(value, six.string_types):
            value = value[:500]
            left = value[500:]
            if left:
                warnings.warn(
                    "Truncating primary key that is over 500 characters. " "THIS IS AN ERROR IN YOUR PROGRAM.",
                    RuntimeWarning,
                )

            # This is a bit of a hack. Basically when you query an integer PK with a
            # string containing an int. SQL seems to return the row regardless of type, and as far as
            # I can tell, Django at no point tries to cast the value to an integer. So, in the
            # case where the internal type is an AutoField, we try to cast the string value
            # I would love a more generic solution... patches welcome!
            # It would be nice to see the SQL output of the lookup_int_as_str test is on SQL, if
            # the string is converted to an int, I'd love to know where!
            if field.get_internal_type() == "AutoField":
                try:
                    value = int(value)
                except (TypeError, ValueError):
                    pass

            value = get_datastore_key(model, value)
        else:
            value = get_datastore_key(model, value)

        return value

    def prep_lookup_date(self, model, value, field):
        if isinstance(value, datetime.datetime):
            return value

        return self.adapt_datefield_value(value)

    def prep_lookup_time(self, model, value, field):
        if isinstance(value, datetime.datetime):
            return value

        return self.adapt_timefield_value(value)

    def prep_lookup_value(self, model, value, field, column=None):
        if field.primary_key and (not column or column == model._meta.pk.column):
            return self.prep_lookup_key(model, value, field)

        db_type = field.db_type(self.connection)

        if db_type == "date":
            return self.prep_lookup_date(model, value, field)
        elif db_type == "time":
            return self.prep_lookup_time(model, value, field)
        elif db_type in ("list", "set"):
            if hasattr(value, "__len__") and not value:
                value = None  # Convert empty lists to None
            elif hasattr(value, "__iter__"):
                # Convert sets to lists
                value = list(value)

        return value

    def value_for_db(self, value, field):
        if value is None:
            return None

        db_type = field.db_type(self.connection)

        if db_type in ("integer", "long"):
            if isinstance(value, float):
                # round() always returns a float, which has a smaller max value than an int
                # so only round() it if it's already a float
                value = round(value)

            value = int(value)
        elif db_type == "float":
            value = float(value)
        elif db_type == "string" or db_type == "text":
            value = coerce_unicode(value)
        elif db_type == "bytes":
            # Store BlobField, DictField and EmbeddedModelField values as Blobs.
            # We encode to bytes, as that's what the Cloud Datastore API expects
            # we use ASCII to make sure there's no funky unicode data, it should
            # be binary
            if isinstance(value, str):
                value = value.encode("ascii")
        elif db_type == "decimal":
            value = self.adapt_decimalfield_value(value, field.max_digits, field.decimal_places)
        elif db_type in ("list", "set"):
            if hasattr(value, "__len__") and not value:
                value = None  # Convert empty lists to None
            elif hasattr(value, "__iter__"):
                # Convert sets to lists
                value = list(value)

        return value

    def last_insert_id(self, cursor, db_table, column):
        return cursor.lastrowid

    def last_executed_query(self, cursor, sql, params):
        """
            We shouldn't have to override this, but Django's BaseOperations.last_executed_query
            assumes does u"QUERY = %r" % (sql) which blows up if you have encode unicode characters
            in your SQL. Technically this is a bug in Django for assuming that sql is ASCII but
            it's only our backend that will ever trigger the problem
        """
        return u"QUERY = {}".format(smart_text(sql))

    def fetch_returned_insert_id(self, cursor):
        return cursor.lastrowid

    def adapt_datetimefield_value(self, value):
        value = make_utc_compatible(value)
        return value

    def value_to_db_datetime(self, value):  # Django 1.8 compatibility
        return self.adapt_datetimefield_value(value)

    def adapt_datefield_value(self, value):
        if value is not None:
            value = datetime.datetime.combine(value, datetime.time())
        return value

    def adapt_timefield_value(self, value):
        if value is not None:
            value = make_utc_compatible(value)
            value = datetime.datetime.combine(datetime.datetime.utcfromtimestamp(0), value)
        return value

    def adapt_decimalfield_value(self, value, max_digits, decimal_places):
        if isinstance(value, decimal.Decimal):
            return decimal_to_string(value, max_digits, decimal_places)
        return value

    def value_to_db_decimal(self, value, max_digits, decimal_places):  # Django 1.8 compatibility
        return self.adapt_decimalfield_value(value, max_digits, decimal_places)

    # Unlike value_to_db, these are not overridden or standard Django, it's just nice to have symmetry
    def value_from_db_datetime(self, value):
        if isinstance(value, int):
            # App Engine Query's don't return datetime fields (unlike Get) I HAVE NO IDEA WHY
            value = ensure_datetime(value)

        if value is not None and settings.USE_TZ and timezone.is_naive(value):
            value = value.replace(tzinfo=timezone.utc)
        return value

    def value_from_db_date(self, value):
        if isinstance(value, int):
            # App Engine Query's don't return datetime fields (unlike Get) I HAVE NO IDEA WHY
            value = ensure_datetime(value)

        if value:
            value = value.date()
        return value

    def value_from_db_time(self, value):
        if isinstance(value, int):
            # App Engine Query's don't return datetime fields (unlike Get) I HAVE NO IDEA WHY
            value = ensure_datetime(value).time()

        if value is not None and settings.USE_TZ and timezone.is_naive(value):
            value = value.replace(tzinfo=timezone.utc)

        if value:
            value = value.time()

        return value

    def value_from_db_decimal(self, value):
        if value:
            value = decimal.Decimal(value)
        return value


class DatabaseClient(BaseDatabaseClient):
    pass


class DatabaseCreation(BaseDatabaseCreation):
    data_types = {
        "AutoField": "key",
        "RelatedAutoField": "key",
        "ForeignKey": "key",
        "OneToOneField": "key",
        "ManyToManyField": "key",
        "BigIntegerField": "long",
        "BooleanField": "bool",
        "CharField": "string",
        "CommaSeparatedIntegerField": "string",
        "DateField": "date",
        "DateTimeField": "datetime",
        "DecimalField": "decimal",
        "DurationField": "long",
        "EmailField": "string",
        "FileField": "string",
        "FilePathField": "string",
        "FloatField": "float",
        "ImageField": "string",
        "IntegerField": "integer",
        "IPAddressField": "string",
        "NullBooleanField": "bool",
        "PositiveIntegerField": "integer",
        "PositiveSmallIntegerField": "integer",
        "SlugField": "string",
        "SmallIntegerField": "integer",
        "TimeField": "time",
        "URLField": "string",
        "TextField": "text",
        "BinaryField": "bytes",
    }

    def __init__(self, *args, **kwargs):
        self.testbed = None
        super(DatabaseCreation, self).__init__(*args, **kwargs)

    def sql_create_model(self, model, *args, **kwargs):
        return [], {}

    def sql_for_pending_references(self, model, *args, **kwargs):
        return []

    def sql_indexes_for_model(self, model, *args, **kwargs):
        return []

    def _create_test_db(self, verbosity, autoclobber, *args):
        pass

    def _destroy_test_db(self, name, verbosity):
        pass


class DatabaseIntrospection(BaseDatabaseIntrospection):
    def get_table_list(self, cursor):
        query = cursor.connection.gclient.query(kind="__kind__")
        query.keys_only()
        kinds = [entity.key.id_or_name for entity in query.fetch()]
        return [TableInfo(x, "t") for x in kinds]

    def get_sequences(self, cursor, table_name, table_fields=()):
        # __key__ is the only column that can auto-populate
        return [{'table': table_name, 'column': '__key__'}]


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    def column_sql(self, model, field, include_default=False):
        return "", {}

    def create_model(self, model):
        """ Don't do anything when creating tables """
        pass

    def alter_unique_together(self, *args, **kwargs):
        pass

    def alter_field(self, from_model, from_field, to_field):
        pass

    def remove_field(self, from_model, field):
        pass

    def add_field(self, model, field):
        pass

    def alter_index_together(self, model, old_index_together, new_index_together):
        pass

    def delete_model(self, model):
        pass


class DatabaseFeatures(BaseDatabaseFeatures):
    empty_fetchmany_value = []
    supports_transactions = False  # FIXME: Make this True!
    can_return_id_from_insert = True
    supports_select_related = False
    autocommits_when_autocommit_is_off = True
    uses_savepoints = False
    allows_auto_pk_0 = False
    has_native_duration_field = False


class DatabaseWrapper(BaseDatabaseWrapper):

    data_types = DatabaseCreation.data_types  # These moved in 1.8

    operators = {
        "exact": "= %s",
        "iexact": "iexact %s",
        "contains": "contains %s",
        "icontains": "icontains %s",
        "regex": "regex %s",
        "iregex": "iregex %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "startswith %s",
        "endswith": "endswith %s",
        "istartswith": "istartswith %s",
        "iendswith": "iendswith %s",
        "isnull": "isnull %s",
    }

    Database = Database

    # These attributes are only used by Django >= 1.11
    client_class = DatabaseClient
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    features_class = DatabaseFeatures
    ops_class = DatabaseOperations
    creation_class = DatabaseCreation
    validation_class = BaseDatabaseValidation

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)

        if not hasattr(self, "client"):
            # Django 1.11 creates these automatically, when we call super
            # These are here for Django <= 1.10
            self.features = DatabaseFeatures(self)
            self.ops = DatabaseOperations(self)
            self.client = DatabaseClient(self)
            self.creation = DatabaseCreation(self)
            self.introspection = DatabaseIntrospection(self)
            self.validation = BaseDatabaseValidation(self)

        self.gcloud_project = self.settings_dict["PROJECT"]
        self.namespace = self.settings_dict.get("NAMESPACE") or None
        self.autocommit = True

    def is_usable(self):
        return True

    def get_connection_params(self):
        if not self.settings_dict.get("INDEXES_FILE"):
            raise ImproperlyConfigured("You must specify an INDEXES_FILE in the DATABASES setting")

        return self.settings_dict.copy()

    def get_new_connection(self, params):
        conn = Connection(self, params)
        load_special_indexes(conn)  # make sure special indexes are loaded
        return conn

    def init_connection_state(self):
        pass

    def _start_transaction_under_autocommit(self):
        pass

    def _set_autocommit(self, enabled):
        self.autocommit = enabled

    def create_cursor(self, name=None):
        self.name = name
        if not self.connection:
            self.connection = self.get_new_connection(self.settings_dict)

        return Cursor(self.connection)

    def schema_editor(self, *args, **kwargs):
        return DatabaseSchemaEditor(self, *args, **kwargs)

    def validate_no_broken_transaction(self):
        # Override this to do nothing, because it's not relevant to the Datastore
        pass

    def on_commit(self, func):
        # FIXME: This override needs to go away when we implement in_atomic_block
        # on the connection - which we can't do unless we move wholesale to Django's
        # atomic decorator (see issue #10)

        from django.db.transaction import TransactionManagementError
        from gcloudc.db.transaction import in_atomic_block

        if in_atomic_block(using=self.alias or "default"):
            # Transaction in progress; save for execution on commit.
            self.run_on_commit.append((set(self.savepoint_ids), func))
        elif not self.get_autocommit():
            raise TransactionManagementError('on_commit() cannot be used in manual transaction management')
        else:
            # No transaction in progress and in autocommit mode; execute
            # immediately.
            func()

    def run_and_clear_commit_hooks(self):
        # FIXME: This override needs to go away when we implement in_atomic_block
        # on the connection - which we can't do unless we move wholesale to Django's
        # atomic decorator (see issue #10)

        from gcloudc.db.transaction import in_atomic_block

        if in_atomic_block(using=self.alias or "default"):
            # FIXME: This should throw an error, but we can't because
            # Django's atomic blocks toggle autocommit in get_or_create
            # We need to unify with Django's decorators (see issue #10)
            return

        current_run_on_commit = self.run_on_commit
        self.run_on_commit = []
        while current_run_on_commit:
            sids, func = current_run_on_commit.pop(0)
            func()

    def close(self):
        """Close the connection to the database."""

        from gcloudc.db.transaction import in_atomic_block

        # FIXME: This override needs to go away when we implement in_atomic_block
        # on the connection - which we can't do unless we move wholesale to Django's
        # atomic decorator (see issue #10)

        self.validate_thread_sharing()
        self.run_on_commit = []

        # Don't call validate_no_atomic_block() to avoid making it difficult
        # to get rid of a connection in an invalid state. The next connect()
        # will reset the transaction state anyway.
        if self.closed_in_transaction or self.connection is None:
            return
        try:
            self._close()
        finally:
            if in_atomic_block(using=self.alias or "default"):
                self.closed_in_transaction = True
                self.needs_rollback = True
            else:
                self.connection = None
