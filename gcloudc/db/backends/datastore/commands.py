import copy
import decimal
import logging
from datetime import datetime

import django
from django.db import (
    DatabaseError,
    IntegrityError,
)
from django.utils import six
from django.utils.encoding import (
    force_str,
    python_2_unicode_compatible,
)
from google.cloud.datastore.entity import Entity
from google.cloud.datastore.key import Key
from google.cloud.datastore.query import Query

from . import (
    POLYMODEL_CLASS_ATTRIBUTE,
    meta_queries,
    transaction,
    utils,
)
from .caching import remove_entities_from_cache_by_key
from .dbapi import NotSupportedError
from .dnf import normalize_query
from .formatting import generate_sql_representation
from .query import transform_query
from .query_utils import (
    get_filter,
    has_filter,
)
from .unique_utils import query_is_unique
from .utils import (
    MockInstance,
    django_instance_to_entities,
    ensure_datetime,
    get_datastore_key,
    get_field_from_column,
    has_concrete_parents,
)

logger = logging.getLogger(__name__)

OPERATORS_MAP = {
    'exact': '=',
    'gt': '>',
    'gte': '>=',
    'lt': '<',
    'lte': '<=',

    # The following operators are supported with special code below.
    'isnull': None,
    'in': None,
    'range': None,
}

EXTRA_SELECT_FUNCTIONS = {
    '+': lambda x, y: x + y,
    '-': lambda x, y: x - y,
    '/': lambda x, y: x / y,
    '*': lambda x, y: x * y,
    '<': lambda x, y: x < y,
    '>': lambda x, y: x > y,
    '=': lambda x, y: x == y
}

REVERSE_OP_MAP = {
    '=': 'exact',
    '>': 'gt',
    '>=': 'gte',
    '<': 'lt',
    '<=': 'lte',
}

INEQUALITY_OPERATORS = frozenset(['>', '<', '<=', '>='])


def _cols_from_where_node(where_node):
    cols = where_node.get_cols() if hasattr(where_node, 'get_cols') else where_node.get_group_by_cols()
    return cols


def _get_tables_from_where(where_node):
    cols = _cols_from_where_node(where_node)
    if django.VERSION[1] < 8:
        return list(set([x[0] for x in cols if x[0]]))
    else:
        return list(set([x.alias for x in cols]))


def field_conv_year_only(value):
    value = ensure_datetime(value)
    return datetime(value.year, 1, 1, 0, 0)


def field_conv_month_only(value):
    value = ensure_datetime(value)
    return datetime(value.year, value.month, 1, 0, 0)


def field_conv_day_only(value):
    value = ensure_datetime(value)
    return datetime(value.year, value.month, value.day, 0, 0)


def coerce_unicode(value):
    if isinstance(value, bytes):
        try:
            value = value.decode('utf-8')
        except UnicodeDecodeError:
            # This must be a Django databaseerror, because the exception happens too
            # early before Django's exception wrapping can take effect (e.g. it happens on SQL
            # construction, not on execution.
            raise DatabaseError("Bytestring is not encoded in utf-8")

    # The SDK raises BadValueError for unicode sub-classes like SafeText.
    return str(value)


def log_once(logging_call, text, args):
    """
        Only logs one instance of the combination of text and arguments to the passed
        logging function
    """
    identifier = "%s:%s" % (text, args)
    if identifier in log_once.logged:
        return
    logging_call(text % args)
    log_once.logged.add(identifier)


log_once.logged = set()


def convert_django_ordering_to_gae(ordering):
    return ordering


def limit_results_generator(results, limit):
    for result in results:
        yield result
        limit -= 1
        if not limit:
            raise StopIteration


def can_perform_datastore_get(normalized_query):
    """
        Given a normalized query, returns True if there is an equality
        filter on a key in each branch of the where
    """
    assert normalized_query.is_normalized

    for and_branch in normalized_query.where.children:
        if and_branch.is_leaf:
            if (and_branch.column != "__key__" or and_branch.operator != "="):
                return False
        else:
            key_found = False
            for filter_node in and_branch.children:
                assert filter_node.is_leaf

                if filter_node.column == "__key__":
                    if filter_node.operator == "=":
                        key_found = True
                        break

            if not key_found:
                return False

    return True


class EntityTransforms:
    @staticmethod
    def convert_key_to_entity(result):
        class FakeEntity(dict):
            def __init__(self, key):
                self._key = key

            @property
            def key(self):
                return self._key

        return FakeEntity(result)

    @staticmethod
    def rename_pk_field(model, concrete_model, result):
        if result is None:
            return result

        value = result.key.id_or_name
        result[model._meta.pk.column] = value
        result[concrete_model._meta.pk.column] = value
        return result

    @staticmethod
    def process_extra_selects(query, result):
        """
            We handle extra selects by generating the new columns from
            each result. We can handle simple boolean logic and operators.
        """
        if result is None:
            return result

        extra_selects = query.extra_selects
        model_fields = query.model._meta.fields

        DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S")

        def process_arg(arg):
            if arg.startswith("'") and arg.endswith("'"):
                # String literal
                arg = arg.strip("'")
                # Check to see if this is a date
                for date in DATE_FORMATS:
                    try:
                        value = datetime.strptime(arg, date)
                        return value
                    except ValueError:
                        continue
                return arg
            elif arg in [x.column for x in model_fields]:
                # Column value
                return result.get(arg)

            # Handle NULL
            if arg.lower() == 'null':
                return None
            elif arg.lower() == 'true':
                return True
            elif arg.lower() == 'false':
                return False

            # See if it's an integer
            try:
                arg = int(arg)
            except (TypeError, ValueError):
                pass

            # Just a plain old literal
            return arg

        for col, select in extra_selects:
            result[col] = select[0](*[process_arg(x) for x in select[1]])

        return result

    @staticmethod
    def convert_datetime_fields(query, result):
        if result is None:
            return result

        fields = [
            x for x in query.model._meta.fields
            if x.get_internal_type() in ("DateTimeField", "DateField", "TimeField")
        ]

        for field in fields:
            column = field.column
            if isinstance(result, dict):  # sometimes it's a key!
                value = result.get(column)
            else:
                value = None

            if value is not None:
                result[column] = ensure_datetime(value)
        return result

    @staticmethod
    def fix_projected_values_type(query, result):
        """
            String values returned from projection queries return as 'str' not 'unicode'
            See https://github.com/potatolondon/djangae/issues/1026

            FIXME: This was the case on App Engine, probably not on Cloud Datastore. When
            all original Djangae tests pass, let's remove this function and see if they still pass!
        """

        if result is None:
            return None

        fields = [
            x for x in query.model._meta.fields
            if x.get_internal_type() in ("CharField",)
        ]

        for field in fields:
            col = field.column
            if col in result and isinstance(result[col], bytes):
                result[col] = str(result[col], "utf-8")

        return result

    @staticmethod
    def ignore_excluded_pks(excluded_pks, result):
        if result is None:
            return result

        if result.key in excluded_pks:
            return None

        return result


class SelectCommand(object):
    def __init__(self, connection, query, keys_only=False):
        self.connection = connection.alias
        self.namespace = connection.ops.connection.settings_dict.get("NAMESPACE")

        self.query = transform_query(connection, query)
        self.query.prepare()
        self.query = normalize_query(self.query)

        self.original_query = query

        # We enable keys only queries if they have been forced, or, if
        # someone did only("pk") or someone did values_list("pk") this is a little
        # inconsistent with other fields which aren't projected if just values(_list) is used
        self.keys_only = keys_only or (
            query.deferred_loading[1] is False and
            len(query.deferred_loading[0]) == 1 and query.model._meta.pk.column in query.deferred_loading[0]
        ) or (
            len(query.select) == 1 and query.select[0].field == query.model._meta.pk
        )

        # MultiQuery doesn't support keys_only
        if self.query.where and len(self.query.where.children) > 1:
            self.keys_only = False

    def __eq__(self, other):
        return (isinstance(other, self.__class__) and
                self.query.serialize() == other.query.serialize())

    def __ne__(self, other):
        return not self.__eq__(other)

    def _sanity_check(self):
        if self.query.distinct and not self.query.columns:
            raise NotSupportedError(
                "Tried to perform distinct query when projection wasn't possible"
            )

    def _exclude_pk(self, columns):
        if columns is None:
            return None

        opts = self.query.model._meta
        copts = self.query.concrete_model._meta

        return [
            x for x in columns if x not in
            (opts.pk.column, copts.pk.column)
        ]

    def _build_query(self):
        self._sanity_check()

        queries = []
        projection = self._exclude_pk(self.query.columns) or None

        query_kwargs = {
            "kind": self.query.concrete_model._meta.db_table,
            "distinct_on": self.query.distinct or (),
            "projection": projection or (),
            "namespace": self.namespace,
        }

        ordering = convert_django_ordering_to_gae(self.query.order_by)

        if self.query.distinct and not ordering:
            # If we specified we wanted a distinct query, but we didn't specify
            # an ordering, we must set the ordering to the distinct columns, otherwise
            # App Engine shouts at us. Nastily. And without remorse.
            # The order of the columns in `ordering` makes a difference, but `distinct` is a set
            # and therefore unordered, but in  this situation (where the ordering has not been
            # explicitly defined) any order of the columns will do
            ordering = list(self.query.columns)

        # Deal with the no filters case
        if self.query.where is None:
            query = transaction._rpc(self.connection).query(
                **query_kwargs
            )

            if self.keys_only:
                query.keys_only()

            query.order = ordering
            return query

        assert self.query.where

        # Go through the normalized query tree
        for and_branch in self.query.where.children:
            query = transaction._rpc(self.connection).query(
                **query_kwargs
            )

            if self.keys_only:
                query.keys_only()

            # This deals with the oddity that the root of the tree may well be a leaf
            filters = [and_branch] if and_branch.is_leaf else and_branch.children

            for filter_node in filters:
                lookup = (filter_node.column, filter_node.operator)

                value = filter_node.value

                # This is a special case. Annoyingly Django's decimal field doesn't
                # ever call ops.get_prep_save or lookup or whatever when you are filtering
                # on a query. It *does* do it on a save, so we basically need to do a
                # conversion here, when really it should be handled elsewhere
                if isinstance(value, decimal.Decimal):
                    field = get_field_from_column(self.query.model, filter_node.column)
                    value = self.connection.ops.adapt_decimalfield_value(
                        value, field.max_digits, field.decimal_places
                    )
                elif isinstance(value, six.string_types):
                    value = coerce_unicode(value)
                elif isinstance(value, Key):
                    # Make sure we apply the current namespace to any lookups
                    # by key. Fixme: if we ever add key properties this will break if
                    # someone is trying to filter on a key which has a different namespace
                    # to the active one.
                    value = Key(
                        value.kind,
                        value.id_or_name,
                        project=value.project,
                        namespace=self.namespace
                    )

                # If there is already a value for this lookup, we need to make the
                # value a list and append the new entry
                filter_value = get_filter(query, lookup)
                if (
                    has_filter(query, lookup) and
                    not isinstance(filter_value, (list, tuple)) and
                    filter_value != value
                ):
                    new_value = [filter_value] + [value]
                    query.add_filter(lookup[0], lookup[1], new_value)
                else:
                    # If the value is a list, we can't just assign it to the query
                    # which will treat each element as its own value. So in this
                    # case we nest it. This has the side effect of throwing a BadValueError
                    # which we could throw ourselves, but the datastore might start supporting
                    # list values in lookups.. you never know!
                    # FIXME: I can't remember the reason for this rather than actually just
                    # throwing an error?
                    if isinstance(value, (list, tuple)):
                        query.add_filter(lookup[0], lookup[1], [value])
                    else:
                        # Common case: just add the raw where constraint
                        query.add_filter(lookup[0], lookup[1], value)

            if ordering:
                query.order = ordering

            queries.append(query)

        if can_perform_datastore_get(self.query):
            # Yay for optimizations!
            return meta_queries.QueryByKeys(
                self.connection,
                self.query.model,
                queries,
                ordering,
                self.namespace
            )

        if len(queries) == 1:
            identifier = query_is_unique(self.query.model, queries[0])
            if identifier:
                # Yay for optimizations!
                return meta_queries.UniqueQuery(
                    identifier, queries[0], self.query.model, self.namespace
                )

            return queries[0]
        else:
            return meta_queries.AsyncMultiQuery(queries, ordering)

    def _fetch_results(self, query):
        # If we're manually excluding PKs, and we've specified a limit to the results
        # we need to make sure that we grab more than we were asked for otherwise we could filter
        # out too many! These are again limited back to the original request limit
        # while we're processing the results later
        # Apply the namespace before excluding
        rpc = transaction._rpc(self.connection)

        excluded_pks = [
            rpc.key(x.kind, x.id_or_name, namespace=self.namespace)
            for x in self.query.excluded_pks
        ]

        high_mark = self.query.high_mark
        low_mark = self.query.low_mark

        excluded_pk_count = 0
        if excluded_pks and high_mark:
            excluded_pk_count = len(excluded_pks)
            high_mark += excluded_pk_count

        limit = None if high_mark is None else (high_mark - (low_mark or 0))
        offset = low_mark or 0

        if self.query.kind == "COUNT":
            if excluded_pks:
                # If we're excluding pks, relying on a traditional count won't work
                # so we have two options:
                # 1. Do a keys_only query instead and count the results excluding keys
                # 2. Do a count, then a pk__in=excluded_pks to work out how many to subtract
                # Here I've favoured option one as it means a single RPC call. Testing locally
                # didn't seem to indicate much of a performance difference, even when doing the pk__in
                # with GetAsync while the count was running. That might not be true of prod though so
                # if anyone comes up with a faster idea let me know!
                if isinstance(query, meta_queries.QueryByKeys):
                    # If this is a QueryByKeys, just do the datastore Get and count the results
                    resultset = (x.key for x in query.fetch(limit=limit, offset=offset) if x)
                else:
                    count_query = Query(
                        query._Query__kind, keys_only=True, namespace=self.namespace
                    )
                    count_query.update(query)
                    resultset = count_query.Run(limit=limit, offset=offset)
                self.results = [len([y for y in resultset if y not in excluded_pks])]
                self.results_returned = 1
            else:
                query.keys_only()

                self.results = [
                    len(list(query.fetch(limit=limit, offset=offset)))
                ]
                self.results_returned = 1
            return
        elif self.query.kind == "AVERAGE":
            raise ValueError("AVERAGE not yet supported")

        # Ensure that the results returned is reset
        self.results_returned = 0
        self.results = []

        seen = set()

        def dedupe(result):
            # FIXME: This logic can't be right. I think we need to store the distinct fields
            # somewhere on the query
            if getattr(self.original_query, "annotation_select", None):
                columns = self.original_query.annotation_select.keys()
            else:
                columns = self.query.columns or []
            if not columns:
                return result

            key = tuple([result[x] for x in self._exclude_pk(columns) if x in result])
            if key in seen:
                return None
            seen.add(key)
            return result

        for entity in query.fetch(limit=limit, offset=offset):
            # If this is a keys only query, we need to generate a fake entity
            # for each key in the result set
            if isinstance(entity, Key):
                entity = EntityTransforms.convert_key_to_entity(entity)

            entity = EntityTransforms.ignore_excluded_pks(excluded_pks, entity)
            entity = EntityTransforms.convert_datetime_fields(self.query, entity)
            entity = EntityTransforms.fix_projected_values_type(self.query, entity)
            entity = EntityTransforms.rename_pk_field(
                self.query.model, self.query.concrete_model, entity
            )
            entity = EntityTransforms.process_extra_selects(self.query, entity)

            if self.query.distinct and self.query.extra_selects:
                entity = dedupe(entity)

            if entity:
                self.results.append(entity)
                self.results_returned += 1

            if limit and self.results_returned >= (limit - excluded_pk_count):
                break

    def execute(self):
        self.gae_query = self._build_query()
        self._fetch_results(self.gae_query)
        self.results = iter(self.results)
        return self.results_returned

    def __repr__(self):
        return force_str(generate_sql_representation(self))

    def __mod__(self, params):
        return repr(self)

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return unicode(self).lower()


class FlushCommand(object):
    """
        sql_flush returns the SQL statements to flush the database,
        which are then executed by cursor.execute()

        We instead return a list of FlushCommands which are called by
        our cursor.execute
    """
    def __init__(self, table, connection):
        self.connection = connection.alias
        self.table = table
        self.namespace = connection.ops.connection.settings_dict.get("NAMESPACE")

    def execute(self):
        table = self.table
        query = transaction._rpc(self.connection).query(
            kind=table, namespace=self.namespace
        )

        query.keys_only()

        results = [x.key for x in query.fetch()]
        while results:
            transaction._rpc(self.connection).delete(results)
            results = [x.key for x in query.fetch()]


def reserve_id(connection, kind, id_or_name, namespace):
    if not isinstance(id_or_name, int):
        # Nothing to do if the ID is a string, no-need to reserve that
        return

    # https://github.com/googleapis/google-cloud-python/issues/7111
    gclient = connection.connection.gclient
    reserve_ids = gclient._datastore_api
    reserve_ids(
        project=gclient.project,
        keys=[gclient.key(kind, id_or_name, namespace=namespace).to_protobuf()]
    )


class BulkInsertError(IntegrityError, NotSupportedError):
    pass


@python_2_unicode_compatible
class InsertCommand(object):
    def __init__(self, connection, model, objs, fields, raw):
        self.has_pk = any(x.primary_key for x in fields)
        self.model = model
        self.objs = objs
        self.connection = connection
        self.namespace = connection.ops.connection.settings_dict.get("NAMESPACE")
        self.raw = raw
        self.fields = fields

        self.entities = []
        self.included_keys = []

        for obj in self.objs:
            if self.has_pk:
                # We must convert the PK value here, even though this normally happens in
                # django_instance_to_entities otherwise
                # custom PK fields don't work properly
                value = self.model._meta.pk.get_db_prep_save(
                    self.model._meta.pk.pre_save(obj, True),
                    self.connection
                )
                self.included_keys.append(
                    get_datastore_key(self.connection, self.model, value)
                    if value else None
                )

                if value == 0:
                    raise IntegrityError("The datastore doesn't support 0 as a key value")

                if not self.model._meta.pk.blank and self.included_keys[-1] is None:
                    raise IntegrityError("You must specify a primary key value for {} instances".format(self.model))
            else:
                # We zip() self.entities and self.included_keys in execute(), so they should be the same length
                self.included_keys.append(None)

            # We don't use the values returned, but this does make sure we're
            # doing the same validation as Django. See issue #493 for an
            # example of how not doing this can mess things up
            for field in fields:
                field.get_db_prep_save(
                    getattr(obj, field.attname) if raw else field.pre_save(obj, True),
                    connection=connection,
                )

            primary, descendents = django_instance_to_entities(
                self.connection, self.fields, self.raw, obj
            )

            # Append the entity, and any descendents to the list to insert
            self.entities.append((primary, descendents))

    def execute(self):
        check_existence = self.has_pk and not has_concrete_parents(self.model)

        def perform_insert(entities):
            results = []
            for primary, descendents in entities:
                if primary.key.is_partial:
                    primary.key = primary.key.completed_key(
                        transaction._rpc(self.connection)._generate_id()
                    )

                transaction._rpc(self.connection).put(primary)
                new_key = primary.key

                if descendents:
                    for i, descendent in enumerate(descendents):
                        key = transaction._rpc(self.connection).key(
                            descendent.kind,
                            descendent.key.id_or_name,
                            parent=new_key,
                            namespace=new_key.namespace
                        )
                        descendents[i] = Entity(key)
                        descendents[i].update(descendent)

                    for descendent in descendents:
                        transaction._rpc(self.connection).put(descendent)

                results.append(new_key)
            return results

        def insert_chunk(keys, entities):
            @transaction.atomic(enable_cache=False)
            def txn():
                for key in keys:
                    if check_existence and key is not None:
                        if utils.key_exists(self.connection, key):
                            raise IntegrityError("Tried to INSERT with existing key")

                        id_or_name = key.id_or_name
                        if isinstance(id_or_name, six.string_types) and id_or_name.startswith("__"):
                            raise NotSupportedError("Datastore ids cannot start with __. Id was %s" % id_or_name)

                        # Notify App Engine of any keys we're specifying intentionally
                        reserve_id(self.connection, key.kind, key.id_or_name, self.namespace)

                results = perform_insert(entities)

                return results

            return txn()

        return insert_chunk(self.included_keys, self.entities)

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return unicode(self).lower()

    def __str__(self):
        return generate_sql_representation(self)


@python_2_unicode_compatible
class DeleteCommand(object):
    def __init__(self, connection, query):
        self.model = query.model
        self.select = SelectCommand(connection, query, keys_only=True)
        self.query = self.select.query
        self.namespace = connection.ops.connection.settings_dict.get("NAMESPACE")

        # It seems query.tables is populated in most cases, but I have seen cases (albeit in testing)
        # where this isn't the case (particularly when not filtering on anything). In that case
        # fallback to the model table (perhaps we should do
        self.table_to_delete = (
            query.tables[0] if query.tables else
            utils.get_top_concrete_parent(query.model)._meta.db_table
        )

    def __str__(self):
        return generate_sql_representation(self)

    def execute(self):
        """
            Ideally we'd just be able to tell appengine to delete all the entities
            which match the query, that would be nice wouldn't it?

            Except we can't. Firstly Delete() only accepts keys so we first have to
            execute a keys_only query to find the entities that match the query, then send
            those keys to Delete(), except it's not as easy as that either because the
            query might be eventually consistent and so we might delete entities which
            were updated in another request and no-longer match the query. Bugger.

            And then there might be constraints... in which case we need to grab the entity
            in its entirety, release any constraints and then delete the entity.

            And then there are polymodels (model inheritence) which means we might not even be
            deleting the entity after all, only deleting some of the fields from it.

            What we do then is do a keys_only query, then iterate the entities in batches of
            25 (well _MAX_EG_PER_TXN), each entity in the batch has its polymodel fields wiped out
            (if necessary) and then we do either a PutAsync or DeleteAsync all inside a transaction.

            Oh, and we wipe out memcache and delete the constraints in an independent transaction.

            Things to improve:

             - Delete the constraints in a background thread. We don't need to wait for them, and
             really, we don't want the non-deletion of them to affect the deletion of the entity.
             Lingering markers are handled automatically they just case a small performance hit on
             write.
             - Check the entity matches the query still (there's a fixme there)
        """
        from .indexing import indexers_for_model
        from .constraints import has_active_unique_constraints, release

        self.select.execute()

        constraints_enabled = constraints.has_active_unique_constraints(self.model)
        keys = [x.key() for x in self.select.results]

        def wipe_polymodel_from_entity(entity, db_table):
            """
                Wipes out the fields associated with the specified polymodel table
            """
            polymodel_value = entity.get('class', [])
            if polymodel_value and self.table_to_delete in polymodel_value:
                # Remove any local fields from this model from the entity
                model = utils.get_model_from_db_table(self.table_to_delete)
                for field in model._meta.local_fields:
                    col = field.column
                    if col in entity:
                        del entity[col]

                # Then remove this model from the polymodel heirarchy
                polymodel_value.remove(self.table_to_delete)
                if polymodel_value:
                    entity['class'] = polymodel_value

        @db.transactional(xg=True)
        def delete_batch(key_slice):
            entities = rpc.Get(key_slice)

            # FIXME: We need to make sure the entity still matches the query!
#            entities = (x for x in entities if utils.entity_matches_query(x, self.select.gae_query))

            to_delete = []
            to_update = []
            updated_keys = []

            # Go through the entities
            for entity in entities:
                if entity is None:
                    continue

                wipe_polymodel_from_entity(entity, self.table_to_delete)
                if not entity.get('class'):
                    to_delete.append(entity.key())
                    if constraints_enabled:
                        constraints.release(self.model, entity)
                else:
                    to_update.append(entity)
                updated_keys.append(entity.key())

            rpc.DeleteAsync(to_delete)
            rpc.PutAsync(to_update)

            # Clean up any special index things that need to be cleaned
            for indexer in indexers_for_model(self.model):
                for key in to_delete:
                    indexer.cleanup(key)

            # Remove any cache keys
            remove_entities_from_cache_by_key(
                updated_keys, self.namespace
            )

            return len(updated_keys)

        # if unique constraints are active, we have to take some additional
        # steps to make sure all references are cleaned up as we go. We do
        # this here to avoid some overhead in the batch operation loop
        model_has_active_constraints = has_active_unique_constraints(self.model)
        deleted = 0
        while keys:
            deleted += delete_batch(keys[:datastore_stub_util._MAX_EG_PER_TXN])
            keys = keys[datastore_stub_util._MAX_EG_PER_TXN:]

        return deleted

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return unicode(self).lower()


@python_2_unicode_compatible
class UpdateCommand(object):
    def __init__(self, connection, query):
        self.model = query.model
        self.select = SelectCommand(connection, query, keys_only=True)
        self.query = self.select.query
        self.values = query.values
        self.connection = connection
        self.namespace = connection.ops.connection.settings_dict.get("NAMESPACE")

    def __str__(self):
        return generate_sql_representation(self)

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return unicode(self).lower()

    def _update_entity(self, key):
        markers_to_acquire = []
        markers_to_release = []

        # This is a list rather than a straight bool, because we need to pass
        # by reference so we can set it in the nested function. 'global' doesnt
        # work on nested functions
        rollback_markers = [False]

        @transaction.atomic(cache_enabled=False)
        def txn():
            result = transaction._rpc(self.connection).get(key)
            if result is None:
                # Return false to indicate update failure
                return False

            if (
                isinstance(self.select.gae_query, (Query, meta_queries.UniqueQuery)) # ignore QueryByKeys and NoOpQuery
                and not utils.entity_matches_query(result, self.select.gae_query)
            ):
                # Due to eventual consistency they query may have returned an entity which no longer
                # matches the query
                return False

            original = copy.deepcopy(result)

            instance_kwargs = {field.attname: value for field, param, value in self.values}

            # Note: If you replace MockInstance with self.model, you'll find that some delete
            # tests fail in the test app. This is because any unspecified fields would then call
            # get_default (even though we aren't going to use them) which may run a query which
            # fails inside this transaction. Given as we are just using MockInstance so that we can
            # call django_instance_to_entities it on it with the subset of fields we pass in,
            # what we have is fine.
            meta = self.model._meta
            instance = MockInstance(
                _original=MockInstance(_meta=meta, **result),
                _meta=meta,
                **instance_kwargs
            )

            # Convert the instance to an entity
            primary, descendents = django_instance_to_entities(
                self.connection,
                [x[0] for x in self.values],  # Pass in the fields that were updated
                True, instance,
                model=self.model
            )

            # Update the entity we read above with the new values
            result.update(primary)

            # Remove fields which have been marked to be unindexed
            for col in getattr(primary, "_properties_to_remove", []):
                if col in result:
                    del result[col]

            # Make sure that any polymodel classes which were in the original entity are kept,
            # as django_instance_to_entities may have wiped them as well as added them.
            polymodel_classes = list(set(
                original.get(POLYMODEL_CLASS_ATTRIBUTE, []) + result.get(POLYMODEL_CLASS_ATTRIBUTE, [])
            ))
            if polymodel_classes:
                result[POLYMODEL_CLASS_ATTRIBUTE] = polymodel_classes

            def perform_insert():
                """
                    Inserts result, and any descendents with their ancestor
                    value set
                """
                inserted_key = transaction._rpc(self.connection).put(result)
                if descendents:
                    for i, descendent in enumerate(descendents):
                        descendents[i] = Entity(
                            descendent.kind(),
                            parent=inserted_key,
                            namespace=inserted_key.namespace(),
                            id=descendent.key().id() or None,
                            name=descendent.key().name() or None
                        )
                        descendents[i].update(descendent)
                    transaction._rpc(self.connection).put(descendents)

            perform_insert()

            # Return true to indicate update success
            return True

        try:
            return txn()
        except:
            if rollback_markers[0]:
                constraints.update_identifiers(markers_to_release, markers_to_acquire, key)
            raise

    def execute(self):
        self.select.execute()

        i = 0
        for result in self.select.results:
            if self._update_entity(result.key):
                # Only increment the count if we successfully updated
                i += 1

        return i
