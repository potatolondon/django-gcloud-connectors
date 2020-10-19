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
    caching,
    meta_queries,
    transaction,
    utils,
)
from .caching import remove_entities_from_cache_by_key
from .constraints import (
    CONSTRAINT_VIOLATION_MSG,
    check_unique_markers_in_memory,
    has_active_unique_constraints,
)
from .dbapi import NotSupportedError
from .dnf import normalize_query
from .formatting import generate_sql_representation
from .query import transform_query
from .query_utils import (
    get_filter,
    has_filter,
)
from .unique_utils import (
    _unique_combinations,
    query_is_unique,
)
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
    "exact": "=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    # The following operators are supported with special code below.
    "isnull": None,
    "in": None,
    "range": None,
}

EXTRA_SELECT_FUNCTIONS = {
    "+": lambda x, y: x + y,
    "-": lambda x, y: x - y,
    "/": lambda x, y: x / y,
    "*": lambda x, y: x * y,
    "<": lambda x, y: x < y,
    ">": lambda x, y: x > y,
    "=": lambda x, y: x == y,
}

REVERSE_OP_MAP = {"=": "exact", ">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}

INEQUALITY_OPERATORS = frozenset([">", "<", "<=", ">="])


def _cols_from_where_node(where_node):
    cols = where_node.get_cols() if hasattr(where_node, "get_cols") else where_node.get_group_by_cols()
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
            value = value.decode("utf-8")
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
            if and_branch.column != "__key__" or and_branch.operator != "=":
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
            if arg.lower() == "null":
                return None
            elif arg.lower() == "true":
                return True
            elif arg.lower() == "false":
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
            x for x in query.model._meta.fields if x.get_internal_type() in ("DateTimeField", "DateField", "TimeField")
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

        fields = [x for x in query.model._meta.fields if x.get_internal_type() in ("CharField",)]

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
        self.namespace = connection.namespace

        self.query = transform_query(connection, query)
        self.query.prepare()
        self.query = normalize_query(self.query)

        self.original_query = query

        # We enable keys only queries if they have been forced, or, if
        # someone did only("pk") or someone did values_list("pk") this is a little
        # inconsistent with other fields which aren't projected if just values(_list) is used
        self.keys_only = (
            keys_only
            or (
                query.deferred_loading[1] is False
                and len(query.deferred_loading[0]) == 1
                and query.model._meta.pk.column in query.deferred_loading[0]
            )
            or (len(query.select) == 1 and query.select[0].field == query.model._meta.pk)
        )

        # MultiQuery doesn't support keys_only
        if self.query.where and len(self.query.where.children) > 1:
            self.keys_only = False

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.query.serialize() == other.query.serialize()

    def __ne__(self, other):
        return not self.__eq__(other)

    def _sanity_check(self):
        if self.query.distinct and not self.query.columns:
            raise NotSupportedError("Tried to perform distinct query when projection wasn't possible")

    def _exclude_pk(self, columns):
        if columns is None:
            return None

        opts = self.query.model._meta
        copts = self.query.concrete_model._meta

        return [x for x in columns if x not in (opts.pk.column, copts.pk.column)]

    def _build_query(self):
        self._sanity_check()

        queries = []
        projection = self._exclude_pk(self.query.columns) or None

        # FIXME: Should this always be the projection fields if distinct is True?
        query_kwargs = {
            "kind": self.query.concrete_model._meta.db_table,
            "distinct_on": projection if self.query.distinct else (),
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
            query = transaction._rpc(self.connection).query(**query_kwargs)

            if self.keys_only:
                query.keys_only()

            query.order = ordering
            return query

        assert self.query.where

        rpc = transaction._rpc(self.connection)
        # Go through the normalized query tree
        for and_branch in self.query.where.children:
            query = rpc.query(**query_kwargs)

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
                    value = self.connection.ops.adapt_decimalfield_value(value, field.max_digits, field.decimal_places)
                elif isinstance(value, six.string_types):
                    value = coerce_unicode(value)
                elif isinstance(value, Key):
                    # Make sure we apply the current namespace to any lookups
                    # by key. Fixme: if we ever add key properties this will break if
                    # someone is trying to filter on a key which has a different namespace
                    # to the active one.
                    value = rpc.key(value.kind, value.id_or_name)

                # If there is already a value for this lookup, we need to make the
                # value a list and append the new entry
                filter_value = get_filter(query, lookup)
                if has_filter(query, lookup) and not isinstance(filter_value, (list, tuple)) and filter_value != value:
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
            return meta_queries.QueryByKeys(self.connection, self.query.model, queries, ordering, self.namespace)

        if len(queries) == 1:
            identifier = query_is_unique(self.query.model, queries[0])
            if identifier:
                # Yay for optimizations!
                return meta_queries.UniqueQuery(identifier, queries[0], self.query.model, self.namespace)

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

        excluded_pks = [rpc.key(x.kind, x.id_or_name) for x in self.query.excluded_pks]

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
                    count_query = Query(query._Query__kind, keys_only=True, namespace=self.namespace)
                    count_query.update(query)
                    resultset = count_query.Run(limit=limit, offset=offset)
                self.results = [len([y for y in resultset if y not in excluded_pks])]
                self.results_returned = 1
            else:
                query.keys_only()

                self.results = [len(list(query.fetch(limit=limit, offset=offset)))]
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
            entity = EntityTransforms.rename_pk_field(self.query.model, self.query.concrete_model, entity)
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
        return str(self).lower()


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
        self.namespace = connection.namespace

    def execute(self):
        table = self.table
        query = transaction._rpc(self.connection).query(kind=table, namespace=self.namespace)
        query.keys_only()

        # The local datastore emulator explodes if you try to delete more than 500
        # things in a single batch. So we just iterate in batches of 500.
        limit = 500

        results = [x.key for x in query.fetch(limit=limit)]
        while results:
            transaction._rpc(self.connection).delete(results)
            results = [x.key for x in query.fetch(limit=limit)]


def reserve_id(connection, kind, id_or_name, namespace):
    if not isinstance(id_or_name, int):
        # Nothing to do if the ID is a string, no-need to reserve that
        return

    gclient = connection.connection.gclient
    gclient.reserve_ids_sequential(gclient.key(kind, id_or_name, namespace=namespace), 1)


def perform_unique_checks(model, rpc, primary, test_fn):
    combinations = _unique_combinations(model, ignore_pk=True)
    for combination in combinations:
        query = rpc.query(kind=primary.kind)

        for field in combination:
            col_name = model._meta.get_field(field).column
            value = primary.get(col_name)
            if isinstance(value, list):
                for item in value:
                    query.add_filter(col_name, '=', item)
            elif value is not None:
                query.add_filter(col_name, '=', value)

        # only perform the query if there are filters on it
        if len(query.filters):
            res = list(query.fetch(1))
            if test_fn(res):
                raise IntegrityError(CONSTRAINT_VIOLATION_MSG.format(model._meta.db_table, ", ".join(combination)))


class BulkInsertError(IntegrityError, NotSupportedError):
    pass


class BulkDeleteError(IntegrityError, NotSupportedError):
    pass


@python_2_unicode_compatible
class InsertCommand(object):
    def __init__(self, connection, model, objs, fields, raw):
        self.has_pk = any(x.primary_key for x in fields)
        self.model = model
        self.objs = objs
        self.connection = connection
        self.namespace = connection.namespace
        self.raw = raw
        self.fields = fields

        self.entities = []
        self.included_keys = []

        for obj in self.objs:
            if self.has_pk:
                # We must convert the PK value here, even though this normally happens in
                # django_instance_to_entities otherwise
                # custom PK fields don't work properly
                value = self.model._meta.pk.get_db_prep_save(self.model._meta.pk.pre_save(obj, True), self.connection)
                self.included_keys.append(get_datastore_key(self.connection, self.model, value) if value else None)

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
                    getattr(obj, field.attname) if raw else field.pre_save(obj, True), connection=connection
                )

            primary, descendents = django_instance_to_entities(self.connection, self.fields, self.raw, obj)

            # Append the entity, and any descendents to the list to insert
            self.entities.append((primary, descendents))

    def execute(self):
        """
        Returns the keys of all entities succesfully put into the datastore.

        Under the hood this handles a few implementation specific details,
        such as checking that any unique constraints defined on the entity
        model are respected.
        """
        check_existence = self.has_pk and not has_concrete_parents(self.model)

        def perform_insert(entities):
            results = []
            rpc = transaction._rpc(self.connection.alias)
            must_handle_unique = has_active_unique_constraints(self.model)

            for primary, descendents in entities:
                if primary.key.is_partial:
                    primary.key = primary.key.completed_key(
                        rpc._generate_id()
                    )

                # thanks to cloud firestore in datastore mode strong consistency
                # we can query for the relevant entities to enforce uniqueness
                if must_handle_unique:
                    perform_unique_checks(self.model, rpc, primary, test_fn=lambda stored: stored and len(stored) > 0)

                rpc.put(primary)
                new_key = primary.key

                if descendents:
                    for i, descendent in enumerate(descendents):
                        key = rpc.key(
                            descendent.kind, descendent.key.id_or_name, parent=new_key
                        )
                        descendents[i] = Entity(key)
                        descendents[i].update(descendent)

                    for descendent in descendents:
                        rpc.put(descendent)

                results.append(new_key)
            return results

        @transaction.atomic()
        def insert_chunk(keys, entities):
            for key in keys:
                # sanity check the key isn't already taken
                if check_existence and key is not None:
                    if utils.key_exists(self.connection.alias, key):
                        raise IntegrityError("Tried to INSERT with existing key")

                    # quick validation of the ID value
                    id_or_name = key.id_or_name
                    if isinstance(id_or_name, str) and id_or_name.startswith("__"):
                        raise NotSupportedError("Datastore ids cannot start with __. Id was {}".format(id_or_name))

                    # notify App Engine of any keys we're specifying intentionally
                    reserve_id(self.connection, key.kind, key.id_or_name, self.namespace)

            results = perform_insert(entities)

            if has_active_unique_constraints(self.model):
                # if we're doing a bulk insert, due to the isolation of the
                # datastore inside transactions we won't find duplicate entities
                # to avoid this we can do an in memory comparison instead
                if len(entities) > 1:
                    check_unique_markers_in_memory(self.model, entities)

            caching.add_entities_to_cache(
                self.model,
                [x[0] for x in entities],
                caching.CachingSituation.DATASTORE_GET_PUT,
                self.namespace
            )

            return results

        return insert_chunk(self.included_keys, self.entities)

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return str(self).lower()

    def __str__(self):
        return generate_sql_representation(self)


class DeleteCommand(object):
    """
    Delete an entity / multiple entities.

    Limits imposed by the Firestore in Datastore mode (such as 500 write operations
    per batch) and the backend internal implementation details are handled under the hood.
    """

    def __init__(self, connection, query):
        self.connection = connection
        self.model = query.model
        self.namespace = connection.namespace

        self.select = SelectCommand(connection, query, keys_only=True)
        self.query = self.select.query  # we only need this for the generate_sql_formatter caller...

        # It seems query.tables is populated in most cases, but I have seen cases (albeit in testing)
        # where this isn't the case (particularly when not filtering on anything). In that case
        # fallback to the model table (perhaps we should do
        try:
            table = query.tables[0]
        except (AttributeError, IndexError):
            table = utils.get_top_concrete_parent(query.model)._meta.db_table
        self.table_to_delete = table  # used in wipe_polymodel_from_entity

    def execute(self):
        """
            Ideally we'd just be able to tell appengine to delete all the entities
            which match the query, that would be nice wouldn't it?

            Except we can't. Firstly delete() only accepts keys so we first have to
            execute a keys_only query to find the entities that match the query, then send
            those keys to delete().

            And then there are polymodels (model inheritence) which means we might not even be
            deleting the entity after all, only deleting some of the fields from it.

            What we do then is do a keys_only query, then iterate the entities in batches of
            500, each entity in the batch has its polymodel fields wiped out
            (if necessary) and then we do either a put() or delete() all inside a transaction.

            Oh, and we wipe out memcache in an independent transaction.

            Things to improve:

            - Check the entity matches the query still (there's a fixme there)
        """
        from .indexing import indexers_for_model

        @transaction.atomic()
        def delete_batch(key_slice):
            """
                Batch fetch entities, wiping out any polymodel fields if
                necessary, before deleting the entities by key.

                Any memcache references are also removed.
            """
            entities_to_delete = []
            entities_to_update = []
            updated_keys = []

            # get() expects Key objects, not just dicts with id keys
            keys_in_slice = [get_datastore_key(self.connection, self.model, key_id) for key_id in key_slice]
            entities = transaction._rpc(self.connection.alias).get(keys_in_slice)
            for entity in entities:

                # make sure the entity still exists
                if entity is None:
                    continue

                # handle polymodels
                _wipe_polymodel_from_entity(entity, self.table_to_delete)

                if not entity.get("class"):
                    entities_to_delete.append(entity)
                else:
                    entities_to_update.append(entity)
                updated_keys.append(entity)

            # we don't need an explicit batch here, as we are inside a transaction
            # which already applies this behaviour of non blocking RPCs until
            # the transaction is commited

            client = transaction._rpc(self.connection.alias)

            for entity in entities_to_delete:
                client.delete(entity.key)

            for entity in entities_to_update:
                client.put(entity)

            # Clean up any special indexes that need to be removed
            for indexer in indexers_for_model(self.model):
                for entity in entities_to_delete:
                    indexer.cleanup(client, entity.key)

            # Remove any cache keys
            remove_entities_from_cache_by_key(updated_keys, self.namespace)

            return len(updated_keys)

        # grab the result of the keys only query (see __init__)
        self.select.execute()
        key_ids = [x[self.model._meta.pk.name] for x in self.select.results]

        # for now we can only process 500 entities
        # otherwise we need to handle rollback of independent transactions
        # and race conditions between items being deleted and restored...
        max_batch_size = transaction.TRANSACTION_ENTITY_LIMIT

        if len(key_ids) > max_batch_size:
            raise BulkDeleteError(
                "Bulk deletes for {} can only delete {} instances per batch".format(self.model, max_batch_size)
            )

        return delete_batch(key_ids)

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return str(self).lower()

    def __str__(self):
        return generate_sql_representation(self)


def _wipe_polymodel_from_entity(entity, db_table):
    """
        Wipes out the fields associated with the specified polymodel table.
    """
    polymodel_value = entity.get("class", [])
    if polymodel_value and db_table in polymodel_value:
        # Remove any local fields from this model from the entity
        model = utils.get_model_from_db_table(db_table)
        for field in model._meta.local_fields:
            col = field.column
            if col in entity:
                del entity[col]

        # Then remove this model from the polymodel heirarchy
        polymodel_value.remove(db_table)
        if polymodel_value:
            entity["class"] = polymodel_value


@python_2_unicode_compatible
class UpdateCommand(object):
    def __init__(self, connection, query):
        self.model = query.model
        self.select = SelectCommand(connection, query, keys_only=True)
        self.query = self.select.query
        self.values = query.values
        self.connection = connection
        self.namespace = connection.namespace
        self.results = []

    def __str__(self):
        return generate_sql_representation(self)

    def lower(self):
        """
            This exists solely for django-debug-toolbar compatibility.
        """
        return str(self).lower()

    def _update_entity(self, key):
        def update_txt():
            result = transaction._rpc(self.connection.alias).get(key)
            if result is None:
                # Return false to indicate update failure
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
            instance = MockInstance(_original=MockInstance(_meta=meta, **result), _meta=meta, **instance_kwargs)

            # Convert the instance to an entity
            primary, descendents = django_instance_to_entities(
                self.connection,
                [x[0] for x in self.values],  # Pass in the fields that were updated
                True,
                instance,
                model=self.model,
            )

            # Update the entity we read above with the new values
            result.update(primary)

            # Remove fields which have been marked to be unindexed
            for col in getattr(primary, "_properties_to_remove", []):
                if col in result:
                    del result[col]

            # Make sure that any polymodel classes which were in the original entity are kept,
            # as django_instance_to_entities may have wiped them as well as added them.
            polymodel_classes = list(
                set(original.get(POLYMODEL_CLASS_ATTRIBUTE, []) + result.get(POLYMODEL_CLASS_ATTRIBUTE, []))
            )
            if polymodel_classes:
                result[POLYMODEL_CLASS_ATTRIBUTE] = polymodel_classes

            def perform_insert():
                """
                    Inserts result, and any descendents with their ancestor
                    value set
                """
                client = transaction._rpc(self.connection.alias)

                def test_fn(stored):
                    return stored and len(stored) == 1 and stored[0].key != result.key

                if has_active_unique_constraints(self.model):
                    perform_unique_checks(self.model, client, primary, test_fn)

                inserted_key = client.put(result)
                self.results.append((result, None))

                if descendents:
                    for i, descendent in enumerate(descendents):
                        descendents[i] = Entity(
                            client.key(
                                descendent.kind,
                                descendent.key.name if descendent.key.id is None else descendent.key.id,
                                parent=inserted_key,
                            )
                        )
                        descendents[i].update(descendent)
                    client.put_multi(descendents)

            # this will be async as we're inside a transaction block
            perform_insert()

            # Update the cache with the entity
            caching.add_entities_to_cache(
                self.model,
                [result],
                caching.CachingSituation.DATASTORE_PUT,
                self.namespace
            )

            # Return true to indicate update success
            return True

        return update_txt()

    def execute(self):
        must_handle_unique = has_active_unique_constraints(self.model)

        # TODO: potential optimisation - only use transaction if updating things
        # that require unique checks. This could potentially open a can of worms
        # but would have the benefit of removing any per-commit limits the datastore
        # currently imposes on transactions
        @transaction.atomic()
        def perform_update(results):
            i = 0
            for result in results:
                if self._update_entity(result.key):
                    i += 1

            # due to the isolation of the datastore inside transactions we won't
            # find unique clashes as part of the bulk operation. To avoid this
            # we can do an in memory comparison on the entities themselves
            if must_handle_unique:
                check_unique_markers_in_memory(self.model, self.results)

            return i

        self.select.execute()
        results = list(self.select.results)

        # optimisation: if update is being performed on more than 1 entity,
        # check that the fields in the update are not, on their own, one
        # of the unique constraint combination - otherwise, it means a unique
        # constraint would be violated and there would be no point in continuing
        if len(results) > 1 and must_handle_unique:
            combinations = _unique_combinations(self.model, ignore_pk=True)
            updating_attributes = set([field.attname for field, _, _ in self.values])
            for combination in combinations:
                if set(combination) == updating_attributes:
                    raise IntegrityError('UPDATE on {} field(s) violates unique constraint'.format(combination))

        return perform_update(results)
