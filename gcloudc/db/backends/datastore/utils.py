from datetime import datetime
from decimal import Decimal
from itertools import chain

from django.apps import apps
from django.conf import settings
from django.db import IntegrityError
from django.db.backends.utils import format_number
from django.utils import timezone
from google.cloud.datastore.entity import Entity
from google.cloud.datastore.key import Key

from gcloudc.utils import memoized

from .query_utils import get_filter

try:
    from django.db.models.expressions import BaseExpression
except ImportError:
    from django.db.models.expressions import ExpressionNode as BaseExpression


def make_utc_compatible(value):
    if value is None:
        return None

    if timezone.is_aware(value):
        if settings.USE_TZ:
            value = value.astimezone(timezone.utc)
        else:
            raise ValueError("Djangae backend does not support timezone-aware datetimes when USE_TZ is False.")
    return value


@memoized
def get_model_from_db_table(db_table):
    # We use include_swapped=True because tests might need access to gauth User models which are
    # swapped if the user has a different custom user model

    kwargs = {"include_auto_created": True, "include_swapped": True}

    for model in apps.get_models(**kwargs):
        if model._meta.db_table == db_table:
            return model


def decimal_to_string(value, max_digits=16, decimal_places=0):
    """
    Converts decimal to a unicode string for storage / lookup by nonrel
    databases that don't support decimals natively.

    This is an extension to `django.db.backends.utils.format_number`
    that preserves order -- if one decimal is less than another, their
    string representations should compare the same (as strings).

    TODO: Can't this be done using string.format()?
          Not in Python 2.5, str.format is backported to 2.6 only.
    """

    # Handle sign separately.
    if value.is_signed():
        sign = u"-"
        value = abs(value)
    else:
        sign = u""

    # Let Django quantize and cast to a string.
    value = format_number(value, max_digits, decimal_places)

    # Pad with zeroes to a constant width.
    n = value.find(".")
    if n < 0:
        n = len(value)
    if n < max_digits - decimal_places:
        value = u"0" * (max_digits - decimal_places - n) + value
    return sign + value


def normalise_field_value(value):
    """ Converts a field value to a common type/format to make comparable to another. """
    if isinstance(value, datetime):
        return make_utc_compatible(value)
    elif isinstance(value, Decimal):
        return decimal_to_string(value)
    return value


def get_datastore_kind(model):
    return get_top_concrete_parent(model)._meta.db_table


def get_prepared_db_value(connection, instance, field, raw=False):
    value = getattr(instance, field.attname) if raw else field.pre_save(instance, instance._state.adding)

    if isinstance(value, BaseExpression):
        from gcloudc.db.backends.datastore.expressions import evaluate_expression

        # We can't actually support F expressions on the datastore, but we can simulate
        # them, evaluating the expression in place.

        # TODO: For saves and updates we should raise a Warning. When evaluated in a filter
        # we should raise an Error
        value = evaluate_expression(value, instance, connection)

    if hasattr(value, "prepare_database_save"):
        value = value.prepare_database_save(field)
    else:
        value = field.get_db_prep_save(value, connection=connection)

    value = connection.ops.value_for_db(value, field)

    return value


def get_concrete_parents(model, ignore_leaf=False):
    ret = [x for x in model.mro() if hasattr(x, "_meta") and not x._meta.abstract and not x._meta.proxy]
    if ignore_leaf:
        ret = [x for x in ret if x != model]
    return ret


@memoized
def get_top_concrete_parent(model):
    return get_concrete_parents(model)[-1]


def get_concrete_fields(model, ignore_leaf=False):
    """
        Returns all the concrete fields for the model, including those
        from parent models
    """
    concrete_classes = get_concrete_parents(model, ignore_leaf)
    fields = []
    for klass in concrete_classes:
        fields.extend(klass._meta.fields)

    return fields


@memoized
def get_concrete_db_tables(model):
    return [x._meta.db_table for x in get_concrete_parents(model)]


@memoized
def has_concrete_parents(model):
    return get_concrete_parents(model) != [model]


@memoized
def get_field_from_column(model, column):
    for field in model._meta.fields:
        if field.column == column:
            return field
    return None


def django_instance_to_entities(connection, fields, raw, instance, check_null=True, model=None):
    """
        Converts a Django Model instance to an App Engine `Entity`

        Arguments:
            connection: Djangae appengine connection object
            fields: A list of fields to populate in the Entity
            raw: raw flag to pass to get_prepared_db_value
            instance: The Django model instance to convert
            check_null: Whether or not we should enforce NULL during conversion
            (throws an error if None is set on a non-nullable field)
            model: Model class to use instead of the instance one

        Returns:
            entity, [entity, entity, ...]

       Where the first result in the tuple is the primary entity, and the
       remaining entities are optionally descendents of the primary entity. This
       is useful for special indexes (e.g. contains)
    """

    from gcloudc.db.backends.datastore.indexing import special_indexes_for_column, get_indexer, IgnoreForIndexing
    from gcloudc.db.backends.datastore import POLYMODEL_CLASS_ATTRIBUTE

    model = model or type(instance)
    inheritance_root = get_top_concrete_parent(model)

    db_table = get_datastore_kind(inheritance_root)

    def value_from_instance(_instance, _field):
        value = get_prepared_db_value(connection, _instance, _field, raw)

        # If value is None, but there is a default, and the field is not nullable then we should populate it
        # Otherwise thing get hairy when you add new fields to models
        if value is None and _field.has_default() and not _field.null:
            # We need to pass the default through get_db_prep_save to properly do the conversion
            # this is how
            value = _field.get_db_prep_save(_field.get_default(), connection)

        if check_null and (not _field.null and not _field.primary_key) and value is None:
            raise IntegrityError("You can't set %s (a non-nullable field) to None!" % _field.name)

        is_primary_key = False
        if _field.primary_key and _field.model == inheritance_root:
            is_primary_key = True

        return value, is_primary_key

    field_values = {}
    primary_key = None

    descendents = []
    fields_to_unindex = set()

    for field in fields:
        value, is_primary_key = value_from_instance(instance, field)
        if is_primary_key:
            primary_key = value
        else:
            field_values[field.column] = value

        # Add special indexed fields
        for index in special_indexes_for_column(model, field.column):
            indexer = get_indexer(field, index)

            unindex = False
            try:
                values = indexer.prep_value_for_database(
                    value, index, model=model, column=field.column, connection=connection
                )
            except IgnoreForIndexing as e:
                # We mark this value as being wiped out for indexing
                unindex = True
                values = e.processed_value

            if not hasattr(values, "__iter__") or isinstance(values, (bytes, str)):
                values = [values]

            # If the indexer returns additional entities (instead of indexing a special column)
            # then just store those entities
            if indexer.PREP_VALUE_RETURNS_ENTITIES:
                descendents.extend(values)
            else:
                for i, v in enumerate(values):
                    column = indexer.indexed_column_name(field.column, v, index)

                    if unindex:
                        fields_to_unindex.add(column)
                        continue

                    # If the column already exists in the values, then we convert it to a
                    # list and append the new value
                    if column in field_values:
                        if not isinstance(field_values[column], list):
                            field_values[column] = [field_values[column], v]
                        else:
                            field_values[column].append(v)
                    else:
                        # Otherwise we just set the column to the value
                        field_values[column] = v

    args = [db_table]
    if primary_key is not None:
        args.append(primary_key)

    key = Key(*args, namespace=connection.namespace, project=connection.gcloud_project)

    exclude_from_indexes = tuple(
        field.column for field in model._meta.fields
        if field.db_type(connection) in ('text', 'bytes')
    )

    entity = Entity(key, exclude_from_indexes)
    entity.update(field_values)

    if fields_to_unindex:
        entity._properties_to_remove = fields_to_unindex

    classes = get_concrete_db_tables(model)
    if len(classes) > 1:
        entity[POLYMODEL_CLASS_ATTRIBUTE] = list(set(classes))

    return entity, descendents


def get_datastore_key(connection, model, pk):
    """ Return a datastore.Key for the given model and primary key.
    """
    factory = connection.connection.gclient.key
    kind = get_top_concrete_parent(model)._meta.db_table
    return factory(kind, pk, namespace=connection.namespace)


class MockInstance(object):
    """
        This creates a mock instance for use when passing a datastore entity
        into get_prepared_db_value. This is used when performing updates to prevent a complete
        conversion to a Django instance before writing back the entity
    """

    def __init__(self, **kwargs):
        is_adding = kwargs.pop("_is_adding", False)
        self._original = kwargs.pop("_original", None)
        self._meta = kwargs.pop("_meta", None)

        class State:
            adding = is_adding

        self.fields = {}
        for field_name, value in kwargs.items():
            self.fields[field_name] = value

        self._state = State()

    def __getattr__(self, attr):
        if attr in self.fields:
            return self.fields[attr]
        raise AttributeError(attr)


def key_exists(connection, key):
    from . import transaction
    qry = transaction._rpc(connection).query(namespace=key.namespace, ancestor=key)
    qry.keys_only()
    qry.add_filter("__key__", "=", key)
    return count_query(qry) > 0


# Null-friendly comparison functions


def lt(x, y):
    if x is None and y is None:
        return False
    if x is None and y is not None:
        return True
    elif x is not None and y is None:
        return False
    elif isinstance(x, Key) and isinstance(y, Key):
        lhs = tuple([x.namespace] + list(x.flat_path))
        rhs = tuple([y.namespace] + list(y.flat_path))
        return lhs < rhs
    else:
        return x < y


def gt(x, y):
    if x is None and y is None:
        return False
    if x is None and y is not None:
        return False
    elif x is not None and y is None:
        return True
    elif isinstance(x, Key) and isinstance(y, Key):
        lhs = tuple([x.namespace] + list(x.flat_path))
        rhs = tuple([y.namespace] + list(y.flat_path))
        return lhs > rhs
    else:
        return x > y


def gte(x, y):
    return not lt(x, y)


def lte(x, y):
    return not gt(x, y)


def django_ordering_comparison(ordering, lhs, rhs):
    if not ordering:
        return -1  # Really doesn't matter

    ASCENDING = 1
    DESCENDING = 2

    for order in ordering:
        direction = DESCENDING if order.startswith("-") else ASCENDING
        order = order.lstrip("-")

        if lhs is not None:
            lhs_value = lhs.key if order == "__key__" else lhs.get(order)
        else:
            lhs_value = None

        if rhs is not None:
            rhs_value = rhs.key if order == "__key__" else rhs.get(order)
        else:
            rhs_value = None

        if direction == ASCENDING and lhs_value != rhs_value:
            return -1 if lt(lhs_value, rhs_value) else 1
        elif direction == DESCENDING and lhs_value != rhs_value:
            return 1 if lt(lhs_value, rhs_value) else -1

    return 0


def entity_matches_query(entity, query):
    """
        Return True if the entity would potentially be returned by the datastore
        query
    """
    from . import meta_queries

    OPERATORS = {"=": lambda x, y: x == y, "<": lt, ">": gt, "<=": lte, ">=": gte}

    queries = [query]
    if isinstance(query, meta_queries.AsyncMultiQuery):
        raise NotImplementedError(
            "We just need to separate the multiquery " "into 'queries' then everything should work"
        )

    for query in queries:
        comparisons = chain([("__kind__", "=")], [(x[0], x[1]) for x in query.filters])

        for ent_attr, op in comparisons:
            if ent_attr == "__key__":
                continue

            compare = OPERATORS[op]  # We want this to throw if there's some op we don't know about

            if ent_attr == "__kind__":
                ent_value = entity.kind
                query_value = query.kind
            else:
                query_value = get_filter(query, (ent_attr, op))
                ent_value = entity.get(ent_attr)

            if not isinstance(query_value, (list, tuple)):
                query_values = [query_value]
            else:
                # The query value can be a list of ANDed values
                query_values = query_value

            if not isinstance(ent_value, (list, tuple)):
                ent_value = [ent_value]

            matches = False
            for value in query_values:  # [22, 23]
                # If any of the values don't match then this query doesn't match
                if not any(compare(attr, value) for attr in ent_value):
                    matches = False
                    break
            else:
                # One of the ent_attrs matches the query_attrs
                matches = True

            if not matches:
                # One of the AND values didn't match
                break
        else:
            # If we got through the loop without breaking, then the entity matches
            return True

    return False


def ensure_datetime(value):
    """
        Painfully, sometimes the Datastore returns dates as datetime objects, and sometimes
        it returns them as unix timestamps in microseconds!!
    """
    if isinstance(value, int):
        return datetime.utcfromtimestamp(value / 1e6)
    return value


def count_query(query):
    """
        The Google Cloud Datastore API doesn't expose a way to count a query
        the traditional method of doing a keys-only query is apparently actually
        slower than this method
    """

    # Largest 32 bit number, fairly arbitrary but I've seen Java Cloud Datastore
    # code that uses Integer.MAX_VALUE which is this value
    MAX_INT = 2147483647

    # Setting a limit of zero and an offset of max int will make
    # the server (rather than the client) skip the entities and then
    # return the number of skipped entities, fo realz yo!
    iterator = query.fetch(limit=0, offset=MAX_INT)
    [x for x in iterator]  # Force evaluation of the iterator

    count = iterator._skipped_results
    while iterator._more_results:
        # If we have more results then use cursor offsetting and repeat
        iterator = query.fetch(limit=0, offset=MAX_INT, start_cursor=iterator.next_page_token)
        [x for x in iterator]  # Force evaluation of the iterator

        count += iterator._skipped_results

    return count
