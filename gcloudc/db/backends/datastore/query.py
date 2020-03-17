import datetime
import json
import logging
import re
from itertools import chain

from django.db import (
    NotSupportedError,
    connections,
)
from django.db.models import AutoField
from django.db.models.sql.datastructures import EmptyResultSet
from django.utils import six

from . import POLYMODEL_CLASS_ATTRIBUTE
from .indexing import (
    add_special_index,
    get_indexer,
)
from .utils import (
    ensure_datetime,
    get_field_from_column,
    get_top_concrete_parent,
    has_concrete_parents,
)

logger = logging.getLogger(__name__)


VALID_QUERY_KINDS = ("SELECT", "UPDATE", "INSERT", "DELETE", "COUNT", "AVERAGE")

VALID_ANNOTATIONS = {"MIN": min, "MAX": max, "SUM": sum, "COUNT": len, "AVG": lambda x: (sum(x) / len(x))}

VALID_CONNECTORS = ("AND", "OR")


VALID_OPERATORS = ("=", "<", ">", "<=", ">=", "IN")


def convert_operator(operator):
    if operator == "exact":
        return "="
    elif operator == "gt":
        return ">"
    elif operator == "lt":
        return "<"
    elif operator == "gte":
        return ">="
    elif operator == "lte":
        return "<="

    return operator.upper()


class WhereNode(object):
    def __init__(self, using):
        self.using = using

        self.column = None
        self.operator = None
        self.value = None
        self.output_field = None
        self.will_never_return_results = False
        self.lookup_name = None

        self.children = []
        self.connector = "AND"
        self.negated = False

    @property
    def is_leaf(self):
        return bool(self.column and self.operator)

    def set_connector(self, connector):
        self.connector = connector

    def append_child(self, node):
        self.children.append(node)

    def set_leaf(self, column, operator, value, is_pk_field, negated, lookup_name, namespace, target_field=None):

        # We need access to the Datastore client to access the Key factory
        wrapper = connections[self.using]
        wrapper.ensure_connection()
        gclient = wrapper.connection.gclient

        assert column
        assert operator
        assert isinstance(is_pk_field, bool)
        assert isinstance(negated, bool)

        if operator == "iexact" and isinstance(target_field, AutoField):
            # When new instance is created, automatic primary key 'id'
            # does not generate '_idx_iexact_id'.
            # As the primary key 'id' (AutoField) is integer and is always case insensitive,
            # we can deal with 'id_iexact=' query by using 'exact' rather than 'iexact'.
            operator = "exact"
            value = int(value)

        if is_pk_field:
            # If this is a primary key, we need to make sure that the value
            # we pass to the query is a datastore Key. We have to deal with IN queries here
            # because they aren't flattened until the DNF stage
            model = get_top_concrete_parent(target_field.model)
            table = model._meta.db_table

            if isinstance(value, (list, tuple)):
                value = [gclient.key(table, x, namespace=namespace) for x in value if x]
            else:
                # Django 1.11 has operators as symbols, earlier versions use "exact" etc.
                if (operator == "isnull" and value is True) or (
                    operator in ("exact", "lt", "lte", "<", "<=", "=") and not value
                ):
                    # id=None will never return anything and
                    # Empty strings and 0 are forbidden as keys
                    self.will_never_return_results = True
                elif operator in ("gt", "gte", ">", ">=") and not value:
                    # If the value is 0 or "", then we need to manipulate the value and operator here to
                    # get the right result (given that both are invalid keys) so for both we return
                    # >= 1 or >= "\0" for strings
                    if isinstance(value, six.integer_types):
                        value = 1
                    else:
                        value = "\0"

                    value = gclient.key(table, value, namespace=namespace)
                    operator = "gte"
                else:
                    value = gclient.key(table, value, namespace=namespace)
            column = "__key__"

        # Do any special index conversions necessary to perform this lookup
        special_indexer = get_indexer(target_field, operator)

        if special_indexer:
            if is_pk_field:
                column = model._meta.pk.column
                value = six.text_type(value.id_or_name)

            add_special_index(connections[self.using], target_field.model, column, special_indexer, operator, value)
            index_type = special_indexer.prepare_index_type(operator, value)
            value = special_indexer.prep_value_for_query(
                value, model=target_field.model, column=column, connection=connections[self.using]
            )
            column = special_indexer.indexed_column_name(column, value, index_type)
            operator = special_indexer.prep_query_operator(operator)

        self.column = column
        self.operator = convert_operator(operator)
        self.value = value
        self.lookup_name = lookup_name

    def __iter__(self):
        for child in chain(*map(iter, self.children)):
            yield child
        yield self

    def __repr__(self):
        if self.is_leaf:
            return "[%s%s%s]" % (self.column, self.operator, self.value)
        else:
            return "(%s:%s%s)" % (
                self.connector,
                "!" if self.negated else "",
                ",".join([repr(x) for x in self.children]),
            )

    def __eq__(self, rhs):
        if self.is_leaf != rhs.is_leaf:
            return False

        if self.is_leaf:
            return self.column == rhs.column and self.value == rhs.value and self.operator == rhs.operator
        else:
            return self.connector == rhs.connector and self.children == rhs.children

    def __hash__(self):
        if self.is_leaf:
            return hash((self.column, self.value, self.operator))
        else:
            return hash((self.connector,) + tuple([hash(x) for x in self.children]))


class Query(object):
    def __init__(self, model, kind):
        assert kind in VALID_QUERY_KINDS

        self.model = model
        self.concrete_model = get_top_concrete_parent(model)
        self.kind = kind

        self.projection_possible = True
        self.tables = []

        self.columns = None  # None means all fields
        self.init_list = []

        self.distinct = False
        self.order_by = []
        self.row_data = []  # For insert/updates
        self._where = None
        self.low_mark = self.high_mark = None

        self.annotations = []
        self.per_entity_annotations = []
        self.extra_selects = []
        self.polymodel_filter_added = False

        # A list of PKs that should be excluded from the resultset
        self.excluded_pks = set()

    @property
    def is_normalized(self):
        """
            Returns True if this query has a normalized where tree
        """
        if not self.where:
            return True

        # Only a leaf node, return True
        if not self.where.is_leaf:
            return True

        # If we have children, and they are all leaf nodes then this is a normalized
        # query
        return self.where.connector == "OR" and self.where.children and all(x.is_leaf for x in self.where.children)

    def add_extra_select(self, column, lookup):
        if lookup.lower().startswith("select "):
            raise ValueError("SQL statements aren't supported with extra(select=)")

        # Boolean expression test
        bool_expr = r"(?P<lhs>[a-zA-Z0-9_]+)\s?(?P<op>[=|>|<]{1,2})\s?(?P<rhs>[\w+|']+)"

        # Operator expression test
        op_expr = r"(?P<lhs>[a-zA-Z0-9_]+)\s?(?P<op>[+|-|/|*])\s?(?P<rhs>[\w+|']+)"

        OP_LOOKUP = {
            "=": lambda x, y: x == y,
            "is": lambda x, y: x == y,
            "<": lambda x, y: x < y,
            ">": lambda x, y: x > y,
            ">=": lambda x, y: x >= y,
            "<=": lambda x, y: x <= y,
            "+": lambda x, y: x + y,
            "-": lambda x, y: x - y,
            "/": lambda x, y: x / y,
            "*": lambda x, y: x * y,
        }

        for regex in (bool_expr, op_expr):
            match = re.match(regex, lookup)
            if match:
                lhs = match.group("lhs")
                rhs = match.group("rhs")
                op = match.group("op").lower()
                if op in OP_LOOKUP:
                    self.extra_selects.append((column, (OP_LOOKUP[op], (lhs, rhs))))
                else:
                    raise ValueError("Unsupported operator")
                return

        # Assume literal
        self.extra_selects.append((column, (lambda x: x, [lookup])))

    def add_source_table(self, table):
        if table in self.tables:
            return

        self.tables.append(table)

    def set_distinct(self, distinct_fields):
        self.distinct = True
        if distinct_fields:
            for field in distinct_fields:
                self.add_projected_column(field)
        elif not self.columns:
            for field in self.model._meta.fields:
                self.add_projected_column(field.column)

    def add_order_by(self, column):
        self.order_by.append(column)

    def add_annotation(self, column, annotation):
        # The Trunc annotation class doesn't exist in Django 1.8, hence we compare by
        # strings, rather than importing the class to compare it
        name = annotation.__class__.__name__
        if name == "Count":
            return  # Handled elsewhere

        if name not in ("Trunc", "Col", "Date", "DateTime"):
            raise NotSupportedError("Unsupported annotation %s" % name)

        def process_date(value, lookup_type):
            value = ensure_datetime(value)
            ret = datetime.datetime.utcfromtimestamp(0)

            POSSIBLE_LOOKUPS = ("year", "month", "day", "hour", "minute", "second")

            ret = ret.replace(
                value.year,
                value.month if lookup_type in POSSIBLE_LOOKUPS[1:] else ret.month,
                value.day if lookup_type in POSSIBLE_LOOKUPS[2:] else ret.day,
                value.hour if lookup_type in POSSIBLE_LOOKUPS[3:] else ret.hour,
                value.minute if lookup_type in POSSIBLE_LOOKUPS[4:] else ret.minute,
                value.second if lookup_type in POSSIBLE_LOOKUPS[5:] else ret.second,
            )

            return ret

        # Abuse the extra_select functionality
        if name == "Col":
            self.extra_selects.append((column, (lambda x: x, [column])))
        elif name in ("Trunc", "Date", "DateTime"):
            # Trunc stores the source column and the lookup type differently to Date
            # which is why we have the getattr craziness here
            lookup_column = (
                annotation.lhs.output_field.column if name == "Trunc" else getattr(annotation, "lookup", column)
            )

            lookup_type = getattr(annotation, "lookup_type", getattr(annotation, "kind", None))
            assert lookup_type

            self.extra_selects.append((column, (lambda x: process_date(x, lookup_type), [lookup_column])))
            # Override the projection so that we only get this column
            self.columns = set([lookup_column])

    def add_projected_column(self, column):
        self.init_list.append(column)

        if not self.projection_possible:
            # If we previously tried to add a column that couldn't be
            # projected, we don't try and add any more
            return

        field = get_field_from_column(self.model, column)

        if field is None:
            raise NotSupportedError(
                "{} is not a valid column for the queried model. Did you try to join?".format(column)
            )

        if field.db_type(self.connection) in ("bytes", "text", "list", "set"):
            logger.warn("Disabling projection query as %s is an unprojectable type", column)
            self.columns = None
            self.projection_possible = False
            return

        if not self.columns:
            self.columns = set([column])
        else:
            self.columns.add(column)

    def add_row(self, data):
        assert self.columns
        assert len(data) == len(self.columns)

        self.row_data.append(data)

    def prepare(self):
        if not self.init_list:
            self.init_list = [x.column for x in self.model._meta.fields]

        self._remove_impossible_branches()
        self._remove_erroneous_isnull()
        self._remove_negated_empty_in()
        self._add_inheritence_filter()
        self._populate_excluded_pks()
        self._disable_projection_if_fields_used_in_equality_filter()
        self._check_only_single_inequality_filter()

    @property
    def where(self):
        return self._where

    @where.setter
    def where(self, where):
        assert where is None or isinstance(where, WhereNode)

        self._where = where

    def _populate_excluded_pks(self):
        if not self._where:
            return

        self.excluded_pks = set()

        def walk(node, negated):
            if node.connector == "OR":
                # We can only process AND nodes, if we hit an OR we can't
                # use the excluded PK optimization
                return

            if node.negated:
                negated = not negated

            for child in node.children[:]:
                # As more than one inequality filter is not allowed on the datastore
                # this leaf + count check is probably pointless, but at least if you
                # do try to exclude two things it will blow up in the right place and not
                # return incorrect results
                if child.is_leaf and len(node.children) == 1:
                    if negated and child.operator == "=" and child.column == "__key__":
                        self.excluded_pks.add(child.value)
                        node.children.remove(child)
                    elif negated and child.operator == "IN" and child.column == "__key__":
                        [self.excluded_pks.add(x) for x in child.value]
                        node.children.remove(child)
                else:
                    walk(child, negated)

            node.children = [x for x in node.children if x.children or x.column]

        walk(self._where, False)

        if not self._where.children:
            self._where = None

    def _remove_negated_empty_in(self):
        """
            An empty exclude(id__in=[]) is pointless, but can cause trouble
            during denormalization. We remove such nodes here.
        """
        if not self._where:
            return

        def walk(node, negated):
            if node.negated:
                negated = node.negated

            for child in node.children[:]:
                if negated and child.operator == "IN" and not child.value:
                    node.children.remove(child)

                walk(child, negated)

            node.children = [x for x in node.children if x.children or x.column]

        had_where = bool(self._where.children)
        walk(self._where, False)

        # Reset the where if that was the only filter
        if had_where and not bool(self._where.children):
            self._where = None

    def _remove_erroneous_isnull(self):
        # This is a little crazy, but bear with me...
        # If you run a query like this:  filter(thing=1).exclude(field1="test") where field1 is
        # null-able you'll end up with a negated branch in the where tree which is:

        #           AND (negated)
        #          /            \
        #   field1="test"   field1__isnull=False

        # This is because on SQL, field1 != "test" won't give back rows where field1 is null, so
        # django has to include the negated isnull=False as well in order to get back the null rows
        # as well.  On App Engine though None is just a value, not the lack of a value, so it's
        # enough to just have the first branch in the negated node and in fact, if you try to use
        # the above tree, it will result in looking for:
        #  field1 < "test" and field1 > "test" and field1__isnull=True
        # which returns the wrong result (it'll return when field1 == None only)

        def walk(node, negated):
            if node.negated:
                negated = not negated

            if not node.is_leaf:
                equality_fields = set()
                negated_isnull_fields = set()
                isnull_lookup = {}

                for child in node.children[:]:
                    if negated:
                        if child.lookup_name != "isnull":
                            equality_fields.add(child.column)
                            if child.column in negated_isnull_fields:
                                node.children.remove(isnull_lookup[child.column])
                        else:
                            negated_isnull_fields.add(child.column)
                            if child.column in equality_fields:
                                node.children.remove(child)
                            else:
                                isnull_lookup[child.column] = child

                    walk(child, negated)

        if self.where:
            walk(self._where, False)

    def _remove_impossible_branches(self):
        """
            If we mark a child node as never returning results we either need to
            remove those nodes, or remove the branches of the tree they are on depending
            on the connector of the parent node.
        """
        if not self._where:
            return

        def walk(node, negated):
            if node.negated:
                negated = not negated

            for child in node.children[:]:
                walk(child, negated)

                if child.will_never_return_results:
                    if node.connector == "AND":
                        if child.negated:
                            node.children.remove(child)
                        else:
                            node.will_never_return_results = True
                    else:
                        # OR
                        if not child.negated:
                            node.children.remove(child)
                            if not node.children:
                                node.will_never_return_results = True
                        else:
                            node.children[:] = []

        walk(self._where, False)

        if self._where.will_never_return_results:
            # We removed all the children of the root where node, so no results
            raise EmptyResultSet()

    def _check_only_single_inequality_filter(self):
        inequality_fields = set()

        def walk(node, negated):
            if node.negated:
                negated = not negated

            for child in node.children[:]:
                if (negated and child.operator == "=") or child.operator in (">", "<", ">=", "<="):
                    inequality_fields.add(child.column)
                walk(child, negated)

            if len(inequality_fields) > 1:
                raise NotSupportedError(
                    "You can only have one inequality filter per query on the rpc. "
                    "Filters were: %s" % " ".join(inequality_fields)
                )

        if self.where:
            walk(self._where, False)

    def _disable_projection_if_fields_used_in_equality_filter(self):
        if not self._where or not self.columns:
            return

        equality_columns = set()

        def walk(node):
            if not node.is_leaf:
                for child in node.children:
                    walk(child)
            elif node.operator == "=" or node.operator == "IN":
                equality_columns.add(node.column)

        walk(self._where)

        if equality_columns and equality_columns.intersection(self.columns):
            self.columns = None
            self.projection_possible = False

    def _add_inheritence_filter(self):
        """
            We support inheritence with polymodels. Whenever we set
            the 'where' on this query, we manipulate the tree so that
            the lookups are ANDed with a filter on 'class = db_table'
            and on inserts, we add the 'class' column if the model is part
            of an inheritance tree.

            We only do any of this if the model has concrete parents and isn't
            a proxy model
        """
        if has_concrete_parents(self.model) and not self.model._meta.proxy:
            if self.polymodel_filter_added:
                return

            new_filter = WhereNode(self.connection.alias)
            new_filter.column = POLYMODEL_CLASS_ATTRIBUTE
            new_filter.operator = "="
            new_filter.value = self.model._meta.db_table

            # We add this bare AND just to stay consistent with what Django does
            new_and = WhereNode(self.connection.alias)
            new_and.connector = "AND"
            new_and.children = [new_filter]

            new_root = WhereNode(self.connection.alias)
            new_root.connector = "AND"
            new_root.children = [new_and]
            if self._where:
                # Add the original where if there was one
                new_root.children.append(self._where)
            self._where = new_root

            self.polymodel_filter_added = True

    def serialize(self):
        """
            The idea behind this function is to provide a way to serialize this
            query to a string which can be compared to another query. Pickle won't
            work as some of the values etc. might not be picklable.

            FIXME: This function is incomplete! Not all necessary members are serialized
        """
        if not self.is_normalized:
            raise ValueError("You cannot serialize queries unless they are normalized")

        result = {}
        result["kind"] = self.kind
        result["table"] = self.model._meta.db_table
        result["concrete_table"] = self.concrete_model._meta.db_table
        result["columns"] = list(self.columns or [])  # set() is not JSONifiable
        result["projection_possible"] = self.projection_possible
        result["init_list"] = self.init_list
        result["distinct"] = self.distinct
        result["order_by"] = self.order_by
        result["low_mark"] = self.low_mark
        result["high_mark"] = self.high_mark
        result["excluded_pks"] = list(map(str, self.excluded_pks))

        where = []

        if self.where:
            assert self.where.connector == "OR"
            for node in self.where.children:
                assert node.connector == "AND"

                query = {}

                if node.children:
                    for lookup in node.children:

                        query["".join([lookup.column, lookup.operator])] = _serialize_sql_value(lookup.value)
                else:
                    query["".join([node.column, node.operator])] = _serialize_sql_value(node.value)

                where.append(query)

        result["where"] = where

        return json.dumps(result)


INVALID_ORDERING_FIELD_MESSAGE = (
    "Ordering on TextField or BinaryField is not supported on the rpc. "
    "You might consider using a ComputedCharField which stores the first "
    "_MAX_STRING_LENGTH (from google.appengine.api.datastore_types) bytes of the "
    "field and instead order on that."
)


def _serialize_sql_value(value):
    if isinstance(value, six.integer_types):
        return value
    else:
        return six.text_type("NULL" if value is None else value)


def _get_parser(query, connection=None):
    from gcloudc.db.backends.datastore.parsers import base

    return base.BaseParser(query, connection)


def transform_query(connection, query):
    return _get_parser(query, connection).get_transformed_query()


def extract_ordering(query):
    return _get_parser(query).get_extracted_ordering()
