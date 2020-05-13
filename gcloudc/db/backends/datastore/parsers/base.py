import logging

from django.core.exceptions import FieldError
from django.db import NotSupportedError
from django.db.models.aggregates import Aggregate
from django.db.models.expressions import Star
from django.db.models.fields import FieldDoesNotExist
from django.db.models.query import FlatValuesListIterable, QuerySet
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.query import Query as DjangoQuery

from ..query import Query, WhereNode
from ..utils import get_top_concrete_parent


# Django >= 1.9
class ValuesListQuerySet(object):
    pass


def _iterable(x):
    # Returns True if x is iterable and not a string type
    return hasattr(x, "__iter__") and not isinstance(x, (str, bytes))


logger = logging.getLogger(__name__)


INVALID_ORDERING_FIELD_MESSAGE = (
    "Ordering on TextField or BinaryField is not supported on the Datastore. "
    "You might consider using a ComputedCharField which stores the first "
    "_MAX_STRING_LENGTH (from google.appengine.api.datastore_types) bytes of the "
    "field and instead order on that."
)


def _get_concrete_fields_with_model(model):
    return [
        (f, f.model if f.model != model else None)
        for f in model._meta.get_fields()
        if f.concrete and (not f.is_relation or f.one_to_one or (f.many_to_one and f.related_model))
    ]


def _walk_django_where(query, trunk_callback, leaf_callback, **kwargs):
    """
        Walks through a Django where tree. If a leaf node is encountered
        the leaf_callback is called, otherwise the trunk_callback is called
    """

    def walk_node(node, **kwargs):
        negated = kwargs["negated"]

        if node.negated:
            negated = not negated

        for child in node.children:
            new_kwargs = kwargs.copy()
            new_kwargs["negated"] = negated
            if not getattr(child, "children", []):
                leaf_callback(child, **new_kwargs)
            else:
                new_parent = trunk_callback(child, **new_kwargs)

                if new_parent:
                    new_kwargs["new_parent"] = new_parent

                walk_node(child, **new_kwargs)

    kwargs.setdefault("negated", False)
    walk_node(query.where, **kwargs)


class BaseParser(object):
    """
        Supports the most recent version of Django. Subclasses can
        override parts of this to support older versions.

        (NOTE: this goes against all my thoughts on good OO but I can't think
        of a cleaner way of doing things at the moment!)
    """

    def __init__(self, django_query, connection=None):
        self.django_query = django_query
        self.connection = connection
        self.model = self.django_query.model

    def _determine_query_kind(self):
        """ Basically returns SELECT or COUNT """
        query = self.django_query

        if query.annotations:
            if "__count" in query.annotations:
                field = query.annotations["__count"].source_expressions[0]
                if isinstance(field, Star) or field.value == "*":
                    return "COUNT"

        return "SELECT"

    def _prepare_for_transformation(self):
        from django.db.models.sql.where import NothingNode

        query = self.django_query

        def where_will_always_be_empty(where):
            if isinstance(where, NothingNode):
                return True

            if where.connector == "AND" and any(isinstance(x, NothingNode) for x in where.children):
                return True

            if where.connector == "OR" and len(where.children) == 1 and isinstance(where.children[0], NothingNode):
                return True

            return False

        # It could either be a NothingNode, or a WhereNode(AND NothingNode)
        if where_will_always_be_empty(query.where):
            # Empty where means return nothing!
            raise EmptyResultSet()

    def _extract_projected_columns(self):
        result = []
        query = self.django_query

        if query.select:
            for x in query.select:
                column = x.target.column
                result.append(column)
            return result
        else:
            # If the query uses defer()/only() then we need to process deferred. We have to get all deferred columns
            # for all (concrete) inherited models and then only include columns if they appear in that list
            only_load = query.get_loaded_field_names()
            if only_load:
                for field, model in _get_concrete_fields_with_model(query.model):
                    model = model or query.model
                    try:
                        if field.column in only_load[model]:
                            # Add a field that has been explicitly included
                            result.append(field.column)
                    except KeyError:
                        # Model wasn't explicitly listed in the only_load table
                        # Therefore, we need to load all fields from this model
                        result.append(field.column)
                return result
            else:
                return []

    def _apply_ordering_to_query(self, query):
        # Extract the ordering of the query results
        for order_col in self.get_extracted_ordering():
            query.add_order_by(order_col)

    def _set_projected_columns_on_query(self, query):
        # Extract any projected columns (values/values_list/only/defer)
        for projected_col in self._extract_projected_columns():
            query.add_projected_column(projected_col)

    def _apply_extra_selects_to_query(self, query):
        # Add any extra selects
        for col, select in self.django_query.extra_select.items():
            query.add_extra_select(col, select[0])

    def _apply_distinct_columns_to_query(self, query):
        if self.django_query.distinct:
            # This must happen after extracting projected cols
            query.set_distinct(list(self.django_query.distinct_fields))

    def _apply_annotations_to_query(self, query):
        # Process annotations!
        if self.django_query.annotation_select:
            for k, v in self.django_query.annotation_select.items():
                query.add_annotation(k, v)

    def _where_node_trunk_callback(self, node, negated, new_parent, **kwargs):
        new_node = WhereNode(new_parent.using)
        new_node.connector = node.connector
        new_node.negated = node.negated

        new_parent.children.append(new_node)

        return new_node

    def _where_node_leaf_callback(self, node, negated, new_parent, connection, model, compiler):
        new_node = WhereNode(new_parent.using)

        def convert_rhs_op(node):
            db_rhs = getattr(node.rhs, "_db", None)
            if db_rhs is not None and db_rhs != connection.alias:
                raise ValueError(
                    "Subqueries aren't allowed across different databases. Force "
                    "the inner query to be evaluated using `list(inner_query)`."
                )

            value = node.get_rhs_op(connection, node.rhs)
            operator = value.split()[0].lower().strip()
            if operator == "between":
                operator = "range"
            return operator

        if not hasattr(node, "lhs"):
            raise NotSupportedError("Attempted probable subquery, these aren't supported on the Datastore")

        # Don't call on querysets
        if not hasattr(node.rhs, "_as_sql") and not isinstance(node.rhs, DjangoQuery):
            try:
                # Although we do nothing with this. We need to call it as many lookups
                # perform validation etc.
                node.process_rhs(compiler, connection)
            except EmptyResultSet:
                if node.lookup_name == "in":
                    node.rhs = []
                else:
                    raise

        # Leaf
        if hasattr(node.lhs, "target"):
            # from Django 1.9, some node.lhs might not have a target attribute
            # as they might be wrapping date fields
            field = node.lhs.target
            operator = convert_rhs_op(node)
        elif isinstance(node.lhs, Aggregate):
            raise NotSupportedError("Aggregate filters are not supported on the Datastore")
        else:
            field = node.lhs.lhs.target
            operator = convert_rhs_op(node)

            # This deals with things like datefield__month__gt=X which means from this point
            # on, operator will have two parts in that particular case and will probably need to
            # be dealt with by a special indexer
            if node.lookup_name != node.lhs.lookup_name:
                operator = "{}__{}".format(node.lhs.lookup_name, node.lookup_name)

        if get_top_concrete_parent(field.model) != get_top_concrete_parent(model):
            raise NotSupportedError("Cross-join where filters are not supported on the Datastore")

        # Make sure we don't let people try to filter on a text field, otherwise they just won't
        # get any results!

        lookup_supports_text = getattr(node, "lookup_supports_text", False)

        if field.db_type(connection) in ("bytes", "text") and not lookup_supports_text:
            raise NotSupportedError("You can't filter on text or blob fields on the Datastore")

        if operator == "isnull" and field.model._meta.parents.values():
            raise NotSupportedError("isnull lookups on inherited relations aren't supported on the Datastore")

        lhs = field.column

        if hasattr(node.rhs, "get_compiler"):
            if len(node.rhs.select) == 1:
                # In Django >= 1.11 this is a values list type query, which we explicitly handle
                # because of the common case of pk__in=Something.objects.values_list("pk", flat=True)
                qs = QuerySet(query=node.rhs, using=self.connection.alias)

                # We make the query for the values, but wrap in a list to trick the
                # was_iter code below. This whole set of if/elif statements needs rethinking!
                rhs = [list(qs.values_list("pk", flat=True))]
            else:
                # This is a subquery
                raise NotSupportedError("Attempted to run a subquery on the Datastore")
        elif isinstance(node.rhs, ValuesListQuerySet):
            # We explicitly handle ValuesListQuerySet because of the
            # common case of pk__in=Something.objects.values_list("pk", flat=True)
            # this WILL execute another query, but that is to be expected on a
            # non-relational database.

            rhs = [x for x in node.rhs]  # Evaluate the queryset

        elif isinstance(node.rhs, QuerySet):
            # In Django 1.9, ValuesListQuerySet doesn't exist anymore, and instead
            # values_list returns a QuerySet
            if node.rhs._iterable_class == FlatValuesListIterable:
                # if the queryset has FlatValuesListIterable as iterable class
                # then it's a flat list, and we just need to evaluate the
                # queryset converting it into a list
                rhs = [x for x in node.rhs]
            else:
                # otherwise, we try to get the PK from the queryset
                rhs = list(node.rhs.values_list("pk", flat=True))
        else:
            rhs = node.rhs

        was_iter = _iterable(node.rhs)
        rhs = node.get_db_prep_lookup(rhs, connection)[-1]
        if rhs and not was_iter and _iterable(rhs):
            rhs = rhs[0]

        new_node.set_leaf(
            lhs,
            operator,
            rhs,
            is_pk_field=field == model._meta.pk,
            negated=negated,
            lookup_name=node.lookup_name,
            namespace=connection.namespace,
            target_field=field,
        )

        # For some reason, this test:
        # test_update_with_related_manager (get_or_create.tests.UpdateOrCreateTests)
        # ends up with duplicate nodes in the where tree. I don't know why. But this
        # weirdly causes the Datastore query to return nothing.
        # so here we don't add duplicate nodes, I can't think of a case where that would
        # change the query if it's under the same parent.
        if new_node in new_parent.children:
            return

        new_parent.children.append(new_node)

    def _generate_where_node(self, query):
        output = WhereNode(query.connection.alias)
        output.connector = self.django_query.where.connector

        _walk_django_where(
            self.django_query,
            self._where_node_trunk_callback,
            self._where_node_leaf_callback,
            new_parent=output,
            connection=self.connection,
            negated=self.django_query.where.negated,
            model=self.model,
            compiler=self.django_query.get_compiler(self.connection.alias),
        )

        return output

    def get_transformed_query(self):
        self._prepare_for_transformation()

        kind = self._determine_query_kind()

        ret = Query(self.model, kind)
        ret.connection = self.connection

        # Add the root concrete table as the source table
        root_table = get_top_concrete_parent(self.model)._meta.db_table
        ret.add_source_table(root_table)

        self._apply_ordering_to_query(ret)
        self._set_projected_columns_on_query(ret)
        self._apply_extra_selects_to_query(ret)
        self._apply_distinct_columns_to_query(ret)
        self._apply_annotations_to_query(ret)

        # Extract any query offsets and limits
        ret.low_mark = self.django_query.low_mark
        ret.high_mark = self.django_query.high_mark

        output = self._generate_where_node(ret)

        # If there no child nodes, just wipe out the where
        if not output.children:
            output = None

        ret.where = output
        return ret

    def get_extracted_ordering(self):
        from ..commands import log_once
        from django.db.models.expressions import OrderBy, F

        query = self.django_query

        # Add any orderings
        if not query.default_ordering:
            result = list(query.order_by)
        else:
            result = list(query.order_by or query.get_meta().ordering or [])

        if query.extra_order_by:
            result = list(query.extra_order_by)

            # we need some extra logic to handle dot seperated ordering
            new_result = []
            cross_table_ordering = set()
            for ordering in result:
                if "." in ordering:
                    dot_based_ordering = ordering.split(".")
                    if dot_based_ordering[0] == query.model._meta.db_table:
                        ordering = dot_based_ordering[1]
                    elif dot_based_ordering[0].lstrip("-") == query.model._meta.db_table:
                        ordering = "-{}".format(dot_based_ordering[1])
                    else:
                        cross_table_ordering.add(ordering)
                        continue  # we don't want to add this ordering value
                new_result.append(ordering)

            if len(cross_table_ordering):
                log_once(
                    logger.warning,
                    "The following orderings were ignored as cross-table orderings"
                    " are not supported on the Datastore: %s",
                    cross_table_ordering,
                )

            result = new_result

        final = []

        opts = query.model._meta

        # Apparently expression ordering is absolute and so shouldn't be flipped
        # if the standard_ordering is False. This keeps track of which columns
        # were expressions and so don't need flipping
        expressions = set()

        for col in result:
            if isinstance(col, OrderBy):
                descending = col.descending
                col = col.expression.name
                if descending:
                    col = "-" + col
                expressions.add(col)

            elif isinstance(col, F):
                col = col.name

            if isinstance(col, int):
                # If you do a Dates query, the ordering is set to [1] or [-1]... which is weird
                # I think it's to select the column number but then there is only 1 column so
                # unless the ordinal is one-based I have no idea. So basically if it's an integer
                # subtract 1 from the absolute value and look up in the select for the column (guessing)
                idx = abs(col) - 1
                try:
                    field_name = query.select[idx].col.col[-1]
                    field = query.model._meta.get_field(field_name)
                    final.append("-" + field.column if col < 0 else field.column)
                except IndexError:
                    raise NotSupportedError("Unsupported order_by %s" % col)
            elif col.lstrip("-") == "pk":
                pk_col = "__key__"
                final.append("-" + pk_col if col.startswith("-") else pk_col)
            elif col == "?":
                raise NotSupportedError("Random ordering is not supported on the Datastore")
            elif col.lstrip("-").startswith("__") and col.endswith("__"):
                # Allow stuff like __scatter__
                final.append(col)
            elif "__" in col:
                continue
            else:
                try:
                    column = col.lstrip("-")

                    # This is really 1.8 only, but I didn't want to duplicate this function
                    # just for this. Suggestions for doing this more cleanly welcome!
                    if column in getattr(query, "annotation_select", {}):
                        # It's an annotation, if it's a supported one, return the
                        # original column
                        annotation = query.annotation_select[column]
                        name = annotation.__class__.__name__

                        # We only support a few expressions
                        if name not in ("Trunc", "Col", "Date", "DateTime"):
                            raise NotSupportedError("Tried to order by unsupported expression")
                        elif name == "Trunc":
                            column = annotation.lhs.output_field.column
                        else:
                            # Retrieve the original column and use that for ordering
                            if name == "Col":
                                column = annotation.output_field.column
                            else:
                                column = annotation.col.output_field.column

                    field = query.model._meta.get_field(column)

                    if field.get_internal_type() in (u"TextField", u"BinaryField"):
                        raise NotSupportedError(INVALID_ORDERING_FIELD_MESSAGE)

                    # If someone orders by 'fk' rather than 'fk_id' this complains as that should take
                    # into account the related model ordering. Note the difference between field.name == column
                    # and field.attname (X_id)
                    if field.related_model and field.name == column and field.related_model._meta.ordering:
                        raise NotSupportedError("Related ordering is not supported on the Datastore")

                    column = "__key__" if field.primary_key else field.column
                    final.append("-" + column if col.startswith("-") else column)
                except FieldDoesNotExist:
                    if col in query.extra_select:
                        # If the column is in the extra select we transform to the original
                        # column
                        try:
                            field = opts.get_field(query.extra_select[col][0])
                            column = "__key__" if field.primary_key else field.column
                            final.append("-" + column if col.startswith("-") else column)
                            continue
                        except FieldDoesNotExist:
                            # Just pass through to the exception below
                            pass

                    available = opts.get_all_field_names()
                    raise FieldError(
                        "Cannot resolve keyword %r into field. " "Choices are: %s" % (col, ", ".join(available))
                    )

        # Reverse if not using standard ordering
        def swap(col):
            if col.startswith("-"):
                return col.lstrip("-")
            else:
                return "-{}".format(col)

        if not query.standard_ordering:
            final = [x if x in expressions else swap(x) for x in final]

        if len(final) != len(result):
            diff = set(result) - set(final)
            log_once(
                logger.warning,
                "The following orderings were ignored as cross-table and random orderings"
                " are not supported on the Datastore: %s",
                diff,
            )

        return final
