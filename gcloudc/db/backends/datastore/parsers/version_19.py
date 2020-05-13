from django.db import NotSupportedError
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.query import QuerySet

try:
    from django.db.models.query import FlatValuesListIterable
except ImportError:
    # Django 1.8
    class FlatValuesListIterable(object):
        pass


try:
    from django.db.models.query import ValuesListQuerySet
except ImportError:
    # Django >= 1.9
    class ValuesListQuerySet(object):
        pass


from .base import BaseParser
from ..query import WhereNode


from djangae.db.utils import get_top_concrete_parent


class Parser(BaseParser):
    def _where_node_leaf_callback(self, node, negated, new_parent, connection, model, compiler):
        new_node = WhereNode(new_parent.using)

        if not hasattr(node, "lhs"):
            raise NotSupportedError("Attempted probable subquery, these aren't supported on the datastore")

        # Leaf
        if hasattr(node.lhs, "target"):
            # from Django 1.9, some node.lhs might not have a target attribute
            # as they might be wrapping date fields
            field = node.lhs.target
            operator = node.lookup_name
        else:
            field = node.lhs.lhs.target
            operator = node.lhs.lookup_name

            # This deals with things like datefield__month__gt=X which means from this point
            # on, operator will have two parts in that particular case and will probably need to
            # be dealt with by a special indexer
            if node.lookup_name != operator:
                operator = "{}__{}".format(operator, node.lookup_name)

        # FIXME: This is a hack. In the base parser (> 1.9) we make use of the get_rhs_op
        # method on lookups to work out what these operators should be
        if field.db_type(connection) in ("set", "list"):
            if operator == "isempty":
                operator = "isnull"
            elif operator == "overlap":
                operator = "in"
            elif operator == "contains":
                operator = "exact"

        if get_top_concrete_parent(field.model) != get_top_concrete_parent(model):
            raise NotSupportedError("Cross-join where filters are not supported on the datastore")

        # Make sure we don't let people try to filter on a text field, otherwise they just won't
        # get any results!

        lookup_supports_text = getattr(node, "lookup_supports_text", False)

        if field.db_type(connection) in ("bytes", "text") and not lookup_supports_text:
            raise NotSupportedError("You can't filter on text or blob fields on the datastore")

        if operator == "isnull" and field.model._meta.parents.values():
            raise NotSupportedError("isnull lookups on inherited relations aren't supported on the datastore")

        lhs = field.column

        try:
            if hasattr(node.rhs, "get_compiler"):
                # This is a subquery
                raise NotSupportedError("Attempted to run a subquery on the datastore")
            elif isinstance(node.rhs, ValuesListQuerySet):
                # We explicitly handle ValuesListQuerySet because of the
                # common case of pk__in=Something.objects.values_list("pk", flat=True)
                # this WILL execute another query, but that is to be expected on a
                # non-relational datastore.

                node.rhs = [x for x in node.rhs]  # Evaluate the queryset
                rhs = node.process_rhs(None, connection)  # Process the RHS as if it was a list

            elif isinstance(node.rhs, QuerySet):
                # In Django 1.9, ValuesListQuerySet doesn't exist anymore, and instead
                # values_list returns a QuerySet
                if node.rhs._iterable_class == FlatValuesListIterable:
                    # if the queryset has FlatValuesListIterable as iterable class
                    # then it's a flat list, and we just need to evaluate the
                    # queryset converting it into a list
                    node.rhs = [x for x in node.rhs]
                else:
                    # otherwise, we try to get the PK from the queryset
                    node.rhs = list(node.rhs.values_list("pk", flat=True))

                rhs = node.process_rhs(None, connection)  # Process the RHS as if it was a list

            else:
                rhs = node.process_rhs(None, connection)
        except EmptyResultSet:
            if operator == "in":
                # Deal with this later
                rhs = [[]]
            else:
                raise

        if operator in ("in", "range"):
            rhs = rhs[-1]
        elif operator == "isnull":
            rhs = node.rhs
        else:
            rhs = rhs[-1][0]

        new_node.set_leaf(
            lhs,
            "__".join([operator, node.path]) if hasattr(node, "path") else operator,
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
        # weirdly causes the datastore query to return nothing.
        # so here we don't add duplicate nodes, I can't think of a case where that would
        # change the query if it's under the same parent.
        if new_node in new_parent.children:
            return

        new_parent.children.append(new_node)
