from django.db.models.expressions import Star
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.where import SubqueryConstraint

from .version_19 import Parser as BaseParser


class Parser(BaseParser):
    def _prepare_for_transformation(self):
        from django.db.models.sql.where import EmptyWhere

        if isinstance(self.django_query.where, EmptyWhere):
            # Empty where means return nothing!
            raise EmptyResultSet()

    def _where_node_leaf_callback(self, node, negated, new_parent, connection, model, compiler):
        if not isinstance(node, SubqueryConstraint):
            # Only do this test if it's not a subquery, the parent method deals with that
            if not hasattr(node, "lhs") and not hasattr(node, "rhs"):
                # Empty Q() object - basically an empty where node that needs nothing doing to it
                return

        return super(Parser, self)._where_node_leaf_callback(node, negated, new_parent, connection, model, compiler)

    def _determine_query_kind(self):
        query = self.django_query
        if query.annotations:
            if "__count" in query.annotations:
                field = query.annotations["__count"].input_field
                if isinstance(field, Star) or field.value == "*":
                    return "COUNT"

        return "SELECT"
