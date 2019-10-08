from django.db import NotSupportedError
from django.db.models.aggregates import Aggregate
from django.db.models.expressions import (
    Col,
    F,
)
from django.utils import six
from gcloudc.db.backends.datastore.utils import get_prepared_db_value

CONNECTORS = {
    F.ADD: lambda l, r: l + r,
    F.SUB: lambda l, r: l - r,
    F.MUL: lambda l, r: l * r,
    F.DIV: lambda l, r: l / r,
}


def evaluate_expression(expression, instance, connection):
    """ A limited evaluator for Django's F expressions. This are evaluated within
        the get/put transaction in _update_entity so these will happen atomically
    """

    if isinstance(expression, (six.string_types, int, float)):
        return expression

    if isinstance(expression, Aggregate):
        raise NotSupportedError("Aggregate expressions are not supported on the datastore")

    if hasattr(expression, "name"):
        field = instance._meta.get_field(expression.name)
        return get_prepared_db_value(connection, instance._original, field)

    if isinstance(expression, Col):
        field = expression.field
        return get_prepared_db_value(connection, instance._original, field)

    if hasattr(expression, "value"):
        return expression.value

    if hasattr(expression, "connector") and expression.connector in CONNECTORS:
        if hasattr(expression, "children"):
            lhs, rhs = expression.children
        else:
            lhs, rhs = expression.lhs, expression.rhs

        return CONNECTORS[expression.connector](
            evaluate_expression(lhs, instance, connection), evaluate_expression(rhs, instance, connection)
        )

    raise NotImplementedError("Support for expression %r isn't implemented", expression)
