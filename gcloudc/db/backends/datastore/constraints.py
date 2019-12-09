"""
The Datastore does not provide database level constraints around uniqueness
(unlike relational a SQL database, where you can ensure uniqueness for a certain
column, or a combination of columns).

To mimic the ability to define these constraints using the Django API,
we have implemented an approach where, thanks to Cloud Firestore strong
consistency we check unique constraints transactionally before any write

This allows us to efficiently check for existing constraints before doing a put().
"""

from .dbapi import IntegrityError
from .unique_utils import (
    unique_identifiers_from_entity,
    _has_enabled_constraints,
    _has_unique_constraints,
)


UNIQUE_MARKER_KIND = "uniquemarker"
CONSTRAINT_VIOLATION_MSG = "Unique constraint violation for kind {} on fields: {}"


def has_active_unique_constraints(model_or_instance):
    """
    Returns a boolean to indicate if we should respect any unique constraints
    defined on the provided instance / model, taking into account any model
    or global related flags.
    """
    # are unique constraints disabled on the provided model take precident
    constraints_enabled = _has_enabled_constraints(model_or_instance)
    if not constraints_enabled:
        return False

    # does the object have unique constraints defined in the model definition
    return _has_unique_constraints(model_or_instance)


def check_unique_markers_in_memory(model, entities):
    """
    Compare the entities using their in memory properties, to see if any
    unique constraints are violated.

    This would always need to be used in conjunction with RPC checks against
    persisted markers to ensure data integrity.
    """
    all_unique_marker_key_values = set([])
    for entity, _ in entities:
        unique_marker_key_values = unique_identifiers_from_entity(model, entity, ignore_pk=True)
        for named_key in unique_marker_key_values:
            if named_key not in all_unique_marker_key_values:
                all_unique_marker_key_values.add(named_key)
            else:
                table_name = named_key.split("|")[0]
                unique_fields = named_key.split("|")[1:]
                raise IntegrityError(CONSTRAINT_VIOLATION_MSG.format(table_name, unique_fields))
