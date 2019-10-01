"""
The Datastore does not provide database level constraints around uniqueness
(unlike relational a SQL database, where you can ensure uniqueness for a certain
column, or a combination of columns).

To mimic the ability to define these constraints using the Django API,
we have implemented an approach where a Datastore Entity is used to represent
each existing constraint. This uses a named key, which is generated from a
combination of the django database model and the unique field/values.

This allows us to efficiently check for existing constraints before doing a put().
"""
import datetime

from . import transaction
from .dbapi import DataError, IntegrityError
from .unique_utils import (
    unique_identifiers_from_entity,
    _has_enabled_constraints,
    _has_unique_constraints,
    _get_kind_from_named_marker_key,
    _get_unique_fields_from_named_marker_key,
)
from .utils import key_exists


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


# we need this transaction to be independent from any outer transaction,
# to avoid eating away at the 500 entity write limit per transaction block
@transaction.atomic(independent=True)
def acquire_unique_markers(model, entity, connection):
    """
    Attempt to acquire all unique marker entities needed by the model definition
    for the given entity property values. If a marker already exists and
    references an existing entity (e.g. is not stale) we raise an IntegrityError.

    Returns an iterable of all newly acquired keys, to support a rollback
    in the outer transaction if required.
    """

    def _put_unique_marker(unique_marker_entity):
        """
        Update a unique marker entity with some new properties and persist
        the update/insertion into the datastore.
        """
        unique_marker_entity["updated_at"] = datetime.datetime.utcnow()
        unique_marker_entity["instance"] = entity.key
        # any put() is essentially async inside a transaction/batch operation
        transaction._rpc(connection).put(unique_marker_entity)

        # update the in memory reference of any markers acquired
        acquired_markers.append(unique_marker_entity.key)

    acquired_markers = []

    # get all the key objects we need to represent the entities we put
    unique_marker_keys = _get_unique_marker_keys_for_entity(model, entity, connection, refetch=False)

    # we can now pass these to the client to fetch the existing unique markers
    # (note that the raw datastore API get has an odd signature, where you
    # need to pass a list kwarg to explicity identify which ones are missing)
    missing_markers = []
    existing_unique_markers = transaction._rpc(connection).get(unique_marker_keys, missing=missing_markers)

    # handle the markers we know don't exist yet - these can be a straight put()
    for missing_entity in missing_markers:
        _put_unique_marker(missing_entity)

    # for the remaining entities, decide what action to take...
    for existing_marker in existing_unique_markers:
        put_unique_marker = False

        # if there is a stale unique marker but it doesn't have an instance
        # associated with it, we can grab it and reference our entity
        if existing_marker["instance"] is None:
            put_unique_marker = True

        # if any of unique markers exist but reference a different underlying
        # entity (which isn't stale / deleted now), we need to raise an error
        # as the unique constraint is already satisfied
        elif existing_marker["instance"] != entity.key:

            # double check the instance isn't stale / deleted now
            if key_exists(connection, existing_marker["instance"]):

                # we can reverse parse the named key to find the fields
                # which have failed the unique constraint check
                table_name = _get_kind_from_named_marker_key(existing_marker.key)
                unique_fields = _get_unique_fields_from_named_marker_key(existing_marker.key)
                raise IntegrityError(CONSTRAINT_VIOLATION_MSG.format(table_name, ", ".join(unique_fields)))

            # if the referened entity doesn't exist, we can claim the marker
            else:
                put_unique_marker = True

        # update the unique marker entity
        if put_unique_marker:
            _put_unique_marker(existing_marker)

    # we return all the keys of markers we have touched to the caller, to handle
    # cases where the final put() of the actual entity fails, allowing us to
    # cleanup these markers / avoid leaving stale entity references
    return acquired_markers


@transaction.atomic(independent=True)
def delete_unique_markers(unique_marker_keys, connection):
    """
    Thin wrapper around a RPC delete operation on an iterable of keys inside
    an independent transaction block.

    If you don't know what unique markers you need to delete, you can use
    `delete_unique_markers_for_entity` which calculates the named keys
    from the entity values.
    """
    transaction._rpc(connection).delete(unique_marker_keys)


@transaction.atomic()
def delete_unique_markers_for_entity(model, entity, connection, refetch=True):
    """
    Delete all UniqueMarkers which reference a given entity.

    Rather than do a query to find all references, we can grab all the
    UniqueMarker entities by key for a small performance win.
    """
    unique_marker_keys = _get_unique_marker_keys_for_entity(model, entity, connection, refetch=refetch)
    transaction._rpc(connection).delete(unique_marker_keys)


@transaction.atomic()
def _get_unique_marker_keys_for_entity(model, entity, connection, refetch=True):
    """
    This function encapsulates a common pattern of refetching the given entity,
    generating the named key string given the model unique constraints /
    corresponding entity property values, and then fetching these entities
    using the datastore client.
    """
    # if we refetch an object which has just been put() in the same transaction
    # it won't be found, so we need to support avoiding this...
    if refetch:
        entity = transaction._rpc(connection).get(entity.key)
        if entity is None:
            # TODO what would be the best exception to raise if the entity is gone
            raise DataError("Entity no longer exists")

    marker_key_values = unique_identifiers_from_entity(model, entity)
    return [
        connection.connection.gclient.key(UNIQUE_MARKER_KIND, identifier, namespace=connection.namespace)
        for identifier in marker_key_values
    ]


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
