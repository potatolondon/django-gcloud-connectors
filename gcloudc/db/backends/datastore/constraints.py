import datetime

from django.conf import settings
from django.core.exceptions import NON_FIELD_ERRORS
from django.utils.six import iteritems
from google.cloud.datastore.key import Key

from . import transaction
from .dbapi import (
    IntegrityError,
    NotSupportedError,
)
from .unique_utils import unique_identifiers_from_entity
from .utils import key_exists


def has_active_unique_constraints(model_or_instance):
    """
        Returns true if the model/instance has unique fields or unique_together fields and unique
        constraint checking is enabled on the model
    """

    django_opts = getattr(model_or_instance, "_meta", None)

    # If there are no unique fields on the model, return false
    if not django_opts.unique_together and not any(x.unique for x in django_opts.fields):
        return False

    opts = getattr(model_or_instance, "Djangae", None)
    if opts:
        if hasattr(opts, "disable_constraint_checks"):
            if opts.disable_constraint_checks:
                return False
            else:
                return True

    return not getattr(settings, "DJANGAE_DISABLE_CONSTRAINT_CHECKS", False)


class EntityModel:
    """
        Really basic Model wrapper around Entity

        PROPERTIES = (
            (name, type, required?, auto_now_add?)
        )
    """

    PROPERTIES = []

    @staticmethod
    def kind():
        raise NotImplementedError()

    def __init__(self, entity):
        assert(0)
        pass

    @classmethod
    def get(self, keys):
        assert(0)
        pass

    def put(self):
        assert(0)
        pass


class UniqueMarker(EntityModel):
    PROPERTIES = [
        ("instance", Key, False, False),
        ("created", datetime, True, True)
    ]

    @staticmethod
    def kind():
        return "_djangae_unique_marker"


@transaction.atomic(xg=True)
def acquire_identifiers(identifiers, entity_key):
    return _acquire_identifiers(identifiers, entity_key)


def _acquire_identifiers(identifiers, entity_key):
    # This must always be in a cross-group transaction, because even if there's only 1 identifider,
    # in the case where that identifier already exists, we then check if its `instance` exists
    assert entity_key
    namespace = entity_key.namespace() or None
    identifier_keys = [
        Key.from_path(UniqueMarker.kind(), identifier, namespace=namespace) for identifier in identifiers
    ]
    existing_markers = UniqueMarker.get(identifier_keys)
    markers_to_create = []
    markers = []

    for identifier_key, existing_marker in zip(identifier_keys, existing_markers):

        # Backwards compatability: we used to create the markers first in an independent transaction
        # and then create the entity and update the `instance` on the markers.  This meant that it
        # was possible that the independent marker creation transaction finished first and the outer
        # transaction failed, causing stale markers to be left behind.  We no longer do it this way
        # but we still want to ignore any old stale markers, hence if instance is None we overwrite.
        now = datetime.datetime.utcnow()
        if not existing_marker or existing_marker.instance is None:
            markers_to_create.append(UniqueMarker(
                key=identifier_key,
                instance=entity_key,
                created=now
            ))
        elif existing_marker.instance != entity_key and key_exists(existing_marker.instance):
            fields_and_values = identifier_key.name().split("|")
            table_name = fields_and_values[0]
            fields_and_values = fields_and_values[1:]
            fields = [x.split(":")[0] for x in fields_and_values]
            raise IntegrityError("Unique constraint violation for kind {} on fields: {}".format(table_name, ", ".join(fields)))
        elif existing_marker.instance != entity_key:
            markers_to_create.append(UniqueMarker(
                key=identifier_key,
                instance=entity_key,
                created=now
            ))
        else:
            # The marker is ours anyway
            markers.append(existing_marker)

    db.put(markers_to_create)
    return markers + markers_to_create


def get_markers_for_update(model, old_entity, new_entity):
    """
        Given an old entity state, and the new state, updates the identifiers
        appropriately. Should be called before saving the new_state
    """
    old_ids = set(unique_identifiers_from_entity(model, old_entity, ignore_pk=True))
    new_ids = set(unique_identifiers_from_entity(model, new_entity, ignore_pk=True))

    to_release = old_ids - new_ids
    to_acquire = new_ids - old_ids

    return to_acquire, to_release


def update_instance_on_markers(entity, markers):
    # TODO: fix me!
    def update(marker, instance):
        marker = UniqueMarker.get(marker.key())
        if not marker:
            return

        marker.instance = instance
        marker.put()

    @db.transactional(propagation=TransactionOptions.INDEPENDENT, xg=True)
    def update_all():
        instance = entity.key()
        for marker in markers:
            update(marker, instance)
    update_all()


def acquire(model, entity):
    """
        Given a model and entity, this tries to acquire unique marker locks for the instance. If
        the locks already exist then an IntegrityError will be thrown.
    """
    identifiers = unique_identifiers_from_entity(model, entity, ignore_pk=True)
    return acquire_identifiers(identifiers, entity.key())


def release_markers(markers):
    """ Delete the given UniqueMarker objects. """
    # Note that these should all be from the same Django model instance, and therefore there should
    # be a maximum of 25 of them (because everything blows up if you have more than that - limitation)

    @transaction.atomic(independent=True, enable_cache=False)
    def txn():
        transaction._rpc().delete([marker.key() for marker in markers])

    txn()


def release_identifiers(identifiers, namespace):

    @transaction.atomic(independent=True, enable_cache=False)
    def txn():
        _release_identifiers(identifiers, namespace)
    txn()


def _release_identifiers(identifiers, namespace):
    keys = [Key.from_path(UniqueMarker.kind(), x, namespace=namespace) for x in identifiers]
    Delete(keys)


def release(model, entity):
    """ Delete the UniqueMarker objects for the given entity. """
    if not has_active_unique_constraints(model):
        return
    identifiers = unique_identifiers_from_entity(model, entity, ignore_pk=True)
    # Key.from_path expects None for an empty namespace, but Key.namespace() returns ''
    namespace = entity.key().namespace() or None
    release_identifiers(identifiers, namespace=namespace)


@transaction.atomic(xg=True, enable_cache=False)
def update_identifiers(to_acquire, to_release, key):
    """ A combination of acquire_identifiers and release_identifiers in a combined transaction. """
    _acquire_identifiers(to_acquire, key)
    _release_identifiers(to_release, key.namespace() or None)


class UniquenessMixin(object):
    """ Mixin overriding the methods checking value uniqueness.

    For models defining unique constraints this mixin should be inherited from.
    When iterable (list or set) fields are marked as unique it must be used.
    This is a copy of Django's implementation, save for the part marked by the comment.
    """
    def _perform_unique_checks(self, unique_checks):
        errors = {}
        for model_class, unique_check in unique_checks:
            lookup_kwargs = {}
            for field_name in unique_check:
                f = self._meta.get_field(field_name)
                lookup_value = getattr(self, f.attname)
                if lookup_value is None:
                    continue
                if f.primary_key and not self._state.adding:
                    continue

                ##########################################################################
                # This is a modification to Django's native implementation of this method;
                # we conditionally build a __in lookup if the value is an iterable.
                lookup = str(field_name)
                if isinstance(lookup_value, (list, set, tuple)):
                    lookup = "%s__overlap" % lookup

                lookup_kwargs[lookup] = lookup_value
                ##########################################################################
                # / end of changes

            if len(unique_check) != len(lookup_kwargs):
                continue

            #######################################################
            # Deal with long __in lookups by doing multiple queries in that case
            # This is a bit hacky, but we really have no choice due to App Engine's
            # 30 multi-query limit. This also means we can't support multiple list fields in
            # a unique combination
            #######################################################

            if len([x for x in lookup_kwargs if x.endswith("__in")]) > 1:
                raise NotSupportedError("You cannot currently have two list fields in a unique combination")

            # Split IN queries into multiple lookups if they are too long
            lookups = []
            for k, v in iteritems(lookup_kwargs):
                if (k.endswith("__in") or k.endswith("__overlap")) and len(v) > MAX_ALLOWABLE_QUERIES:
                    v = list(v)
                    while v:
                        new_lookup = lookup_kwargs.copy()
                        new_lookup[k] = v[:30]
                        v = v[30:]
                        lookups.append(new_lookup)
                    break
            else:
                # Otherwise just use the one lookup
                lookups = [lookup_kwargs]

            for lookup_kwargs in lookups:
                qs = model_class._default_manager.filter(**lookup_kwargs).values_list("pk", flat=True)
                model_class_pk = self._get_pk_val(model_class._meta)
                result = list(qs)

                if not self._state.adding and model_class_pk is not None:
                    # If we are saving an instance, we ignore it's PK in the result
                    try:
                        result.remove(model_class_pk)
                    except ValueError:
                        pass

                if result:
                    if len(unique_check) == 1:
                        key = unique_check[0]
                    else:
                        key = NON_FIELD_ERRORS
                    errors.setdefault(key, []).append(self.unique_error_message(model_class, unique_check))
                    break
        return errors
