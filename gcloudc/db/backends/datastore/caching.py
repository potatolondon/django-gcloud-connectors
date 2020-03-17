import logging
import threading

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from . import utils
from .context import (
    ContextCache,
    key_or_entity_compare,
)
from .unique_utils import (
    _format_value_for_identifier,
    unique_identifiers_from_entity,
)

_local = threading.local()

logger = logging.getLogger("djangae")


def get_context():
    try:
        return _local.context
    except AttributeError:
        _local.context = ContextCache()
        return _local.context


CACHE_ENABLED = getattr(settings, "GCLOUDC_CACHE_ENABLED", True)

# The max number of entities in a resultset that will be cached.
# If a query returns more than this number then only the first ones
# will be cached
DEFAULT_MAX_ENTITY_COUNT = 8
MAX_CACHE_COUNT = getattr(settings, "GCLOUDC_CACHE_MAX_ENTITY_COUNT", DEFAULT_MAX_ENTITY_COUNT)


class CachingSituation:
    DATASTORE_GET = 0
    DATASTORE_PUT = 1
    DATASTORE_GET_PUT = 2  # When we are doing an update


def _apply_namespace(value_or_map, namespace):
    """ Add the given namespace to the given cache key(s). """
    if hasattr(value_or_map, "keys"):
        return {"{}:{}".format(namespace, k): v for k, v in value_or_map.items()}
    elif hasattr(value_or_map, "__iter__") and not isinstance(value_or_map, str):
        return ["{}:{}".format(namespace, x) for x in value_or_map]
    else:
        return "{}:{}".format(namespace, value_or_map)


def _strip_namespace(value_or_map):
    """ Remove the namespace part from the given cache key(s). """

    def _strip(value):
        return value.split(":", 1)[-1]

    if hasattr(value_or_map, "keys"):
        return {_strip(k): v for k, v in value_or_map.items()}
    elif hasattr(value_or_map, "__iter__"):
        return [_strip(x) for x in value_or_map]
    else:
        return _strip(value_or_map)


def _get_cache_key_and_model_from_datastore_key(key):
    from django.db.migrations.recorder import MigrationRecorder

    # The django migration model isn't registered with the app registry so this
    # is special cased here
    MODELS_WHICH_ARENT_REGISTERED_WITH_DJANGO = {
        MigrationRecorder.Migration._meta.db_table: MigrationRecorder.Migration
    }

    kind = key.kind()
    model = utils.get_model_from_db_table(kind)

    if not model:
        if kind in MODELS_WHICH_ARENT_REGISTERED_WITH_DJANGO:
            model = MODELS_WHICH_ARENT_REGISTERED_WITH_DJANGO[kind]
        else:
            # This should never happen.. if it does then we can edit get_model_from_db_table to pass
            # include_deferred=True/included_swapped=True to get_models, whichever makes it better
            raise ImproperlyConfigured(
                "Unable to locate model for db_table '{}' - are you missing an INSTALLED_APP?".format(key.kind())
            )

    # We build the cache key for the ID of the instance
    cache_key = "|".join(
        [key.kind(), "{}:{}".format(model._meta.pk.column, _format_value_for_identifier(key.id_or_name()))]
    )

    return (cache_key, model)


def add_entities_to_cache(model, entities, situation, namespace):
    from gcloudc.db.transaction import in_atomic_block

    if not CACHE_ENABLED:
        return None

    context = get_context()

    if not context.context_enabled:
        # Don't cache anything if caching is disabled
        return

    # Don't cache on Get if we are inside a transaction, even in the context
    # This is because transactions don't see the current state of the datastore
    # We can still cache in the context on Put()
    if situation == CachingSituation.DATASTORE_GET and in_atomic_block():
        return

    identifiers = [unique_identifiers_from_entity(model, entity) for entity in entities]

    for ent_identifiers, entity in zip(identifiers, entities):
        get_context().stack.top.cache_entity(_apply_namespace(ent_identifiers, namespace), entity, situation)


def remove_entities_from_cache_by_key(keys, namespace):
    """
        Given an iterable of datastore.Keys objects, remove the corresponding entities from cache
    """

    if not CACHE_ENABLED:
        return None

    context = get_context()

    for key in keys:
        identifiers = context.stack.top.cache.get_reversed(key, compare_func=key_or_entity_compare)

        for identifier in identifiers:
            if identifier in context.stack.top.cache:
                del context.stack.top.cache[identifier]


def get_from_cache_by_key(key):
    """
        Given a datastore.Key (which should already have the namespace applied to it), return an
        entity from the context cache
    """

    if not CACHE_ENABLED:
        return None

    context = get_context()
    ret = None
    if context.context_enabled:
        # It's safe to hit the context cache, because a new one was pushed on the stack at the start of the transaction
        ret = context.stack.top.get_entity_by_key(key)

    return ret


def get_from_cache(unique_identifier, namespace):
    """
        Return an entity from the context cache
    """
    context = get_context()

    if not CACHE_ENABLED:
        return None

    cache_key = _apply_namespace(unique_identifier, namespace)
    ret = None
    if context.context_enabled:
        # It's safe to hit the context cache, because a new one was pushed on the stack at the start of the transaction
        ret = context.stack.top.get_entity(cache_key)

    return ret


def reset_context(keep_disabled_flags=False, *args, **kwargs):
    """
        Called at the beginning and end of each request, resets the thread local
        context. If you pass keep_disabled_flags=True the context_enabled
        flags will be preserved, this is really only useful for testing.
    """

    context = get_context()
    context.reset(keep_disabled_flags=keep_disabled_flags)
