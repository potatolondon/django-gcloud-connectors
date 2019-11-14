import copy
import sys

from django.conf import settings

from google.cloud.datastore.key import Key


def key_or_entity_compare(lhs, rhs):
    """
        Returns true if lhs is an entity with a key that matches rhs or
        if lhs is a key which matches rhs, or if lhs is a key which maches
        rhs.key()
    """
    if hasattr(rhs, "key"):
        rhs = rhs.key

    if hasattr(lhs, "key"):
        lhs = lhs.key

    return lhs == rhs


# 8M default cache size. Fairly arbitrary but the lowest instance class (F1) has 128M
# of ram, and can serve 8 Python requests at the same time which gives us 16M per request
# so we use 50% of that by default.
DEFAULT_MAX_CACHE_DICT_SIZE = 1024 * 1024 * 8
MAX_CACHE_DICT_SETTING_NAME = "DJANGAE_CACHE_MAX_CONTEXT_SIZE"


class CacheDict(object):
    """
        This is a special dictionary-like object which does the following:

        1. Copies items in and out to prevent storing references
        2. The cache dict is restricted to a maximum size in bytes
        3. Unaccessed entries are removed first

        The priority of eviction is based on the *value* and not the *keys*. If multiple
        keys point to the same object reference then an access to any of them will mark the
        value as used, if a value is evicted, all the keys pointing to it are removed.
    """

    def __init__(self, max_size_in_bytes=None):
        max_size_in_bytes = max_size_in_bytes or getattr(
            settings, MAX_CACHE_DICT_SETTING_NAME, DEFAULT_MAX_CACHE_DICT_SIZE
        )

        # This is a list of `id(value)` values in priority of most recently used
        # to least recently used
        self.value_priority = []

        # This is a reverse lookup dict of id(value): {key1, key2, ...}
        self.value_references = {}

        # The actual entries, values are normally entities but it makes it easy to
        # understand this class if you think of them as the references they are.
        # Multiple keys can map to the same entity reference
        self._entries = {}

        # THe total size of all values in bytes
        self.total_value_size = 0

        # The max size in bytes that the total values can become
        self.max_size_in_bytes = max_size_in_bytes

    def __deepcopy__(self, memo):
        new_one = CacheDict()
        new_one.update(self)
        return new_one

    def _set_value(self, k, v):
        """
            Sets a value in the _entries dictionary but manages the associated
            data in value_priority and value_references including when a key already
            exists with a different value
        """

        if k in self._entries:
            # We already have a value, we need to clean up
            old_value = self._entries[k]

            # Same object, do nothing
            if id(old_value) == id(v):
                return

            old_key = id(old_value)

            self.value_references[old_key].remove(k)
            del self._entries[k]

            if not self.value_references[old_key]:
                self._purge_value(old_value)

        priority_key = id(v)

        existing_value = priority_key in self.value_priority

        self.value_references.setdefault(priority_key, set()).add(k)
        if priority_key not in self.value_priority:
            self.value_priority.insert(len(self.value_priority) // 2, priority_key)

        self._entries[k] = v

        # If we added a new value to the dict, we increase the used size
        if not existing_value:
            self.total_value_size += sys.getsizeof(v)

    def _check_size_and_limit(self):
        """
            If the dict size is larger than the max specified bytes,
            we remove entities by deleting all their associated keys
        """
        while self.total_value_size > self.max_size_in_bytes:
            next_priority_key = self.value_priority[-1]

            # We intentionally copy the result with list() as this will be manipulated
            # in del self[reference]
            for reference in list(self.value_references[next_priority_key]):
                del self[reference]

    def _set(self, k, v):
        self._set_value(k, v)
        self._check_size_and_limit()

    def set_multi(self, keys, value):
        """
            This is the only public setting API because we don't want to
            duplicate values across keys unnecessarily but we *do* want to copy
            the value passed in by the user to protect against accidental manipulation
            of the cached value
        """

        value = copy.deepcopy(value)  # Copy once
        for k in set(keys):
            # Set the same value for multiple keys
            self._set(k, value)

    def __getitem__(self, k):
        v = self._entries[k]  # Find the entry

        # Move the value up the value priority (remove the id() and add it back at the front)
        priority_key = id(v)
        self.value_priority.remove(priority_key)
        self.value_priority.insert(0, priority_key)
        return copy.deepcopy(v)

    def _purge_value(self, v):
        priority_key = id(v)
        del self.value_references[priority_key]
        self.value_priority.remove(priority_key)
        self.total_value_size -= sys.getsizeof(v)

    def __delitem__(self, k):
        assert set([id(x) for x in self._entries.values()]) == set(self.value_priority)
        v = self._entries[k]
        priority_key = id(v)

        self.value_references[priority_key].remove(k)
        # Only remove from the priority (and adjust the size)
        # if the value no longer exists in the dictionary
        if not self.value_references[priority_key]:
            self._purge_value(v)

        del self._entries[k]

        assert set([id(x) for x in self._entries.values()]) == set(self.value_priority)

    def __repr__(self):
        return "{%s}" % ", ".join([":".join([repr(k), repr(v)]) for k, v in self.items()])

    def __eq__(self, rhs):
        unshared_items = set(self.items()).difference(set(rhs.items()))
        return len(unshared_items) == 0

    def __contains__(self, k):
        return k in self.keys()

    def update(self, other):
        """
            The code here might look weird, but it's the fastest way to do it.

            Go through the other values (remember, we work with reference values)
            and build a dictionary of the id(value): value. This deduplicates values
            which are accessible through multiple keys.

            Then, set_multi the value in self using other.value_references to get the
            list of keys without iterating `other`
        """
        # Find the unique values in the other dictionary
        to_update = {}
        for v in other.values():
            to_update[id(v)] = v

        # Go through, and call set multi (which will perform one copy per value)
        for k, v in to_update.items():
            keys = other.value_references[k]
            self.set_multi(keys, v)

    def __iter__(self):
        return iter(self.keys())

    def get(self, k, default=None):
        try:
            return self[k]
        except KeyError:
            return default

    def keys(self):
        return self._entries.keys()

    def values(self):
        return self._entries.values()

    def items(self):
        """
            Iterate the items, this will not update priorities
        """
        for k in self.keys():
            # Intentionally don't reorganize the key priority if we're iterating
            # that would be *slow* and unlikely to lead to what you want
            yield (k, copy.deepcopy(self._entries[k]))

    def get_reversed(self, value, compare_func=None):
        """
            Returns the keys for the specified value.

            If compare_func is specified then it will be called for
            each value in the cache dict with value until compare_func
            returns True
        """
        results = []
        for k, v in self._entries.items():
            if compare_func and compare_func(v, value):
                results.append(k)
            elif v == value:
                results.append(k)
        return results


class ContextCache(object):
    """ Object via which the stack of Context objects and the settings for the context caching are
        accessed. A separate instance of this should exist per thread.
    """

    def __init__(self):
        self.memcache_enabled = True
        self.context_enabled = True
        self.stack = ContextStack()

    def reset(self, keep_disabled_flags=False):
        from gcloudc.db.transaction import in_atomic_block

        if in_atomic_block():
            raise RuntimeError(
                "Clearing the context cache inside a transaction breaks everything, " "we can't let you do that"
            )

        self.stack = ContextStack()
        if not keep_disabled_flags:
            self.memcache_enabled = True
            self.context_enabled = True


class Context(object):
    def __init__(self, stack):
        self.cache = CacheDict()
        self._stack = stack

    def apply(self, other):
        self.cache.update(other.cache)

        # We have to delete things that don't exist in the other
        keys = list(self.cache)
        for k in keys:
            if k not in other.cache:
                del self.cache[k]

    def cache_entity(self, identifiers, entity, situation):
        assert hasattr(identifiers, "__iter__")

        self.cache.set_multi(identifiers, entity)

    def remove_entity(self, entity_or_key):
        if not isinstance(entity_or_key, Key):
            entity_or_key = entity_or_key.key()

        for identifier in self.cache.get_reversed(entity_or_key, compare_func=key_or_entity_compare):
            del self.cache[identifier]

    def get_entity(self, identifier):
        return self.cache.get(identifier)

    def get_entity_by_key(self, key):
        try:
            identifier = self.cache.get_reversed(key, compare_func=key_or_entity_compare)[0]
        except IndexError:
            return None
        return self.get_entity(identifier)


class ContextStack(object):
    """
        A stack of contexts. This is used to support in-context
        caches for multi level transactions.
    """

    def __init__(self):
        self.stack = [Context(self)]
        self.staged = []

    def push(self):
        self.stack.append(Context(self))  # Empty context

    def pop(self, apply_staged=False, clear_staged=False, discard=False):
        """
            apply_staged: pop normally takes the top of the stack and adds it to a FIFO
            queue. By passing apply_staged it will pop to the FIFO queue then apply the
            queue to the top of the stack.

            clear_staged: pop, and then wipe out any staged contexts.

            discard: Ignores the popped entry in the stack, it's just discarded

            The staged queue will be wiped out if the pop makes the size of the stack one,
            regardless of whether you pass clear_staged or not. This is for safety!
        """
        from . import caching

        if not discard:
            self.staged.insert(0, self.stack.pop())
        else:
            self.stack.pop()

        if apply_staged:
            while self.staged:
                to_apply = self.staged.pop()
                keys = [x.key for x in to_apply.cache.values()]
                if keys:
                    # This assumes that all keys are in the same namespace,
                    # which is almost definitely
                    # going to be the case, but it feels a bit dirty
                    namespace = keys[0].namespace
                    caching.remove_entities_from_cache_by_key(keys, namespace=namespace)

                self.top.apply(to_apply)

        if clear_staged or len(self.stack) == 1:
            self.staged = []

    @property
    def top(self):
        return self.stack[-1]

    @property
    def size(self):
        return len(self.stack)

    @property
    def staged_count(self):
        return len(self.staged)
