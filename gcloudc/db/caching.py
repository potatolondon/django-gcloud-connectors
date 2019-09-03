import functools
from gcloudc.db.backends.datastore import caching


class DisableCache(object):
    """
        Decorator and context manager for disabling the caches,
        passing no args disables everything, but you can pass
        memcache=False or context=False to only disable one or
        the other.
    """

    def __init__(self, context=True, memcache=True):
        self.func = None

        self.memcache = memcache
        self.context = context

    def __call__(self, *args, **kwargs):
        def call_func(*_args, **_kwargs):
            try:
                self.__enter__()
                return self.func(*_args, **_kwargs)
            finally:
                self.__exit__()

        if not self.func:
            assert args and callable(args[0])
            self.func = args[0]
            return functools.wraps(self.func)(call_func)

        if self.func:
            return call_func(*args, **kwargs)

    def __enter__(self):
        ctx = caching.get_context()

        self.orig_memcache = ctx.memcache_enabled
        self.orig_context = ctx.context_enabled

        ctx.memcache_enabled = not self.memcache
        ctx.context_enabled = not self.context

    def __exit__(self, *args, **kwargs):
        ctx = caching.get_context()

        ctx.memcache_enabled = self.orig_memcache
        ctx.context_enabled = self.orig_context

disable_cache = DisableCache
