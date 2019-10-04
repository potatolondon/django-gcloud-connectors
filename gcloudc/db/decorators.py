

from contextlib import ContextDecorator


class DisableCache(ContextDecorator):
    def __enter__(self):
        return self

    def __exit__(self):
        return False


disable_cache = DisableCache
