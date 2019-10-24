

from contextlib import ContextDecorator
import logging


class DisableCache(ContextDecorator):
    def __enter__(self):
        logging.warn("disable_cache NOT IMPLEMENTED YET")
        return self

    def __exit__(self, *args, **kwargs):
        return False


disable_cache = DisableCache
