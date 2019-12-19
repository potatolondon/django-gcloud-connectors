

from gcloudc.db.backends.datastore import caching
from contextlib import ContextDecorator
import logging


class DisableCache(ContextDecorator):
    def __enter__(self):
        self.context = caching.get_context()
        self.context.context_enabled = False
        logging.warn("disable_cache NOT IMPLEMENTED YET")
        return self

    def __exit__(self, *args, **kwargs):
        self.context.context_enabled = True
        return False


disable_cache = DisableCache
