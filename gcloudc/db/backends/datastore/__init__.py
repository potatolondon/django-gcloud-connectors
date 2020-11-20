from django.core import signals
from .caching import reset_context

POLYMODEL_CLASS_ATTRIBUTE = "class"


def reset_context_cache(**kwargs):
    reset_context()


# We connect to request_started in case something stopped request_finished
# being fired. We connect to request_finished to minimize the time (and risk)
# that something is left in memory
signals.request_started.connect(reset_context_cache)
signals.request_finished.connect(reset_context_cache)
