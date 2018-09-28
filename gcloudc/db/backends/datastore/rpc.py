
import django.dispatch
from django.db import connections
from google.cloud.datastore.key import Key  # noqa
from google.cloud.datastore.query import Query  # noqa

# This signal exists mainly so the atomic decorator can find out what's happened
datastore_get = django.dispatch.Signal(providing_args=["keys", "connection"])


def Get(keys, using='default', **kwargs):
    conn = connections[using]
    datastore_get.send(sender=Get, keys=keys if isinstance(keys, (list, tuple)) else [keys], connection=conn)
    return conn.get(keys, **kwargs)


def Put(using='default', *args, **kwargs):
    conn = connections[using]
    return conn.put(*args, **kwargs)


def PutAsync(using='default', *args, **kwargs):
    conn = connections[using]
    return conn.put_async(*args, **kwargs)


def Delete(using='default', *args, **kwargs):
    conn = connections[using]
    return conn.delete(*args, **kwargs)


def DeleteAsync(using='default', *args, **kwargs):
    conn = connections[using]
    return conn.delete_async(*args, **kwargs)
