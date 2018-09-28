"""
    This is a wrapper around the individual backend transactions,
    switching on the connection type.
"""

from django.db import connections

from gcloudc.context_decorator import ContextDecorator
from gcloudc.db.backends.datastore import transaction as datastore_transaction
from gcloudc.db.backends.datastore.base import Connection as DatastoreConnection


class Atomic(ContextDecorator):
    # This should be the superset of any connector args (just Datastore for now)
    VALID_ARGUMENTS = datastore_transaction.AtomicDecorator

    @classmethod
    def _do_enter(cls, state, decorator_args):
        using = decorator_args.get("using", "default")
        conn = connections[using]

        if isinstance(conn, DatastoreConnection):
            state.decorator = datastore_transaction.AtomicDecorator
        else:
            raise ValueError()

        state.decorator._do_enter(state, decorator_args)

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        state.decorator._do_exit(state, decorator_args, exception)


atomic = Atomic


def in_atomic_block(using='default'):
    conn = connections[using]

    if isinstance(conn, DatastoreConnection):
        return datastore_transaction.in_atomic_block(using=using)
    else:
        raise ValueError()
