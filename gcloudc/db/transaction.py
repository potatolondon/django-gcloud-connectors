"""
    This is a wrapper around the individual backend transactions,
    switching on the connection type.
"""

from django.db import (
    DEFAULT_DB_ALIAS,
    connections,
)
from django.db.transaction import TransactionManagementError
from gcloudc.context_decorator import ContextDecorator
from gcloudc.db.backends.datastore import transaction as datastore_transaction


TransactionManagementError = TransactionManagementError


class Atomic(ContextDecorator):
    # This should be the superset of any connector args (just Datastore for now)
    VALID_ARGUMENTS = datastore_transaction.AtomicDecorator.VALID_ARGUMENTS[:]

    @classmethod
    def _do_enter(cls, state, decorator_args):
        using = decorator_args.get("using", "default") or "default"

        try:
            connections[using]
            state.decorator = datastore_transaction.AtomicDecorator
        except (KeyError, TypeError):
            raise ValueError("Unable to get connection for %s" % using)

        return state.decorator._do_enter(state, decorator_args)

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        state.decorator._do_exit(state, decorator_args, exception)


atomic = Atomic


class NonAtomic(ContextDecorator):

    @classmethod
    def _do_enter(cls, state, decorator_args):
        using = decorator_args.get("using", "default") or "default"

        try:
            connections[using]
            state.decorator = datastore_transaction.NonAtomicDecorator
        except (KeyError, TypeError):
            raise ValueError("Unable to get connection for %s" % using)

        return state.decorator._do_enter(state, decorator_args)

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        state.decorator._do_exit(state, decorator_args, exception)


non_atomic = NonAtomic


def in_atomic_block(using="default"):
    try:
        connections[using]
        return datastore_transaction.in_atomic_block(using=using)
    except (KeyError, TypeError):
        raise ValueError("Unable to find connection with alias: %s" % using)


def get_connection(using=None):
    """
    Get a database connection by name, or the default database connection
    if no name is provided. This is a private API.
    """
    if using is None:
        using = DEFAULT_DB_ALIAS
    return connections[using]


def on_commit(func, using=None):
    """
    Register `func` to be called when the current transaction is committed.
    If the current transaction is rolled back, `func` will not be called.
    """
    get_connection(using).on_commit(func)
