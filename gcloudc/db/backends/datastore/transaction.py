

import copy
import threading

from django.db import connections
from gcloudc import context_decorator

from . import (
    caching,
    rpc,
)


def in_atomic_block(using='default'):
    connection = connections[using]
    return bool(connection.client.current_transaction())


def _datastore_get_handler(signal, sender, keys, **kwargs):
    txn = current_transaction()
    if txn:
        txn._seen_keys.update(set(keys))


rpc.datastore_get.connect(_datastore_get_handler, dispatch_uid='_datastore_get_handler')


class Transaction(object):
    def __init__(self, transaction, connection):
        self._transaction = transaction
        self._connection = connection
        self._previous_connection = None
        self._seen_keys = set()

    def enter(self):
        self._seen_keys = set()
        self._enter()

    def exit(self):
        self._exit()
        self._seen_keys = set()

    def _enter(self):
        raise NotImplementedError()

    def _exit(self):
        raise NotImplementedError()

    def has_already_been_read(self, instance):
        if instance.pk is None:
            return False

        key = rpc.Key.from_path(
            instance._meta.db_table,
            instance.pk,
            namespace=self._connection.settings_dict.get('NAMESPACE', '')
        )

        return key in self._seen_keys

    def refresh_if_unread(self, instance):
        """
            Calls instance.refresh_from_db() if the instance hasn't already
            been read this transaction. This helps prevent oddities if you
            call nested transactional functions. e.g.

            @atomic()
            def my_method(self):
                self.refresh_from_db()   # << Refresh state from the start of the transaction
                self.update()
                self.save()

            with atomic():
                instance = MyModel.objects.get(pk=1)
                instance.other_update()
                instance.my_method()  # << Oops! Undid work!
                instance.save()

            Instead, this will fix it

            def my_method(self):
                with atomic() as txn:
                    txn.refresh_if_unread(self)
                    self.update()
                    self.save()
        """

        if self.has_already_been_read(instance):
            # If the instance has already been read this transaction,
            # then don't refresh it again.
            return
        else:
            instance.refresh_from_db()

    def _commit(self):
        if self._transaction:
            return self._transaction.commit()

    def _rollback(self):
        if self._transaction:
            self._transaction.rollback()


class IndependentTransaction(Transaction):
    def __init__(self, options):
        self._options = options
        super(IndependentTransaction, self).__init__(None)

    def _enter(self):
        if IsInTransaction():
            self._previous_connection = _GetConnection()
            assert(isinstance(self._previous_connection, TransactionalConnection))

            _PopConnection()

        self._connection = _GetConnection().new_transaction(self._options)
        _PushConnection(self._connection)

    def _exit(self):
        _PopConnection()
        if self._previous_connection:
            _PushConnection(self._previous_connection)


class NestedTransaction(Transaction):
    def _enter(self):
        pass

    def _exit(self):
        pass


class NormalTransaction(Transaction):
    def __init__(self, options):
        self._options = options
        connection = _GetConnection().new_transaction(options)
        super(NormalTransaction, self).__init__(connection)

    def _enter(self):
        _PushConnection(self._connection)

    def _exit(self):
        _PopConnection()


class NoTransaction(Transaction):
    def _enter(self):
        if IsInTransaction():
            self._previous_connection = _GetConnection()
            _PopConnection()

    def _exit(self):
        if self._previous_connection:
            _PushConnection(self._previous_connection)


_STORAGE = threading.local()


def current_transaction(using='default'):
    """
        Returns the current 'Transaction' object (which may be a NoTransaction). This is useful
        when atomic() is used as a decorator rather than a context manager. e.g.

        @atomic()
        def my_function(apple):
            current_transaction().refresh_if_unread(apple)
            apple.thing = 1
            apple.save()
    """

    _init_storage()

    active_transaction = None

    # Return the last Transaction object with a connection
    for txn in reversed(_STORAGE.transaction_stack):
        if isinstance(txn, IndependentTransaction):
            active_transaction = txn
            break
        elif isinstance(txn, NormalTransaction):
            active_transaction = txn
            # Keep searching... there may be an independent or further transaction
        elif isinstance(txn, NoTransaction):
            # Bail immediately for non_atomic blocks. There is no transaction there.
            active_transaction = None
            break

    return active_transaction


def _init_storage():
    if not hasattr(_STORAGE, "transaction_stack"):
        _STORAGE.transaction_stack = []


class TransactionFailedError(Exception):
    pass


class AtomicDecorator(context_decorator.ContextDecorator):
    VALID_ARGUMENTS = ("independent", "mandatory", "using", "read_only")

    @classmethod
    def _do_enter(cls, state, decorator_args):
        _init_storage()

        mandatory = decorator_args.get("mandatory", False)
        independent = decorator_args.get("independent", False)
        read_only = decorator_args.get("read_only", False)
        using = decorator_args.get("using", "default")

        options = CreateTransactionOptions(
            xg=xg,
            propagation=TransactionOptions.INDEPENDENT if independent else None
        )

        new_transaction = None

        if independent:
            new_transaction = IndependentTransaction(options)
        elif in_atomic_block():
            new_transaction = NestedTransaction(None)
        elif mandatory:
            raise TransactionFailedError(
                "You've specified that an outer transaction is mandatory, but one doesn't exist"
            )
        else:
            new_transaction = NormalTransaction(options)

        _STORAGE.transaction_stack.append(new_transaction)
        _STORAGE.transaction_stack[-1].enter()

        if isinstance(new_transaction, (IndependentTransaction, NormalTransaction)):
            caching.get_context().stack.push()

        # We may have created a new transaction, we may not. current_transaction() returns
        # the actual active transaction (highest NormalTransaction or lowest IndependentTransaction)
        # or None if we're in a non_atomic, or there are no transactions
        return current_transaction()

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        _init_storage()
        context = caching.get_context()

        transaction = _STORAGE.transaction_stack.pop()

        try:
            if transaction._connection:
                if exception:
                    transaction._connection.rollback()
                else:
                    if not transaction._connection.commit():
                        raise TransactionFailedError()
        finally:
            if isinstance(transaction, (IndependentTransaction, NormalTransaction)):
                # Clear the context cache at the end of a transaction
                if exception:
                    context.stack.pop(discard=True)
                else:
                    context.stack.pop(apply_staged=True, clear_staged=True)

            transaction.exit()
            transaction._connection = None


atomic = AtomicDecorator
commit_on_success = AtomicDecorator  # Alias to the old Django name for this kinda thing
