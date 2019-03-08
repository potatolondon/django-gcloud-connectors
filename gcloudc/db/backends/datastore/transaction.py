

import uuid
import threading

from django.db import connections
from gcloudc import context_decorator
from google.cloud import exceptions


def in_atomic_block(using='default'):
    connection = connections[using].connection
    return bool(connection.gclient.current_transaction)


class Transaction(object):
    def __init__(self, connection, datastore_transaction=None):
        self._connection = connection
        self._datastore_transaction = datastore_transaction

    def _generate_id(self):
        """
            The Datastore API won't generate keys automatically until a
            transaction commits, that's too late!

            This is a hack, it might be the only hack we can do :(

            Note. Even though the Datastore can handle negative IDs (it's a signed
            64 bit integer) the default allocate never does, and also, this breaks
            Django URL regexes etc. So like the allocator we just do 32 bit ones.
        """
        unsigned = uuid.uuid4().int & (1 << 32) - 1
        return unsigned

    def put(self, entity):
        putter = (
            self._datastore_transaction.put
            if self._datastore_transaction
            else self._connection.gclient.put
        )

        putter(entity)

        assert(entity.key)
        return entity.key

    def key(self, *args, **kwargs):
        return self._connection.gclient.key(*args, **kwargs)

    def query(self, *args, **kwargs):
        return self._connection.gclient.query(*args, **kwargs)

    def get(self, key_or_keys):
        # For some reason Datastore Transactions don't provide their
        # own get
        if hasattr(key_or_keys, "__iter__"):
            getter = self._connection.gclient.get_multi
        else:
            getter = self._connection.gclient.get
        return getter(key_or_keys)

    def delete(self, key_or_keys):
        if hasattr(key_or_keys, '__iter__'):
            deleter = (
                self._datastore_transaction.delete_multi
                if self._datastore_transaction
                else self._connection.gclient.delete_multi
            )
        else:
            deleter = (
                self._datastore_transaction.delete
                if self._datastore_transaction
                else self._connection.gclient.delete
            )

        return deleter(key_or_keys)

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

        if not self._datastore_transaction:
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
    def __init__(self, connection):
        txn = connection.gclient.transaction()
        super().__init__(connection, txn)

    def _enter(self):
        self._datastore_transaction.begin()

    def _exit(self):
        self._datastore_transaction = None


class NestedTransaction(Transaction):
    def _enter(self):
        pass

    def _exit(self):
        pass


class NormalTransaction(Transaction):
    def __init__(self, connection):
        txn = connection.gclient.transaction()
        super().__init__(connection, txn)

    def _enter(self):
        self._datastore_transaction.begin()

    def _exit(self):
        self._datastore_transaction = None


class NoTransaction(Transaction):
    def _enter(self):
        raise NotImplementedError()

    def _exit(self):
        pass


_STORAGE = threading.local()


def _rpc(using):
    """
        In the low-level connector code, we use this function
        to return a transaction to perform a Get/Put/Delete on
        this effectively returns the current_transaction or a new
        RootTransaction() which is basically no transaction at all.
    """

    class RootTransaction(Transaction):
        def _enter(self):
            pass

        def _exit(self):
            pass

    return (
        current_transaction(using) or
        RootTransaction(connections[using].connection)
    )


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
    """
    Exposes a decorator based API for transaction use. This in turn allows us
    to define the expected behaviour of each transaction via kwargs.

    For example passing `independent` creates a new transaction instance using
    the Datastore client under the hood. This is useful to workaround the
    limitations of 500 entity writes per transaction/batch.
    """
    VALID_ARGUMENTS = (
        "independent",
        "mandatory",
        "using",
        "read_only",
        "enable_cache",
    )

    @classmethod
    def _do_enter(cls, state, decorator_args):
        _init_storage()

        mandatory = decorator_args.get("mandatory", False)
        independent = decorator_args.get("independent", False)
        read_only = decorator_args.get("read_only", False)
        using = decorator_args.get("using", "default")

        mandatory = False if mandatory is None else mandatory
        independent = False if independent is None else independent
        read_only = False if read_only is None else read_only
        using = "default" if using is None else using

        # FIXME: Implement context caching for transactions
        enable_cache = decorator_args.get("enable_cache", True)

        new_transaction = None
        connection = connections[using].connection

        if independent:
            new_transaction = IndependentTransaction(connection)
        elif in_atomic_block():
            new_transaction = NestedTransaction()
        elif mandatory:
            raise TransactionFailedError(
                "You've specified that an outer transaction is mandatory, but one doesn't exist"
            )
        else:
            new_transaction = NormalTransaction(connection)

        _STORAGE.transaction_stack.append(new_transaction)
        _STORAGE.transaction_stack[-1].enter()

        # We may have created a new transaction, we may not. current_transaction() returns
        # the actual active transaction (highest NormalTransaction or lowest IndependentTransaction)
        # or None if we're in a non_atomic, or there are no transactions
        return current_transaction()

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        _init_storage()

        transaction = _STORAGE.transaction_stack.pop()

        try:
            if transaction._datastore_transaction:
                if exception:
                    transaction._datastore_transaction.rollback()
                else:
                    try:
                        transaction._datastore_transaction.commit()
                    except exceptions.GoogleAPIError:
                        raise TransactionFailedError()
        finally:
            transaction.exit()


atomic = AtomicDecorator
commit_on_success = AtomicDecorator  # Alias to the old Django name for this kinda thing
