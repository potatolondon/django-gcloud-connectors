import copy
import threading
import uuid

from google.cloud import exceptions
from google.cloud.datastore.transaction import \
    Transaction as DatastoreTransaction

from django.db import connections
from gcloudc import context_decorator
from gcloudc.db.backends.datastore import caching

TRANSACTION_ENTITY_LIMIT = 500


def in_atomic_block(using="default"):
    txn = current_transaction()
    if not txn:
        return False

    datastore_transaction = getattr(txn, "_datastore_transaction")
    if not datastore_transaction:
        return False

    # Returns True if this is a Datastore transaction, and not a normal "Batch"
    return isinstance(datastore_transaction, DatastoreTransaction)


class Transaction(object):
    def __init__(self, connection, datastore_transaction=None):
        self._connection = connection
        self._datastore_transaction = datastore_transaction
        self._seen_keys = set()

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

    def key(self, *args, **kwargs):
        """
        Interface to the Datastore client Key factory.
        """
        assert("namespace" not in kwargs)
        namespace = self._connection.namespace
        kwargs["namespace"] = namespace

        parent = kwargs.get("parent")
        if parent:
            assert(parent.namespace == namespace)

        return self._connection.gclient.key(*args, **kwargs)

    def get(self, key_or_keys, missing=None):
        # For some reason Datastore Transactions don't provide their
        # own get
        if hasattr(key_or_keys, "__iter__") and not isinstance(key_or_keys, str):
            getter = self._connection.gclient.get_multi
            ret = getter(key_or_keys, missing=missing)
            if ret:
                [self._seen_keys.add(x.key) for x in ret]
                return ret
        else:
            ret = self._connection.gclient.get(key_or_keys)
            if ret:
                self._seen_keys.add(ret.key)

        return ret

    def put_multi(self, entities):
        for entity in entities:
            self.put(entity)

    def put(self, entity):
        putter = self._datastore_transaction.put if self._datastore_transaction else self._connection.gclient.put

        putter(entity)

        assert entity.key

        self._seen_keys.add(entity.key)

        return entity.key

    def delete(self, key_or_keys):
        """
        Delete an entity or entities using a different API depending if we
        are currently in a transaction batch or not.
        """
        # if we've got an iterable of keys....
        if hasattr(key_or_keys, "__iter__"):
            # there is no delete_multi on the transaction object directly
            if self._datastore_transaction:
                for key in key_or_keys:
                    self._datastore_transaction.delete(key)
            else:
                self._connection.gclient.delete_multi(key_or_keys)
        else:
            if self._datastore_transaction:
                self._datastore_transaction.delete(key_or_keys)
            else:
                # delete() is just a wrapper around delete_multi anyway...
                self._connection.gclient.delete_multi([key_or_keys])

    def query(self, *args, **kwargs):
        return self._connection.gclient.query(*args, **kwargs)

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

        key = self.key(instance._meta.db_table, instance.pk)
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

        self.owner = connection.ops.connection
        self.previous_on_commit = []

    def _enter(self):
        self.previous_on_commit = self.owner.run_on_commit
        self.owner.run_on_commit = []
        self._datastore_transaction.begin()

    def _exit(self):
        self._datastore_transaction = None
        self.owner.run_on_commit = self.previous_on_commit


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
        pass

    def _exit(self):
        self._datastore_transaction = None


_STORAGE = threading.local()


def _rpc(using):
    """
        In the low-level connector code, we use this function
        to return a transaction to perform a Get/Put/Delete on
        this effectively returns the current_transaction or a new
        RootTransaction() which is basically no transaction at all.
    """

    assert(isinstance(using, str))
    assert(using)

    class RootTransaction(Transaction):
        def _enter(self):
            pass

        def _exit(self):
            pass

    return current_transaction(using) or RootTransaction(connections[using].connection)


def current_transaction(using="default"):
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
    for txn in reversed(_STORAGE.transaction_stack.get(using, [])):
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
        _STORAGE.transaction_stack = {}


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

    VALID_ARGUMENTS = ("independent", "mandatory", "using", "read_only", "enable_cache")

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
        state.using = using = "default" if using is None else using

        enable_cache = decorator_args.get("enable_cache", True)
        context = caching.get_context()
        state.original_context_enabled = context.context_enabled

        # Only force-disable cache, do not force enable (e.g. if already disabled
        # in the context, leave as is)
        if enable_cache is False:
            context.context_enabled = enable_cache

        new_transaction = None

        connection = connections[using]

        # Connect if necessary (mainly in tests)
        if not connection.connection:
            connection.connect()

        connection = connection.connection
        assert(connection)

        if independent:
            new_transaction = IndependentTransaction(connection)
        elif in_atomic_block():
            new_transaction = NestedTransaction(connection)
        elif mandatory:
            raise TransactionFailedError(
                "You've specified that an outer transaction is mandatory, but one doesn't exist"
            )
        else:
            new_transaction = NormalTransaction(connection)

        _STORAGE.transaction_stack.setdefault(using, []).append(new_transaction)
        _STORAGE.transaction_stack[using][-1].enter()

        if isinstance(new_transaction, (IndependentTransaction, NormalTransaction)):
            caching.get_context().stack.push()

        # We may have created a new transaction, we may not. current_transaction() returns
        # the actual active transaction (highest NormalTransaction or lowest IndependentTransaction)
        # or None if we're in a non_atomic, or there are no transactions
        return current_transaction()

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        _init_storage()

        connection = connections[state.using]
        transaction = _STORAGE.transaction_stack[state.using].pop()

        try:
            if transaction._datastore_transaction:
                if exception or connection.needs_rollback:
                    transaction._datastore_transaction.rollback()
                else:
                    try:
                        transaction._datastore_transaction.commit()

                        if isinstance(transaction, (IndependentTransaction, NormalTransaction)):
                            with non_atomic(using=state.using):
                                # Run Django commit hooks (if any)
                                connection.run_and_clear_commit_hooks()

                    except exceptions.GoogleCloudError:
                        raise TransactionFailedError()
        finally:
            if isinstance(transaction, (IndependentTransaction, NormalTransaction)):
                context = caching.get_context()
                # Clear the context cache at the end of a transaction
                if exception:
                    context.stack.pop(discard=True)
                else:
                    context.stack.pop(apply_staged=True, clear_staged=True)

                context.context_enabled = state.original_context_enabled

            if exception:
                connection.run_on_commit = []

            transaction.exit()


atomic = AtomicDecorator
commit_on_success = AtomicDecorator  # Alias to the old Django name for this kinda thing


class NonAtomicDecorator(AtomicDecorator):
    VALID_ARGUMENTS = ("using",)

    @classmethod
    def _do_enter(cls, state, decorator_args):
        _init_storage()

        context = caching.get_context()

        state.using = using = decorator_args.get("using", "default")
        connection = connections[using]

        # Connect if necessary (mainly in tests)
        if not connection.connection:
            connection.connect()

        connection = connection.connection
        assert(connection)

        # For non_atomic blocks we pass a Batch as the transaction
        new_transaction = NoTransaction(connection.gclient.batch())

        _STORAGE.transaction_stack.setdefault(using, []).append(new_transaction)
        _STORAGE.transaction_stack[using][-1].enter()

        # Store the current state of the stack (aside from the first entry)
        state.original_stack = copy.deepcopy(context.stack.stack[1:])

        # Unwind the in-context stack leaving just the first entry
        while len(context.stack.stack) > 1:
            context.stack.pop(discard=True)

        return current_transaction()

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        _init_storage()

        context = caching.get_context()
        transaction = _STORAGE.transaction_stack[state.using].pop()

        try:
            assert(not transaction._datastore_transaction)
        finally:
            # Restore the context stack as it was
            context.stack.stack = context.stack.stack + state.original_stack
            transaction.exit()


non_atomic = NonAtomicDecorator
