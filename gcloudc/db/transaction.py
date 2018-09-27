

import copy
import functools
import threading

from django.db import router, connections


def in_atomic_block(using='default'):
    # At the moment just a wrapper around App Engine so that
    # users don't have to use two different APIs
    return IsInTransaction()


def _datastore_get_handler(signal, sender, keys, **kwargs):
    txn = current_transaction()
    if txn:
        txn._seen_keys.update(set(keys))


# FIXME: Fix this when stuff becomes per-connection
# rpc.datastore_get.connect(_datastore_get_handler, dispatch_uid='_datastore_get_handler')

class Transaction(object):
    def __init__(self, connection):
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

    def has_already_been_read(self, instance, connection=None):
        if instance.pk is None:
            return False

        if not connection:
            connection = router.db_for_read(instance.__class__, instance=instance)
            connection = connections[connection]

        key = rpc.Key.from_path(
            instance._meta.db_table,
            instance.pk,
            namespace=connection.settings_dict.get('NAMESPACE', '')
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
        if self._connection:
            return self._connection.commit()

    def _rollback(self):
        if self._connection:
            self._connection.rollback()


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


# Because decorators are only instantiated once per function, we need to make sure any state
# stored on them is both thread-local (to prevent function calls in different threads
# interacting with each other) and safe to use recursively (by using a stack of state)

class ContextState(object):
    "Stores state per-call of the ContextDecorator"
    pass


class ContextDecorator(object):
    """
        A thread-safe ContextDecorator. Subclasses should implement classmethods
        called _do_enter(state, decorator_args) and _do_exit(state, decorator_args, exception)

        state is a thread.local which can store state for each enter/exit. Decorator args holds
        any arguments passed into the decorator or context manager when called.
    """
    VALID_ARGUMENTS = ()

    def __init__(self, func=None, **kwargs):
        # Func will be passed in if this has been called without parenthesis
        # as a @decorator

        # Make sure only valid decorator arguments were passed in
        if len(kwargs) > len(self.__class__.VALID_ARGUMENTS):
            raise ValueError("Unexpected decorator arguments: {}".format(
                set(kwargs.keys()) - set(self.__class__.VALID_ARGUMENTS))
            )

        self.func = func
        self.decorator_args = {x: kwargs.get(x) for x in self.__class__.VALID_ARGUMENTS}
        # Add thread local state for variables that change per-call rather than
        # per insantiation of the decorator
        self.state = threading.local()
        self.state.stack = []

    def __get__(self, obj, objtype=None):
        """ Implement descriptor protocol to support instance methods. """
        # Invoked whenever this is accessed as an attribute of *another* object
        # - as it is when wrapping an instance method: `instance.method` will be
        # the ContextDecorator, so this is called.
        # We make sure __call__ is passed the `instance`, which it will pass onto
        # `self.func()`
        return functools.partial(self.__call__, obj)

    def __call__(self, *args, **kwargs):
        # Called if this has been used as a decorator not as a context manager

        def decorated(*_args, **_kwargs):
            decorator_args = self.decorator_args.copy()
            exception = False
            self.__class__._do_enter(self._push_state(), decorator_args)
            try:
                return self.func(*_args, **_kwargs)
            except Exception:
                exception = True
                raise
            finally:
                self.__class__._do_exit(self._pop_state(), decorator_args, exception)

        if not self.func:
            # We were instantiated with args
            self.func = args[0]
            return decorated
        else:
            return decorated(*args, **kwargs)

    def _push_state(self):
        "We need a stack for state in case a decorator is called recursively"
        # self.state is a threading.local() object, so if the current thread is not the one in
        # which ContextDecorator.__init__ was called (e.g. is not the thread in which the function
        # was decorated), then the 'stack' attribute may not exist
        if not hasattr(self.state, 'stack'):
            self.state.stack = []

        self.state.stack.append(ContextState())
        return self.state.stack[-1]

    def _pop_state(self):
        return self.state.stack.pop()

    def __enter__(self):
        return self.__class__._do_enter(self._push_state(), self.decorator_args.copy())

    def __exit__(self, exc_type, exc_value, traceback):
        self.__class__._do_exit(self._pop_state(), self.decorator_args.copy(), exc_type)


class TransactionFailedError(Exception):
    pass


class AtomicDecorator(ContextDecorator):
    VALID_ARGUMENTS = ("xg", "independent", "mandatory", "using")

    @classmethod
    def _do_enter(cls, state, decorator_args):
        _init_storage()

        mandatory = decorator_args.get("mandatory", False)
        independent = decorator_args.get("independent", False)
        xg = decorator_args.get("xg", False)
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


class NonAtomicDecorator(ContextDecorator):
    VALID_ARGUMENTS = ("using",)

    @classmethod
    def _do_enter(cls, state, decorator_args):
        _init_storage()

        using = decorator_args.get("using", "default")

        context = caching.get_context()

        new_transaction = NoTransaction(None)
        _STORAGE.transaction_stack.append(new_transaction)
        _STORAGE.transaction_stack[-1]._enter()

        # Store the current state of the stack (aside from the first entry)
        state.original_stack = copy.deepcopy(context.stack.stack[1:])

        # Unwind the in-context stack leaving just the first entry
        while len(context.stack.stack) > 1:
            context.stack.pop(discard=True)

    @classmethod
    def _do_exit(cls, state, decorator_args, exception):
        context = caching.get_context()
        transaction = _STORAGE.transaction_stack.pop()
        transaction._exit()

        # Restore the context stack as it was
        context.stack.stack = context.stack.stack + state.original_stack


non_atomic = NonAtomicDecorator
