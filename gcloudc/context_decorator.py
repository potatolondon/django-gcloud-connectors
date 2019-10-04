import threading
import functools


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

    VALID_ARGUMENTS = tuple()

    def __init__(self, func=None, **kwargs):
        # Func will be passed in if this has been called without parenthesis
        # as a @decorator

        # Make sure only valid decorator arguments were passed in
        if len(kwargs) > len(self.__class__.VALID_ARGUMENTS):
            raise ValueError(
                "Unexpected decorator arguments: {}".format(set(kwargs.keys()) - set(self.__class__.VALID_ARGUMENTS))
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
        if not hasattr(self.state, "stack"):
            self.state.stack = []

        self.state.stack.append(ContextState())
        return self.state.stack[-1]

    def _pop_state(self):
        return self.state.stack.pop()

    def __enter__(self):
        return self.__class__._do_enter(self._push_state(), self.decorator_args.copy())

    def __exit__(self, exc_type, exc_value, traceback):
        self.__class__._do_exit(self._pop_state(), self.decorator_args.copy(), exc_type)
