from functools import wraps
from typing import Callable, Any, Iterable
from inspect import iscoroutinefunction

from anyio import to_thread, Semaphore


def awrap(func: Callable):
    if iscoroutinefunction(func):
        raise TypeError(f'Cannot wrap {func} in async function, already async.')
    @wraps(func)
    async def wrapped(*args, **kwargs):
        return await to_thread.run_sync(func, *args, **kwargs)
    return wrapped


def forward_func(func: Callable):
    if iscoroutinefunction(func):
        raise TypeError(f'Cannot synchronously forward {func}, it is async.')
    @wraps(func)
    def wrapped(self: Forwarder, *args, **kwargs):
        return func(self.object, *args, **kwargs)
    return wrapped

def forward_property(prop: property):
    if not isinstance(prop, property):
        raise TypeError(f'Cannot forward {prop}, it is not a property.')
    def getter(self: Forwarder):
        prop.fget(self.object)
    if prop.fset:
        def setter(self: Forwarder, value):
            prop.fset(self.object, value)
    else:
        setter = None
    return property(getter, setter, prop.fdel, prop.__doc__)


def forward(func_or_prop: Callable | property):
    if isinstance(func_or_prop, property):
        return forward_property(func_or_prop)
    else:
        return forward_func(func_or_prop)


def aforward(func: Callable):
    if iscoroutinefunction(func):
        raise TypeError(f'Cannot asynchronously forward {func}, already async.')
    @wraps(func)
    async def wrapped(self: Forwarder, *args, **kwargs):
        return await to_thread.run_sync(func, self.object, *args, **kwargs)
    return wrapped


def locked(func: Callable):
    if not iscoroutinefunction(func):
        raise TypeError(f'Cannot wrap {func} in a lock, it is not an async function.')
    @wraps(func)
    async def wrapped(self: Forwarder, *args, **kwargs):
        async with self.lock:
            return await func(self, *args, **kwargs)
    return wrapped


def wrap_return(wrap_class: 'Forwarder'):
    def inner(func: Callable):
        if not iscoroutinefunction(func):
            raise TypeError(f'Cannot wrap {func} return value in a {wrap_class}, it is not an async function.')
        @wraps(func)
        async def wrapped(self: Forwarder, *args, **kwargs):
            value = await func(self, *args, **kwargs)
            return wrap_class(value, self.lock)
        return wrapped
    return inner


def unwrap_params(func: Callable):
    if not iscoroutinefunction(func):
        raise TypeError(f'Cannot wrap {func} to handle params, it is not an async function.')
    @wraps(func)
    async def wrapped(self: Forwarder, sql: str, parameters: Iterable[Any] | None = None):
        parameters = parameters if parameters is not None else []
        return await func(self, sql, parameters)
    return wrapped


class Forwarder:
    def __init__(self, object: Any, lock: Semaphore):
        self._object = object
        self._lock = lock

    @property
    def object(self) -> Any:
        return self._object

    @property
    def lock(self) -> Semaphore:
        return self._lock
