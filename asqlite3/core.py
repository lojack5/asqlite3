from functools import wraps, partial
import sqlite3

from anyio import to_thread

from .connection import Connection


@wraps(sqlite3.connect)
async def connect(*args, **kwargs) -> Connection:
    kwargs['check_same_thread'] = False
    fn = partial(sqlite3.connect, *args, **kwargs)
    connection = await to_thread.run_sync(fn)
    return Connection(connection)
