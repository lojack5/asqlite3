import sqlite3
from functools import wraps
from typing import Union

from anyio import Semaphore, to_thread

from .wrappers import (
    Forwarder,
    forward,
    aforward,
    wrap_return,
    unwrap_params,
    locked,
)
from .cursor import Cursor


class Connection(Forwarder):
    def __init__(
        self,
        connection: sqlite3.Connection,
    ):
        super().__init__(connection, Semaphore(1))

    @property
    def connection(self) -> sqlite3.Connection:
        return self.object

    isolation_level = forward(sqlite3.Connection.isolation_level)
    in_transaction = forward(sqlite3.Connection.in_transaction)
    commit = locked(aforward(sqlite3.Connection.commit))
    rollback = locked(aforward(sqlite3.Connection.rollback))
    execute = wrap_return(Cursor)(unwrap_params(locked(aforward(sqlite3.Connection.execute))))
    executemany = wrap_return(Cursor)(unwrap_params(locked(aforward(sqlite3.Connection.executemany))))
    executescript = wrap_return(Cursor)(locked(aforward(sqlite3.Connection.executescript)))
    close = aforward(sqlite3.Connection.close)
    create_function = aforward(sqlite3.Connection.create_function)
    create_aggregate = aforward(sqlite3.Connection.create_aggregate)
    create_collation = aforward(sqlite3.Connection.create_collation)
    interrupt = forward(sqlite3.Connection.interrupt)
    set_progress_handler = forward(sqlite3.Connection.set_progress_handler)
    enable_load_extension = forward(sqlite3.Connection.enable_load_extension)
    load_extension = forward(sqlite3.Connection.load_extension)
    row_factory = forward(sqlite3.Connection.row_factory)
    text_factory = forward(sqlite3.Connection.text_factory)
    total_changes = forward(sqlite3.Connection.total_changes)

    @wraps(sqlite3.Connection.iterdump)
    async def iterdump(self):
        for line in self.connection.iterdump():
            yield line

    @wraps(sqlite3.Connection.backup)
    async def backup(self, target: Union[sqlite3.Connection, 'Connection'], **kwargs):
        if isinstance(target, Connection):
            target = target.connection
        return await to_thread.run_sync(self.connection.backup, target, **kwargs)

    # New methods
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    cursor = wrap_return(Cursor)(aforward(sqlite3.Connection.cursor))
