import sqlite3
from functools import wraps

from .wrappers import (
    Forwarder,
    aforward,
    forward,
    locked,
    unwrap_params,
)


class Cursor(Forwarder):
    # Wrapped/forwarded methods
    execute = unwrap_params(locked(aforward(sqlite3.Cursor.execute)))
    executemany = unwrap_params(locked(aforward(sqlite3.Cursor.executemany)))
    executescript = locked(aforward(sqlite3.Cursor.executescript))
    fetchone = aforward(sqlite3.Cursor.fetchone)
    fetchmany = aforward(sqlite3.Cursor.fetchmany)
    fetchall = aforward(sqlite3.Cursor.fetchall)

    @wraps(sqlite3.Cursor.setinputsizes)
    def setinputsizes(self, sizes):
        pass

    @wraps(sqlite3.Cursor.setoutputsize)
    def setoutputsize(self, size, column=None):
        pass

    row_count = forward(sqlite3.Cursor.rowcount)
    lastrowid = forward(sqlite3.Cursor.lastrowid)
    arraysize = forward(sqlite3.Cursor.arraysize)
    description = forward(sqlite3.Cursor.description)

    @property
    def connection(self) -> sqlite3.Connection:
        return self.cursor.connection

    close = aforward(sqlite3.Cursor.close)

    # New methods
    @property
    def cursor(self) -> sqlite3.Cursor:
        return self.object

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
