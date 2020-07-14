# -*- coding: utf-8 -*-

"""
The MIT License (MIT)

Copyright (c) 2019-2020 Rapptz

Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the "Software"),
to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""

import sqlite3
import threading
import queue
import asyncio

PARSE_DECLTYPES = sqlite3.PARSE_DECLTYPES
PARSE_COLNAMES = sqlite3.PARSE_COLNAMES

class _WorkerEntry:
    __slots__ = ('func', 'args', 'kwargs', 'future', 'cancelled')

    def __init__(self, func, args, kwargs, future):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.future = future

class _Worker(threading.Thread):
    def __init__(self, *, loop):
        super().__init__(name='asqlite-worker-thread', daemon=True)
        self.loop = loop
        self._worker_queue = queue.Queue()
        self._end = threading.Event()

    def _call_entry(self, entry):
        fut = entry.future
        if fut.cancelled():
            return

        try:
            result = entry.func(*entry.args, **entry.kwargs)
        except Exception as e:
            self.loop.call_soon_threadsafe(fut.set_exception, e)
        else:
            self.loop.call_soon_threadsafe(fut.set_result, result)

    def run(self):
        _queue = self._worker_queue
        while not self._end.is_set():
            try:
                entry = _queue.get(timeout=0.2)
            except queue.Empty:
                continue
            else:
                self._call_entry(entry)

    def post(self, func, *args, **kwargs):
        future = self.loop.create_future()
        entry = _WorkerEntry(func=func, args=args, kwargs=kwargs, future=future)
        self._worker_queue.put_nowait(entry)
        return future

    def stop(self):
        self._end.set()

class _ContextManagerMixin:
    def __init__(self, _queue, _factory, func, *args, timeout=None, **kwargs):
        self._worker = _queue
        self.func = func
        self.timeout = timeout
        self._factory = _factory
        self.args = args
        self.kwargs = kwargs
        self.__result = None

    async def _runner(self):
        future = self._worker.post(self.func, *self.args, **self.kwargs)
        if self.timeout is not None:
            ret = await asyncio.wait_for(future, timeout=self.timeout)
        else:
            ret = await future
        self.__result = result = self._factory(ret)
        return result

    def __await__(self):
        return self._runner().__await__()

    async def __aenter__(self):
        ret = await self._runner()
        try:
            return await ret.__aenter__()
        except AttributeError:
            return ret

    async def __aexit__(self, exc_type, exc, tb):
        if self.__result is not None:
            await self.__result.close()

class Cursor:
    """An asyncio-compatible version of :class:`sqlite3.Cursor`.

    Create these with :meth:`Connection.cursor`.
    """

    def __init__(self, connection, cursor):
        self._conn = connection
        self._cursor = cursor
        self._post = connection._post

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def get_cursor(self):
        """Retrieves the internal :class:`sqlite3.Cursor` object."""
        return self._cursor

    @property
    def connection(self):
        """Retrieves the :class:`Connection` that made this cursor."""
        return self._conn

    async def close(self):
        """Asynchronous version of :meth:`sqlite3.Cursor.close`."""
        return await self._post(self._cursor.close)

    async def execute(self, sql, *parameters):
        """Asynchronous version of :meth:`sqlite3.Cursor.execute`."""
        if len(parameters) == 1 and isinstance(parameters[0], (dict, tuple)):
            parameters = parameters[0]
        return await self._post(self._cursor.execute, sql, parameters)

    async def executemany(self, sql, seq_of_parameters):
        """Asynchronous version of :meth:`sqlite3.Cursor.executemany`."""
        return await self._post(self._cursor.executemany, sql, seq_of_parameters)

    async def executescript(self, sql_script):
        """Asynchronous version of :meth:`sqlite3.Cursor.executescript`."""
        return await self._post(self._cursor.executescript, sql_script)

    async def fetchone(self):
        """Asynchronous version of :meth:`sqlite3.Cursor.fetchone`."""
        return await self._post(self._cursor.fetchone)

    async def fetchmany(self, size=None):
        """Asynchronous version of :meth:`sqlite3.Cursor.fetchmany`."""
        size = self._cursor.arraysize if size is None else size
        return await self._post(self._cursor.fetchmany, size)

    async def fetchall(self):
        """Asynchronous version of :meth:`sqlite3.Cursor.fetchall`."""
        return await self._post(self._cursor.fetchall)

class Transaction:
    """An asyncio-compatible transaction for sqlite3.

    This can be used as a context manager as well.
    """
    def __init__(self, conn):
        self.conn = conn

    async def start(self):
        """Starts the transaction."""
        await self.conn.execute('BEGIN TRANSACTION;')

    async def rollback(self):
        """Exits the transaction and doesn't save."""
        await self.conn.rollback()

    async def commit(self):
        """Saves the transaction."""
        await self.conn.commit()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            # no error
            await self.commit()
        else:
            await self.rollback()

class _CursorWithTransaction(Cursor):
    async def start(self):
        await self._conn.execute('BEGIN TRANSACTION;')

    async def rollback(self):
        await self._conn.rollback()

    async def commit(self):
        await self._conn.commit()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                # no error
                await self.commit()
            else:
                await self.rollback()
        finally:
            await self.close()

class Connection:
    """An asyncio-compatible version of :class:`sqlite3.Connection`.

    Create these with :func:`.connect`.

    .. note::

        For a saner API, :attr:`sqlite3.Connection.row_factory`
        is automatically set to :class:`sqlite3.Row`.

        Along with :attr:`sqlite3.Connection.isolation_level`
        set to ``None`` and the journal_mode is set to ``WAL``.
    """
    def __init__(self, connection, queue):
        self._conn = connection
        self._queue = queue
        self._post = queue.post

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def get_connection(self):
        """Retrieves the internal :class:`sqlite3.Connection` object."""
        return self._conn

    def transaction(self):
        """Gets a transaction object.

        This can be used similarly to ``asyncpg.Transaction``.
        """
        return Transaction(self)

    def cursor(self, *, transaction=False):
        """Asynchronous version of :meth:`sqlite3.Connection.cursor`.

        Much like :func:`connect` this can be used as both a coroutine
        and an asynchronous context manager.

        Parameters
        ------------
        transaction: bool
            Whether to open a transaction as well, defaults to False.

        Returns
        --------
        :class:`Cursor`
            The cursor.
        """

        def factory(cur):
            if transaction:
                return _CursorWithTransaction(self, cur)
            else:
                return Cursor(self, cur)

        return _ContextManagerMixin(self._queue, factory, self._conn.cursor)

    async def commit(self):
        """Asynchronous version of :meth:`sqlite3.Connection.commit`."""
        return await self._post(self._conn.commit)

    async def rollback(self):
        """Asynchronous version of :meth:`sqlite3.Connection.rollback`."""
        return await self._post(self._conn.rollback)

    async def close(self):
        """Asynchronous version of :meth:`sqlite3.Connection.close`."""
        await self._post(self._conn.close)
        self._queue.stop()

    def execute(self, sql, *parameters):
        """Asynchronous version of :meth:`sqlite3.Connection.execute`.

        Note that this returns a :class:`Cursor` instead of a :class:`sqlite3.Cursor`.
        """
        if len(parameters) == 1 and isinstance(parameters[0], (dict, tuple)):
            parameters = parameters[0]

        factory = lambda cur: Cursor(self, cur)
        return _ContextManagerMixin(self._queue, factory, self._conn.execute, sql, parameters)

    def executemany(self, sql, seq_of_parameters):
        """Asynchronous version of :meth:`sqlite3.Connection.executemany`.

        Note that this returns a :class:`Cursor` instead of a :class:`sqlite3.Cursor`.
        """
        factory = lambda cur: Cursor(self, cur)
        return _ContextManagerMixin(self._queue, factory, self._conn.executemany, sql, seq_of_parameters)

    def executescript(self, sql_script):
        """Asynchronous version of :meth:`sqlite3.Connection.executescript`.

        Note that this returns a :class:`Cursor` instead of a :class:`sqlite3.Cursor`.
        """
        factory = lambda cur: Cursor(self, cur)
        return _ContextManagerMixin(self._queue, factory, self._conn.executescript, sql_script)

    async def fetchone(self, query, *parameters):
        """Shortcut method version of :meth:`sqlite3.Cursor.fetchone` without making a cursor."""
        async with self.execute(query, *parameters) as cursor:
            return await cursor.fetchone()

    async def fetchmany(self, query, *parameters, size=None):
        """Shortcut method version of :meth:`sqlite3.Cursor.fetchmany` without making a cursor."""
        async with self.execute(query, *parameters) as cursor:
            return await cursor.fetchmany(size)

    async def fetchall(self, query, *parameters):
        """Shortcut method version of :meth:`sqlite3.Cursor.fetchall` without making a cursor."""
        async with self.execute(query, *parameters) as cursor:
            return await cursor.fetchall()

def _connect_pragmas(db, **kwargs):
    connection = sqlite3.connect(db, **kwargs)
    connection.execute('pragma journal_mode=wal')
    connection.execute('pragma foreign_keys=ON')
    connection.isolation_level = None
    connection.row_factory = sqlite3.Row
    return connection

def connect(database, *, init=None, timeout=None, loop=None, **kwargs):
    """asyncio-compatible version of :func:`sqlite3.connect`.

    This can be used as a regular coroutine or in an async-with statement.

    For example, both are equivalent:

    .. code-block:: python3

        conn = await connect(":memory:")
        try:
            ...
        finally:
            await conn.close()

    .. code-block:: python3

        async with connect(":memory:") as conn:
            ...

    Resolves to a :class:`Connection` object.

    A special keyword-only parameter named ``init`` can be passed which allows
    one to customize the :class:`sqlite3.Connection` before it is converted
    to a :class:`Connection` object.
    """
    loop = loop or asyncio.get_event_loop()
    queue = _Worker(loop=loop)
    queue.start()
    def factory(con):
        return Connection(con, queue)

    if init is not None:
        def new_connect(db, **kwargs):
            con = _connect_pragmas(db, **kwargs)
            init(con)
            return con
    else:
        new_connect = _connect_pragmas

    return _ContextManagerMixin(queue, factory, new_connect, database, timeout=timeout, **kwargs)
