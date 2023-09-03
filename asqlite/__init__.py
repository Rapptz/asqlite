# -*- coding: utf-8 -*-

"""
The MIT License (MIT)

Copyright (c) 2019-2023 Rapptz

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

from __future__ import annotations

import sqlite3
import threading
import queue
import asyncio
from typing import (
    Any,
    AsyncContextManager,
    Callable,
    Dict,
    Generator,
    Generic,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from types import TracebackType

__version__ = '2.0.0a'

PARSE_DECLTYPES = sqlite3.PARSE_DECLTYPES
PARSE_COLNAMES = sqlite3.PARSE_COLNAMES

T = TypeVar('T')
U = TypeVar('U', covariant=True, bound=AsyncContextManager[Any])


class _WorkerEntry:
    __slots__ = ('func', 'args', 'kwargs', 'future', 'cancelled')

    def __init__(self, func: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any], future: asyncio.Future):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.future = future


class _Worker(threading.Thread):
    def __init__(self, *, loop: asyncio.AbstractEventLoop, index: Optional[int] = None):
        name = 'asqlite-worker-thread'
        if index is not None:
            name = f'{name}-{index}'

        super().__init__(name=name, daemon=True)
        self._loop = loop
        self._worker_queue: queue.Queue[_WorkerEntry] = queue.Queue()
        self._end = threading.Event()

    def _call_entry(self, entry: _WorkerEntry) -> None:
        fut = entry.future
        if fut.cancelled():
            return

        try:
            result = entry.func(*entry.args, **entry.kwargs)
        except Exception as e:
            self._loop.call_soon_threadsafe(fut.set_exception, e)
        else:
            self._loop.call_soon_threadsafe(fut.set_result, result)

    def run(self) -> None:
        _queue = self._worker_queue
        while not self._end.is_set():
            try:
                entry = _queue.get(timeout=0.2)
            except queue.Empty:
                continue
            else:
                self._call_entry(entry)

    def post(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> asyncio.Future[T]:
        future: asyncio.Future[T] = self._loop.create_future()
        entry = _WorkerEntry(func=func, args=args, kwargs=kwargs, future=future)
        self._worker_queue.put_nowait(entry)
        return future

    def stop(self) -> None:
        self._end.set()


class _ContextManagerMixin(Generic[T, U]):
    def __init__(
        self,
        _queue: _Worker,
        _factory: Callable[[T], U],
        func: Callable[..., Any],
        *args: Any,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ):
        self._worker: _Worker = _queue
        self.func: Callable[..., Any] = func
        self.timeout: Optional[float] = timeout
        self._factory: Callable[[T], U] = _factory
        self.args: Tuple[Any, ...] = args
        self.kwargs: Dict[str, Any] = kwargs
        self.__result: Optional[U] = None

    async def _runner(self) -> U:
        future = self._worker.post(self.func, *self.args, **self.kwargs)
        if self.timeout is not None:
            ret = await asyncio.wait_for(future, timeout=self.timeout)
        else:
            ret = await future
        self.__result = result = self._factory(ret)
        return result

    def __await__(self) -> Generator[Any, None, U]:
        return self._runner().__await__()

    async def __aenter__(self) -> U:
        ret = await self._runner()
        return await ret.__aenter__()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self.__result is not None:
            await self.__result.__aexit__(exc_type, exc_value, traceback)


C = TypeVar('C', bound='Cursor')
TR = TypeVar('TR', bound='Transaction')


class Cursor:
    """An asyncio-compatible version of :class:`sqlite3.Cursor`.

    Create these with :meth:`Connection.cursor`.
    """

    def __init__(self, connection: Connection, cursor: sqlite3.Cursor):
        self._conn = connection
        self._cursor = cursor
        self._post = connection._post

    async def __aenter__(self: C) -> C:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()

    def get_cursor(self) -> sqlite3.Cursor:
        """Retrieves the internal :class:`sqlite3.Cursor` object."""
        return self._cursor

    @property
    def connection(self) -> Connection:
        """Retrieves the :class:`Connection` that made this cursor."""
        return self._conn

    async def close(self) -> None:
        """Asynchronous version of :meth:`sqlite3.Cursor.close`."""
        return await self._post(self._cursor.close)

    @overload
    async def execute(self: C, sql: str, parameter: Dict[str, Any], /) -> C:
        ...

    @overload
    async def execute(self: C, sql: str, parameter: Tuple[Any, ...], /) -> C:
        ...

    @overload
    async def execute(self: C, sql: str, /, *parameters: Any) -> C:
        ...

    async def execute(self: C, sql: str, /, *parameters: Any) -> C:
        """Asynchronous version of :meth:`sqlite3.Cursor.execute`."""
        if len(parameters) == 1 and isinstance(parameters[0], (dict, tuple)):
            parameters = parameters[0]  # type: ignore
        await self._post(self._cursor.execute, sql, parameters)
        return self

    async def executemany(self: C, sql: str, seq_of_parameters: Iterable[Iterable[Any]]) -> C:
        """Asynchronous version of :meth:`sqlite3.Cursor.executemany`."""
        await self._post(self._cursor.executemany, sql, seq_of_parameters)
        return self

    async def executescript(self: C, sql_script: str) -> C:
        """Asynchronous version of :meth:`sqlite3.Cursor.executescript`."""
        await self._post(self._cursor.executescript, sql_script)
        return self

    async def fetchone(self) -> sqlite3.Row:
        """Asynchronous version of :meth:`sqlite3.Cursor.fetchone`."""
        return await self._post(self._cursor.fetchone)

    async def fetchmany(self, size: Optional[int] = None) -> List[sqlite3.Row]:
        """Asynchronous version of :meth:`sqlite3.Cursor.fetchmany`."""
        size = self._cursor.arraysize if size is None else size
        return await self._post(self._cursor.fetchmany, size)

    async def fetchall(self) -> List[sqlite3.Row]:
        """Asynchronous version of :meth:`sqlite3.Cursor.fetchall`."""
        return await self._post(self._cursor.fetchall)


class Transaction:
    """An asyncio-compatible transaction for sqlite3.

    This can be used as a context manager as well.
    """

    def __init__(self, conn: Connection):
        self.conn: Connection = conn

    async def start(self) -> None:
        """Starts the transaction."""
        await self.conn.execute('BEGIN TRANSACTION;')

    async def rollback(self) -> None:
        """Exits the transaction and doesn't save."""
        await self.conn.rollback()

    async def commit(self) -> None:
        """Saves the transaction."""
        await self.conn.commit()

    async def __aenter__(self: TR) -> TR:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if exc_type is None:
            # no error
            await self.commit()
        else:
            await self.rollback()


class _CursorWithTransaction(Cursor):
    async def start(self) -> None:
        await self._conn.execute('BEGIN TRANSACTION;')

    async def rollback(self) -> None:
        await self._conn.rollback()

    async def commit(self) -> None:
        await self._conn.commit()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
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

    def __init__(self, connection: sqlite3.Connection, queue: _Worker):
        self._conn = connection
        self._queue = queue
        self._post = queue.post

    async def __aenter__(self: T) -> T:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()

    def get_connection(self) -> sqlite3.Connection:
        """Retrieves the internal :class:`sqlite3.Connection` object."""
        return self._conn

    def transaction(self) -> Transaction:
        """Gets a transaction object.

        This can be used similarly to ``asyncpg.Transaction``.
        """
        return Transaction(self)

    @overload
    def cursor(self, *, transaction: Literal[True]) -> _ContextManagerMixin[sqlite3.Cursor, _CursorWithTransaction]:
        ...

    @overload
    def cursor(self, *, transaction: Literal[False] = False) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        ...

    def cursor(self, *, transaction: bool = False) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
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

        def factory(cur: sqlite3.Cursor) -> Cursor:
            if transaction:
                return _CursorWithTransaction(self, cur)
            else:
                return Cursor(self, cur)

        return _ContextManagerMixin(self._queue, factory, self._conn.cursor)

    async def commit(self) -> None:
        """Asynchronous version of :meth:`sqlite3.Connection.commit`."""
        return await self._post(self._conn.commit)

    async def rollback(self) -> None:
        """Asynchronous version of :meth:`sqlite3.Connection.rollback`."""
        return await self._post(self._conn.rollback)

    async def close(self) -> None:
        """Asynchronous version of :meth:`sqlite3.Connection.close`."""
        await self._post(self._conn.close)
        self._queue.stop()

    @overload
    def execute(self, sql: str, parameter: Dict[str, Any], /) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        ...

    @overload
    def execute(self, sql: str, parameter: Tuple[Any, ...], /) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        ...

    @overload
    def execute(self, sql: str, /, *parameters: Any) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        ...

    def execute(self, sql: str, /, *parameters: Any) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        """Asynchronous version of :meth:`sqlite3.Connection.execute`.

        Note that this returns a :class:`Cursor` instead of a :class:`sqlite3.Cursor`.
        """
        if len(parameters) == 1 and isinstance(parameters[0], (dict, tuple)):
            parameters = parameters[0]  # type: ignore

        def factory(cur: sqlite3.Cursor):
            return Cursor(self, cur)

        return _ContextManagerMixin(self._queue, factory, self._conn.execute, sql, parameters)

    def executemany(
        self, sql: str, seq_of_parameters: Iterable[Iterable[Any]]
    ) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        """Asynchronous version of :meth:`sqlite3.Connection.executemany`.

        Note that this returns a :class:`Cursor` instead of a :class:`sqlite3.Cursor`.
        """

        def factory(cur: sqlite3.Cursor):
            return Cursor(self, cur)

        return _ContextManagerMixin(self._queue, factory, self._conn.executemany, sql, seq_of_parameters)

    def executescript(self, sql_script: str) -> _ContextManagerMixin[sqlite3.Cursor, Cursor]:
        """Asynchronous version of :meth:`sqlite3.Connection.executescript`.

        Note that this returns a :class:`Cursor` instead of a :class:`sqlite3.Cursor`.
        """

        def factory(cur: sqlite3.Cursor):
            return Cursor(self, cur)

        return _ContextManagerMixin(self._queue, factory, self._conn.executescript, sql_script)

    @overload
    async def fetchone(self, query: str, parameter: Dict[str, Any], /) -> sqlite3.Row:
        ...

    @overload
    async def fetchone(self, query: str, parameter: Tuple[Any, ...], /) -> sqlite3.Row:
        ...

    @overload
    async def fetchone(self, query: str, *parameters: Any) -> sqlite3.Row:
        ...

    async def fetchone(self, query: str, *parameters: Any) -> sqlite3.Row:
        """Shortcut method version of :meth:`sqlite3.Cursor.fetchone` without making a cursor."""
        async with self.execute(query, *parameters) as cursor:
            return await cursor.fetchone()

    @overload
    async def fetchmany(self, query: str, parameter: Dict[str, Any], /, *, size: Optional[int] = None) -> List[sqlite3.Row]:
        ...

    @overload
    async def fetchmany(self, query: str, parameter: Tuple[Any, ...], /, *, size: Optional[int] = None) -> List[sqlite3.Row]:
        ...

    @overload
    async def fetchmany(self, query: str, /, *parameters: Any, size: Optional[int] = None) -> List[sqlite3.Row]:
        ...

    async def fetchmany(self, query: str, /, *parameters: Any, size: Optional[int] = None) -> List[sqlite3.Row]:
        """Shortcut method version of :meth:`sqlite3.Cursor.fetchmany` without making a cursor."""
        async with self.execute(query, *parameters) as cursor:
            return await cursor.fetchmany(size)

    @overload
    async def fetchall(self, query: str, parameter: Dict[str, Any], /) -> List[sqlite3.Row]:
        ...

    @overload
    async def fetchall(self, query: str, parameter: Tuple[Any, ...], /) -> List[sqlite3.Row]:
        ...

    @overload
    async def fetchall(self, query: str, /, *parameters: Any) -> List[sqlite3.Row]:
        ...

    async def fetchall(self, query: str, /, *parameters: Any) -> List[sqlite3.Row]:
        """Shortcut method version of :meth:`sqlite3.Cursor.fetchall` without making a cursor."""
        async with self.execute(query, *parameters) as cursor:
            return await cursor.fetchall()


def _connect_pragmas(db: Union[str, bytes], **kwargs: Any) -> sqlite3.Connection:
    connection = sqlite3.connect(db, **kwargs)
    connection.execute('pragma journal_mode=wal')
    connection.execute('pragma foreign_keys=ON')
    connection.isolation_level = None
    connection.row_factory = sqlite3.Row
    return connection


def connect(
    database: Union[str, bytes],
    *,
    init: Optional[Callable[[sqlite3.Connection], None]] = None,
    timeout: Optional[float] = None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    **kwargs: Any,
) -> _ContextManagerMixin[sqlite3.Connection, Connection]:
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

    def factory(con: sqlite3.Connection) -> Connection:
        return Connection(con, queue)

    if init is not None:

        def new_connect(db: Union[str, bytes], **kwargs: Any) -> sqlite3.Connection:
            con = _connect_pragmas(db, **kwargs)
            init(con)
            return con

    else:
        new_connect = _connect_pragmas  # type: ignore

    return _ContextManagerMixin(queue, factory, new_connect, database, timeout=timeout, **kwargs)


class ProxiedConnection(Connection):
    def __init__(self, pool: Pool, index: int, connection: sqlite3.Connection, queue: _Worker) -> None:
        super().__init__(connection, queue)
        self._pool: Pool = pool
        self._index: int = index
        self._in_use: Optional[asyncio.Future[None]] = None

    async def close(self) -> None:
        if self._in_use:
            await self._pool.release(self)

    async def wait_until_released(self) -> None:
        if self._in_use:
            await self._in_use

    async def _force_close(self) -> None:
        await super().close()


class _AcquireProxyContextManager:
    def __init__(self, pool: Pool) -> None:
        self._pool: Pool = pool
        self._connection: Optional[ProxiedConnection] = None

    async def __aenter__(self) -> ProxiedConnection:
        self._connection = await self._pool._acquire()
        return self._connection

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._connection is not None:
            await self._pool.release(self._connection)
            self._connection = None

    def __await__(self) -> Generator[Any, None, ProxiedConnection]:
        return self._pool._acquire().__await__()


class Pool:
    """A connection pool.

    Connection pools can be used to manage multiple connections to the database to allow for greater concurrency.
    Connections are first acquired from the pool, used, and then finally released back into the pool.

    Pools are created using :func:`create_pool`.

    They function similarly to ``asyncpg.Pool`` and ``asyncpg.create_pool``.

    .. versionadded:: 2.0
    """

    def __init__(self, workers: List[_Worker], connections: List[ProxiedConnection]) -> None:
        self._workers: List[_Worker] = workers
        self._connections: List[ProxiedConnection] = connections
        self._queue: asyncio.LifoQueue[ProxiedConnection] = asyncio.LifoQueue(maxsize=len(workers))
        self._loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self._closing: bool = False
        self._closed: bool = False

    @classmethod
    async def _create(
        cls,
        database: Union[str, bytes],
        size: int,
        init: Optional[Callable[[sqlite3.Connection], None]],
        **kwargs: Any,
    ) -> Pool:

        loop = asyncio.get_running_loop()
        workers: List[_Worker] = [_Worker(loop=loop, index=i + 1) for i in range(size)]
        for worker in workers:
            worker.start()

        connections: List[ProxiedConnection] = []

        pool = cls(workers, connections)
        if init is not None:

            def new_connect(db: Union[str, bytes], **kwargs: Any) -> sqlite3.Connection:
                con = _connect_pragmas(db, **kwargs)
                init(con)
                return con

        else:
            new_connect = _connect_pragmas  # type: ignore

        for index, worker in enumerate(workers):
            connection = await worker.post(new_connect, database, **kwargs)
            connections.append(ProxiedConnection(pool, index, connection, worker))

        for connection in connections:
            pool._queue.put_nowait(connection)

        return pool

    async def __aenter__(self) -> Pool:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def _acquire(self) -> ProxiedConnection:
        if self._closing:
            raise sqlite3.ProgrammingError('Pool is closing')

        if self._closed:
            raise sqlite3.ProgrammingError('Pool is closed')

        connection = await self._queue.get()
        connection._in_use = self._loop.create_future()
        return connection

    def acquire(self) -> _AcquireProxyContextManager:
        """Acquires a connection from the pool.

        This can be used as a regular coroutine or in an async-with statement.

        For example, both are equivalent:

        .. code-block:: python

            async with pool.acquire() as conn:
                await conn.execute(...)

        Or:

        .. code-block:: python

            conn = await pool.acquire()
            try:
                await conn.execute(...)
            finally:
                await pool.release(conn)

        """
        return _AcquireProxyContextManager(self)

    async def release(self, connection: ProxiedConnection) -> None:
        """Releases a connection back into the pool.

        Parameters
        -----------
        connection: :class:`ProxiedConnection`
            The connection to release back into the pool.
            This must be a connection that *belongs* to the pool.

        Raises
        -------
        sqlite3.ProgrammingError
            The connection is not a proxied connection from the pool or
            does not belong to this pool.
        """

        if type(connection) is not ProxiedConnection:
            raise sqlite3.ProgrammingError('Expected a proxied connection from the pool.')

        if connection._pool is not self:
            raise sqlite3.ProgrammingError('The connection does not belong to this pool.')

        # Already released
        if connection._in_use is None:
            return

        if not connection._in_use.done():
            connection._in_use.set_result(None)

        connection._in_use = None
        self._queue.put_nowait(connection)

    async def close(self) -> None:
        """Attempts to gracefully close the connection pool and all connections in the pool.

        This will wait for all connections to be released back into the pool before closing.
        If any error happens during the wait, then the pool will be forcefully closed using
        :meth:`Pool.terminate`.
        """

        if self._closed:
            return

        self._closing = True

        # Maybe warn on close taking too long?
        try:
            wait_for_release = [connection.wait_until_released() for connection in self._connections]
            await asyncio.gather(*wait_for_release)

            close_all = [connection._force_close() for connection in self._connections]
            await asyncio.gather(*close_all)
        except (Exception, asyncio.CancelledError):
            await self.terminate()
            raise
        else:
            for worker in self._workers:
                worker.stop()
        finally:
            self._closed = True
            self._closing = False

    async def terminate(self) -> None:
        """Forcefully terminate all connections in the pool."""

        if self._closed:
            return

        close_all = [connection._force_close() for connection in self._connections]
        await asyncio.gather(*close_all)

        for worker in self._workers:
            worker.stop()

        self._closed = True
        self._closing = False


class PoolContextManager:
    def __init__(
        self,
        database: Union[str, bytes],
        *,
        size: int = 10,
        init: Optional[Callable[[sqlite3.Connection], None]] = None,
        **kwargs: Any,
    ):
        self.__database: Union[str, bytes] = database
        self.__size: int = size
        self.__init: Optional[Callable[[sqlite3.Connection], None]] = init
        self.__kwargs: Dict[str, Any] = kwargs
        self.__result: Optional[Pool] = None

    async def _runner(self) -> Pool:
        self.__result = await Pool._create(self.__database, self.__size, self.__init, **self.__kwargs)
        return self.__result

    def __await__(self) -> Generator[Any, None, Pool]:
        return self._runner().__await__()

    async def __aenter__(self) -> Pool:
        ret = await self._runner()
        return await ret.__aenter__()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self.__result is not None:
            await self.__result.__aexit__(exc_type, exc_value, traceback)


def create_pool(
    database: Union[str, bytes],
    *,
    init: Optional[Callable[[sqlite3.Connection], None]] = None,
    size: int = 10,
    **kwargs: Any,
) -> PoolContextManager:
    r"""Creates a connection pool.

    This can be used as a regular coroutine or in an async-with statement.

    For example, both are equivalent:

    .. code-block:: python3

        conn = await create_pool(":memory:")
        try:
            ...
        finally:
            await conn.close()

    .. code-block:: python3

        async with create_pool(":memory:") as pool:
            ...

    Resolves to a :class:`Pool` object.

    A special keyword-only parameter named ``init`` can be passed which allows
    one to customize the :class:`sqlite3.Connection` before it is converted
    to a :class:`Connection` object when managed by the pool.

    Parameters
    -----------
    database: Union[:class:`str`, :class:`bytes`]
        The database to connect to. This can be a path to a file or ``:memory:`` for an in-memory database.
    init: Optional[Callable[[:class:`sqlite3.Connection`], None]]
        A function that is called to customise the connection before it is used by the pool.
    size: :class:`int`
        The number of connections to initialise the pool with. Note that this creates 1 daemon thread per connection.
        Defaults to 10.
    \*\*kwargs: Any
        Any additional keyword arguments are passed to :func:`sqlite3.connect`.
    """

    return PoolContextManager(database, init=init, size=size, **kwargs)
