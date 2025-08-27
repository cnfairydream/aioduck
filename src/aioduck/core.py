# -*- coding: utf-8 -*-

import asyncio
from asyncio import Queue, Future
from typing import Callable

import duckdb
from duckdb import DuckDBPyConnection


class Agent:

    def __init__(self):
        self._worker = None
        self._running = False
        self._queue = Queue()
        self._loop = asyncio.get_running_loop()

    async def submit(self, func: Callable, *args, **kwargs) -> Future:
        future = self._loop.create_future()
        await self._queue.put((future, func, args, kwargs))
        return future

    async def start(self):
        if not self._running:
            self._running = True
            self._worker = asyncio.create_task(self.run())

    async def stop(self):
        if self._running:
            self._running = False
            await self._worker

    async def run(self):
        while True:
            if not self._running and self._queue.empty():
                break

            elif self._running and self._queue.empty():
                await asyncio.sleep(0)

            else:
                future, func, args, kwargs = await self._queue.get()

                try:
                    result = await asyncio.to_thread(func, *args, **kwargs)
                    future.set_result(result)
                except BaseException as e:
                    future.set_exception(e)

                self._queue.task_done()


class AsyncConnection:

    def __init__(self, database: str, config: dict = None):
        self._connection = duckdb.connect(database, config=config or {})
        self.agent = Agent()

    async def __aenter__(self):
        await self.agent.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        await self.agent.stop()

    async def _execute(self, func, *args, **kwargs):
        future = await self.agent.submit(func, *args, **kwargs)
        await future
        return future.result()

    def cursor(self):
        return AsyncCursor(self, self._connection.cursor())

    async def close(self):
        await self._execute(self._connection.close)

    async def begin(self):
        await self._execute(self._connection.begin)

    async def commit(self):
        await self._execute(self._connection.commit)

    async def rollback(self):
        await self._execute(self._connection.rollback)

    async def execute(self, query: str, parameters=None):
        cursor = await self._execute(self._connection.execute, query, parameters)
        return AsyncCursor(self, cursor)

    async def executemany(self, query: str, parameters=None):
        cursor = await self._execute(self._connection.executemany, query, parameters)
        return AsyncCursor(self, cursor)

    async def checkpoint(self):
        await self._execute(self._connection.checkpoint)

    async def load_extension(self):
        await self._execute(self._connection.load_extension)

    async def create_function(self):
        await self._execute(self._connection.create_function)


class AsyncCursor:

    def __init__(self, connection: AsyncConnection, cursor: DuckDBPyConnection | None = None):
        self._connection = connection
        self._cursor = cursor or connection.cursor()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def __aiter__(self):
        rows = await self.fetchall()
        if rows:
            for row in rows:
                yield row

    async def _execute(self, func, *args, **kwargs):
        future = await self._connection.agent.submit(func, *args, **kwargs)
        await future
        return future.result()

    async def close(self):
        await self._execute(self._cursor.close)

    async def execute(self, query: str, parameters=None):
        return await self._execute(self._cursor.execute, query, parameters)

    async def executemany(self, query: str, parameters=None):
        return await self._execute(self._cursor.executemany, query, parameters)

    async def fetchall(self):
        return await self._execute(self._cursor.fetchall)

    async def fetchmany(self, size: int = 1):
        return await self._execute(self._cursor.fetchmany, size)

    async def fetchone(self):
        return await self._execute(self._cursor.fetchone)

    async def pl(self, *args, **kwargs):
        return await self._execute(self._cursor.pl, *args, **kwargs)
