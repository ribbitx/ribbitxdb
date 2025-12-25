import asyncio
from typing import List, Dict, Any, Optional
from .connection import Connection
from .cursor import Cursor

class AsyncConnection:
    def __init__(self, database: str):
        self.database = database
        self._conn = None
        self._loop = None
    
    async def __aenter__(self):
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
    
    async def connect(self):
        loop = asyncio.get_event_loop()
        self._conn = await loop.run_in_executor(None, Connection, self.database)
        return self
    
    async def close(self):
        if self._conn:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._conn.close)
    
    async def cursor(self) -> 'AsyncCursor':
        if not self._conn:
            raise RuntimeError("Connection not established. Call connect() first.")
        return AsyncCursor(self._conn)
    
    async def commit(self):
        if not self._conn:
            raise RuntimeError("Connection not established")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._conn.commit)
    
    async def rollback(self):
        if not self._conn:
            raise RuntimeError("Connection not established")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._conn.rollback)
    
    async def execute(self, sql: str, parameters: tuple = ()):
        cursor = await self.cursor()
        await cursor.execute(sql, parameters)
        return cursor
    
    async def executemany(self, sql: str, parameters_list: List[tuple]):
        cursor = await self.cursor()
        await cursor.executemany(sql, parameters_list)
        return cursor
    
    def table_exists(self, table_name: str) -> bool:
        return self._conn.table_exists(table_name) if self._conn else False

class AsyncCursor:
    def __init__(self, connection: Connection):
        self._conn = connection
        self._cursor = connection.cursor()
        self._results = []
    
    async def execute(self, sql: str, parameters: tuple = ()):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._cursor.execute, sql, parameters)
        return self
    
    async def executemany(self, sql: str, parameters_list: List[tuple]):
        loop = asyncio.get_event_loop()
        for params in parameters_list:
            await loop.run_in_executor(None, self._cursor.execute, sql, params)
        return self
    
    async def fetchone(self) -> Optional[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._cursor.fetchone)
    
    async def fetchall(self) -> List[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._cursor.fetchall)
    
    async def fetchmany(self, size: int = 1) -> List[Dict[str, Any]]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._cursor.fetchmany, size)
    
    @property
    def rowcount(self) -> int:
        return self._cursor.rowcount
    
    @property
    def description(self):
        return self._cursor.description

async def connect_async(database: str) -> AsyncConnection:
    conn = AsyncConnection(database)
    await conn.connect()
    return conn
