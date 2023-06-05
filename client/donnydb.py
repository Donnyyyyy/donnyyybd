import asyncio
from asyncio import Lock, StreamReader, StreamWriter
from contextlib import asynccontextmanager
from typing import AsyncIterator, Self


class DonnydbClient:
    _reader_stream: StreamReader
    _reader_lock: Lock

    _writer_stream: StreamWriter
    _writer_lock: Lock

    RESPONSE_SIZE = 3
    GET = b'GET'
    SET = b'SET'
    OK_RESP = b'OK\0'

    def __init__(self, host: str = 'localhost', port: int = 1337, timeout: float = 0.3):
        self._host = host
        self._port = port
        self.timeout = timeout

        self._writer_lock = Lock()
        self._reader_lock = Lock()

        self._initialized = False

    @asynccontextmanager
    async def _reader(self) -> AsyncIterator[StreamReader]:
        assert self._initialized
        try:
            await self._reader_lock.acquire()
            # print(f'reader lock acquired')
            yield self._reader_stream
        finally:
            self._reader_lock.release()
            # print(f'reader lock released')

    @asynccontextmanager
    async def _writer(self) -> AsyncIterator[StreamWriter]:
        assert self._initialized
        try:
            await self._writer_lock.acquire()
            # print(f'writer lock acquired')
            yield self._writer_stream
        finally:
            self._writer_lock.release()
            # print(f'writer lock released')

    async def __aenter__(self) -> Self:
        self._reader_stream, self._writer_stream = await asyncio.open_connection(
            self._host, self._port
        )
        self._initialized = True
        return self

    async def __aexit__(self, *args):
        async with self._writer() as stream_w, self._reader() as stream_r:
            stream_w.close()
            await stream_w.wait_closed()
            stream_r.feed_eof()
            self._initialized = False

    async def set(self, key: str, value: bytes) -> bool:
        async with self._writer() as stream:
            stream: StreamWriter
            stream.write(self.SET)

            stream.write(len(key).to_bytes(8, 'little'))
            stream.write(key.encode())

            stream.write(len(value).to_bytes(8, 'little'))
            stream.write(value)
            await stream.drain()

        async with self._reader() as stream:
            stream: StreamReader
            resp = await asyncio.wait_for(
                stream.readexactly(self.RESPONSE_SIZE), timeout=self.timeout
            )
        if resp == self.OK_RESP:
            return True
        return False

    async def get(self, key: str) -> bytes:
        async with self._writer() as stream:
            stream: StreamWriter
            stream.write(self.GET)
            stream.write(len(key).to_bytes(8, 'little'))
            stream.write(key.encode())
            await stream.drain()

        async with self._reader() as stream:
            stream: StreamReader
            value_length_bytes = await asyncio.wait_for(
                stream.readexactly(8), timeout=self.timeout
            )
            value_size = int.from_bytes(value_length_bytes, 'little')

            return await asyncio.wait_for(stream.readexactly(value_size), timeout=self.timeout)
