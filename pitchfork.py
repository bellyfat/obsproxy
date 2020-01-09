import asyncio
import enum
import functools
import os
import random
from collections import namedtuple
from struct import Struct

NUM_CONNECTIONS = 16
MAX_BUFFER_SIZE = 10 * 1e6
READ_SIZE = 512
MAX_WRITE_SIZE = 1200

Connection = namedtuple("Connection", ("reader", "writer"))

ConnectionId = Struct("<16sB")
DataSegment = Struct("<H")


# Downstream


class DownstreamSession:
    def __init__(self, src_conn, upstream_host, upstream_port):
        self._id = os.urandom(16)
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port

        self._source_connection = src_conn
        self._connections = {}
        self._up_buffer = bytearray()

        self._buffer_has_data_cond = asyncio.Condition()

    async def _connect_upstream(self):
        # open connections
        async def connect(i):
            reader, writer = await asyncio.open_connection(
                self.upstream_host, self.upstream_port
            )
            self._connections[i] = Connection(reader, writer)
            data = ConnectionId.pack(self._id, i)
            writer.write(data)
            await writer.drain()

        connect_tasks = [connect(i) for i in range(NUM_CONNECTIONS)]
        connect_task = asyncio.gather(*connect_tasks)

        # Try to connect all, cancel/close open connections if any fail
        try:
            await connect_task
        except Exception:
            connect_task.cancel()
            conns = list(self._connections.values())
            for conn in conns:
                conn.writer.close()

            await asyncio.gather(*(conn.writer.wait_closed() for conn in conns))

            raise

    async def _read_into_buffer_loop(self):
        # Read from the source into the buffer
        while True:
            data = await self._source_connection.reader.read(READ_SIZE)
            if len(data) == 0:
                raise EOFError()

            old_len = len(self._up_buffer)
            self._up_buffer.extend(data)

            if len(self._up_buffer) > MAX_BUFFER_SIZE:
                raise BufferError("Buffer is full")

            if old_len == 0:
                async with self._buffer_has_data_cond:
                    self._buffer_has_data_cond.notify_all()

    async def _write_loop(self):
        write_idx = 0
        while True:
            while len(self._up_buffer) == 0:
                async with self._buffer_has_data_cond:
                    await self._buffer_has_data_cond.wait()

            conn = self._connections[write_idx]
            to_write = self._up_buffer[:MAX_WRITE_SIZE]
            del self._up_buffer[: len(to_write)]

            out_data = DataSegment.pack(len(to_write)) + to_write
            conn.writer.write(out_data)
            write_idx = (write_idx + 1) % NUM_CONNECTIONS
            await conn.writer.drain()

    async def _read_from_upstream_loop(self):
        while True:
            data = await self._connections[0].reader.read(READ_SIZE)
            if len(data) == 0:
                raise EOFError()

            self._source_connection.writer.write(data)
            await self._source_connection.writer.drain()

    async def run(self):
        await self._connect_upstream()
        tasks = [
            asyncio.create_task(self._read_from_upstream_loop()),
            asyncio.create_task(self._write_loop()),
            asyncio.create_task(self._read_into_buffer_loop()),
        ]

        all_tasks = asyncio.gather(*tasks)

        try:
            await all_tasks
        finally:
            end_tasks = []

            for conn in self._connections.values():
                conn.writer.close()
                end_tasks.append(conn.writer.wait_closed())

            self._source_connection.writer.close()
            end_tasks.append(self._source_connection.writer.wait_closed())
            all_tasks.cancel()
            await asyncio.gather(*end_tasks)


class Downstream:
    def __init__(self, listen_host, listen_port, upstream_host, upstream_port):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port

        self._server = None
        self._sessions = []

    async def _connected(self, reader, writer):
        conn = Connection(reader, writer)
        sess = DownstreamSession(conn, self.upstream_host, self.upstream_port)
        self._sessions.append(sess)

        try:
            await sess.run()
        except (OSError, EOFError, BufferError):
            pass

    async def start(self):
        self._server = await asyncio.start_server(
            self._connected, self.listen_host, self.listen_port
        )
        await self._server.serve_forever()
