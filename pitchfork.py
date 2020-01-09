#!/usr/bin/env python3
import asyncio
import enum
import functools
import logging
import os
import random
import struct
import sys
from collections import namedtuple
from struct import Struct

NUM_CONNECTIONS = 16
MAX_BUFFER_SIZE = 10 * 1e6
READ_SIZE = 512
MAX_WRITE_SIZE = 1200

Connection = namedtuple("Connection", ("reader", "writer"))

ConnectionId = Struct("<16sB")
DataSegment = Struct("<H")

formatter = logging.Formatter("%(message)s")
handler = logging.StreamHandler()
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


# Downstream


class DownstreamSession:
    def __init__(self, src_conn, upstream_host, upstream_port):
        self.id = os.urandom(16)
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
            data = ConnectionId.pack(self.id, i)
            writer.write(data)
            await writer.drain()

        connect_tasks = [connect(i) for i in range(NUM_CONNECTIONS)]
        connect_task = asyncio.gather(*connect_tasks)

        # Try to connect all, cancel/close open connections if any fail
        try:
            await connect_task
            logger.info("Opening connections to upstream")
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
            logger.info("Starting proxy session")
            await all_tasks
        finally:
            logger.info("Stopping proxy session")
            end_tasks = []

            for conn in self._connections.values():
                conn.writer.close()
                end_tasks.append(conn.writer.wait_closed())

            self._source_connection.writer.close()
            end_tasks.append(self._source_connection.writer.wait_closed())
            all_tasks.cancel()
            await asyncio.gather(*end_tasks)
            logger.info("Proxy session stopped")


class Downstream:
    def __init__(self, listen_host, listen_port, upstream_host, upstream_port):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port

        self._server = None
        self._sessions = {}

    async def _connected(self, reader, writer):
        conn = Connection(reader, writer)
        sess = DownstreamSession(conn, self.upstream_host, self.upstream_port)

        task = asyncio.create_task(sess.run())
        self._sessions[sess.id] = (sess, task)

        try:
            logger.info("Accepted connection")
            await task
        except (OSError, EOFError, BufferError, struct.error):
            pass
        finally:
            del self._sessions[sess.id]

    async def start(self):
        logger.info("Starting proxy server")
        self._server = await asyncio.start_server(
            self._connected, self.listen_host, self.listen_port
        )

        try:
            await self._server.serve_forever()
        finally:
            logger.info("Stopping proxy server")
            self._server.close()
            await self._server.wait_closed()

            tasks = list(self._sessions.values())
            for sess, task in tasks:
                task.cancel()

            logger.info("Server stopped")


class UpstreamSession:
    def __init__(self, id_, target_host, target_port):
        self.id = id_
        self.target_host = target_host
        self.target_port = target_port

        self._target_conn = None

        self._indices = []
        self._connections = {}
        self._all_connections = False
        self._conn_tasks = {}

        self._write_ready_cond = asyncio.Condition()

        self._read_idx = 0
        self._read_idx_cond = asyncio.Condition()

        self._run_task = None

    async def _connect_target(self):
        logger.info("Connecting to target")
        reader, writer = await asyncio.open_connection(
            self.target_host, self.target_port
        )
        conn = Connection(reader, writer)
        self._target_conn = conn

        async with self._write_ready_cond:
            self._write_ready_cond.notify_all()

    async def _read_downstream_loop(self, idx):
        conn = self._connections[idx]
        buffer = bytearray()

        while True:
            while len(buffer) < DataSegment.size:
                data = await conn.reader.read(READ_SIZE)
                if len(data) == 0:
                    raise EOFError()

                buffer.extend(data)

            if self._all_connections is False:
                self._all_connections = True

                async with self._write_ready_cond:
                    self._write_ready_cond.notify_all()

            seg_size, = DataSegment.unpack_from(buffer)
            del buffer[: DataSegment.size]

            while len(buffer) < seg_size:
                data = await conn.reader.read(READ_SIZE)
                if len(data) == 0:
                    raise EOFError()

                buffer.extend(data)

            while self._target_conn is None or not self._all_connections:
                async with self._write_ready_cond:
                    await self._write_ready_cond.wait()

            while self._indices[self._read_idx] != idx:
                async with self._read_idx_cond:
                    await self._read_idx_cond.wait()

            write_data = buffer[:seg_size]
            del buffer[:seg_size]
            self._target_conn.writer.write(write_data)
            await self._target_conn.writer.drain()

            self._read_idx = (self._read_idx + 1) % len(self._indices)
            async with self._read_idx_cond:
                self._read_idx_cond.notify_all()

    async def _read_upstream_loop(self):

        while self._target_conn is None or not self._all_connections:
            async with self._write_ready_cond:
                await self._write_ready_cond.wait()

        idx = self._indices[0]
        conn = self._connections[idx]

        while True:
            data = await self._target_conn.reader.read(READ_SIZE)
            if len(data) == 0:
                raise EOFError()

            conn.writer.write(data)
            await conn.writer.drain()

    async def add_connecion(self, conn, idx):
        if idx in self._connections or self._all_connections:
            conn.writer.close()
            await conn.writer.wait_closed()
            return

        self._connections[idx] = conn
        self._indices.append(idx)
        self._indices.sort()

        if not self._run_task:
            self._run_task = asyncio.create_task(self._run())

        task = asyncio.create_task(self._read_downstream_loop(idx))
        self._conn_tasks[idx] = task

        try:
            logger.info("Added connection {}".format(idx))
            await task
        except Exception:
            await self._cancel_all()
            raise

    async def _cancel_all(self):
        end_tasks = []
        if self._target_conn:
            self._target_conn.writer.close()
            end_tasks.append(self._target_conn.writer.wait_closed())
            self._target_conn = None

        for task in self._conn_tasks.values():
            task.cancel()

        self._conn_tasks.clear()

        for conn in self._connections.values():
            conn.writer.close()
            end_tasks.append(conn.writer.wait_closed())

        self._connections.clear()

        if self._run_task:
            self._run_task.cancel()

        self._run_task = None

        await asyncio.gather(*end_tasks)

    async def close(self):
        await self._cancel_all()

    async def _run(self):
        tasks = [
            asyncio.create_task(self._connect_target()),
            asyncio.create_task(self._read_upstream_loop()),
        ]

        task = asyncio.gather(*tasks)

        try:
            logger.info("Starting proxy session")
            await task
        except Exception:
            task.cancel()
            await self._cancel_all()
            logger.info("Stopping proxy session")
            raise


class Upstream:
    def __init__(self, listen_host, listen_port, target_host, target_port):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port

        self._server = None
        self._sessions = {}

    async def _connected(self, reader, writer):
        try:
            # get connection data
            data = await reader.readexactly(ConnectionId.size)
            id_, idx = ConnectionId.unpack(data)

        except (OSError, EOFError, struct.error):
            writer.close()
            await writer.wait_closed()
            return

        if id_ in self._sessions:
            sess = self._sessions[id_]
        else:
            sess = UpstreamSession(id_, self.target_host, self.target_port)
            self._sessions[id_] = sess

        # Run session
        conn = Connection(reader, writer)

        try:
            await sess.add_connecion(conn, idx)
        except (OSError, EOFError, struct.error):
            pass
        finally:
            if id_ in self._sessions:
                del self._sessions[id_]

    async def start(self):
        logger.info("Starting proxy server")
        self._server = await asyncio.start_server(
            self._connected, self.listen_host, self.listen_port
        )
        try:
            await self._server.serve_forever()
        finally:
            logger.info("Stopping proxy server")
            self._server.close()
            await self._server.wait_closed()
            for sess in self._sessions.values():
                await sess.close()

            logger.info("Server stopped")
