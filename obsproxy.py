#!/usr/bin/env python3
import argparse
import asyncio
import logging
import sys
import time
from asyncio import CancelledError

logger = logging.getLogger(__name__)


class DisconnectError(Exception):
    pass


class Proxy:
    def __init__(self, downstream_reader, downstream_writer):
        self._downstream_reader = downstream_reader
        self._downstream_writer = downstream_writer
        self._upstream_reader = None
        self._upstream_writer = None

        self._buffer = bytearray()
        self._lock = asyncio.Lock()
        self._condition = asyncio.Condition()

        self._last_read_ct = 0
        self._last_write_ct = 0
        self._avg_bytes_sec = 0

    async def _connect_to_upstream(self, host, port):
        reader, writer = await asyncio.open_connection(host, port)
        self._upstream_reader = reader
        self._upstream_writer = writer

    async def _downstream_read_loop(self):
        while True:
            # Read data from downstream socket
            data = await self._downstream_reader.read(512)

            if len(data) == 0:
                raise DisconnectError()

            # Place into buffer
            async with self._lock:
                self._buffer.extend(data)
                self._last_read_ct += len(data)

            # Notify upstream loop that buffer has data
            async with self._condition:
                self._condition.notify(1)

    async def _upstream_write_loop(self):
        while True:
            # wait for buffer to have data
            async with self._condition:
                await self._condition.wait()

            async with self._lock:
                buf_len = len(self._buffer)

            if buf_len == 0:
                continue

            # Send data in the buffer as fast as we can
            while buf_len > 0:
                async with self._lock:
                    buf_len = len(self._buffer)
                    if buf_len == 0:
                        continue

                    # write some data
                    data = self._buffer[:512]

                    self._upstream_writer.write(data)

                    del self._buffer[: len(data)]

                await self._upstream_writer.drain()
                self._last_write_ct += len(data)

                async with self._lock:
                    buf_len = len(self._buffer)

    async def _upstream_read_loop(self):
        # Read from upstream and write to downstream
        while True:
            data = await self._upstream_reader.read(512)

            if len(data) == 0:
                raise DisconnectError()

            self._downstream_writer.write(data)
            await self._downstream_writer.drain()

    async def _report_loop(self):
        last_time = int(round(time.time() * 1000))
        loop_ct = 0
        while True:
            await asyncio.sleep(1)
            loop_ct += 1
            now = int(round(time.time() * 1000))
            diff = now - last_time
            last_time = now

            if self._last_read_ct == 0:
                avg_read = 0
            else:
                avg_read = self._last_read_ct / (diff / 1000)

            if self._last_write_ct == 0:
                avg_write = 0
            else:
                avg_write = self._last_write_ct / (diff / 1000)

            self._last_read_ct = 0
            self._last_write_ct = 0
            self._avg_bytes_sec = avg_read

            buf_size = len(self._buffer)
            if avg_read == 0:
                buf_time = 0
            else:
                buf_time = buf_size / avg_read

            if loop_ct == 5:
                loop_ct = 0
                logger.info(
                    "Recv: {:.2f} kbps, Send: {:.2f} kbps, Buffer: {:.2f} kB / {:.1f} s".format(
                        avg_read / 1000 * 8,
                        avg_write / 1000 * 8,
                        buf_size / 1000,
                        buf_time,
                    )
                )

                if buf_time >= 5:
                    logger.warning("The buffer is very large!")

    async def run(self, host, port):
        # connect to upstream
        logger.info("Connecting to {}:{}".format(host, port))
        await self._connect_to_upstream(host, port)

        async def msg():
            logger.info("Proxy started")

        coros = [
            self._upstream_write_loop(),
            self._upstream_read_loop(),
            self._downstream_read_loop(),
            msg(),
            self._report_loop(),
        ]
        tasks = [asyncio.create_task(c) for c in coros]
        gather = asyncio.gather(*tasks)

        try:
            await gather
        except DisconnectError:
            # A read operation returned 0 bytes (EOF)
            logger.info("Disconnected, stopping")
            [t.cancel() for t in tasks]
            await self._stop()
        except CancelledError:
            # Task was cancelled
            [t.cancel() for t in tasks]
            await self._stop()
            raise
        except KeyboardInterrupt:
            # Ctrl-C
            [t.cancel() for t in tasks]
            await self._stop()
            raise
        except IOError:
            # IO error, disconnect
            logger.info("Disconnected, stopping")
            [t.cancel() for t in tasks]
            await self._stop()

    async def _stop(self):
        async def closer(obj):
            if obj:
                obj.close()
                await obj.wait_closed()

        await asyncio.gather(
            closer(self._upstream_writer), closer(self._downstream_writer)
        )


# Run a server to accept connections and start Proxy instances to handle them
async def run_proxy(listen_host, listen_port, upstream_host, upstream_port):
    async def conn_handler(reader, writer):
        proxy = Proxy(reader, writer)
        try:
            await proxy.run(upstream_host, upstream_port)
        finally:
            logger.info("Proxy stopped")

    server = await asyncio.start_server(conn_handler, listen_host, listen_port)

    async with server:
        logger.info("Listening on {}:{}".format(listen_host, listen_port))
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OBS buffering proxy")
    parser.add_argument(
        "--listen-host", type=str, default="127.0.0.1", help="the host to listen on"
    )
    parser.add_argument(
        "--listen-port", type=int, default=1935, help="the port to listen on"
    )
    parser.add_argument(
        "--upstream-host", type=str, help="the stream service host", required=True
    )
    parser.add_argument(
        "--upstream-port", type=int, default=1935, help="the stream service port"
    )

    args = parser.parse_args()

    log_fmt = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(log_fmt)
    root_logger = logging.getLogger()
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)

    logger.info("Starting proxy")
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(
            run_proxy(
                args.listen_host,
                args.listen_port,
                args.upstream_host,
                args.upstream_port,
            )
        )
    except KeyboardInterrupt:
        all_tasks = asyncio.gather(*asyncio.Task.all_tasks(), return_exceptions=True)
        all_tasks.cancel()

        try:
            loop.run_until_complete(all_tasks)
        except CancelledError:
            pass

    sys.exit(0)
