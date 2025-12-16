import asyncio
import contextlib
import json
import logging
import os
import socket

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def context():
    log.info("[runner] file")
    file = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
    with contextlib.suppress(FileNotFoundError):
        os.unlink(file)
    log.info("[runner] file unlink")

    log.info("[runner] sock")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    # keep buffers modest so backpressure is visible quickly
    with contextlib.suppress(Exception):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024)
    with contextlib.suppress(Exception):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)

    sock.bind(file)
    sock.listen(1)
    sock.setblocking(False)

    log.info("[runner] sock ready")

    loop = asyncio.get_running_loop()

    try:

        class Context:
            async def data(self) -> dict:
                buf = await loop.sock_recv(conn, 1024 * 1024)  # 1 MiB cap for control
                if not buf:
                    return {}
                return json.loads(buf.decode())

            async def step(self, o):
                payload = json.dumps(o).encode()
                await loop.sock_sendall(conn, payload)

        conn, _ = await loop.sock_accept(sock)
        conn.setblocking(False)
        try:
            yield Context()
        finally:
            with contextlib.suppress(Exception):
                conn.close()

    finally:
        log.info("[runner] sock close")
        with contextlib.suppress(Exception):
            sock.close()
        log.info("[runner] file unlink")
        with contextlib.suppress(FileNotFoundError):
            os.unlink(file)
