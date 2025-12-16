import asyncio
import contextlib
import logging
import os
import socket

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Interface:
    def __init__(self):
        self._file_ = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
        self._sock_ = None  # set in ready()
        print(f"[interface] init addr=ipc://{self._file_}")

    async def ready(self, timeout: float = 2.0, max: int = 300):
        print(f"[interface] connect addr=ipc://{self._file_}")

        async def f_sock() -> socket.socket:
            print("[interface] sock")
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            sock.setblocking(False)
            loop = asyncio.get_running_loop()
            print("[interface] sock connect")
            await loop.sock_connect(sock, self._file_)
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024)
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
            print("[interface] sock connected")
            return sock

        for i in range(max):
            try:
                self._sock_ = await asyncio.wait_for(f_sock(), timeout=timeout)
                print("[ready(async)] connected")
                return
            except Exception as e:
                if (i + 1) % 10 == 0:
                    print(f"[ready(async)] still waiting… tries={i+1}")
                await asyncio.sleep(timeout)
        else:
            raise TimeoutError(f"Unix socket server not ready: {e}")

    async def close(self):
        print("[interface] close")
        if self._sock_ is not None:
            with contextlib.suppress(Exception):
                self._sock_.close()

    async def tick(self, data: bytes) -> bytes:
        loop = asyncio.get_running_loop()
        await loop.sock_sendall(self._sock_, data)
        data = await loop.sock_recv(self._sock_, 1024 * 1024)
        if not data:
            raise ConnectionAbortedError("Socket closed by peer (EOF)")
        return data
