import asyncio
import contextlib
import logging
import os
import socket
import time

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Interface:
    """
    Unix domain socket wrapper (no ZMQ, no ping).

    Depending on constructor:
      - sync=False (async mode):
          * tick=False -> exposes send()/recv()
          * tick=True  -> exposes tick()
      - sync=True  (pure sync mode; no asyncio anywhere):
          * tick is always False -> exposes send()/recv()

    All instances expose ready() and close() (bound to closures).
    """

    def __init__(self, tick: bool, sync: bool):
        assert not (sync and tick), "sync=True cannot be used with tick=True"

        self._file_ = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
        self._sock_ = None  # set in ready()

        async def f_async_sock() -> socket.socket:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            sock.setblocking(False)
            loop = asyncio.get_running_loop()
            await loop.sock_connect(sock, self._file_)
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024)
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
            return sock

        def f_sync_sock() -> socket.socket:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
            # small buffers -> backpressure visible immediately
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024)
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)
            return sock

        # ---------------- async closures ----------------
        async def f_async_ready(timeout: float = 2.0, max: int = 300):
            """Just connect; connection success == ready."""
            log.info(f"[ready(async)] connect addr=ipc://{self._file_}")
            for i in range(max):
                try:
                    self._sock_ = await asyncio.wait_for(
                        f_async_sock(), timeout=timeout
                    )
                    log.info("[ready(async)] connected")
                    return
                except Exception as e:
                    if (i + 1) % 10 == 0:
                        log.info(f"[ready(async)] still waiting… tries={i+1}")
                    await asyncio.sleep(timeout)
            else:
                raise TimeoutError(f"Unix socket server not ready: {e}")

        async def f_async_close():
            log.info("[interface] close(async)")
            if self._sock_ is not None:
                with contextlib.suppress(Exception):
                    self._sock_.close()

        async def f_async_tick(data: bytes) -> bytes:
            loop = asyncio.get_running_loop()
            await loop.sock_sendall(self._sock_, data)
            data = await loop.sock_recv(self._sock_, 1024 * 1024)
            if not data:
                raise ConnectionAbortedError("Socket closed by peer (EOF)")
            return data

        async def f_async_send(data: bytes):
            # best-effort; drop if busy
            try:
                self._sock_.send(data, socket.MSG_DONTWAIT)
            except (BlockingIOError, InterruptedError):
                log.info("[run_node] socket drop (busy)")

        async def f_async_recv() -> bytes:
            loop = asyncio.get_running_loop()
            data = await loop.sock_recv(self._sock_, 1024 * 1024)
            if not data:
                raise ConnectionAbortedError("Socket closed by peer (EOF)")
            return data

        # ---------------- sync closures -----------------
        def f_sync_ready(timeout: float = 2.0, max: int = 300):
            """Just connect; connection success == ready."""
            log.info(f"[ready(sync)] connect addr=ipc://{self._file_}")
            for i in range(max):
                try:
                    self._sock_ = f_sync_sock()
                    self._sock_.settimeout(timeout)
                    self._sock_.connect(self._file_)
                    log.info("[ready(sync)] connected")
                    return
                except Exception as e:
                    if self._sock_ is not None:
                        with contextlib.suppress(Exception):
                            self._sock_.close()
                    if (i + 1) % 10 == 0:
                        log.info(f"[ready(sync)] still waiting… tries={i+1}")
                    # backoff
                    with contextlib.suppress(Exception):
                        time.sleep(timeout)
            else:
                raise TimeoutError(f"Unix socket server not ready: {e}")

        def f_sync_close():
            log.info("[interface] close(sync)")
            if self._sock_ is not None:
                with contextlib.suppress(Exception):
                    self._sock_.close()

        def f_sync_send(data: bytes):
            # best-effort non-blocking send; drop if would block
            try:
                self._sock_.send(data, socket.MSG_DONTWAIT)
            except (BlockingIOError, InterruptedError, TimeoutError):
                log.info("[run_node] socket drop (busy)")

        def f_sync_recv():
            # non-blocking poll; return None if nothing
            with contextlib.suppress(BlockingIOError, InterruptedError, TimeoutError):
                data = self._sock_.recv(1024 * 1024, socket.MSG_DONTWAIT)
                if not data:
                    raise ConnectionAbortedError("Socket closed by peer (EOF)")
                return data
            return None

        # --- bind depending on mode ---
        if sync:
            self.ready = f_sync_ready
            self.send = f_sync_send
            self.recv = f_sync_recv
            self.close = f_sync_close
        else:
            self.ready = f_async_ready
            self.close = f_async_close
            if tick:
                self.tick = f_async_tick
            else:
                self.send = f_async_send
                self.recv = f_async_recv
