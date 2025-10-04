import contextlib
import logging
import os

import zmq
import zmq.asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Interface:
    """
    ZMQ DEALER wrapper for runner communication.

    Depending on constructor:
      - sync=False (async mode):
          * tick=False -> exposes send()/recv()
          * tick=True  -> exposes tick()
      - sync=True  (pure sync mode; no asyncio anywhere):
          * tick is always False -> exposes send()/recv()

    All instances expose ready() and close() (bound to closures).
    """

    def __init__(self, tick: bool = False, sync: bool = False):
        assert not (sync and tick), "sync=True cannot be used with tick=True"

        self._file_ = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
        self._mode_ = b"TICK" if tick else b"NORM"

        # ZMQ DEALER
        if sync:
            self._context_ = zmq.Context.instance()
        else:
            self._context_ = zmq.asyncio.Context.instance()
        self._sock_ = self._context_.socket(zmq.DEALER)

        # Keep local queues tiny so pressure is visible immediately
        self._sock_.setsockopt(zmq.SNDHWM, 1)
        self._sock_.setsockopt(zmq.RCVHWM, 1)

        self._sock_.connect(f"ipc://{self._file_}")
        log.info(f"[Interface] zmq connect addr=ipc://{self._file_}")

        self._available_ = False

        # --- define all closures once ---
        async def f_async_tick(data: bytes):
            await self._sock_.send_multipart([b"", data])
            _, step = await self._sock_.recv_multipart()
            return step

        async def f_async_send(data: bytes):
            try:
                await self._sock_.send_multipart([b"", data], flags=zmq.DONTWAIT)
            except zmq.Again:
                log.info(f"[run_node] zmq drop (busy)")

        async def f_async_recv():
            _, step = await self._sock_.recv_multipart()
            return step

        async def f_async_ready(timeout: float = 2.0, max: int = 300):
            """Ping/pong loop, then send __MODE__ exactly once (async)."""
            poller = zmq.asyncio.Poller()
            poller.register(self._sock_, zmq.POLLIN)

            log.info(f"[wait_ready(async)] start timeout={timeout}s max={max}")
            for i in range(max):
                with contextlib.suppress(zmq.Again):
                    await self._sock_.send_multipart(
                        [b"", b"__PING__"], flags=zmq.NOBLOCK
                    )

                events = dict(await poller.poll(int(timeout * 1000)))
                if self._sock_ in events and (events[self._sock_] & zmq.POLLIN):
                    _, msg = await self._sock_.recv_multipart()
                    if msg == b"__PONG__":
                        log.info("[wait_ready(async)] ready")
                        if not self._available_:
                            await self._sock_.send_multipart(
                                [b"", b"__MODE__", self._mode_]
                            )
                            self._available_ = True
                            log.info(f"[run_node] sent mode {self._mode_.decode()}")
                        return
                if (i + 1) % 10 == 0:
                    log.info(f"[wait_ready(async)] still waiting… tries={i+1}")
            raise TimeoutError(
                f"ZMQ server not ready after {max} tries (timeout={timeout}s each)"
            )

        async def f_async_close():
            log.info("[run_node] zmq close")
            self._sock_.close(0)

        def f_sync_ready(timeout: float = 2.0, max: int = 300):
            """Ping/pong loop, then send __MODE__ exactly once (blocking)."""
            poller = zmq.Poller()
            poller.register(self._sock_, zmq.POLLIN)

            log.info(f"[wait_ready(sync)] start timeout={timeout}s max={max}")
            for i in range(max):
                with contextlib.suppress(zmq.Again):
                    # May queue until server binds (we do not set IMMEDIATE)
                    self._sock_.send_multipart([b"", b"__PING__"], flags=zmq.NOBLOCK)

                events = dict(poller.poll(int(timeout * 1000)))
                if self._sock_ in events and (events[self._sock_] & zmq.POLLIN):
                    _, msg = self._sock_.recv_multipart()
                    if msg == b"__PONG__":
                        log.info("[wait_ready(sync)] ready")
                        if not self._available_:
                            self._sock_.send_multipart([b"", b"__MODE__", self._mode_])
                            self._available_ = True
                            log.info(f"[run_node] sent mode {self._mode_.decode()}")
                        return
                if (i + 1) % 10 == 0:
                    log.info(f"[wait_ready(sync)] still waiting… tries={i+1}")
            raise TimeoutError(
                f"ZMQ server not ready after {max} tries (timeout={timeout}s each)"
            )

        def f_sync_close():
            log.info("[run_node] zmq close")
            self._sock_.close(0)

        def f_sync_send(data: bytes):
            try:
                self._sock_.send_multipart([b"", data], flags=zmq.DONTWAIT)
            except zmq.Again:
                log.info("[run_node] zmq drop (busy)")

        def f_sync_recv():
            with contextlib.suppress(zmq.Again):
                _, step = self._sock_.recv_multipart(flags=zmq.NOBLOCK)
                return step
            return None

        # --- bind depending on mode ---
        if sync:
            # sync mode: expose ready/send/recv/close; no tick
            self.ready = f_sync_ready
            self.send = f_sync_send
            self.recv = f_sync_recv
            self.close = f_sync_close
        else:
            # async mode
            self.ready = f_async_ready
            self.close = f_async_close
            if tick:
                self.tick = f_async_tick
            else:
                self.send = f_async_send
                self.recv = f_async_recv
