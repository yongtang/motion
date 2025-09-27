import contextlib
import logging
import os

import zmq
import zmq.asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("count")


class Interface:
    """
    Async wrapper around a ZMQ DEALER socket for runner communication.

    Depending on constructor `tick`:
      - tick=False -> exposes send()/recv()
      - tick=True  -> exposes tick()

    All instances expose ready() and close().
    """

    def __init__(self, tick: bool = False):
        self._file_ = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
        self._mode_ = b"TICK" if tick else b"NORM"

        # ZMQ DEALER
        self._context_ = zmq.asyncio.Context.instance()
        self._sock_ = self._context_.socket(zmq.DEALER)

        # Keep local queues tiny so pressure is visible immediately
        self._sock_.setsockopt(zmq.SNDHWM, 1)
        self._sock_.setsockopt(zmq.RCVHWM, 1)

        self._sock_.connect(f"ipc://{self._file_}")
        log.info(f"[Interface] zmq connect addr=ipc://{self._file_}")

        self._available_ = False

        # --- define all closures once ---
        async def f_tick(data: bytes):
            await self._sock_.send_multipart([b"", data])
            _, step = await self._sock_.recv_multipart()
            return step

        async def f_send(data: bytes):
            try:
                await self._sock_.send_multipart([b"", data], flags=zmq.DONTWAIT)
            except zmq.Again:
                log.info(f"[run_node] zmq drop (busy)")

        async def f_recv():
            _, step = await self._sock_.recv_multipart()
            return step

        # --- bind depending on mode ---
        if tick:
            self.tick = f_tick
        else:
            self.send = f_send
            self.recv = f_recv

    async def ready(self, timeout: float = 2.0, max: int = 300) -> None:
        """Ping/pong loop, then send __MODE__ exactly once."""
        poller = zmq.asyncio.Poller()
        poller.register(self._sock_, zmq.POLLIN)

        log.info(f"[wait_ready] start timeout={timeout}s max={max}")
        for i in range(max):
            with contextlib.suppress(zmq.Again):
                # May queue until server binds (we do not set IMMEDIATE)
                await self._sock_.send_multipart([b"", b"__PING__"], flags=zmq.NOBLOCK)

            events = dict(await poller.poll(int(timeout * 1000)))
            if self._sock_ in events and events[self._sock_] & zmq.POLLIN:
                _, msg = await self._sock_.recv_multipart()
                if msg == b"__PONG__":
                    log.info(f"[wait_ready] ready")
                    if not self._available_:
                        await self._sock_.send_multipart(
                            [b"", b"__MODE__", self._mode_]
                        )
                        self._available_ = True
                        log.info(f"[run_node] sent mode {self._mode_.decode()}")
                    return

            if (i + 1) % 10 == 0:
                log.info(f"[wait_ready] still waiting… tries={i+1}")

        raise TimeoutError(
            f"ZMQ server not ready after {max} tries (timeout={timeout}s each)"
        )

    async def close(self) -> None:
        log.info(f"[run_node] zmq close")
        self._sock_.close(0)
