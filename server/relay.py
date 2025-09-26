import asyncio
import contextlib
import json
import logging
import os
import time

import zmq
import zmq.asyncio

from .channel import Channel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("relay")


async def wait_ready(
    sock: zmq.asyncio.Socket, timeout: float = 2.0, max: float | None = None
):
    """
    Send __PING__ and wait up to `timeout` seconds for __PONG__.
    Repeat until success or total elapsed time exceeds `max`.
    If max is None, wait indefinitely.
    """

    start = time.monotonic()
    poller = zmq.asyncio.Poller()
    poller.register(sock, zmq.POLLIN)

    while True:
        try:
            # May queue until server binds (we do not set IMMEDIATE)
            await sock.send_multipart([b"", b"__PING__"], flags=zmq.NOBLOCK)
        except zmq.Again:
            # Only occurs if IMMEDIATE=1; safe to ignore
            pass

        events = dict(await poller.poll(int(timeout * 1000)))
        if sock in events and events[sock] & zmq.POLLIN:
            _, msg = await sock.recv_multipart()
            if msg == b"__PONG__":
                return

        if max is not None and (time.monotonic() - start) >= max:
            raise TimeoutError(f"ZMQ server not ready within {max} seconds")


async def run_tick(channel: Channel, session: str, sock: zmq.asyncio.Socket):
    """
    Strict 1->1: on each Channel step, send to ZMQ and wait for one reply, then publish back.
    """

    async def f(msg):
        # Channel -> ZMQ
        log.info(f"[run_node] zmq send {msg}")
        data = msg.data
        """
        await sock.send_multipart([b"", data])
        _, step = await sock.recv_multipart()
        """
        step = data.decode()
        log.info(f"[run_node] zmq recv {step}")
        await channel.publish_data(session, step)
        log.info(f"[run_node] zmq loop done")

    sub = await channel.subscribe_step(session, f)
    try:
        await asyncio.Future()  # run forever
    finally:
        await sub.unsubscribe()


async def run_norm(channel: Channel, session: str, sock: zmq.asyncio.Socket):
    """
    Streaming: callback just sends; one background receiver publishes replies.
    One client <-> one server => reply order matches send order; no pairing needed.
    """

    async def f(msg):
        data = msg.data
        log.info(f"[run_node] zmq send {data}")
        await sock.send_multipart([b"", msg.data])

    sub = await channel.subscribe_step(session, f)

    async def g():
        while True:
            _, msg = await sock.recv_multipart()
            step = msg.decode()
            log.info(f"[run_node] zmq recv {step}")
            await channel.publish_data(session, step)

    task = asyncio.create_task(g())
    try:
        await asyncio.Future()
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await sub.unsubscribe()


async def run_node(session: str, tick: bool):
    log.info(f"[run_node] session={session} tick={tick}")

    file = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")

    log.info(f"[run_node] file={file}")

    # ZMQ DEALER
    context = zmq.asyncio.Context.instance()
    sock = context.socket(zmq.DEALER)
    sock.connect(f"ipc://{file}")

    log.info(f"[run_node] zmq connect")

    # Channel
    channel = Channel()
    await channel.start()
    log.info(f"[run_node] channel start")

    await channel.publish_data(session, json.dumps({"op": "none"}))  # initial seed

    log.info(f"[run_node] channel inital data")

    # Wait for ROUTER to be ready (server has __PING__/__PONG__ built-in)
    await wait_ready(sock, timeout=2.0, max=None)  # wait indefinitely by default

    log.info(f"[run_node] zmq ready")

    try:
        if tick:
            await run_tick(channel, session, sock)
        else:
            await run_norm(channel, session, sock)
    finally:
        log.info(f"[run_node] channel close")
        await channel.close()
        log.info(f"[run_node] zmq close")
        sock.close(0)


async def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())
    log.info(f"[main] meta={meta}")

    session, tick = meta["session"], meta["tick"]

    await run_node(session, tick)


if __name__ == "__main__":
    asyncio.run(main())
