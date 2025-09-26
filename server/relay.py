import asyncio
import contextlib
import json
import logging
import os

import zmq
import zmq.asyncio

from .channel import Channel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("relay")


async def wait_ready(sock: zmq.asyncio.Socket, timeout: float = 2.0, max: int = 300):
    """
    Send __PING__ and wait up to `timeout` seconds for __PONG__.
    Try at most `max` times; then raise TimeoutError.
    """
    poller = zmq.asyncio.Poller()
    poller.register(sock, zmq.POLLIN)

    log.info(f"[wait_ready] start timeout={timeout}s max={max}")
    for i in range(max):
        try:
            # May queue until server binds (we do not set IMMEDIATE)
            await sock.send_multipart([b"", b"__PING__"], flags=zmq.NOBLOCK)
        except zmq.Again:
            # Only if IMMEDIATE were set; safe to ignore
            pass

        events = dict(await poller.poll(int(timeout * 1000)))
        if sock in events and events[sock] & zmq.POLLIN:
            _, msg = await sock.recv_multipart()
            if msg == b"__PONG__":
                log.info(f"[wait_ready] ready")
                return

        if (i + 1) % 10 == 0:
            log.info(f"[wait_ready] still waiting… tries={i+1}")

    raise TimeoutError(
        f"ZMQ server not ready after {max} tries (timeout={timeout}s each)"
    )


async def run_tick(channel: Channel, session: str, sock: zmq.asyncio.Socket):
    """
    Strict 1->1: on each Channel step, send to ZMQ and wait for one reply, then publish back.
    """

    async def f(msg):
        data = msg.data
        log.info(f"[run_node] zmq send ({data})")
        await sock.send_multipart([b"", data])
        _, step = await sock.recv_multipart()
        log.info(f"[run_node] zmq recv ({step})")
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
        log.info(f"[run_node] zmq send ({data})")
        await sock.send_multipart([b"", data])

    sub = await channel.subscribe_step(session, f)

    async def g():
        while True:
            _, step = await sock.recv_multipart()
            log.info(f"[run_node] zmq recv ({step})")
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
    log.info(f"[run_node] zmq connect addr=ipc://{file}")

    # Channel
    channel = Channel()
    await channel.start()
    log.info(f"[run_node] channel start")

    await channel.publish_data(
        session, json.dumps({"op": "none"}).encode()
    )  # initial seed
    log.info(f"[run_node] channel inital data")

    # Wait for ROUTER to be ready (server has __PING__/__PONG__ built-in)
    await wait_ready(sock, timeout=2.0, max=300)
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
