import asyncio
import contextlib
import itertools
import json
import logging

from .channel import Channel
from .interface import Interface

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("count")


async def run_tick(session: str, interface: Interface, channel: Channel):
    """
    Strict 1->1: on each Channel step, send to ZMQ and wait for one reply, then publish back.
    """

    for i in itertools.count():
        data = json.dumps({"joint": {"count": i}}, sort_keys=True).encode()

        log.info(f"[run_tick] channel send ({data})")
        await channel.publish_data(session, data)

        log.info(f"[run_tick] zmq send ({data})")
        step = await interface.tick(data)
        log.info(f"[run_tick] zmq recv ({step})")

        await asyncio.sleep(1)


async def run_norm(session: str, interface: Interface, channel: Channel):
    """
    Streaming: callback just sends; one background receiver publishes replies.
    One client <-> one server => reply order matches send order; no pairing needed.

    In norm mode, drop on pressure: non-blocking send, ignore if ZMQ pipe is full.
    """

    async def f(msg):
        data = msg.data
        log.info(f"[run_node] zmq send ({data})")
        # Non-blocking send: if ROUTER/pipe is full (HWM), drop this update
        await interface.send(data)

    sub = await channel.subscribe_step(session, f)

    async def g():
        while True:
            step = await interface.recv()
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

    # ZMQ DEALER (encapsulated by Interface)
    interface = Interface(tick=tick)

    # Channel
    channel = Channel()
    await channel.start()
    log.info(f"[run_node] channel start")

    # Wait for ROUTER to be ready (server has __PING__/__PONG__ built-in)
    # Send mode exactly once; runner requires it before first real payload
    await interface.ready(timeout=2.0, max=300)
    log.info(f"[run_node] zmq ready")

    try:
        if tick:
            await run_tick(session, interface, channel)
        else:
            await run_norm(session, interface, channel)
    finally:
        log.info(f"[run_node] channel close")
        await channel.close()
        log.info(f"[run_node] zmq close")
        await interface.close()


async def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())
    log.info(f"[main] meta={meta}")

    session, tick = meta["session"], meta["tick"]
    await run_node(session, tick)


if __name__ == "__main__":
    asyncio.run(main())
