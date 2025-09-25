import asyncio
import json
import logging

from .channel import Channel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def run_node(session):
    channel = Channel()
    await channel.start()
    log.info("[run_node] Channel started")

    await channel.publish_data(session, json.dumps({"op": "none"}))
    log.info(f"[run_node] Sent none to {session}")

    async def f(msg):
        log.info(f"[callback] [Echo {session}] Step: {msg}")
        step = json.loads(msg.data)
        data = json.dumps(step)
        log.info(f"[callback] [Echo {session}] Step->data ({len(data)} bytes): {data}")

        await channel.publish_data(session, data)
        log.info(f"[callback] [Echo {session}] Published")

    subscribe = await channel.subscribe_step(session, f)
    log.info(f"[run_node] Subscribed for {session}")

    try:
        log.info("[run_node] Waiting for events")
        await asyncio.Future()
    finally:
        await subscribe.unsubscribe()
        log.info(f"[run_node] Unsubscribed for {session}")
        await channel.close()
        log.info("[run_node] Channel closed")


async def main():
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    session = metadata["uuid"]
    log.info(f"[main] Loaded session {session}")

    try:
        log.info("[main] [Node] Running")
        await run_node(session)
        log.info("[main] [Node] Stopped")
    except Exception as e:
        log.execption(f"[main] [Exception]")


if __name__ == "__main__":
    asyncio.run(main())
