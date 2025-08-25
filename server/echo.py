import asyncio
import contextlib
import json
import logging

from .channel import Channel
from .node import run_data, run_http

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("echo")


async def run_echo(session: str, channel: Channel):
    """
    Subscribe to motion.step.{session} and republish each message to motion.data.{session}.
    """
    sub = await channel.subscribe_step(session)
    log.info(f"[node] subscribed step for session={session}")

    try:
        while True:
            try:
                msgs = await sub.fetch(batch=1, timeout=5)
            except asyncio.TimeoutError:
                continue  # no message, just loop again

            if not msgs:
                continue

            msg = msgs[0]
            payload = msg.data.decode("utf-8", errors="ignore")
            log.info(f"[node] step→data session={session}: {payload[:120]!r}")

            await channel.publish_data(session, payload)
            await msg.ack()
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
        log.info(f"[node] unsubscribed step for session={session}")


async def main():
    with open("/storage/node/session.json", "r") as f:
        session = json.loads(f.read())["session"]

    async with run_http():
        async with run_data() as channel:
            await run_echo(session, channel)


if __name__ == "__main__":
    asyncio.run(main())
