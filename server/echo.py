import asyncio
import functools
import json
import logging

from .channel import Channel
from .node import run_data, run_http, run_step

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("echo")


async def f_echo(session: str, channel: Channel, msg):
    log.info(f"[echo] step session={session}: {msg}")
    data = json.loads(msg.data)
    payload = json.dumps(data)
    log.info(f"[echo] step->data session={session}: {payload[:120]!r}")

    await channel.publish_data(session, payload)


async def main():
    with open("/storage/node/session.json", "r") as f:
        session = json.loads(f.read())["session"]

    async with run_http():
        async with run_data() as channel:
            async with run_step(
                session=session,
                channel=channel,
                callback=functools.partial(f_echo, session, channel),
            ) as subscribe:
                await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
