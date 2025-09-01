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
    step = json.loads(msg.data)
    data = json.dumps(step)
    log.info(f"[echo] step->data session={session}: {data[:120]!r}")

    await channel.publish_data(session, data)


async def main():
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())

    session = metadata["session"]
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
