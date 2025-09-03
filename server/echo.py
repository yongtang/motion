import asyncio
import functools
import json
import logging

from .channel import Channel
from .node import run_data, run_http, run_link, run_step

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("echo")


def f_step(metadata, channel):
    log.info(f"[echo] step: {metadata}")

    session = metadata["session"]

    async def run_echo(msg):
        log.info(f"[echo] step session={session}: {msg}")
        step = json.loads(msg.data)
        data = {k: v for k, v in step.items() if k != "session"}
        log.info(f"[echo] step->data session={session}: {data}")

        await run_data(session=session, channel=channel, data=data, callback=None)

    return run_echo


async def main():
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())

    session = metadata["session"]
    async with run_http():
        async with run_link() as channel:
            async with run_step(
                session=session,
                channel=channel,
                callback=f_step(metadata=metadata, channel=channel),
            ) as subscribe:
                await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
