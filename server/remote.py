import argparse
import asyncio
import json
import logging
import uuid

from server import channel, runner

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--session", type=uuid.UUID)
    parser.add_argument("--tick", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    session, tick = str(args.session), bool(args.tick)

    log.info(f"[main] session={session} tick={tick}")

    q = asyncio.Queue()

    async def f(msg):
        log.info(f"[main] msg={msg}")
        await q.put(msg.data)

    ch = channel.Channel()
    await ch.start()
    log.info(f"[main] channel start")

    sub = await ch.subscribe_step(session, f)
    log.info(f"[main] sub subscribe")

    try:
        log.info(f"[main] model start")
        with runner.context() as context:
            while True:
                log.info(f"[main] recv")
                data = context.data()
                log.info(f"[main] recv data={data}")

                await asyncio.sleep(0.1)

                # model: remote from step
                # Wait for at least one message
                step = await q.get()
                # Flush the queue to keep latest
                try:
                    while True:
                        step = q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                step = json.loads(step)
                log.info(f"[main] model data={data} step={step}")

                context.step(step)
                log.info(f"[main] send step={step}")
    finally:
        log.info(f"[main] sub unsubscribe")
        await sub.unsubscribe()
        log.info(f"[main] channel close")
        await ch.close()


if __name__ == "__main__":
    asyncio.run(main())
