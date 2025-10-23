import argparse
import asyncio
import json
import logging
import uuid

from server import channel, runner

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def run_norm(context, session, ch):
    async def f(msg):
        log.info(f"[main] msg={msg}")

        step = json.loads(msg.data)
        log.info(f"[main] model step={step}")

        context.step(step)

        log.info(f"[main] send step={step}")

    sub = await ch.subscribe_step(session, f)
    log.info(f"[main] sub subscribe")

    try:
        # No data recv
        log.info(f"[main] model start")
        await asyncio.Future()
    finally:
        log.info(f"[main] sub unsubscribe")
        await sub.unsubscribe()


async def run_tick(context, session, ch):
    q = asyncio.Queue()

    async def f(msg):
        log.info(f"[main] msg={msg}")
        await q.put(msg.data)

    sub = await ch.subscribe_step(session, f)
    log.info(f"[main] sub subscribe")

    try:
        log.info(f"[main] model start")
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


async def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--session", type=uuid.UUID)
    parser.add_argument("--tick", action=argparse.BooleanOptionalAction)

    args = parser.parse_args()

    session, tick = str(args.session), bool(args.tick)

    log.info(f"[main] session={session} tick={tick}")

    ch = channel.Channel()
    await ch.start()
    log.info(f"[main] channel start")

    try:
        with runner.context() as context:
            await (run_tick if tick else run_norm)(context, session, ch)
    finally:
        log.info(f"[main] channel close")
        await ch.close()


if __name__ == "__main__":
    asyncio.run(main())
