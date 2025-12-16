import argparse
import asyncio
import json
import logging
import traceback
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

    queue = asyncio.Queue()

    async def f(msg):
        log.info(f"[main] message {msg}")
        step = json.loads(msg.data)
        log.info(f"[main] step {step}")
        await queue.put(step)
        log.info(f"[main] step queue")

    # Channel
    ch = channel.Channel()
    await ch.start()
    log.info(f"[main] channel start")
    subscribe = await ch.subscribe_step(session, f)
    log.info(f"[main] channel subscribe")

    try:
        async with runner.context() as context:
            while True:
                log.info(f"[main] wait")
                data = await context.data()
                log.info(f"[main] data={data}")
                step = []
                while len(step) < 1:
                    try:
                        step.append(queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                log.info(f"[main] step={step}")
                await context.step(step)
                log.info(f"[main] done")
                await asyncio.sleep(0)
    except Exception as e:
        log.info(f"[main] exception: {type(e)} {traceback.format_exc()} {e}")
        raise
    finally:
        log.info(f"[main] channel unsubscribe")
        await subscribe.unsubscribe()
        log.info(f"[main] channel close")
        await ch.close()


if __name__ == "__main__":
    asyncio.run(main())
