import asyncio
import json
import logging

import nats

from server import channel, runner

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("remote")


async def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())
    log.info(f"[main] meta={meta}")

    session = meta["session"]

    ch = channel.Channel()
    await ch.start()
    log.info(f"[main] channel start")

    sub = await ch.subscribe_step(session, start=1)
    log.info(f"[main] sub subscribe")

    try:
        log.info(f"[main] model start")
        with runner.context() as context:
            while True:
                log.info(f"[main] recv")
                data = context.data()
                log.info(f"[main] recv data={data}")

                # model: remote from step
                while True:
                    try:
                        # stop after message obtained, otherwise retry
                        msg = await sub.next_msg(timeout=1.0)
                        log.info(f"[main] model msg={msg}")
                        step = json.loads(msg.data)
                        break
                    except nats.errors.TimeoutError:
                        log.info(f"[main] model timeout retry")

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
