import logging

import nats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("message")


async def message_pub(sub: str, data: str):
    log.info("Connect")
    nc = await nats.connect("nats://127.0.0.1:4222")
    js = nc.jetstream()

    try:
        # Ensure the stream exists (idempotent if already created)
        await js.add_stream(name="motion", subjects=["motion.session"])
        log.info("Created stream")
    except nats.js.errors.APIError:
        pass

    log.info(f"Publish {sub} {data}")
    ack = await js.publish(sub, data.encode())
    log.info(f"Ack {ack.stream} {ack.seq}")

    await nc.drain()
    log.info("Close")
