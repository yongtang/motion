import logging

import nats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("message")


async def message_pub(sub, data):
    log.info("Connect")
    nc = await nats.connect("nats://127.0.0.1:4222")
    log.info(f"Publish {sub} {data}")
    await nc.publish(sub, data.encode())
    log.info("Dain")
    await nc.drain()
    log.info("Close")
