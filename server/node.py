import asyncio
import contextlib
import json
import logging

import aiohttp.web

from .channel import Channel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("node")


@contextlib.asynccontextmanager
async def run_http():
    app = aiohttp.web.Application()
    app.add_routes(
        [
            aiohttp.web.get(
                "/health", lambda _: aiohttp.web.json_response({"status": "ok"})
            )
        ]
    )

    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", 8888)
    await site.start()
    log.info("HTTP health at http://0.0.0.0:8888/health")

    try:
        yield
    finally:
        await runner.cleanup()
        log.info("HTTP server stopped")


@contextlib.asynccontextmanager
async def run_data():
    channel = Channel()
    await channel.start()
    log.info("Channel started")
    try:
        yield channel
    finally:
        await channel.close()
        log.info("Channel closed")


async def run_node(session: str, channel: Channel):
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
            await run_node(session, channel)


if __name__ == "__main__":
    asyncio.run(main())
