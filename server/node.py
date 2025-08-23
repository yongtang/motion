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
    async with asyncio.timeout(150):
        for i in range(300):
            data = json.dumps({"session": session, "count": i})
            log.info(f"Publish {data}...")
            await channel.publish_data(session, data)
            await asyncio.sleep(1)


async def main():
    with open("/storage/node/session.json", "r", encoding="utf-8") as f:
        session = json.loads(f.read())["session"]

    async with run_http():
        async with run_data() as channel:
            await run_node(session, channel)


if __name__ == "__main__":
    asyncio.run(main())
