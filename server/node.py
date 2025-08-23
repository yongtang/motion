import asyncio
import json
import logging
import os

import aiohttp.web

from .channel import Channel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("node")


async def node_http():
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
        await asyncio.Future()
    finally:
        await runner.cleanup()
        log.info("HTTP server stopped")


async def node_main(channel: Channel, session: str):
    async with asyncio.timeout(15):
        for i in range(30):
            data = json.dumps({"session": session, "count": i})
            log.info(f"Publish {data}...")
            await channel.publish_data(session, data)
            await asyncio.sleep(1)


async def main():
    with open(os.path.join("/storage/node", "session.json"), "rb") as f:
        data = json.loads(f.read())
    session = data["session"]

    channel = Channel()
    await channel.start()
    try:
        await asyncio.gather(
            node_http(),
            node_main(channel, session),
        )
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
