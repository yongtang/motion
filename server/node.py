import asyncio
import logging

import aiohttp.web

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


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


async def node_main():
    async with asyncio.timeout(15):
        while True:
            print("Main process working...")
            await asyncio.sleep(1)


async def main():
    await asyncio.gather(
        node_http(),
        node_main(),
    )


if __name__ == "__main__":
    asyncio.run(main())
