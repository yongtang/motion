import contextlib
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
