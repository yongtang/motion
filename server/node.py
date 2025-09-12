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
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", 8899)
    await site.start()
    log.info("HTTP health at http://0.0.0.0:8899/health")

    try:
        yield
    finally:
        await runner.cleanup()
        log.info("HTTP server stopped")


@contextlib.asynccontextmanager
async def run_link():
    channel = Channel()
    await channel.start()
    log.info("Channel started")
    try:
        yield channel
    finally:
        await channel.close()
        log.info("Channel closed")


@contextlib.asynccontextmanager
async def run_step(session, channel, callback):
    subscribe = await channel.subscribe_step(session, callback)
    log.info(f"Sbscribed step for session={session}")
    try:
        yield subscribe
    finally:
        with contextlib.suppress(Exception):
            await subscribe.unsubscribe()
        log.info(f"Ubscribed step for session={session}")
