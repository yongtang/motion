import contextlib
import logging

import aiohttp.web

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def run_http(port: int):
    log.info("[run_http] Start")
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
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info(f"[run_http] Listening on :{port}")
    log.info(f"[run_http] Health: http://0.0.0.0:{port}/health")

    try:
        yield
    finally:
        await runner.cleanup()
        log.info("[run_http] Stopped")
