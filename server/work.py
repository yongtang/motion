import asyncio
import logging
import socket

import aiohttp.web
import nats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("work")


async def session_play(session: str):
    log.info(f"Received play session: {session}")


async def session_stop(session: str):
    log.info(f"Received stop session: {session}")


async def task_http():
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
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", 9999)
    await site.start()
    log.info("HTTP health at http://0.0.0.0:9999/health")

    try:
        await asyncio.Future()
    finally:
        await runner.cleanup()
        log.info("HTTP server stopped")


async def task_nats():
    async def cb(msg):
        log.info(f"Received {msg}")
        call, session = msg.data.decode().split(" ", 1)
        if call == "play":
            await session_play(session)
        elif call == "stop":
            await session_stop(session)
        else:
            raise ValueError(f"{msg}")

    while True:
        try:
            log.info("Connect")
            nc = await nats.connect("nats://127.0.0.1:4222")
            log.info("Subscribe")
            await nc.subscribe("motion.session", cb=cb)
            log.info("Future")
            await asyncio.Future()

        except (
            OSError,
            asyncio.TimeoutError,
            socket.gaierror,
            nats.errors.NoServersError,
            nats.errors.ConnectionClosedError,
            nats.errors.TimeoutError,
            UnicodeDecodeError,
            ValueError,
        ) as e:
            log.warning(f"Exception {e} (retrying in 1s)")
            try:
                await nc.drain()
                await nc.close()
            except Exception:
                pass
            log.info("Closed; will reconnect")
            await asyncio.sleep(1)


async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(task_http(), name="http")
        tg.create_task(task_nats(), name="nats")


if __name__ == "__main__":
    asyncio.run(main())
