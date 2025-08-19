import asyncio
import logging
import socket

import aiohttp.web
import nats

from .channel import Channel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


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

    channel = Channel(servers="nats://127.0.0.1:4222")
    await channel.start()
    try:
        # shared durable on "motion.session"
        play_sub = await channel.subscribe_play()
        log.info("NATS worker ready")

        while True:
            # fetch one play (or none)
            try:
                msg = (await play_sub.fetch(batch=1, timeout=5))[0]
            except (nats.errors.TimeoutError, IndexError):
                continue

            # Expect payload: "<session>.play <data>"
            try:
                token, data = msg.data.decode(errors="ignore").split(" ", 1)
                session, action = token.rsplit(".", 1)
                if action != "play":
                    raise ValueError
            except ValueError:
                log.warning(f"Bad play payload: {msg.data!r}. Ack and skip.")
                await msg.ack()
                continue

            log.info(f"Play received for session={session} data={data!r}")

            # per-session stop durable on "motion.session.<session>.stop"
            stop_sub = await channel.subscribe_stop(session)

            # start resource; if we crash before this ack, play is retried elsewhere
            await session_play(session)
            await msg.ack()  # ack play only after successful start

            # wait for exactly one stop (user or resource)
            while True:
                try:
                    msg = (await stop_sub.fetch(batch=1, timeout=5))[0]
                except (nats.errors.TimeoutError, IndexError):
                    continue

                data = msg.data.decode(errors="ignore")
                log.info(f"Stop received for session={session} data={data!r}")
                await session_stop(session)
                await msg.ack()
                break

            # cleanup per-session consumer and purge lingering stop messages
            await channel.subscribe_done(session)
            log.info(f"Session {session} cleaned up")
    finally:
        await channel.close()


async def main():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(task_http(), name="http")
        tg.create_task(task_nats(), name="nats")


if __name__ == "__main__":
    asyncio.run(main())
