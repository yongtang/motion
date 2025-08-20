import argparse
import asyncio
import logging
import uuid

import aiohttp.web
import nats

from .channel import Channel
from .storage import storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


async def session_play(session: str):
    log.info(f"Received play session: {session}")
    # Simple demo: run ~100 seconds of "work"
    await asyncio.sleep(15)
    log.info(f"Session {session} done")


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


async def task_nats(node: str):
    """
    Subjects:
      motion.node.<node>.play  (payload: "<session>")
      motion.node.<node>.stop  (payload: "<session>")

    Rules:
      - Only one session may run at a time.
      - While a session is running, we only fetch from *.stop (no preemption).
      - Stop only acts if it matches the currently running session.

    Behavior change:
      - session_play() runs in a background task.
      - Loop checks once per second if the play task finished naturally.
      - If completed, we clear 'running' and return to idle without needing a stop.
    """
    channel = Channel(servers="nats://127.0.0.1:4222")
    await channel.start()

    sub_play = await channel.subscribe_play(node)
    sub_stop = await channel.subscribe_stop(node)
    log.info(f"NATS worker ready for node={node}")

    running: str | None = None
    play_task: asyncio.Task | None = None

    try:
        while True:
            # If a play task exists, see if it finished by itself.
            if play_task and play_task.done():
                try:
                    await play_task  # re-raise if it errored/cancelled
                except asyncio.CancelledError:
                    # cancelled via stop or shutdown; nothing else to do here
                    pass
                except Exception:
                    log.exception("Play task error")
                finally:
                    running = None
                    play_task = None
                    log.info("Playback completed; back to idle")

            if running is None:
                # Idle → fetch a play message
                try:
                    msg = (await sub_play.fetch(batch=1, timeout=1))[0]
                except (nats.errors.TimeoutError, IndexError):
                    continue

                session = msg.data.decode(errors="ignore").strip()
                await msg.ack()

                if not session:
                    log.warning("Empty play payload; skip")
                    continue

                log.info(f"Starting session {session}")
                # Launch playback in background so we can keep checking stop/completion
                play_task = asyncio.create_task(
                    session_play(session), name=f"play:{session}"
                )
                running = session

            else:
                # Busy → only fetch stop messages
                try:
                    msg = (await sub_stop.fetch(batch=1, timeout=1))[0]
                except (nats.errors.TimeoutError, IndexError):
                    # No stop this second; loop will re-check play_task.done()
                    continue

                session = msg.data.decode(errors="ignore").strip()
                await msg.ack()

                if not session:
                    log.warning("Empty stop payload; skip")
                    continue

                if session == running:
                    log.info(f"Stopping session {session}")
                    if play_task and not play_task.done():
                        play_task.cancel()
                        try:
                            await play_task
                        except asyncio.CancelledError:
                            pass
                    await session_stop(session)
                    running = None
                    play_task = None
                else:
                    # Stop for another session: ignore but ack so it doesn't loop
                    log.info(f"Ignoring stop for {session}; running={running!r}")

    finally:
        # Best-effort cleanup on shutdown.
        if running:
            log.info(f"Shutting down; cancelling session {running}")
            if play_task and not play_task.done():
                play_task.cancel()
                try:
                    await play_task
                except asyncio.CancelledError:
                    pass
            await session_stop(running)
        await channel.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uuid", help="Node UUID")
    args = parser.parse_args()

    # Validate node is a UUID
    node = (
        uuid.UUID(args.uuid)
        if args.uuid
        else uuid.UUID(open("/etc/machine-id").read().strip())
    )

    log.info(f"Registering node {node}")
    storage_kv_set("node", f"{node}.json", "{}".encode("utf-8"))

    async with asyncio.TaskGroup() as tg:
        tg.create_task(task_http(), name="http")
        tg.create_task(task_nats(node), name="nats")


if __name__ == "__main__":
    asyncio.run(main())
