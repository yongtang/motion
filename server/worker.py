import argparse
import asyncio
import io
import json
import logging
import os
import shutil
import uuid
import zipfile

import aiohttp.web
import nats

from .channel import Channel
from .storage import storage_kv_get, storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


async def session_play(session: str):
    log.info(f"Received play session: {session}")

    directory = "/storage/node"

    try:
        data = storage_kv_get("session", f"{session}.json")
        log.info(f"Session {session}: {data}")
        data = json.loads(data)
        log.info(f"Session {session}: {data}")

        scene = data["scene"]
    except FileNotFoundError:
        log.warning(f"Session {session} not valid")
        return

    try:
        data = storage_kv_get("scene", f"{scene}.json")
        log.info(f"Scene {scene}: {data}")

        data = storage_kv_get("scene", f"{scene}.zip")

        shutil.rmtree(directory, ignore_errors=True)
        log.info(f"Directory {directory} removed")

        os.makedirs(directory, exist_ok=True)
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            zf.extractall(directory)
        log.info(f"Directory {directory}: {os.listdir(directory)}")

        with open(os.path.join(directory, "meta.json"), "rb") as f:
            meta = json.loads(f.read())
        log.info(f"Scene meta: {meta}")
    except FileNotFoundError:
        log.warning("Scene %s not found", scene)
        return

    image = "python:3.12-slim"
    container = f"{os.environ['SCOPE']}-node" if os.environ["SCOPE"] else "motion-node"

    run = [
        "docker",
        "run",
        "-i",
        "--name",
        container,
        "--rm",
        "--network",
        "host",
        "-w",
        "/app",
        "-v",
        f"{os.environ['MOTION']}:/app",
        "-v",
        "shared-storage:/storage",
        "--health-cmd",
        "curl -sf http://127.0.0.1:8888/health >/dev/null || exit 1",
        "--health-interval",
        "10s",
        "--health-timeout",
        "3s",
        "--health-retries",
        "3",
        "--health-start-period",
        "15s",
        image,
        "/bin/sh",
        "-lc",
        "apt -y -qq update && apt -y -qq install curl && pip install aiohttp && python -m server.node",
    ]
    log.info(f"Proc: {run}")
    proc = await asyncio.create_subprocess_exec(*run, env={**os.environ})

    try:
        return await proc.wait()
    finally:
        run = [
            "docker",
            "rm",
            "-f",
            container,
        ]
        log.info(f"Done: {run}")
        done = await asyncio.create_subprocess_exec(*run, env={**os.environ})
        await done.wait()

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
