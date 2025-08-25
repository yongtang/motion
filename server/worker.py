import argparse
import asyncio
import contextlib
import io
import json
import logging
import os
import shutil
import tempfile
import uuid
import zipfile

import aiohttp.web
import nats

from .channel import Channel
from .storage import storage_kv_get, storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


@contextlib.asynccontextmanager
async def run_done(session: str):
    scene = json.loads(storage_kv_get("session", f"{session}.json"))["scene"]
    log.info(f"[run_done]: session={session}, scene={scene}")

    shutil.rmtree("/storage/node", ignore_errors=True)
    os.makedirs("/storage/node/scene", exist_ok=True)

    buffer = storage_kv_get("scene", f"{scene}.zip")
    with zipfile.ZipFile(io.BytesIO(buffer)) as zf:
        zf.extractall("/storage/node/scene")
    log.info(f"[run_done]: uncompress scene")

    with open("/storage/node/session.json", "w") as f:
        f.write(json.dumps({"session": session}))
    log.info(f"[run_done]: session storage")

    with open("/storage/node/scene/meta.json", "r") as f:
        meta = json.loads(f.read())
    runtime = meta["runtime"]
    log.info(f"[run_done]: runtime={runtime}")

    scope = os.environ.get("SCOPE")
    project = f"{scope}-motion" if scope else "motion"
    service = f"node-{runtime}"

    try:
        yield runtime
    finally:
        done = ["docker", "compose", "-p", project, "rm", "-f", "-s", service]
        log.info(f"[run_done]: done={done}")
        proc = await asyncio.create_subprocess_exec(*done, env={**os.environ})

        await proc.wait()
        shutil.rmtree("/storage/node", ignore_errors=True)
        log.info(f"[run_done]: done")


@contextlib.asynccontextmanager
async def run_node(runtime: str):
    scope = os.environ.get("SCOPE")
    project = f"{scope}-motion" if scope else "motion"
    service = f"node-{runtime}"
    node = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        "/app/docker/docker-compose.yml",
        "up",
        "--no-deps",
        "--force-recreate",
        service,
    ]
    log.info(f"[run_node]: node={node}")
    proc = await asyncio.create_subprocess_exec(*node, env={**os.environ})

    try:
        yield proc
    finally:
        log.info(f"[run_node]: node")


@contextlib.asynccontextmanager
async def run_data(session: str):
    with tempfile.NamedTemporaryFile(prefix=f"{session}-", suffix=".json") as f:
        log.info(f"[run_data] temp={f.name}")
        channel = Channel()
        await channel.start()
        sub = await channel.subscribe_data(session, start=1)
        log.info(f"[run_data] session={session} channel start")

        try:
            yield
        finally:
            while True:
                try:
                    m = await asyncio.wait_for(sub.messages.__anext__(), timeout=30.0)
                except (asyncio.TimeoutError, StopAsyncIteration):
                    break

                f.write(m.data + b"\n")
                await m.ack()

            await sub.unsubscribe()

            f.seek(0)
            storage_kv_set("data", f"{session}.json", f.read())
            log.info(f"[run_data] uploaded s3://data/{session}.json")

            await channel.close()
            log.info(f"[run_data] session={session} channel close")


async def session_play(session: str):
    log.info(f"[session_play] session={session} play")
    async with run_done(session) as runtime:
        async with run_data(session) as session:
            async with run_node(runtime) as proc:
                await proc.wait()
    log.info(f"[session_play] session={session} done")


async def session_stop(session: str):
    log.info(f"[session_stop] session={session} stop")

    with open("/storage/node/scene/meta.json", "r") as f:
        meta = json.loads(f.read())
    runtime = meta["runtime"]

    scope = os.environ.get("SCOPE")
    project = f"{scope}-motion" if scope else "motion"
    service = f"node-{runtime}"

    stop = [
        "docker",
        "compose",
        "-p",
        project,
        "stop",
        service,
    ]
    log.info(f"[session_stop] stop={stop}")
    proc = await asyncio.create_subprocess_exec(*stop, env={**os.environ})

    await proc.wait()

    log.info(f"[session_stop] stop={stop}")
    log.info(f"[session_stop] session={session} done")


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
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", 9999)
    await site.start()
    log.info("HTTP health at http://0.0.0.0:9999/health")
    try:
        yield
    finally:
        await runner.cleanup()
        log.info("HTTP server stopped")


async def run_nats(node: str):
    """
    Subjects:
      motion.node.<node>.play  (payload: "<session>")
      motion.node.<node>.stop  (payload: "<session>")
    """
    channel = Channel(servers="nats://127.0.0.1:4222")
    await channel.start()

    sub_play = await channel.subscribe_play(node)
    sub_stop = await channel.subscribe_stop(node)
    log.info(f"NATS worker ready for node={node}")

    try:
        while True:
            log.info(f"[run_nats]: play fetch")
            try:
                msg = (await sub_play.fetch(batch=1, timeout=60))[0]
            except (nats.errors.TimeoutError, IndexError):
                continue
            session = msg.data.decode(errors="ignore").strip()
            log.info(f"[run_nats]: play session={session}")
            if not session:
                await msg.ack()
                continue
            task = asyncio.create_task(session_play(session), name=f"{session}")
            await msg.ack()
            log.info(f"[run_nats]: play session={session} ack")

            while True:
                log.info(f"[run_nats]: task check")
                if task.done():
                    break
                log.info(f"[run_nats]: stop fetch")
                try:
                    msg = (await sub_stop.fetch(batch=1, timeout=1))[0]
                except (nats.errors.TimeoutError, IndexError):
                    continue
                session = msg.data.decode(errors="ignore").strip()
                log.info(f"[run_nats]: stop session={session}")
                if session != task.get_name():
                    await msg.ack()
                    continue
                await session_stop(session)
                await msg.ack()
                break

            # wait until task fully completed
            try:
                await task
            except Exception:
                log.exception(f"[run_nats]: task session={session}")
            log.info(f"[run_nats]: done session={session}")
    finally:
        await channel.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uuid", help="Node UUID")
    args = parser.parse_args()

    node = (
        uuid.UUID(args.uuid)
        if args.uuid
        else uuid.UUID(open("/etc/machine-id").read().strip())
    )

    log.info(f"Registering node {node}")
    storage_kv_set("node", f"{node}.json", "{}".encode("utf-8"))

    async with run_http():
        await run_nats(str(node))


if __name__ == "__main__":
    asyncio.run(main())
