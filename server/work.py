import asyncio
import contextlib
import functools
import json
import logging
import os
import shutil
import tempfile
import zipfile

import aiohttp.web
import nats

from .channel import Channel
from .storage import storage_kv_get, storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("worker")


@contextlib.asynccontextmanager
async def run_done(session: str):
    shutil.rmtree("/storage/node", ignore_errors=True)
    os.makedirs("/storage/node/scene", exist_ok=True)

    session = json.loads(b"".join(storage_kv_get("session", f"{session}.json")))
    with open("/storage/node/session.json", "w") as f:
        f.write(json.dumps(session))
    log.info(f"[run_done]: session storage: {session}")

    with tempfile.TemporaryFile() as f:
        for chunk in storage_kv_get("scene", f"{session['scene']}.zip"):
            f.write(chunk)
        f.seek(0)
        with zipfile.ZipFile(f) as z:
            z.extractall("/storage/node/scene")
    log.info(f"[run_done]: uncompress scene")

    with open("/storage/node/scene/meta.json", "r") as f:
        meta = json.loads(f.read())
    runtime = meta["runtime"]
    log.info(f"[run_done]: runtime={runtime}")

    scope = os.environ.get("SCOPE")
    project = f"{scope}-motion" if scope else "motion"

    try:
        yield runtime
    finally:
        done = ["docker", "compose", "-p", project, "rm", "-f", "-s", "node"]
        log.info(f"[run_done]: done={done}")
        proc = await asyncio.create_subprocess_exec(*done, env={**os.environ})

        await proc.wait()
        shutil.rmtree("/storage/node", ignore_errors=True)
        log.info(f"[run_done]: done")


@contextlib.asynccontextmanager
async def run_node(runtime: str):
    scope = os.environ.get("SCOPE")
    project = f"{scope}-motion" if scope else "motion"
    node = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        f"/app/docker/docker-compose.yml",
        "-f",
        f"/app/docker/docker-compose-{runtime}.yml",
        "up",
        "--no-deps",
        "--force-recreate",
        "node",
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
                    msg = await sub.next_msg(timeout=30.0)
                except asyncio.TimeoutError:
                    break
                f.write(msg.data + b"\n")

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

    stop = [
        "docker",
        "compose",
        "-p",
        project,
        "stop",
        "node",
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


async def run_work():
    channel = Channel(servers="nats://127.0.0.1:4222")
    await channel.start()

    async def f(session, msg):
        assert msg.subject == f"motion.node.{session}.stop"
        await session_stop(session)

    sub_play = await channel.subscribe_play()
    log.info(f"NATS work ready")

    try:
        while True:
            log.info(f"[run_work]: play fetch")
            try:
                msg = (await sub_play.fetch(batch=1, timeout=60))[0]
            except (nats.errors.TimeoutError, IndexError):
                continue
            log.info(f"[run_work]: play msg={msg}")

            assert msg.subject.startswith("motion.node.") and msg.subject.endswith(
                ".play"
            )
            session = msg.subject.removeprefix("motion.node.").removesuffix(".play")
            log.info(f"[run_work]: play session={session}")

            sub_stop = await channel.subscribe_stop(
                session, functools.partial(f, session)
            )
            try:
                log.info(f"[run_work]: play session={session}")
                await session_play(session)
                log.info(f"[run_work]: play session={session} ack")
                await msg.ack()
            except Exception as e:
                log.info(f"[run_work]: play session={session} e={e}")
                raise
            finally:
                await sub_stop.drain()
            log.info(f"[run_work]: play session={session} done")
    finally:
        await sub_play.unsubscribe()
        await channel.close()


async def main():
    async with run_http():
        await run_work()


if __name__ == "__main__":
    asyncio.run(main())
