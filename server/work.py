import asyncio
import contextlib
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
log = logging.getLogger(__name__)


async def node_play(channel: Channel, session: str):
    shutil.rmtree("/storage/node", ignore_errors=True)
    os.makedirs("/storage/node/scene", exist_ok=True)

    data = json.loads(b"".join(storage_kv_get("session", f"{session}.json")))
    with open("/storage/node/session.json", "w") as f:
        f.write(json.dumps(data))
    log.info(f"[node_play]: session storage: {data}")

    scene = data["scene"]
    log.info(f"[node_play]: scene={scene}")

    with tempfile.TemporaryFile() as f:
        for chunk in storage_kv_get("scene", f"{scene}.zip"):
            f.write(chunk)
        f.seek(0)
        with zipfile.ZipFile(f) as z:
            z.extractall("/storage/node/scene")
    log.info(f"[node_play]: uncompress scene")

    with open("/storage/node/scene/meta.json", "r") as f:
        meta = json.loads(f.read())
    runtime = meta["runtime"]
    log.info(f"[node_play]: runtime={runtime}")

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
    return await asyncio.create_subprocess_exec(*node, env={**os.environ})


async def node_stop(channel: Channel, session: str):
    log.info(f"[node_stop] session={session} stop")

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
    log.info(f"[node_stop] stop={stop}")
    proc = await asyncio.create_subprocess_exec(*stop, env={**os.environ})

    await proc.wait()

    with tempfile.NamedTemporaryFile(prefix=f"{session}-", suffix=".json") as f:
        log.info(f"[node_stop] temp={f.name}")
        sub = await channel.subscribe_data(session, start=1)
        log.info(f"[node_stop] session={session} channel start")

        try:
            while True:
                msg = await sub.next_msg(timeout=30.0)
                f.write(msg.data + b"\n")
        except asyncio.TimeoutError:
            pass
        finally:
            await sub.unsubscribe()

        f.seek(0)
        storage_kv_set("data", f"{session}.json", f.read())
        log.info(f"[node_stop] uploaded s3://data/{session}.json")

    shutil.rmtree("/storage/node", ignore_errors=True)

    log.info(f"[node_stop] session={session} done")


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

    sub_play = await channel.subscribe_play()
    log.info(f"NATS work ready")

    try:
        while True:
            log.info(f"[run_work]: play fetch")
            try:
                msg_play = (await sub_play.fetch(batch=1, timeout=60))[0]
            except (nats.errors.TimeoutError, IndexError):
                continue
            log.info(f"[run_work]: play msg={msg_play}")

            assert msg_play.subject.startswith(
                "motion.node.",
            ) and msg_play.subject.endswith(
                ".play",
            )
            session = msg_play.subject.removeprefix(
                "motion.node.",
            ).removesuffix(
                ".play",
            )
            log.info(f"[run_work]: play session={session}")
            try:
                sub_stop = await channel.subscribe_stop(session)

                log.info(f"[run_work]: play session={session}")
                proc = await node_play(channel, session)

                try:
                    msg_stop = await sub_stop.next_msg(timeout=3600)
                except nats.errors.TimeoutError:
                    pass
                await node_stop(channel, session)

                await asyncio.wait_for(proc.wait(), timeout=300)

                log.info(f"[run_work]: play session={session} ack")
                await msg_play.ack()
            finally:
                await sub_stop.unsubscribe()
            log.info(f"[run_work]: play session={session} done")
    finally:
        await sub_play.unsubscribe()
        await channel.close()


async def main():
    async with run_http():
        await run_work()


if __name__ == "__main__":
    asyncio.run(main())
