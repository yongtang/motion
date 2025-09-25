import asyncio
import contextlib
import json
import logging
import os
import shutil
import tempfile
import time
import uuid
import zipfile

import yaml

from .channel import Channel
from .node import run_http
from .storage import storage_kv_get, storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def f_pipe(prefix: str, stream: asyncio.StreamReader):
    while True:
        line = await stream.readline()
        if not line:
            break
        log.info("%s %s", prefix, line.decode(errors="replace").rstrip())


async def f_proc(node, prefix):
    log.info(f"{prefix} node cmd={node}")

    proc = await asyncio.create_subprocess_exec(
        *node,
        env={**os.environ, "NO_COLOR": "1"},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    asyncio.create_task(f_pipe(f"{prefix} compose:", proc.stdout))

    return proc


async def node_play(session: str):
    shutil.rmtree("/storage/node", ignore_errors=True)
    os.makedirs("/storage/node/scene", exist_ok=True)

    data = json.loads(b"".join(storage_kv_get("session", f"{session}.json")))
    with open("/storage/node/session.json", "w") as f:
        f.write(json.dumps(data))
    log.info(f"[node_play] session storage: {data}")

    scene = data["scene"]
    log.info(f"[node_play] scene={scene}")

    data = json.loads(b"".join(storage_kv_get("scene", f"{scene}.json")))
    with open("/storage/node/scene.json", "w") as f:
        f.write(json.dumps(data))
    log.info(f"[node_play] scene storage: {data}")

    runner = data["runner"]
    log.info(f"[node_play] runner={runner}")

    with tempfile.TemporaryFile() as f:
        for chunk in storage_kv_get("scene", f"{scene}.zip"):
            f.write(chunk)
        f.seek(0)
        with zipfile.ZipFile(f) as z:
            z.extractall("/storage/node/scene")
    log.info("[node_play] uncompressed scene")

    with open("/storage/node/scene/meta.json", "r") as f:
        meta = json.loads(f.read())
    log.info(f"[node_play] meta={meta}")

    def f_node(runner):
        scope = os.environ.get("SCOPE")
        project = f"{scope}-motion" if scope else "motion"

        compose = {"services": {"node": {}}}
        compose["services"]["node"]["image"] = f'motion/motion-node-{runner["image"]}'
        compose["services"]["node"]["deploy"] = (
            {
                "resources": {
                    "reservations": {
                        "devices": [
                            {
                                "driver": "nvidia",
                                "count": "all",
                                "capabilities": ["gpu"],
                            }
                        ]
                    }
                }
            }
            if runner["device"] == "cuda"
            else {}
        )
        with open("/storage/node/docker-compose.yml", "w") as f:
            yaml.dump(compose, f, sort_keys=False)

        with open("/storage/node/docker-compose.yml") as f:
            log.info(f"[node_play] compose: {f.read()}")

        node = [
            "docker",
            "compose",
            "-p",
            project,
            "-f",
            f"/app/docker/docker-compose.yml",
            "-f",
            f"/storage/node/docker-compose.yml",
            "up",
            "--no-deps",
            "--force-recreate",
            "--no-log-prefix",
            "node",
        ]
        return node

    proc = await f_proc(f_node(runner), "[node_play]")

    return proc


async def node_stop(session: str):
    log.info(f"[node_stop] session={session} stop")

    def f_node():
        scope = os.environ.get("SCOPE")
        project = f"{scope}-motion" if scope else "motion"

        node = [
            "docker",
            "compose",
            "-p",
            project,
            "stop",
            "node",
        ]
        return node

    proc = await f_proc(f_node(), "[node_stop]")

    await proc.wait()

    with tempfile.NamedTemporaryFile(prefix=f"{session}-", suffix=".json") as f:
        log.info(f"[node_stop] temp file={f.name}")

        channel = Channel(servers="nats://127.0.0.1:4222")
        await channel.start()
        log.info(f"[node_stop] channel start")

        sub = await channel.subscribe_data(session, start=1)
        log.info(f"[node_stop] session={session} subscribe")

        try:
            while True:
                msg = await sub.next_msg(timeout=30.0)
                f.write(msg.data + b"\n")
        except asyncio.TimeoutError:
            pass
        finally:
            await sub.unsubscribe()
            log.info(f"[node_stop] session={session} unsubscribe")
            await channel.close()
            log.info(f"[node_stop] channel close")

        f.seek(0)
        storage_kv_set("data", f"{session}.json", f.read())
        log.info(f"[node_stop] uploaded s3://data/{session}.json")

    shutil.rmtree("/storage/node", ignore_errors=True)
    log.info(f"[node_stop] session={session} done")


async def run_work(node):
    interval = 1.0

    def f_session(desc):
        with contextlib.suppress(FileNotFoundError):
            log.info(f"[run_work] fetch {desc}")
            data = (b"".join(storage_kv_get("node", f"node/{node}.json"))).decode()
            log.info(f"[run_work] fetch {desc} data={data}")
            session = (json.loads(data) if data.strip() else {}).get("session")
            return session

    while True:
        while True:
            session = f_session("play")
            if session:
                break
            await asyncio.sleep(interval)

        proc = await node_play(session)
        log.info(f"[run_work] session={session} play")

        while True:
            if session != f_session("stop"):
                break
            await asyncio.sleep(interval)

        await node_stop(session)
        await asyncio.wait_for(proc.wait(), timeout=300)
        log.info(f"[run_work] session={session} stop")


async def run_beat(node: str):
    while True:
        deadline = str(int(time.time()) + 60)  # Valid for 1 min
        storage_kv_set("node", f"meta/{node}.json", b"{}")
        log.info(f"[run_beat] node={node} deadline={deadline}")
        await asyncio.sleep(15)  # Update every 15s


async def main():
    with open("/etc/machine-id") as f:
        node = str(uuid.UUID(f.read().strip()))

    async with asyncio.TaskGroup() as g:
        g.create_task(run_http(9999), name="http")
        g.create_task(run_beat(node), name="beat")
        g.create_task(run_work(node), name="work")


if __name__ == "__main__":
    asyncio.run(main())
