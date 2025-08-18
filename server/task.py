import asyncio
import json
import os
import subprocess
import tempfile

from .server import storage_session


async def session_spin(session_id: str):
    session_gpu = os.environ.get("TASK_GPU")
    session_image = os.environ.get("TASK_IMAGE", "nginx")
    sp = os.path.join(storage_session, f"{session_id}.json")

    if not os.path.exists(sp):
        return

    with open(sp) as f:
        d = json.load(f)

    scene_uuid = d.get("scene")
    if not scene_uuid:
        d["status"] = "failed"
        d["error"] = "missing scene in session file"
        with tempfile.NamedTemporaryFile(
            "w", dir=os.path.dirname(sp), delete=False
        ) as tmp:
            json.dump(d, tmp)
            tmp_path = tmp.name
        os.replace(tmp_path, sp)
        return

    if d.get("status") in {"starting", "running"}:
        return

    d["status"] = "starting"
    with tempfile.NamedTemporaryFile("w", dir=os.path.dirname(sp), delete=False) as tmp:
        json.dump(d, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, sp)

    name = f"session-{session_id[:12]}"
    cmd = (
        [
            "docker",
            "run",
            "-d",
            "--name",
            name,
            "--label",
            f"motion.session={session_id}",
            "--label",
            f"motion.scene={scene_uuid}",
            "-v",
            "/storage:/storage:ro",
        ]
        + (["--gpus", "all"] if session_gpu else [])
        + [session_image]
    )
    proc = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True)

    # re-check after run
    if not os.path.exists(sp):
        if proc.returncode == 0:
            await asyncio.to_thread(
                subprocess.run,
                ["docker", "rm", "-f", name],
                capture_output=True,
                text=True,
            )
        return

    with open(sp) as f:
        current = json.load(f)

    if current.get("status") in {"deleting", "stopping", "deleted"}:
        if proc.returncode == 0:
            await asyncio.to_thread(
                subprocess.run,
                ["docker", "rm", "-f", name],
                capture_output=True,
                text=True,
            )
        return

    if proc.returncode == 0:
        current["status"] = "running"
        current["container_name"] = name
    else:
        current["status"] = "failed"
        current["error"] = proc.stderr.strip()

    with tempfile.NamedTemporaryFile("w", dir=os.path.dirname(sp), delete=False) as tmp:
        json.dump(current, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, sp)


async def session_stop(session_id: str):
    sp = os.path.join(storage_session, f"{session_id}.json")

    if os.path.exists(sp):
        with open(sp) as f:
            d = json.load(f)
        d["status"] = "stopping"
        with tempfile.NamedTemporaryFile(
            "w", dir=os.path.dirname(sp), delete=False
        ) as tmp:
            json.dump(d, tmp)
            tmp_path = tmp.name
        os.replace(tmp_path, sp)

    name = f"session-{session_id[:12]}"
    await asyncio.to_thread(
        subprocess.run, ["docker", "rm", "-f", name], capture_output=True, text=True
    )

    if os.path.exists(sp):
        os.remove(sp)
