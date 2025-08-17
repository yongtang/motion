import asyncio
import json
import os
import shutil
import subprocess
import tempfile
import uuid

from fastapi import (
    BackgroundTasks,
    FastAPI,
    File,
    HTTPException,
    Query,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, JSONResponse
from pydantic import UUID4, BaseModel

import motion

app = FastAPI()

storage_scene = "/storage/scene"
storage_session = "/storage/session"
os.makedirs(storage_scene, exist_ok=True)
os.makedirs(storage_session, exist_ok=True)


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})


class SessionRequest(BaseModel):
    scene: UUID4


@app.post("/scene", response_model=motion.scene.SceneBaseModel, status_code=201)
async def scene_create(file: UploadFile = File(...)) -> motion.scene.SceneBaseModel:
    if file.content_type not in ("application/zip", "application/x-zip-compressed"):
        raise HTTPException(status_code=415, detail="zip required")

    scene = uuid.uuid4()
    meta_path = os.path.join(storage_scene, f"{scene}.json")
    final_zip = os.path.join(storage_scene, f"{scene}.zip")

    with tempfile.NamedTemporaryFile(dir=storage_scene, delete=False) as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, final_zip)

    with tempfile.NamedTemporaryFile("w", dir=storage_scene, delete=False) as tmp:
        json.dump({"uuid": str(scene), "status": "uploaded"}, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, meta_path)

    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    meta_path = os.path.join(storage_scene, f"{scene}.json")
    if not os.path.exists(meta_path):
        raise HTTPException(status_code=404, detail="scene not found")
    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}/archive")
async def scene_archive(scene: UUID4):
    zip_path = os.path.join(storage_scene, f"{scene}.zip")
    if not os.path.exists(zip_path):
        raise HTTPException(status_code=404, detail="scene content not found")
    return FileResponse(zip_path, media_type="application/zip", filename=f"{scene}.zip")


@app.delete("/scene/{scene:uuid}")
async def scene_delete(scene: UUID4):
    meta_path = os.path.join(storage_scene, f"{scene}.json")
    zip_path = os.path.join(storage_scene, f"{scene}.zip")

    not_found = not os.path.exists(meta_path)
    if os.path.exists(zip_path):
        os.remove(zip_path)
    if os.path.exists(meta_path):
        os.remove(meta_path)
    if not_found:
        raise HTTPException(status_code=404, detail="scene not found")
    return {"status": "deleted", "uuid": str(scene)}


@app.get("/scene")
async def scene_search(q: str = Query(..., description="search terms; exact uuid")):
    meta_path = os.path.join(storage_scene, f"{q}.json")
    return [q] if os.path.exists(meta_path) else []


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
        + [
            session_image,
        ]
    )
    proc = await asyncio.to_thread(subprocess.run, cmd, capture_output=True, text=True)

    # Re-check state
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


@app.post("/session", response_model=motion.session.SessionBaseModel, status_code=201)
async def session_create(
    body: SessionRequest, background_tasks: BackgroundTasks
) -> motion.session.SessionBaseModel:
    scene_meta_path = os.path.join(storage_scene, f"{body.scene}.json")
    if not os.path.exists(scene_meta_path):
        raise HTTPException(status_code=404, detail="scene not found")

    session = uuid.uuid4()
    payload = motion.session.SessionBaseModel(uuid=session, scene=body.scene)

    sess_path = os.path.join(storage_session, f"{session}.json")
    with tempfile.NamedTemporaryFile(
        "w", dir=os.path.dirname(sess_path), delete=False
    ) as tmp:
        json.dump(
            {
                "uuid": str(payload.uuid),
                "scene": str(payload.scene),
                "status": "queued",
            },
            tmp,
        )
        tmp_path = tmp.name
    os.replace(tmp_path, sess_path)

    background_tasks.add_task(session_spin, str(session))
    return payload


@app.get("/session/{session:uuid}", response_model=motion.session.SessionBaseModel)
async def session_lookup(session: UUID4) -> motion.session.SessionBaseModel:
    sess_path = os.path.join(storage_session, f"{session}.json")
    if not os.path.exists(sess_path):
        raise HTTPException(status_code=404, detail="session not found")
    with open(sess_path) as f:
        d = json.load(f)
    return motion.session.SessionBaseModel(
        uuid=UUID4(d["uuid"]), scene=UUID4(d["scene"])
    )


@app.delete("/session/{session:uuid}")
async def session_delete(session: UUID4, background_tasks: BackgroundTasks):
    sess_path = os.path.join(storage_session, f"{session}.json")

    if not os.path.exists(sess_path):
        return Response(status_code=204)

    with open(sess_path) as f:
        d = json.load(f)

    if d.get("status") in {"deleting", "stopping", "deleted"}:
        return Response(status_code=204)

    d["status"] = "deleting"
    with tempfile.NamedTemporaryFile(
        "w", dir=os.path.dirname(sess_path), delete=False
    ) as tmp:
        json.dump(d, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, sess_path)

    background_tasks.add_task(session_stop, str(session))
    return {"status": "deleting", "uuid": str(session)}


@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    try:
        await ws.send_text("hello from server")
        while True:
            msg = await ws.receive_text()
            await ws.send_text(f"echo: {msg}")
    except WebSocketDisconnect:
        pass
