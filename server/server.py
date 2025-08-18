import json
import os
import tempfile
import uuid
from contextlib import asynccontextmanager

import nats
from fastapi import (
    FastAPI,
    File,
    HTTPException,
    Query,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse
from pydantic import UUID4, BaseModel

import motion

from .storage import storage_kv_del, storage_kv_get, storage_kv_set

storage_session = "/storage/session"
os.makedirs(storage_session, exist_ok=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # connect to NATS before serving
    app.state.nc = await nats.connect("nats://127.0.0.1:4222")
    try:
        yield
    finally:
        # graceful shutdown
        if app.state.nc and not app.state.nc.is_closed:
            await app.state.nc.drain()


app = FastAPI(lifespan=lifespan)


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
    meta_key = f"{scene}.json"
    zip_key = f"{scene}.zip"

    # Read upload into memory and store to S3
    data = await file.read()
    storage_kv_set(zip_key, data)

    # Write meta alongside
    meta = {"uuid": str(scene), "status": "uploaded"}
    storage_kv_set(meta_key, json.dumps(meta).encode("utf-8"))

    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    meta_key = f"{scene}.json"
    try:
        storage_kv_get(meta_key)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene not found")
    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}/archive")
async def scene_archive(scene: UUID4):
    zip_key = f"{scene}.zip"
    try:
        blob = storage_kv_get(zip_key)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene content not found")

    headers = {
        "Content-Disposition": f'inline; filename="{scene}.zip"',
        "Content-Type": "application/zip",
    }
    return Response(content=blob, media_type="application/zip", headers=headers)


@app.delete("/scene/{scene:uuid}")
async def scene_delete(scene: UUID4):
    meta_key = f"{scene}.json"
    zip_key = f"{scene}.zip"

    # Check existence of meta to decide 404 behavior
    try:
        storage_kv_get(meta_key)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene not found")

    # Best-effort deletes
    try:
        storage_kv_del(zip_key)
    except Exception:
        pass
    try:
        storage_kv_del(meta_key)
    except Exception:
        pass

    return {"status": "deleted", "uuid": str(scene)}


@app.get("/scene")
async def scene_search(q: str = Query(..., description="search terms; exact uuid")):
    meta_key = f"{q}.json"
    try:
        storage_kv_get(meta_key)
        return [q]
    except FileNotFoundError:
        return []


@app.post("/session", response_model=motion.session.SessionBaseModel, status_code=201)
async def session_create(body: SessionRequest) -> motion.session.SessionBaseModel:
    # Validate scene existence via S3 meta
    try:
        storage_kv_get(f"{body.scene}.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene not found")

    session_id = uuid.uuid4()
    payload = motion.session.SessionBaseModel(uuid=session_id, scene=body.scene)

    sess_path = os.path.join(storage_session, f"{session_id}.json")
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

    # publish spin command
    msg = {"action": "spin", "session": str(session_id), "scene": str(body.scene)}
    await app.state.nc.publish("motion.session.spin", json.dumps(msg).encode())

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
async def session_delete(session: UUID4):
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

    # publish stop command
    msg = {"action": "stop", "session": str(session)}
    await app.state.nc.publish("motion.session.stop", json.dumps(msg).encode())

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
