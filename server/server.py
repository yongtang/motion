import contextlib
import json
import uuid

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


@contextlib.asynccontextmanager
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
    storage_kv_set("scene", zip_key, data)

    # Write meta alongside
    meta = {"uuid": str(scene), "status": "uploaded"}
    storage_kv_set("scene", meta_key, json.dumps(meta).encode("utf-8"))

    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    meta_key = f"{scene}.json"
    try:
        storage_kv_get("scene", meta_key)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene not found")
    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}/archive")
async def scene_archive(scene: UUID4):
    zip_key = f"{scene}.zip"
    try:
        blob = storage_kv_get("scene", zip_key)
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
        storage_kv_get("scene", meta_key)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene not found")

    # Best-effort deletes
    try:
        storage_kv_del("scene", zip_key)
    except Exception:
        pass
    try:
        storage_kv_del("scene", meta_key)
    except Exception:
        pass

    return {"status": "deleted", "uuid": str(scene)}


@app.get("/scene")
async def scene_search(q: str = Query(..., description="search terms; exact uuid")):
    meta_key = f"{q}.json"
    try:
        storage_kv_get("scene", meta_key)
        return [q]
    except FileNotFoundError:
        return []


@app.post("/session", response_model=motion.session.SessionBaseModel, status_code=201)
async def session_create(body: SessionRequest) -> motion.session.SessionBaseModel:
    # Validate scene existence via storage meta
    try:
        storage_kv_get("scene", f"{body.scene}.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="scene not found")

    session = uuid.uuid4()
    payload = motion.session.SessionBaseModel(uuid=session, scene=body.scene)

    # Store session doc in KV (no state machine)
    storage_kv_set(
        "session",
        f"{session}.json",
        json.dumps({"uuid": str(payload.uuid), "scene": str(payload.scene)}).encode(
            "utf-8"
        ),
    )

    # No auto-start here; use /session/{id}/play to trigger work
    return payload


@app.get("/session/{session:uuid}", response_model=motion.session.SessionBaseModel)
async def session_lookup(session: UUID4) -> motion.session.SessionBaseModel:
    # Load session doc from KV
    try:
        raw = storage_kv_get("session", f"{session}.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="session not found")
    d = json.loads(raw)
    return motion.session.SessionBaseModel(
        uuid=UUID4(d["uuid"]), scene=UUID4(d["scene"])
    )


@app.delete("/session/{session:uuid}")
async def session_delete(session: UUID4):
    # Delete session doc from KV
    try:
        storage_kv_del("session", f"{session}.json")
    except FileNotFoundError:
        return Response(status_code=204)

    # Best-effort: also publish a stop (safe even if not running)
    try:
        await app.state.nc.publish(
            "motion.session.stop",
            json.dumps({"action": "stop", "session": str(session)}).encode(),
        )
    except Exception:
        pass

    return {"status": "deleted", "uuid": str(session)}


@app.post("/session/{session:uuid}/play")
async def session_play(session: UUID4):
    # Ensure session exists; include scene in message
    try:
        raw = storage_kv_get("session", f"{session}.json")
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="session not found")

    d = json.loads(raw)
    msg = {"action": "play", "session": str(session), "scene": d["scene"]}
    await app.state.nc.publish("motion.session.play", json.dumps(msg).encode())
    return {"status": "accepted", "uuid": str(session)}


@app.post("/session/{session:uuid}/stop")
async def session_stop(session: UUID4):
    # Ensure session exists; publish stop only
    try:
        storage_kv_get("session", f"{session}.json")
    except FileNotFoundError:
        return Response(status_code=204)

    msg = {"action": "stop", "session": str(session)}
    await app.state.nc.publish("motion.session.stop", json.dumps(msg).encode())
    return {"status": "accepted", "uuid": str(session)}


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
