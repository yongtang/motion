import json
import os
import shutil
import uuid

from fastapi import (
    FastAPI,
    File,
    HTTPException,
    Query,
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


class SceneCreateRequest(BaseModel):
    pass


class SessionCreateRequest(BaseModel):
    scene: UUID4


@app.post("/scene", response_model=motion.scene.SceneBaseModel, status_code=201)
async def scene_create(_: SceneCreateRequest) -> motion.scene.SceneBaseModel:
    scene = uuid.uuid4()
    meta_path = os.path.join(storage_scene, f"{scene}.json")
    with open(meta_path, "w") as f:
        json.dump({"uuid": str(scene)}, f)
    return motion.scene.SceneBaseModel(uuid=scene)


@app.post("/scene/{scene:uuid}/load")
async def scene_load(scene: UUID4, file: UploadFile = File(...)):
    meta_path = os.path.join(storage_scene, f"{scene}.json")
    if not os.path.exists(meta_path):
        raise HTTPException(status_code=404, detail="scene not found")

    zip_path = os.path.join(storage_scene, f"{scene}.zip")
    with open(zip_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"status": "ok", "uuid": str(scene)}


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    meta_path = os.path.join(storage_scene, f"{scene}.json")
    if not os.path.exists(meta_path):
        raise HTTPException(status_code=404, detail="scene not found")
    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}/save")
async def scene_save(scene: UUID4):
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


@app.post("/session", response_model=motion.session.SessionBaseModel, status_code=201)
async def session_create(body: SessionCreateRequest) -> motion.session.SessionBaseModel:
    # require scene meta to exist
    scene_meta_path = os.path.join(storage_scene, f"{body.scene}.json")
    if not os.path.exists(scene_meta_path):
        raise HTTPException(status_code=404, detail="scene not found")

    session = uuid.uuid4()
    payload = motion.session.SessionBaseModel(uuid=session, scene=body.scene)

    sess_path = os.path.join(storage_session, f"{session}.json")
    with open(sess_path, "w") as f:
        json.dump({"uuid": str(payload.uuid), "scene": str(payload.scene)}, f)

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
        raise HTTPException(status_code=404, detail="session not found")
    os.remove(sess_path)
    return {"status": "deleted", "uuid": str(session)}


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
