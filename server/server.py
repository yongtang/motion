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

app = FastAPI()

storage_scene = "/storage/scene"
os.makedirs(storage_scene, exist_ok=True)


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})


@app.post("/scene")
async def scene_create(file: UploadFile = File(...)):
    scene = str(uuid.uuid4())
    filename = os.path.join(storage_scene, f"{scene}.zip")
    with open(filename, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"uuid": scene}


@app.delete("/scene/{scene:uuid}")
async def scene_delete(scene: uuid.UUID):
    filename = os.path.join(storage_scene, f"{scene}.zip")
    if not os.path.exists(filename):
        raise HTTPException(status_code=404, detail="scene not found")
    os.remove(filename)
    return {"status": "deleted", "uuid": str(scene)}


@app.get("/scene/{scene:uuid}")
async def scene_lookup(scene: uuid.UUID):
    path = os.path.join(storage_scene, f"{scene}.zip")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="scene not found")
    return FileResponse(path, media_type="application/zip", filename=f"{scene}.zip")


@app.get("/scene")
async def scene_search(
    q: str = Query(..., description="search terms; for now, only exact uuid")
):
    filename = os.path.join(storage_scene, f"{q}.zip")
    return [q] if os.path.exists(filename) else []


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
