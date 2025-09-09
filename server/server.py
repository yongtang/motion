import asyncio
import contextlib
import io
import logging
import random
import uuid
import zipfile

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

from .channel import Channel
from .storage import storage_kv_del, storage_kv_get, storage_kv_scan, storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.channel = Channel()
    log.info("[Channel] Initializing...")
    try:
        await app.state.channel.start()
        log.info("[Channel] Started")
        yield
    finally:
        log.info("[Channel] Closing...")
        await app.state.channel.close()
        log.info("[Channel] Closed")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    log.info("[Health] OK")
    return JSONResponse({"status": "ok"})


class SessionRequest(BaseModel):
    scene: UUID4
    camera: list[str] = ["*"]


@app.post("/scene", response_model=motion.scene.SceneBaseModel, status_code=201)
async def scene_create(file: UploadFile = File(...)) -> motion.scene.SceneBaseModel:
    if file.content_type not in ("application/zip", "application/x-zip-compressed"):
        log.warning(f"[Scene N/A] Upload rejected: invalid type {file.content_type}")
        raise HTTPException(status_code=415, detail="zip required")

    # model-first
    scene = motion.scene.SceneBaseModel(uuid=uuid.uuid4())

    data = await file.read()
    storage_kv_set("scene", f"{scene.uuid}.zip", data)
    log.info(f"[Scene {scene.uuid}] Stored archive {file.filename} ({len(data)} bytes)")

    # metadata persisted with Pydantic v1/v2-compatible .json()
    storage_kv_set("scene", f"{scene.uuid}.json", scene.json().encode())
    log.info(f"[Scene {scene.uuid}] Stored metadata")

    return scene


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    try:
        # construct immediately from storage; single source of truth
        scene = motion.scene.SceneBaseModel.parse_raw(
            storage_kv_get("scene", f"{scene}.json")
        )
        log.info(f"[Scene {scene.uuid}] Found")
        return scene
    except FileNotFoundError:
        log.warning(f"[Scene {scene}] Not found")
        raise HTTPException(status_code=404, detail=f"Scene {scene} not found")


@app.get("/scene/{scene:uuid}/archive")
async def scene_archive(scene: UUID4):
    try:
        blob = storage_kv_get("scene", f"{scene}.zip")
        log.info(f"[Scene {scene}] Archive retrieved ({len(blob)} bytes)")
    except FileNotFoundError:
        log.warning(f"[Scene {scene}] Archive not found")
        raise HTTPException(status_code=404, detail=f"Scene {scene} archive not found")

    headers = {
        "Content-Disposition": f'inline; filename="{scene}.zip"',
        "Content-Type": "application/zip",
    }
    return Response(content=blob, media_type="application/zip", headers=headers)


@app.delete("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_delete(scene: UUID4) -> motion.scene.SceneBaseModel:
    # load and construct the model up front; return it at the end
    try:
        scene = motion.scene.SceneBaseModel.parse_raw(
            storage_kv_get("scene", f"{scene}.json")
        )
        log.info(f"[Scene {scene.uuid}] Deleting")
    except FileNotFoundError:
        log.warning(f"[Scene {scene}] Not found")
        raise HTTPException(status_code=404, detail=f"Scene {scene} not found")

    # best-effort deletes
    try:
        storage_kv_del("scene", f"{scene.uuid}.zip")
        log.info(f"[Scene {scene.uuid}] Archive deleted")
    except Exception as e:
        log.error(f"[Scene {scene.uuid}] Error deleting archive: {e}", exc_info=True)
    try:
        storage_kv_del("scene", f"{scene.uuid}.json")
        log.info(f"[Scene {scene.uuid}] Metadata deleted")
    except Exception as e:
        log.error(f"[Scene {scene.uuid}] Error deleting metadata: {e}", exc_info=True)

    return scene


@app.get("/scene", response_model=list[motion.scene.SceneBaseModel])
async def scene_search(q: UUID4 = Query(..., description="exact scene uuid")):
    try:
        scene = motion.scene.SceneBaseModel.parse_raw(
            storage_kv_get("scene", f"{q}.json")
        )
        log.info(f"[Scene {scene.uuid}] Search found")
        return [scene]
    except FileNotFoundError:
        log.warning(f"[Scene {q}] Search not found")
        return []


@app.post("/session", response_model=motion.session.SessionBaseModel, status_code=201)
async def session_create(body: SessionRequest) -> motion.session.SessionBaseModel:
    # model-first (uuid now; scene/camera from body)
    session = motion.session.SessionBaseModel(
        uuid=uuid.uuid4(),
        scene=body.scene,
        camera=body.camera,  # CHANGED: carry through, defaults to ["*"]
    )

    # validate scene exists; only persist if valid
    try:
        storage_kv_get("scene", f"{session.scene}.json")
        log.info(
            f"[Session {session.uuid}] Creating (scene={session.scene}, cameras={'ALL' if session.camera == ['*'] else len(session.camera)})"
        )
    except FileNotFoundError:
        log.warning(
            f"[Session {session.uuid}] Scene {session.scene} not found for create"
        )
        raise HTTPException(status_code=404, detail=f"Scene {session.scene} not found")

    storage_kv_set("session", f"{session.uuid}.json", session.json().encode())
    log.info(
        f"[Session {session.uuid}] Created (scene={session.scene}, cameras={'ALL' if session.camera == ['*'] else len(session.camera)})"
    )

    return session


@app.get("/session/{session:uuid}", response_model=motion.session.SessionBaseModel)
async def session_lookup(session: UUID4) -> motion.session.SessionBaseModel:
    try:
        session = motion.session.SessionBaseModel.parse_raw(
            storage_kv_get("session", f"{session}.json")
        )
        log.info(f"[Session {session.uuid}] Found (scene={session.scene})")
        return session
    except FileNotFoundError:
        log.warning(f"[Session {session}] Not found")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")


@app.get("/session/{session:uuid}/archive")
async def session_archive(session: UUID4):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as z:
        # ensure session exists; construct immediately
        try:
            session = motion.session.SessionBaseModel.parse_raw(
                storage_kv_get("session", f"{session}.json")
            )
            z.writestr("session.json", session.json())
            log.info(f"[Session {session.uuid}] Added session.json to archive")
        except FileNotFoundError:
            log.warning(f"[Session {session}] Not found for archive")
            raise HTTPException(status_code=404, detail=f"Session {session} not found")

        # optional data.json
        try:
            data_blob = storage_kv_get("data", f"{session.uuid}.json")
            z.writestr("data.json", data_blob)
            log.info(
                f"[Session {session.uuid}] Added data.json to archive ({len(data_blob)} bytes)"
            )
        except FileNotFoundError:
            log.info(
                f"[Session {session.uuid}] No data; building archive with session.json only"
            )

    buffer.seek(0)
    size = buffer.getbuffer().nbytes
    log.info(f"[Session {session.uuid}] Archive built ({size} bytes)")

    headers = {
        "Content-Disposition": f'inline; filename="{session.uuid}.zip"',
        "Content-Type": "application/zip",
    }
    return Response(
        content=buffer.read(), media_type="application/zip", headers=headers
    )


@app.delete("/session/{session:uuid}", response_model=motion.session.SessionBaseModel)
async def session_delete(session: UUID4) -> motion.session.SessionBaseModel:
    # load and construct up front; return the model after cleanup
    try:
        session = motion.session.SessionBaseModel.parse_raw(
            storage_kv_get("session", f"{session}.json")
        )
        log.info(f"[Session {session.uuid}] Deleting (scene={session.scene})")
    except FileNotFoundError:
        log.warning(
            f"[Session {session}] Not found during delete; broadcasting stop anyway"
        )
        # best-effort broadcast even if not found, then 404
        try:
            nodes = [
                k.removesuffix(".json")
                for k in storage_kv_scan("node", "")
                if k.endswith(".json")
            ]
            if nodes:
                await asyncio.gather(
                    *(
                        app.state.channel.publish_stop(node, str(session))
                        for node in nodes
                    ),
                    return_exceptions=True,
                )
                log.info(
                    f"[Session {session}] Published stop to {len(nodes)} nodes (not found)"
                )
            else:
                log.warning(
                    f"[Session {session}] No nodes available to stop (not found)"
                )
        except Exception as e:
            log.error(
                f"[Session {session}] Error broadcasting stop: {e}", exc_info=True
            )
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    # broadcast stop before deleting
    try:
        nodes = [
            k.removesuffix(".json")
            for k in storage_kv_scan("node", "")
            if k.endswith(".json")
        ]
        if nodes:
            await asyncio.gather(
                *(
                    app.state.channel.publish_stop(node, str(session.uuid))
                    for node in nodes
                ),
                return_exceptions=True,
            )
            log.info(f"[Session {session.uuid}] Published stop to {len(nodes)} nodes")
        else:
            log.warning(f"[Session {session.uuid}] No nodes available to stop")
    except Exception as e:
        log.error(
            f"[Session {session.uuid}] Error broadcasting stop: {e}", exc_info=True
        )

    # best-effort delete of metadata
    try:
        storage_kv_del("session", f"{session.uuid}.json")
        log.info(f"[Session {session.uuid}] Metadata deleted")
    except Exception as e:
        log.error(
            f"[Session {session.uuid}] Error deleting metadata: {e}", exc_info=True
        )

    return session


@app.post(
    "/session/{session:uuid}/play", response_model=motion.session.SessionBaseModel
)
async def session_play(session: UUID4) -> motion.session.SessionBaseModel:
    try:
        session = motion.session.SessionBaseModel.parse_raw(
            storage_kv_get("session", f"{session}.json")
        )
        log.info(f"[Session {session.uuid}] Play requested (scene={session.scene})")
    except FileNotFoundError:
        log.warning(f"[Session {session}] Not found for play")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    nodes = [
        k.removesuffix(".json")
        for k in storage_kv_scan("node", "")
        if k.endswith(".json")
    ]
    if not nodes:
        log.error(f"[Session {session.uuid}] No nodes available for play")
        raise HTTPException(status_code=503, detail="no nodes available")

    node = random.choice(nodes)
    await app.state.channel.publish_play(node, str(session.uuid))
    log.info(f"[Session {session.uuid}] Published play to node {node}")

    return session


@app.post(
    "/session/{session:uuid}/stop", response_model=motion.session.SessionBaseModel
)
async def session_stop(session: UUID4) -> motion.session.SessionBaseModel:
    try:
        session = motion.session.SessionBaseModel.parse_raw(
            storage_kv_get("session", f"{session}.json")
        )
        log.info(f"[Session {session.uuid}] Stop requested (scene={session.scene})")
    except FileNotFoundError:
        log.warning(f"[Session {session}] Stop requested for nonexistent session")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    nodes = [
        k.removesuffix(".json")
        for k in storage_kv_scan("node", "")
        if k.endswith(".json")
    ]
    await asyncio.gather(
        *(app.state.channel.publish_stop(node, str(session.uuid)) for node in nodes),
        return_exceptions=True,
    )
    log.info(f"[Session {session.uuid}] Published stop to {len(nodes)} nodes")

    return session


@app.websocket("/session/{session:uuid}/step")
async def session_step(ws: WebSocket, session: UUID4):
    # Validate session exists
    try:
        storage_kv_get("session", f"{session}.json")
        log.info(f"[Session {session}] WS step requested")
    except FileNotFoundError:
        log.warning(f"[Session {session}] WS step for nonexistent session")
        await ws.close(code=1008)
        return

    await ws.accept()
    log.info(f"[Session {session}] WS step connected")

    try:
        while True:
            data = await ws.receive_text()
            await app.state.channel.publish_step(str(session), data)
    except WebSocketDisconnect:
        log.info(f"[Session {session}] WS step disconnected")
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
        log.info(f"[Session {session}] WS step closed")


@app.websocket("/session/{session:uuid}/data")
async def session_data(ws: WebSocket, session: UUID4):
    # Validate session exists
    try:
        storage_kv_get("session", f"{session}.json")
        log.info(f"[Session {session}] WS data requested")
    except FileNotFoundError:
        log.warning(f"[Session {session}] WS data for nonexistent session")
        await ws.close(code=1008)
        return

    # Optional ?start= query parameter
    start = ws.query_params.get("start")
    if start:
        if not (start.isdigit() and int(start) > 0):
            log.warning(f"[Session {session}] WS data invalid start={start!r}")
            await ws.close(code=1008)
            return
        start = int(start)

    await ws.accept()
    log.info(f"[Session {session}] WS data connected (start={start})")

    sub = await app.state.channel.subscribe_data(str(session), start=start)

    try:
        while True:
            msg = await sub.next_msg()
            await ws.send_text(msg.data.decode(errors="ignore"))
    except WebSocketDisconnect:
        log.info(f"[Session {session}] WS data disconnected")
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
        with contextlib.suppress(Exception):
            await ws.close()
        log.info(f"[Session {session}] WS data closed (start={start})")
