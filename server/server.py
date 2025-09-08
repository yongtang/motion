import asyncio
import contextlib
import io
import json
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
log = logging.getLogger("server")


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.channel = Channel()
    log.info("Initializing channel...")
    try:
        await app.state.channel.start()
        log.info("Channel started")
        yield
    finally:
        log.info("Channel closing")
        await app.state.channel.close()
        log.info("Channel closed")


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    log.debug("Health check requested")
    return JSONResponse({"status": "ok"})


class SessionRequest(BaseModel):
    scene: UUID4


@app.post("/scene", response_model=motion.scene.SceneBaseModel, status_code=201)
async def scene_create(file: UploadFile = File(...)) -> motion.scene.SceneBaseModel:
    if file.content_type not in ("application/zip", "application/x-zip-compressed"):
        log.warning("Invalid scene upload type: %s", file.content_type)
        raise HTTPException(status_code=415, detail="zip required")

    scene = uuid.uuid4()
    meta_key = f"{scene}.json"
    zip_key = f"{scene}.zip"

    # Read upload into memory and store to S3
    data = await file.read()
    storage_kv_set("scene", zip_key, data)
    log.info("Stored scene %s archive (%d bytes)", scene, len(data))

    # Write meta alongside
    meta = {"uuid": str(scene), "status": "uploaded"}
    storage_kv_set("scene", meta_key, json.dumps(meta).encode("utf-8"))
    log.info("Stored scene %s metadata", scene)

    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    meta_key = f"{scene}.json"
    try:
        storage_kv_get("scene", meta_key)
        log.info("Scene %s found", scene)
    except FileNotFoundError:
        log.warning("Scene %s not found", scene)
        raise HTTPException(status_code=404, detail="scene not found")
    return motion.scene.SceneBaseModel(uuid=scene)


@app.get("/scene/{scene:uuid}/archive")
async def scene_archive(scene: UUID4):
    zip_key = f"{scene}.zip"
    try:
        blob = storage_kv_get("scene", zip_key)
        log.info("Scene %s archive retrieved (%d bytes)", scene, len(blob))
    except FileNotFoundError:
        log.warning("Scene %s archive not found", scene)
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
        log.info("Deleting scene %s", scene)
    except FileNotFoundError:
        log.warning("Scene %s not found", scene)
        raise HTTPException(status_code=404, detail="scene not found")

    # Best-effort deletes
    try:
        storage_kv_del("scene", zip_key)
        log.debug("Deleted scene %s archive", scene)
    except Exception as e:
        log.error("Error deleting archive for scene %s: %s", scene, e)
    try:
        storage_kv_del("scene", meta_key)
        log.debug("Deleted scene %s metadata", scene)
    except Exception as e:
        log.error("Error deleting metadata for scene %s: %s", scene, e)

    return {"status": "deleted", "uuid": str(scene)}


@app.get("/scene")
async def scene_search(q: str = Query(..., description="search terms; exact uuid")):
    meta_key = f"{q}.json"
    try:
        storage_kv_get("scene", meta_key)
        log.info("Search found scene %s", q)
        return [q]
    except FileNotFoundError:
        log.debug("Search did not find scene %s", q)
        return []


@app.post("/session", response_model=motion.session.SessionBaseModel, status_code=201)
async def session_create(body: SessionRequest) -> motion.session.SessionBaseModel:
    # Validate scene existence via storage meta
    try:
        storage_kv_get("scene", f"{body.scene}.json")
        log.info(f"Creating session for scene {body.scene}")
    except FileNotFoundError:
        log.warning(f"Scene {body.scene} not found for session create")
        raise HTTPException(status_code=404, detail="scene not found")

    session = motion.session.SessionBaseModel(uuid=uuid.uuid4(), scene=body.scene)

    # Store session doc in KV (no state machine)
    storage_kv_set("session", f"{str(session.uuid)}.json", session.json())
    log.info(f"Session {session} created")

    # No auto-start here; use /session/{id}/play to trigger work
    return session


@app.get("/session/{session:uuid}", response_model=motion.session.SessionBaseModel)
async def session_lookup(session: UUID4) -> motion.session.SessionBaseModel:
    # Load session doc from KV
    try:
        raw = storage_kv_get("session", f"{session}.json")
        log.info(f"Session {session} found")
    except FileNotFoundError:
        log.warning(f"Session {session} not found")
        raise HTTPException(status_code=404, detail="session not found")
    return motion.session.SessionBaseModel.parse_raw(raw)


@app.get("/session/{session:uuid}/archive")
async def session_archive(session: UUID4):
    # Ensure session exists
    try:
        storage_kv_get("session", f"{session}.json")
    except FileNotFoundError:
        log.warning(f"Session {session} not found for archive")
        raise HTTPException(status_code=404, detail="session not found")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as z:
        try:
            blob = storage_kv_get("data", f"{session}.json")
            # store the single consolidated file as data.json inside the zip
            z.writestr("data.json", blob)
            log.info(
                f"Added data.json for session {session} to archive ({len(blob)} bytes)"
            )
        except FileNotFoundError:
            # No data yet: return an empty archive
            log.info(f"No data for session {session}; returning empty archive")

    buffer.seek(0)
    headers = {
        "Content-Disposition": f'inline; filename="{session}.zip"',
        "Content-Type": "application/zip",
    }
    log.info(f"Built session archive for {session}: {buffer.getbuffer().nbytes} bytes")
    return Response(
        content=buffer.read(), media_type="application/zip", headers=headers
    )


@app.delete("/session/{session:uuid}")
async def session_delete(session: UUID4):
    # Helper: fan-out stop to all nodes (safe even if session doesn't exist or isn't running)
    async def broadcast_stop():
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
                    return_exceptions=True,  # don't let one failure break the rest
                )
                log.info(f"Published stop for session {session} to {len(nodes)} nodes")
            else:
                log.warning(f"No nodes available to stop for session {session}")
        except Exception as e:
            log.error(f"Error broadcasting stop for session {session}: {e}")

    # If session metadata isn't found: broadcast stop, then return 204
    try:
        storage_kv_get("session", f"{session}.json")
    except FileNotFoundError:
        log.warning(
            f"Session {session} not found during delete; broadcasting stop anyway"
        )
        await broadcast_stop()
        return Response(status_code=204)

    # Session exists: broadcast stop, then delete metadata
    log.info(f"Deleting session {session}")
    await broadcast_stop()

    try:
        storage_kv_del("session", f"{session}.json")
        log.debug(f"Deleted session {session} metadata")
    except Exception as e:
        # Do not fail the request if delete races/errs
        log.error(f"Error deleting metadata for session {session}: {e}")

    return {"status": "deleted", "uuid": str(session)}


@app.post("/session/{session:uuid}/play")
async def session_play(session: UUID4):
    # Ensure session exists
    try:
        storage_kv_get("session", f"{session}.json")
        log.info("Play requested for session %s", session)
    except FileNotFoundError:
        log.warning("Session %s not found for play", session)
        raise HTTPException(status_code=404, detail="session not found")

    nodes = list(
        k.removesuffix(".json")
        for k in storage_kv_scan("node", "")
        if k.endswith(".json")
    )
    if not nodes:
        log.error("No nodes available for play")
        raise HTTPException(status_code=503, detail="no nodes available")

    node = random.choice(nodes)
    await app.state.channel.publish_play(node, str(session))
    log.info("Published play for session %s to node %s", session, node)
    return {"status": "accepted", "uuid": str(session)}


@app.post("/session/{session:uuid}/stop")
async def session_stop(session: UUID4):
    # Ensure session exists
    try:
        storage_kv_get("session", f"{session}.json")
        log.info("Stop requested for session %s", session)
    except FileNotFoundError:
        log.warning("Stop requested for nonexistent session %s", session)
        return Response(status_code=204)

    nodes = list(
        k.removesuffix(".json")
        for k in storage_kv_scan("node", "")
        if k.endswith(".json")
    )
    await asyncio.gather(
        *(app.state.channel.publish_stop(node, str(session)) for node in nodes)
    )
    log.info("Published stop for session %s to %d nodes", session, len(nodes))
    return {"status": "accepted", "uuid": str(session)}


@app.websocket("/session/{session:uuid}/step")
async def session_step(ws: WebSocket, session: UUID4):
    # Validate session exists
    try:
        storage_kv_get("session", f"{session}.json")
        log.info(f"WS step requested for session={session}")
    except FileNotFoundError:
        log.warning(f"WS step for nonexistent session={session}")
        await ws.close(code=1008)
        return

    await ws.accept()
    log.info(f"WS step connected: session={session}")

    try:
        while True:
            data = await ws.receive_text()
            await app.state.channel.publish_step(str(session), data)
    except WebSocketDisconnect:
        log.info(f"WS step disconnected: session={session}")
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
        log.info(f"WS step closed: session={session}")


@app.websocket("/session/{session:uuid}/data")
async def session_data(ws: WebSocket, session: UUID4):
    # Validate session exists
    try:
        storage_kv_get("session", f"{session}.json")
        log.info(f"WS data requested for session={session}")
    except FileNotFoundError:
        log.warning(f"WS data for nonexistent session={session}")
        await ws.close(code=1008)
        return

    # Optional ?start= query parameter
    start = ws.query_params.get("start")
    if start:
        if not (start.isdigit() and int(start) > 0):
            log.warning(f"WS data invalid start={start!r} for session={session}")
            await ws.close(code=1008)
            return
        start = int(start)

    await ws.accept()
    log.info(f"WS data connected: session={session}, start={start}")

    sub = await app.state.channel.subscribe_data(str(session), start=start)

    try:
        while True:
            msg = await sub.next_msg()
            await ws.send_text(msg.data.decode(errors="ignore"))
    except WebSocketDisconnect:
        log.info(f"WS data disconnected: session={session}")
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
        with contextlib.suppress(Exception):
            await ws.close()
        log.info(f"WS data closed: session={session}, start={start}")
