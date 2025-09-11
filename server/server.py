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
from pydantic import UUID4

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


@app.post("/scene", response_model=motion.scene.SceneBaseModel, status_code=201)
async def scene_create(file: UploadFile = File(...)) -> motion.scene.SceneBaseModel:
    match file.content_type:
        case "application/zip" | "application/x-zip-compressed":
            pass
        case other:
            log.warning(f"[Scene N/A] Upload rejected: invalid type {other}")
            raise HTTPException(status_code=415, detail="zip required")

    scene = motion.scene.SceneBaseModel(uuid=uuid.uuid4())

    data = await file.read()
    storage_kv_set("scene", f"{scene.uuid}.zip", data)
    log.info(f"[Scene {scene.uuid}] Stored archive {file.filename} ({len(data)} bytes)")

    storage_kv_set("scene", f"{scene.uuid}.json", scene.json().encode())
    log.info(f"[Scene {scene.uuid}] Stored metadata")

    return scene


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBaseModel)
async def scene_lookup(scene: UUID4) -> motion.scene.SceneBaseModel:
    try:
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
    try:
        scene = motion.scene.SceneBaseModel.parse_raw(
            storage_kv_get("scene", f"{scene}.json")
        )
        log.info(f"[Scene {scene.uuid}] Deleting")
    except FileNotFoundError:
        log.warning(f"[Scene {scene}] Not found")
        raise HTTPException(status_code=404, detail=f"Scene {scene} not found")

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
async def session_create(
    body: motion.session.SessionSpecModel,
) -> motion.session.SessionBaseModel:
    session = motion.session.SessionBaseModel(uuid=uuid.uuid4(), **body.dict())

    try:
        storage_kv_get("scene", f"{session.scene}.json")
        log.info(
            f"[Session {session.uuid}] Creating ("
            f"scene={session.scene}, "
            f"joints={'ALL' if session.joint == ['*'] else len(session.joint)}, "
            f"cameras={'ALL' if set(session.camera.keys()) == {'*'} else len(session.camera)}, "
            f"links={'ALL' if session.link == ['*'] else len(session.link)})"
        )
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Scene {session.scene} not found")

    storage_kv_set("session", f"{session.uuid}.json", session.json().encode())

    log.info(
        f"[Session {session.uuid}] Created ("
        f"scene={session.scene}, "
        f"joints={'ALL' if session.joint == ['*'] else len(session.joint)}, "
        f"cameras={'ALL' if set(session.camera.keys()) == {'*'} else len(session.camera)}, "
        f"links={'ALL' if session.link == ['*'] else len(session.link)})"
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
        try:
            session = motion.session.SessionBaseModel.parse_raw(
                storage_kv_get("session", f"{session}.json")
            )
            z.writestr("session.json", session.json())
            log.info(f"[Session {session.uuid}] Added session.json to archive")
        except FileNotFoundError:
            log.warning(f"[Session {session}] Not found for archive")
            raise HTTPException(status_code=404, detail=f"Session {session} not found")

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
    try:
        session = motion.session.SessionBaseModel.parse_raw(
            storage_kv_get("session", f"{session}.json")
        )
        log.info(f"[Session {session.uuid}] Deleting (scene={session.scene})")
    except FileNotFoundError:
        log.warning(
            f"[Session {session}] Not found during delete; broadcasting stop anyway"
        )
        try:
            nodes = [
                k.removesuffix(".json")
                for k in storage_kv_scan("node", "")
                if k.endswith(".json")
            ]
            match len(nodes):
                case 0:
                    log.warning(
                        f"[Session {session}] No nodes available to stop (not found)"
                    )
                case _:
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
        except Exception as e:
            log.error(
                f"[Session {session}] Error broadcasting stop: {e}", exc_info=True
            )
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    try:
        nodes = [
            k.removesuffix(".json")
            for k in storage_kv_scan("node", "")
            if k.endswith(".json")
        ]
        match len(nodes):
            case 0:
                log.warning(f"[Session {session.uuid}] No nodes available to stop")
            case _:
                await asyncio.gather(
                    *(
                        app.state.channel.publish_stop(node, str(session.uuid))
                        for node in nodes
                    ),
                    return_exceptions=True,
                )
                log.info(
                    f"[Session {session.uuid}] Published stop to {len(nodes)} nodes"
                )
    except Exception as e:
        log.error(
            f"[Session {session.uuid}] Error broadcasting stop: {e}", exc_info=True
        )

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
    match len(nodes):
        case 0:
            log.error(f"[Session {session.uuid}] No nodes available for play")
            raise HTTPException(status_code=503, detail="no nodes available")
        case _:
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


@app.websocket("/session/{session:uuid}/stream")
async def session_stream(ws: WebSocket, session: UUID4):
    try:
        storage_kv_get("session", f"{session}.json")
        log.info(f"[Session {session}] WS stream requested")
    except FileNotFoundError:
        log.warning(f"[Session {session}] WS stream for nonexistent session")
        await ws.close(code=1008)
        return

    # Optional backlog offset (?start=n)
    start = ws.query_params.get("start")
    match start:
        case None:
            pass
        case s if s.isdigit() and int(s) > 0:
            start = int(s)
        case other:
            log.warning(f"[Session {session}] WS stream invalid start={other!r}")
            await ws.close(code=1008)
            return

    await ws.accept()
    log.info(f"[Session {session}] WS stream connected (start={start})")

    # server -> client: data
    sub = await app.state.channel.subscribe_data(str(session), start=start)

    async def recv_loop():
        # client -> server: step
        try:
            while True:
                data = await ws.receive_text()
                await app.state.channel.publish_step(str(session), data)
        except WebSocketDisconnect:
            log.info(f"[Session {session}] WS stream recv disconnected")
            raise
        except Exception as e:
            log.error(f"[Session {session}] WS stream recv error: {e}", exc_info=True)
            raise

    async def send_loop():
        # server -> client: data
        try:
            while True:
                msg = await sub.next_msg()
                await ws.send_text(msg.data.decode(errors="ignore"))
        except WebSocketDisconnect:
            log.info(f"[Session {session}] WS stream send disconnected")
            raise
        except Exception as e:
            log.error(f"[Session {session}] WS stream send error: {e}", exc_info=True)
            raise

    recv_task = asyncio.create_task(recv_loop(), name=f"ws-recv-{session}")
    send_task = asyncio.create_task(send_loop(), name=f"ws-send-{session}")

    try:
        done, pending = await asyncio.wait(
            {recv_task, send_task}, return_when=asyncio.FIRST_EXCEPTION
        )
        # cancel the other direction if one side ends/errors
        for e in pending:
            e.cancel()
            with contextlib.suppress(Exception):
                await t
        # surface any exception from the completed task
        for e in done:
            _ = e.result()
    finally:
        with contextlib.suppress(Exception):
            await sub.unsubscribe()
        with contextlib.suppress(Exception):
            await ws.close()
        log.info(f"[Session {session}] WS stream closed (start={start})")
