import asyncio
import contextlib
import datetime
import json
import logging
import random
import re
import tempfile
import uuid
import zipfile

import nats
import pydantic
from fastapi import (
    FastAPI,
    File,
    Form,
    HTTPException,
    Query,
    Response,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse, StreamingResponse

import motion

from .channel import Channel
from .storage import (
    storage_kv_acquire,
    storage_kv_del,
    storage_kv_get,
    storage_kv_head,
    storage_kv_release,
    storage_kv_scan,
    storage_kv_set,
)

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


@app.post("/scene", response_model=motion.scene.SceneBase, status_code=201)
async def scene_create(
    file: UploadFile = File(...),
    runner: motion.scene.RunnerSpec = Form(...),
) -> motion.scene.SceneBase:
    if file.content_type not in {"application/zip", "application/x-zip-compressed"}:
        log.warning(f"[Scene N/A] Upload rejected: invalid type {file.content_type}")
        raise HTTPException(status_code=415, detail="zip required")

    spec = motion.scene.SceneSpec(runner=runner)

    scene = motion.scene.SceneBase(uuid=uuid.uuid4(), **spec.dict())

    storage_kv_set("scene", f"{scene.uuid}.zip", file.file)
    log.info(f"[Scene {scene.uuid}] Stored archive {file.filename}")

    storage_kv_set(
        "scene", f"{scene.uuid}.json", scene.json(exclude_none=True).encode()
    )
    log.info(f"[Scene {scene.uuid}] Stored metadata")

    return scene


@app.get("/scene/{scene:uuid}", response_model=motion.scene.SceneBase)
async def scene_lookup(scene: pydantic.UUID4) -> motion.scene.SceneBase:
    try:
        scene = motion.scene.SceneBase.parse_raw(
            b"".join(storage_kv_get("scene", f"{scene}.json"))
        )
        log.info(f"[Scene {scene.uuid}] Found")
        return scene
    except FileNotFoundError:
        log.warning(f"[Scene {scene}] Not found")
        raise HTTPException(status_code=404, detail=f"Scene {scene} not found")


@app.get("/scene/{scene:uuid}/archive")
async def scene_archive(scene: pydantic.UUID4):
    try:
        stream = storage_kv_get("scene", f"{scene}.zip")
        log.info(f"[Scene {scene}] Streaming archive")
    except FileNotFoundError:
        log.warning(f"[Scene {scene}] Archive not found")
        raise HTTPException(status_code=404, detail=f"Scene {scene} archive not found")

    headers = {
        "Content-Disposition": f'inline; filename="{scene}.zip"',
        "Content-Type": "application/zip",
    }
    return StreamingResponse(stream, media_type="application/zip", headers=headers)


@app.delete("/scene/{scene:uuid}", response_model=motion.scene.SceneBase)
async def scene_delete(scene: pydantic.UUID4) -> motion.scene.SceneBase:
    try:
        scene = motion.scene.SceneBase.parse_raw(
            b"".join(storage_kv_get("scene", f"{scene}.json"))
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


@app.get("/scene", response_model=list[motion.scene.SceneBase])
async def scene_search(
    q: str | None = Query(
        None, description="Scene UUID (full), prefix, or empty for all"
    ),
):
    # Empty or missing -> return ALL scenes
    if not q:
        matches: list[motion.scene.SceneBase] = []
        for key in storage_kv_scan("scene", ""):
            if not key.endswith(".json"):
                continue
            try:
                scene = motion.scene.SceneBase.parse_raw(
                    b"".join(storage_kv_get("scene", key))
                )
                matches.append(scene)
            except Exception:
                continue
        return matches

    # Validate and normalize
    if not re.fullmatch(r"[0-9a-fA-F-]+", q):
        raise HTTPException(status_code=422, detail="UUID prefix must be hex/hyphen")
    norm = q.lower().strip()

    # Fast path: exact UUID
    if len(norm) == 36:
        try:
            _ = pydantic.UUID4(norm)
            scene = motion.scene.SceneBase.parse_raw(
                b"".join(storage_kv_get("scene", f"{norm}.json"))
            )
            return [scene]
        except Exception:
            pass  # fall through to prefix scan

    # Prefix scan: keys are "<uuid>.json"
    matches: list[motion.scene.SceneBase] = []
    for key in storage_kv_scan("scene", norm):
        if not key.endswith(".json"):
            continue
        uuid_part = key[:-5]
        if not uuid_part.startswith(norm):
            continue
        try:
            scene = motion.scene.SceneBase.parse_raw(
                b"".join(storage_kv_get("scene", key))
            )
            matches.append(scene)
        except Exception:
            continue
    return matches


@app.post("/session", response_model=motion.session.SessionBase, status_code=201)
async def session_create(
    body: motion.session.SessionSpec,
) -> motion.session.SessionBase:
    session = motion.session.SessionBase(uuid=uuid.uuid4(), **body.dict())

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

    storage_kv_set(
        "session", f"{session.uuid}.json", session.json(exclude_none=True).encode()
    )

    log.info(
        f"[Session {session.uuid}] Created ("
        f"scene={session.scene}, "
        f"joints={'ALL' if session.joint == ['*'] else len(session.joint)}, "
        f"cameras={'ALL' if set(session.camera.keys()) == {'*'} else len(session.camera)}, "
        f"links={'ALL' if session.link == ['*'] else len(session.link)})"
    )
    return session


@app.get("/session/{session:uuid}", response_model=motion.session.SessionBase)
async def session_lookup(session: pydantic.UUID4) -> motion.session.SessionBase:
    try:
        session = motion.session.SessionBase.parse_raw(
            b"".join(storage_kv_get("session", f"{session}.json"))
        )
        log.info(f"[Session {session.uuid}] Found (scene={session.scene})")
        return session
    except FileNotFoundError:
        log.warning(f"[Session {session}] Not found")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")


@app.get("/session/{session:uuid}/archive")
async def session_archive(session: pydantic.UUID4):
    try:
        session = motion.session.SessionBase.parse_raw(
            b"".join(storage_kv_get("session", f"{session}.json"))
        )
    except FileNotFoundError:
        log.warning(f"[Session {session}] Not found for archive")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    def stream():
        with tempfile.TemporaryFile() as f:
            with zipfile.ZipFile(f, "w", compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr("session.json", session.json(exclude_none=True))
                try:
                    with z.open("data.json", "w") as g:
                        for chunk in storage_kv_get("data", f"{session.uuid}.json"):
                            g.write(chunk)
                except FileNotFoundError:
                    pass
            f.seek(0)
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                yield chunk

    headers = {
        "Content-Disposition": f'inline; filename="{session.uuid}.zip"',
        "Content-Type": "application/zip",
    }
    return StreamingResponse(stream(), media_type="application/zip", headers=headers)


@app.delete("/session/{session:uuid}", response_model=motion.session.SessionBase)
async def session_delete(session: pydantic.UUID4) -> motion.session.SessionBase:
    # 0) Try to stop first (best-effort, idempotent)
    try:
        await session_stop(session)
    except HTTPException as e:
        # If the stop says "not found", we may still have metadata to delete below.
        if e.status_code != 404:
            log.warning(f"[Session {session}] Stop before delete failed: {e.detail}")
    except Exception as e:
        log.error(
            f"[Session {session}] Unexpected error stopping before delete: {e}",
            exc_info=True,
        )

    # 1) Re-read the session metadata to confirm existence and return value
    session = motion.session.SessionBase.parse_raw(
        b"".join(storage_kv_get("session", f"{session}.json"))
    )
    log.info(f"[Session {session.uuid}] Deleting (scene={session.scene})")
    storage_kv_del("session", f"{session.uuid}.json")
    log.info(f"[Session {session.uuid}] Metadata deleted")

    return session


@app.get("/session", response_model=list[motion.session.SessionBase])
async def session_search(
    q: str | None = Query(
        None, description="Session UUID (full), prefix, or empty for all"
    ),
):
    # Empty or missing -> return ALL sessions
    if not q:
        matches: list[motion.session.SessionBase] = []
        for key in storage_kv_scan("session", ""):
            if not key.endswith(".json"):
                continue
            try:
                session = motion.session.SessionBase.parse_raw(
                    b"".join(storage_kv_get("session", key))
                )
                matches.append(session)
            except Exception:
                continue
        return matches

    # Validate and normalize
    if not re.fullmatch(r"[0-9a-fA-F-]+", q):
        raise HTTPException(status_code=422, detail="UUID prefix must be hex/hyphen")
    norm = q.lower().strip()

    # Fast path: exact UUID
    if len(norm) == 36:
        try:
            _ = pydantic.UUID4(norm)
            session = motion.session.SessionBase.parse_raw(
                b"".join(storage_kv_get("session", f"{norm}.json"))
            )
            return [session]
        except Exception:
            pass  # fall through to prefix scan

    # Prefix scan: keys are "<uuid>.json"
    matches: list[motion.session.SessionBase] = []
    for key in storage_kv_scan("session", norm):
        if not key.endswith(".json"):
            continue
        uuid_part = key[:-5]
        if not uuid_part.startswith(norm):
            continue
        try:
            session = motion.session.SessionBase.parse_raw(
                b"".join(storage_kv_get("session", key))
            )
            matches.append(session)
        except Exception:
            continue
    return matches


@app.get("/session/{session:uuid}/status", response_model=motion.session.SessionStatus)
async def session_status(
    session: pydantic.UUID4, response: Response
) -> motion.session.SessionStatus:
    response.headers["Cache-Control"] = "no-store"

    # 1) session must exist
    if storage_kv_head("session", f"{session}.json") is None:
        log.warning(f"[Session {session}] Status requested for nonexistent session")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    # 2) stop: data artifact present
    if storage_kv_head("data", f"{session}.json") is not None:
        log.info(f"[Session {session}] Status derived: stop (data present)")
        return motion.session.SessionStatus(
            uuid=session,
            state=motion.session.SessionStatusSpec.stop,
            update=datetime.datetime.now(datetime.timezone.utc),
        )

    # 3) play: subscribe to motion.data.{session} (LAST) and try to get one message
    subscribe = await app.state.channel.subscribe_data(str(session), start=-1)
    try:
        await subscribe.next_msg(timeout=0.05)
        log.info(f"[Session {session}] Status derived: play (data messages exist)")
        return motion.session.SessionStatus(
            uuid=session,
            state=motion.session.SessionStatusSpec.play,
            update=datetime.datetime.now(datetime.timezone.utc),
        )
    except nats.errors.TimeoutError:
        # no message available means pending
        pass
    finally:
        await subscribe.unsubscribe()

    # 4) pending
    log.info(f"[Session {session}] Status derived: pending (default)")
    return motion.session.SessionStatus(
        uuid=session,
        state=motion.session.SessionStatusSpec.pending,
        update=datetime.datetime.now(datetime.timezone.utc),
    )


@app.post("/session/{session:uuid}/play", response_model=motion.session.SessionBase)
async def session_play(
    session: pydantic.UUID4,
    device: motion.session.DeviceSpec = Query(..., description="cpu or cuda"),
    model: motion.session.ModelSpec = Query(
        motion.session.ModelSpec.remote,
        description="Execution model: 'model', 'bounce', or 'remote'.",
    ),
    tick: bool = Query(True, description="Enable tick mode."),
) -> motion.session.SessionBase:
    ttl = 3600  # one hour lease

    # 1) ensure session exists
    try:
        session = motion.session.SessionBase.parse_raw(
            b"".join(storage_kv_get("session", f"{session}.json"))
        )
        log.info(
            f"[Session {session.uuid}] Play requested (scene={session.scene}, device={device}, model={model}, tick={tick})"
        )
    except FileNotFoundError:
        log.warning(f"[Session {session}] Not found for play")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    # 2) fetch scene
    try:
        scene = motion.scene.SceneBase.parse_raw(
            b"".join(storage_kv_get("scene", f"{session.scene}.json"))
        )
        log.info(
            f"[Session {session.uuid}] Scene fetched (scene={scene.uuid}, runner={scene.runner})"
        )
    except FileNotFoundError:
        log.warning(
            f"[Session {session.uuid}] Scene {session.scene} not found for play"
        )
        raise HTTPException(status_code=404, detail=f"Scene {session.scene} not found")

    # 2.5) normalize & validate lease payload
    available = {
        motion.scene.RunnerSpec.ros: motion.session.RosNodeSpec,
        motion.scene.RunnerSpec.isaac: motion.session.IsaacNodeSpec,
        motion.scene.RunnerSpec.counter: motion.session.CounterNodeSpec,
    }
    payload = {
        "session": session.uuid,
        "runner": scene.runner,
        "device": device,
        "model": model,
        "tick": tick,
    }
    try:
        spec = available[scene.runner](**payload)
    except pydantic.ValidationError:
        raise HTTPException(status_code=422, detail="invalid session node parameters")

    # 3) extract full list, then filter to node ids
    entries = list(storage_kv_scan("node", "meta/"))
    log.info(
        f"[Session {session.uuid}] Registry scan returned {len(entries)} keys under meta/"
    )
    entries = list(
        map(
            lambda e: e[len("meta/") : -len(".json")],
            filter(lambda e: e.endswith(".json"), entries),
        )
    )
    log.info(f"[Session {session.uuid}] Nodes discovered: {len(entries)}")
    if not entries:
        log.info(f"[Session {session.uuid}] No nodes registered")
        raise HTTPException(status_code=503, detail="No nodes registered")

    # 4) shuffle and try to acquire one lease
    random.shuffle(entries)
    log.debug(f"[Session {session.uuid}] Allocation order: {entries}")

    chosen = None
    data = spec.json(exclude_none=True).encode()

    for node in entries:
        if storage_kv_acquire("node", f"node/{node}.json", data, ttl=ttl):
            chosen = node
            log.info(
                f"[Session {session.uuid}] Lease acquired on node={chosen} "
                f"(device={spec.device}, model={spec.model}, tick={spec.tick})"
            )
            break
        else:
            log.debug(f"[Session {session.uuid}] Node busy: {node}")

    if chosen is None:
        log.info(f"[Session {session.uuid}] All nodes busy (no capacity)")
        raise HTTPException(status_code=503, detail="No capacity")

    # 5) record assignment so /stop can free correctly
    storage_kv_set(
        "node",
        f"work/{session.uuid}.json",
        json.dumps({"node": chosen}, sort_keys=True).encode(),
    )
    log.info(f"[Session {session.uuid}] Assigned node={chosen}")

    return session


@app.post("/session/{session:uuid}/stop", response_model=motion.session.SessionBase)
async def session_stop(session: pydantic.UUID4) -> motion.session.SessionBase:
    # 1) ensure session exists
    try:
        session = motion.session.SessionBase.parse_raw(
            b"".join(storage_kv_get("session", f"{session}.json"))
        )
        log.info(f"[Session {session.uuid}] Stop requested (scene={session.scene})")
    except FileNotFoundError:
        log.warning(f"[Session {session}] Stop requested for nonexistent session")
        raise HTTPException(status_code=404, detail=f"Session {session} not found")

    # 2) release lease first (idempotent)
    try:
        chosen = json.loads(
            b"".join(storage_kv_get("node", f"work/{session.uuid}.json"))
        )["node"]
        log.info(f"[Session {session.uuid}] Releasing lease (node={chosen})")

        storage_kv_release("node", f"node/{chosen}.json")

        with contextlib.suppress(Exception):
            storage_kv_del("node", f"work/{session.uuid}.json")

        log.info(f"[Session {session.uuid}] Lease released and assignment cleared")
    except FileNotFoundError:
        log.info(f"[Session {session.uuid}] No assignment to release (maybe expired)")

    # 3) drain NATS data into S3 (only if missing; otherwise RETURN session)
    try:
        # Try reading a single chunk; if file doesn't exist, throws FileNotFoundError
        next(storage_kv_get("data", f"{session.uuid}.json"))
        log.info(f"[Session {session.uuid}] Data already exists — skipping drain")
        return session
    except (FileNotFoundError, StopIteration):
        log.info(f"[Session {session.uuid}] No existing data — will drain stream")
        # proceed to drain
    except Exception as e:
        # Any other error while checking existence is unexpected -> 500
        log.error(
            f"[Session {session.uuid}] Unexpected error checking data object: {e}",
            exc_info=True,
        )
        raise HTTPException(
            status_code=500,
            detail="Unexpected error while checking session {session}",
        )

    try:
        with tempfile.NamedTemporaryFile(
            prefix=f"{session.uuid}-", suffix=".json"
        ) as f:
            log.info(
                f"[Session {session.uuid}] Begin drain of data stream to temp file={f.name}"
            )

            # Subscribe from the first message in the stream
            sub = await app.state.channel.subscribe_data(str(session.uuid), start=1)
            log.info(
                f"[Session {session.uuid}] Subscribed to NATS data stream (start=1)"
            )

            try:
                while True:
                    try:
                        # wait for next message, idle timeout 30s = end of stream
                        msg = await sub.next_msg(timeout=30.0)
                    except nats.errors.TimeoutError:
                        log.info(
                            f"[Session {session.uuid}] No messages for 30s — assuming end of stream"
                        )
                        break

                    f.write(msg.data + b"\n")
            finally:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
                log.info(f"[Session {session.uuid}] Data subscription closed")

            # Stream the temp file to S3 via storage_kv_set
            f.seek(0)
            storage_kv_set("data", f"{session.uuid}.json", f)
            log.info(f"[Session {session.uuid}] Uploaded s3://data/{session.uuid}.json")

    except Exception as e:
        log.error(
            f"[Session {session.uuid}] Error draining/uploading session data: {e}",
            exc_info=True,
        )

    # 4) return session metadata
    return session


@app.websocket("/session/{session:uuid}/stream")
async def session_stream(ws: WebSocket, session: pydantic.UUID4):
    try:
        storage_kv_get("session", f"{session}.json")
        log.info(f"[Session {session}] WS stream requested")
    except FileNotFoundError:
        log.warning(f"[Session {session}] WS stream for nonexistent session")
        await ws.close(code=1008)
        return

    start_q = ws.query_params.get("start")
    match start_q:
        case None:
            start = None
        case s if s.lstrip("-").isdigit():
            val = int(s)
            if val == -1 or val > 0:
                start = val
            else:
                log.warning(f"[Session {session}] WS stream invalid start={s!r}")
                await ws.close(code=1008)
                return
        case other:
            log.warning(f"[Session {session}] WS stream invalid start={other!r}")
            await ws.close(code=1008)
            return

    await ws.accept()
    log.info(f"[Session {session}] WS stream connected (start={start})")

    async def f_send():
        sub = None
        try:
            # Do the (potentially slow) subscribe here so recv in main can start immediately
            sub = await app.state.channel.subscribe_data(str(session), start=start)
            while True:
                try:
                    msg = await sub.next_msg()  # JetStream may raise TimeoutError
                except nats.errors.TimeoutError:
                    continue
                await ws.send_text(msg.data.decode())
                log.info(f"[Session {session}] send={msg}")
        except WebSocketDisconnect as e:
            log.info(f"[Session {session}] WS stream send disconnected (code={e.code})")
        except asyncio.CancelledError:
            log.info(f"[Session {session}] WS stream send cancelled")
        except Exception as e:
            log.error(f"[Session {session}] WS stream send error: {e}", exc_info=True)
            raise
        finally:
            with contextlib.suppress(Exception):
                if sub is not None:
                    await sub.unsubscribe()

    send_task = asyncio.create_task(f_send(), name=f"ws-send-{session}")

    try:
        while True:
            data = await ws.receive_text()  # drain ASAP; don't block this path
            log.info(f"[Session {session}] recv={data}")
            try:
                step = motion.session.SessionStepSpec.parse_obj(json.loads(data))
            except Exception as e:
                log.warning(f"[Session {session}] Invalid step: {e}")
                await ws.close(code=1007, reason="invalid step payload")
                break

            # If this can be slow under load, consider a queue+worker; otherwise publish directly
            await app.state.channel.publish_step(
                str(session), step.json(exclude_none=True).encode()
            )

    except WebSocketDisconnect as e:
        log.info(f"[Session {session}] WS stream recv disconnected (code={e.code})")
    except asyncio.CancelledError:
        log.info(f"[Session {session}] WS stream recv cancelled")
    except Exception as e:
        log.error(f"[Session {session}] WS stream recv error: {e}", exc_info=True)
        raise
    finally:
        # stop background sender and close socket
        send_task.cancel()
        with contextlib.suppress(Exception):
            await send_task
        with contextlib.suppress(Exception):
            await ws.close()
        log.info(f"[Session {session}] WS stream closed (start={start})")
