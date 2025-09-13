import asyncio
import io
import json
import time
import uuid
import zipfile

import httpx
import websockets


def test_server_health(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    r = httpx.get(f"{base}/health", timeout=2.0)
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_server_scene(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"

    # CREATE (+ upload) -> POST /scene with multipart zip: scene.usd + meta.json
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        usd_contents = "#usda 1.0\ndef X {\n}\n"
        z.writestr("scene.usd", usd_contents)
        z.writestr("meta.json", json.dumps({"runtime": "echo"}))
    buf.seek(0)
    files = {"file": ("scene.zip", buf, "application/zip")}
    r = httpx.post(f"{base}/scene", files=files, timeout=5.0)
    assert r.status_code == 201, r.text
    scene = r.json()["uuid"]
    assert scene

    # search
    r = httpx.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == [{"uuid": scene}]

    # lookup
    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": scene}

    # ARCHIVE (download) -> GET /scene/{uuid}/archive
    r = httpx.get(f"{base}/scene/{scene}/archive", timeout=5.0)
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        names = set(z.namelist())
        assert "scene.usd" in names
        assert "meta.json" in names

        with z.open("meta.json") as f:
            meta = json.loads(f.read().decode("utf-8"))
            assert meta.get("runtime") in ("isaac", "echo")  # accept either

        with z.open("scene.usd") as f:
            assert f.read().decode("utf-8") == usd_contents

    # negative search
    bogus = str(uuid.uuid4())
    r = httpx.get(f"{base}/scene", params={"q": bogus}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == []

    # delete
    r = httpx.delete(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": scene}

    # after delete: search empty, lookup/archive 404
    r = httpx.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200 and r.json() == []

    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 404

    r = httpx.get(f"{base}/scene/{scene}/archive", timeout=5.0)
    assert r.status_code == 404


def test_server_session(scene_on_server):
    base, scene = scene_on_server  # fixture: POST /scene with a tiny zip

    # ---- helpers ----

    def f_archive_lines_if_ready(session: str):
        r = httpx.get(f"{base}/session/{session}/archive", timeout=10.0)
        if r.status_code != 200:
            return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            if "data.json" not in set(z.namelist()):
                return None
            with z.open("data.json") as f:
                content = f.read().decode("utf-8", errors="ignore")
                lines = [ln for ln in content.splitlines() if ln.strip()]
                if not lines:
                    return None
                for ln in lines:
                    json.loads(ln)  # validate JSON
                return lines

    def f_wait_for_data_json(
        session: str, timeout_s: float = 60.0, interval_s: float = 1.0
    ):
        deadline = time.monotonic() + timeout_s
        last_err = None
        while time.monotonic() < deadline:
            try:
                lines = f_archive_lines_if_ready(session)
                if lines:
                    return lines
            except Exception as e:
                last_err = e
            time.sleep(interval_s)
        raise AssertionError(
            f"Timed out waiting for data.json for session={session}. Last error={last_err!r}"
        )

    async def f_stream_steps_entire_run(
        ws_url: str,
        duration: float,
        period: float = 0.2,
        retry_backoff: float = 1.0,
    ):
        """
        Send a step every `period` seconds for the full `duration`.
        If the WS drops, reconnect after `retry_backoff` and continue until time is up.
        """
        loop = asyncio.get_running_loop()
        end = loop.time() + duration
        i = 0

        while loop.time() < end:
            try:
                async with websockets.connect(ws_url, ping_interval=None) as ws:
                    while loop.time() < end:
                        await ws.send(json.dumps({"k": "v", "i": i}))
                        i += 1
                        remaining = end - loop.time()
                        if remaining <= 0:
                            break
                        await asyncio.sleep(min(period, remaining))
            except Exception:
                remaining = end - loop.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(retry_backoff, remaining))

    async def f_subscribe_data_for(ws_url: str, duration: float):
        """
        Subscribe for exactly `duration` seconds; collect any messages without ending early.
        Tolerate clean early close (1000 OK) and still honor the wall-clock duration.
        Used for post-stop checks (NEW vs start=1).
        """
        results = []
        loop = asyncio.get_running_loop()
        end = loop.time() + duration
        try:
            async with websockets.connect(ws_url, ping_interval=None) as ws:
                while loop.time() < end:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        try:
                            results.append(json.loads(msg))
                        except Exception:
                            results.append(msg)
                    except asyncio.TimeoutError:
                        continue
        except websockets.exceptions.ConnectionClosedOK:
            remaining = end - loop.time()
            if remaining > 0:
                await asyncio.sleep(remaining)
        return results

    async def f_wait_for_play_ready(ws_url: str, timeout: float = 300.0):
        """
        After POST /play, connect to the session WS and wait until we see the initial
        readiness message from the runtime, typically {"op":"none"}.
        Retries/reconnects until `timeout`.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        last_err = None
        while loop.time() < deadline:
            try:
                async with websockets.connect(ws_url, ping_interval=None) as ws:
                    while loop.time() < deadline:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1.5)
                        except asyncio.TimeoutError:
                            continue
                        try:
                            data = json.loads(msg)
                        except Exception:
                            # Non-JSON or noise; ignore and continue waiting.
                            continue
                        if data.get("op") == "none" or data:
                            return True
            except Exception as e:
                last_err = e
                await asyncio.sleep(0.5)
        raise AssertionError(
            f"Timed out waiting for play readiness via WS. Last error={last_err!r}"
        )

    # 1) ARCHIVE before session exists -> 404
    bogus_session = str(uuid.uuid4())
    r = httpx.get(f"{base}/session/{bogus_session}/archive", timeout=5.0)
    assert r.status_code == 404

    # ---- CASE 1: Explicit stop flow ----
    r = httpx.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session = r.json()["uuid"]

    r = httpx.post(f"{base}/session/{session}/play", timeout=5.0)
    assert r.status_code == 200

    # Build WS URLs:
    ws_url_stream = f"ws://{base.split('://',1)[1]}/session/{session}/stream"  # live
    ws_url_stream_begin = f"{ws_url_stream}?start=1"  # from beginning

    # Wait until the runtime signals it's ready ({"op":"none"}) from the beginning
    asyncio.run(f_wait_for_play_ready(ws_url_stream_begin, timeout=300.0))

    # Stream steps during the entire play window
    RUN_WINDOW = 15.0
    asyncio.run(
        f_stream_steps_entire_run(ws_url_stream, duration=RUN_WINDOW, period=0.2)
    )

    # Stop and allow archiver to flush (active archive wait)
    r = httpx.post(f"{base}/session/{session}/stop", timeout=5.0)
    assert r.status_code == 200
    lines = f_wait_for_data_json(session, timeout_s=60.0, interval_s=1.0)
    assert lines

    # archive must contain valid JSONL (validate the lines we just waited for)
    for ln in lines:
        json.loads(ln)

    # After stop: NEW subscription should see nothing within the flush window (30s)
    ws_url_stream_new_after = f"ws://{base.split('://',1)[1]}/session/{session}/stream"
    got_new_after = asyncio.run(
        f_subscribe_data_for(ws_url_stream_new_after, duration=30.0)
    )
    assert not got_new_after, "did not expect NEW data after stop within 30s"

    # After stop: replay from beginning should work (start=1)
    ws_url_stream_replay = (
        f"ws://{base.split('://',1)[1]}/session/{session}/stream?start=1"
    )
    got_replay = asyncio.run(f_subscribe_data_for(ws_url_stream_replay, duration=10.0))
    assert got_replay, "expected to receive replayed data with start=1"

    r = httpx.delete(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 200

    # ---- CASE 2: Natural completion (no stop) ----
    r = httpx.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session2 = r.json()["uuid"]

    r = httpx.post(f"{base}/session/{session2}/play", timeout=5.0)
    assert r.status_code == 200

    # Build WS URLs for session2
    ws_url2_stream = f"ws://{base.split('://',1)[1]}/session/{session2}/stream"  # live
    ws_url2_stream_begin = f"{ws_url2_stream}?start=1"  # from beginning

    # Wait for play readiness for the second session as well (from the beginning)
    asyncio.run(f_wait_for_play_ready(ws_url2_stream_begin, timeout=300.0))

    # Stream steps during the entire play window
    asyncio.run(
        f_stream_steps_entire_run(ws_url2_stream, duration=RUN_WINDOW, period=0.2)
    )

    # For consistency we still issue an explicit stop (even if node would finish naturally)
    r = httpx.post(f"{base}/session/{session2}/stop", timeout=5.0)
    assert r.status_code == 200

    # Wait for archive population instead of fixed sleep
    _ = f_wait_for_data_json(session2, timeout_s=60.0, interval_s=1.0)

    r = httpx.delete(f"{base}/session/{session2}", timeout=5.0)
    assert r.status_code == 200
