import asyncio
import io
import json
import time
import uuid
import zipfile

import httpx
import pytest
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
        z.writestr("meta.json", json.dumps({}))  # empty {}
    buf.seek(0)
    files = {"file": ("scene.zip", buf, "application/zip")}
    data = {
        "image": "count",
        "device": "cpu",
    }  # runner is now (image, device) form fields
    r = httpx.post(f"{base}/scene", files=files, data=data, timeout=5.0)
    assert r.status_code == 201, r.text
    scene = r.json()["uuid"]
    assert scene

    # search
    r = httpx.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == [{"uuid": scene, "runner": {"image": "count", "device": "cpu"}}]

    # lookup
    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": scene, "runner": {"image": "count", "device": "cpu"}}

    # ARCHIVE (download) -> GET /scene/{uuid}/archive
    r = httpx.get(f"{base}/scene/{scene}/archive", timeout=5.0)
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        names = set(z.namelist())
        assert "scene.usd" in names
        assert "meta.json" in names

        with z.open("meta.json") as f:
            meta = json.loads(f.read().decode("utf-8"))
            # meta.json is intentionally empty for forward-compatibility
            assert meta == {}

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
    assert r.json() == {"runner": {"image": "count", "device": "cpu"}, "uuid": scene}

    # after delete: search empty, lookup/archive 404
    r = httpx.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200 and r.json() == []

    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 404

    r = httpx.get(f"{base}/scene/{scene}/archive", timeout=5.0)
    assert r.status_code == 404


@pytest.mark.parametrize(
    "model",
    [
        pytest.param(
            "bounce",
            id="bounce",
        ),
        pytest.param(
            "remote",
            id="remote",
        ),
    ],
)
def test_server_session(scene_on_server, model):
    base, scene = scene_on_server  # fixture: POST /scene with a tiny zip

    # ---- helpers ----

    def f_wait_status(
        session: str, want: str, timeout_s: float = 60.0, interval_s: float = 0.25
    ):
        """
        Poll /session/{id}/status until state==want or timeout.
        want must be 'play' or 'stop'.
        Raises AssertionError on timeout or impossible transition (waiting for play after stop).
        """
        assert want in ("play", "stop")
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            r = httpx.get(f"{base}/session/{session}/status", timeout=5.0)
            assert r.status_code == 200, r.text
            state = r.json()["state"]
            last = state
            if want == "play":
                if state == "stop":
                    raise AssertionError(
                        "cannot wait for play: session already stopped"
                    )
                if state == "play":
                    return
            else:
                if state == "stop":
                    return
            time.sleep(interval_s)
        raise AssertionError(f"Timed out waiting for state={want}. last_state={last}")

    def f_archive_lines(session: str):
        r = httpx.get(f"{base}/session/{session}/archive", timeout=10.0)
        assert r.status_code == 200, r.text
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            names = set(z.namelist())
            assert "session.json" in names, "archive missing session.json"
            assert "data.json" in names, "archive missing data.json"
            with z.open("data.json") as f:
                content = f.read().decode("utf-8", errors="ignore")
                lines = [ln for ln in content.splitlines() if ln.strip()]
                assert lines, "data.json empty"
                for ln in lines:
                    json.loads(ln)  # validate each line is JSON
                return lines

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

    # 1) ARCHIVE before session exists -> 404
    bogus_session = str(uuid.uuid4())
    r = httpx.get(f"{base}/session/{bogus_session}/archive", timeout=5.0)
    assert r.status_code == 404

    # ---- CASE 1: Explicit stop flow ----
    r = httpx.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session = r.json()["uuid"]

    r = httpx.post(f"{base}/session/{session}/play?model={model}", timeout=5.0)
    assert r.status_code == 200

    # Wait for play via status API (no WS needed just to check readiness)
    f_wait_status(session, want="play", timeout_s=300.0, interval_s=0.25)

    # While playing, drive steps over WS
    ws_url_stream = f"ws://{base.split('://',1)[1]}/session/{session}/stream"
    RUN_WINDOW = 15.0
    asyncio.run(
        f_stream_steps_entire_run(ws_url_stream, duration=RUN_WINDOW, period=0.2)
    )

    # Stop
    r = httpx.post(f"{base}/session/{session}/stop", timeout=5.0)
    assert r.status_code == 200

    # Wait until stopped via status API, then fetch archive and validate data.json
    f_wait_status(session, want="stop", timeout_s=60.0, interval_s=0.5)
    lines = f_archive_lines(session)
    assert lines

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

    r = httpx.post(f"{base}/session/{session2}/play?model={model}", timeout=5.0)
    assert r.status_code == 200

    # Wait for play via status API
    f_wait_status(session2, want="play", timeout_s=300.0, interval_s=0.25)

    # Drive steps
    ws_url2_stream = f"ws://{base.split('://',1)[1]}/session/{session2}/stream"
    asyncio.run(
        f_stream_steps_entire_run(ws_url2_stream, duration=RUN_WINDOW, period=0.2)
    )

    # For consistency issue explicit stop (even if node might finish naturally)
    r = httpx.post(f"{base}/session/{session2}/stop", timeout=5.0)
    assert r.status_code == 200

    # Wait for stop, then fetch archive and validate
    f_wait_status(session2, want="stop", timeout_s=60.0, interval_s=0.5)
    _ = f_archive_lines(session2)

    r = httpx.delete(f"{base}/session/{session2}", timeout=5.0)
    assert r.status_code == 200
