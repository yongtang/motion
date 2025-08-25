import asyncio
import io
import json
import time
import uuid
import zipfile

import requests
import websockets


def test_server_health(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    r = requests.get(f"{base}/health", timeout=2.0)
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
    r = requests.post(f"{base}/scene", files=files, timeout=5.0)
    assert r.status_code == 201, r.text
    scene = r.json()["uuid"]
    assert scene

    # search
    r = requests.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == [scene]

    # lookup
    r = requests.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": scene}

    # ARCHIVE (download) -> GET /scene/{uuid}/archive
    r = requests.get(f"{base}/scene/{scene}/archive", timeout=5.0)
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
    r = requests.get(f"{base}/scene", params={"q": bogus}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == []

    # delete
    r = requests.delete(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "uuid": scene}

    # after delete: search empty, lookup/archive 404
    r = requests.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200 and r.json() == []

    r = requests.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 404

    r = requests.get(f"{base}/scene/{scene}/archive", timeout=5.0)
    assert r.status_code == 404


def test_server_session(scene_on_server):
    base, scene = scene_on_server  # fixture: POST /scene with a tiny zip

    # local helpers
    def f_archive_lines_if_ready(session: str):
        r = requests.get(f"{base}/session/{session}/archive", timeout=10.0)
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
        session: str, timeout_s: float = 120.0, interval_s: float = 2.0
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

    async def f_send_steps(ws_url: str, count: int = 5, delay: float = 0.05):
        import websockets, asyncio, json

        async with websockets.connect(ws_url, ping_interval=None) as ws:
            for i in range(count):
                await ws.send(json.dumps({"k": "v", "i": i}))
                await asyncio.sleep(delay)

    # 1) ARCHIVE before session exists -> 404
    bogus_session = str(uuid.uuid4())
    r = requests.get(f"{base}/session/{bogus_session}/archive", timeout=5.0)
    assert r.status_code == 404

    # CASE 1: Explicit stop flow
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session = r.json()["uuid"]

    r = requests.post(f"{base}/session/{session}/play", timeout=5.0)
    assert r.status_code == 200

    ws_url = f"ws://{base.split('://',1)[1]}/session/{session}/step"
    asyncio.run(f_send_steps(ws_url, count=8, delay=0.05))

    time.sleep(150)
    r = requests.post(f"{base}/session/{session}/stop", timeout=5.0)
    assert r.status_code == 200

    # after stop wait at least 60s as archiver has 30s timeout.
    time.sleep(60)

    r = requests.get(f"{base}/session/{session}/archive", timeout=10.0)
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        names = set(z.namelist())
        assert "data.json" in names
        with z.open("data.json") as f:
            content = f.read().decode("utf-8", errors="ignore")
            lines = [ln for ln in content.splitlines() if ln.strip()]
            assert lines
            for ln in lines:
                json.loads(ln)

    r = requests.delete(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 200

    # CASE 2: Natural completion (no stop)
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session2 = r.json()["uuid"]

    r = requests.post(f"{base}/session/{session2}/play", timeout=5.0)
    assert r.status_code == 200

    ws_url2 = f"ws://{base.split('://',1)[1]}/session/{session2}/step"
    asyncio.run(f_send_steps(ws_url2, count=5, delay=0.05))

    # keep same sleeps as Case 1 so node has time to process
    time.sleep(150)
    r = requests.post(f"{base}/session/{session2}/stop", timeout=5.0)
    assert r.status_code == 200

    # after stop wait at least 60s as archiver has 30s timeout.
    time.sleep(60)

    # ARCHIVE must now contain data.json
    r = requests.get(f"{base}/session/{session2}/archive", timeout=10.0)
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        names = set(z.namelist())
        assert "data.json" in names

    r = requests.delete(f"{base}/session/{session2}", timeout=5.0)
    assert r.status_code == 200


def test_server_websocket_echo(docker_compose):
    url = f"ws://{docker_compose['motion']}:8080/ws"

    async def _run():
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            greeting = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert greeting == "hello from server"
            await ws.send("hello")
            echoed = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert echoed == "echo: hello"

    asyncio.run(_run())
