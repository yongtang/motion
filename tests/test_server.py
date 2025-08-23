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
        z.writestr("meta.json", json.dumps({"runtime": "ros2"}))
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
            assert meta.get("runtime") in ("isaac", "ros2")  # accept either

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
    base, scene = scene_on_server  # fixture already: POST /scene with a tiny zip

    # 1) ARCHIVE before session exists -> 404 (use a random UUID)
    bogus_session = str(uuid.uuid4())
    r = requests.get(f"{base}/session/{bogus_session}/archive", timeout=5.0)
    assert r.status_code == 404

    # 2) create session
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    data = r.json()
    session = data["uuid"]
    assert data == {"uuid": session, "scene": scene}

    # lookup
    r = requests.get(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": session, "scene": scene}

    # 3) ARCHIVE immediately after create -> 200, empty zip (no data.json)
    r = requests.get(f"{base}/session/{session}/archive", timeout=10.0)
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        names = set(z.namelist())
        assert "data.json" not in names
        assert len(names) == 0

    # 4) invoke play, wait long enough, then stop
    r = requests.post(f"{base}/session/{session}/play", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "accepted", "uuid": session}

    # let the worker run for a while
    time.sleep(30)

    r = requests.post(f"{base}/session/{session}/stop", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "accepted", "uuid": session}

    # 5) ARCHIVE after play+stop -> 200, zip contains data.json with NDJSON lines
    deadline = time.time() + 60.0
    found = False
    while time.time() < deadline:
        r = requests.get(f"{base}/session/{session}/archive", timeout=10.0)
        if r.status_code != 200:
            time.sleep(2.0)
            continue
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            names = set(z.namelist())
            if "data.json" in names:
                with z.open("data.json") as f:
                    content = f.read().decode("utf-8", errors="ignore")
                    lines = [ln for ln in content.splitlines() if ln.strip()]
                    if lines:  # at least one non-empty line
                        found = True
                        break
        time.sleep(2.0)

    assert found, "Expected data.json with at least one NDJSON line after play+stop"

    # 6) delete session
    r = requests.delete(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "uuid": session}

    # lookup after delete → 404
    r = requests.get(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 404

    # ARCHIVE after delete -> 404 again
    r = requests.get(f"{base}/session/{session}/archive", timeout=5.0)
    assert r.status_code == 404

    # 7) create with bogus scene → 404
    bogus_scene = str(uuid.uuid4())
    r = requests.post(f"{base}/session", json={"scene": bogus_scene}, timeout=5.0)
    assert r.status_code == 404
    assert r.json().get("detail") == "scene not found"

    # 8) natural-completion path (no stop)
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session2 = r.json()["uuid"]
    assert session2

    r = requests.post(f"{base}/session/{session2}/play", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "accepted", "uuid": session2}

    # Wait longer for natural completion (worker play duration + margin)
    time.sleep(60)

    r = requests.delete(f"{base}/session/{session2}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "uuid": session2}


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
