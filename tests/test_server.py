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

    # create session
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    data = r.json()
    session = data["uuid"]
    assert data == {"uuid": session, "scene": scene}

    # lookup
    r = requests.get(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": session, "scene": scene}

    # invoke play, then stop, and wait
    r = requests.post(f"{base}/session/{session}/play", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "accepted", "uuid": session}
    time.sleep(5)  # allow time to observe publish logs

    r = requests.post(f"{base}/session/{session}/stop", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "accepted", "uuid": session}
    time.sleep(5)  # allow time to observe publish logs

    # delete
    r = requests.delete(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "uuid": session}

    # lookup after delete → 404
    r = requests.get(f"{base}/session/{session}", timeout=5.0)
    assert r.status_code == 404

    # create with bogus scene → 404
    bogus_scene = str(uuid.uuid4())
    r = requests.post(f"{base}/session", json={"scene": bogus_scene}, timeout=5.0)
    assert r.status_code == 404
    assert r.json().get("detail") == "scene not found"

    # natural-completion path (no stop)
    # create another session (reuse the same scene_on_server 'scene')
    r = requests.post(f"{base}/session", json={"scene": scene}, timeout=5.0)
    assert r.status_code == 201, r.text
    session2 = r.json()["uuid"]
    assert session2

    # play but DO NOT stop; worker should finish on its own (15s play + ~1s poll)
    r = requests.post(f"{base}/session/{session2}/play", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "accepted", "uuid": session2}

    # Give enough time for natural completion to occur.
    # (If your worker uses 15s sleep + 1s poll, 30s is a safe margin.)
    time.sleep(30)

    # clean up the session
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
