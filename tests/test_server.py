import asyncio
import io
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

    # CREATE (+ upload) -> POST /scene with multipart zip -> 201 + {"uuid": "..."}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("hello.txt", "world")
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
        assert "hello.txt" in z.namelist()
        with z.open("hello.txt") as f:
            assert f.read().decode("utf-8") == "world"

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
