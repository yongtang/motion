import asyncio
import io
import uuid
import zipfile

import requests
import websockets


def test_server_health(server_container):
    base = f"http://{server_container['addr']}:{server_container['port']}"
    r = requests.get(f"{base}/health", timeout=2.0)
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_server_scene(server_container):
    base = f"http://{server_container['addr']}:{server_container['port']}"

    # create: upload a tiny in-memory zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("hello.txt", "world")
    buf.seek(0)

    files = {"file": ("scene.zip", buf, "application/zip")}
    r = requests.post(f"{base}/scene", files=files, timeout=5.0)
    assert r.status_code == 200
    scene = r.json()["uuid"]
    assert scene

    # search: should find the scene by exact uuid
    r = requests.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == [scene]

    # lookup: download the stored zip and check contents
    r = requests.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        assert "hello.txt" in z.namelist()
        with z.open("hello.txt") as f:
            assert f.read().decode("utf-8") == "world"

    # search for a random uuid should be empty
    bogus = str(uuid.uuid4())
    r = requests.get(f"{base}/scene", params={"q": bogus}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == []

    # delete
    r = requests.delete(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "uuid": scene}

    # search after delete should be empty
    r = requests.get(f"{base}/scene", params={"q": scene}, timeout=5.0)
    assert r.status_code == 200
    assert r.json() == []

    # lookup after delete should 404
    r = requests.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 404


def test_server_websocket_echo(server_container):
    url = f"ws://{server_container['addr']}:{server_container['port']}/ws"

    async def _run():
        async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
            greeting = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert greeting == "hello from server"
            await ws.send("hello")
            echoed = await asyncio.wait_for(ws.recv(), timeout=2.0)
            assert echoed == "echo: hello"

    asyncio.run(_run())
