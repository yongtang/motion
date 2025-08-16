import asyncio
import io
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

    # --- create ---
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("hello.txt", "world")
    buf.seek(0)

    files = {"file": ("scene.zip", buf, "application/zip")}
    r = requests.post(f"{base}/scene", files=files, timeout=5.0)
    assert r.status_code == 200
    scene = r.json()["uuid"]
    assert len(scene) > 0

    # --- lookup ---
    r = requests.get(f"{base}/scene/{scene}", timeout=2.0)
    assert r.status_code == 200
    assert r.json()["uuid"] == scene

    # --- delete ---
    r = requests.delete(f"{base}/scene/{scene}", timeout=2.0)
    assert r.status_code == 200
    assert r.json() == {"status": "deleted", "uuid": scene}

    # --- lookup after delete ---
    r = requests.get(f"{base}/scene/{scene}", timeout=2.0)
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
