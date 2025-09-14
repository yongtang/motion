import asyncio
import json
import pathlib
import tempfile
import time
import uuid

import httpx
import websockets

import motion


def test_client_scene(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    client = motion.client(base=base, timeout=5.0)

    # create from USD + runner (client zips internally and uploads)
    with tempfile.TemporaryDirectory() as tdir:
        tdir = pathlib.Path(tdir)
        usd_path = tdir / "scene.usd"
        usd_contents = "#usda 1.0\ndef X {\n}\n"
        usd_path.write_text(usd_contents, encoding="utf-8")

        runner = "echo"
        scene = client.scene.create(usd_path, runner)
    assert scene

    # search should find the scene
    assert client.scene.search(str(scene.uuid)) == [scene]

    # direct REST check for minimal metadata
    r = httpx.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": str(scene.uuid)}

    # archive via client API and just confirm file exists and is non-empty
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{scene.uuid}.zip"
        client.scene.archive(scene, out)
        assert out.exists() and out.stat().st_size > 0

    # search for a random uuid should be empty
    bogus = str(uuid.uuid4())
    assert client.scene.search(bogus) == []

    # delete
    assert client.scene.delete(scene) is None

    # search after delete should be empty
    assert client.scene.search(str(scene.uuid)) == []

    # after delete, REST lookup should 404
    r = httpx.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
    assert r.status_code == 404


def test_client_session(scene_on_server):
    base, scene = scene_on_server
    client = motion.client(base=base, timeout=5.0)
    scene_obj = motion.Scene(base, scene, timeout=5.0)

    # ---- helpers ----

    async def f_wait_for_play_ready(ws_url: str, timeout: float = 300.0):
        """
        After POST /play, connect to the session WS and wait until we see the initial
        readiness message from the runner, typically {"op":"none"}.
        Returns early once ready; retries/reconnects until `timeout`.
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

    def f_wait_client_archive_nonempty(
        session_obj, out_path, timeout_s: float = 60.0, interval_s: float = 1.0
    ):
        """
        Poll client.session.archive until the written file exists and is non-empty.
        Returns the path once ready.
        """
        deadline = time.monotonic() + timeout_s
        last_err = None
        out_path = pathlib.Path(out_path)
        while time.monotonic() < deadline:
            try:
                if out_path.exists():
                    try:
                        out_path.unlink()
                    except Exception:
                        pass
                client.session.archive(session_obj, out_path)
                if out_path.exists() and out_path.stat().st_size > 0:
                    return out_path
            except Exception as e:
                last_err = e
            time.sleep(interval_s)
        raise AssertionError(
            f"Timed out waiting for non-empty archive for session={session_obj.uuid}. Last error={last_err!r}"
        )

    # explicit stop flow with active readiness + archive waits
    session = client.session.create(scene_obj)
    assert isinstance(session, motion.Session)

    # play -> wait until WS readiness ({"op":"none"}) -> stop
    r = httpx.post(f"{base}/session/{session.uuid}/play", timeout=5.0)
    assert r.status_code == 200

    # Use start=1 so we don't miss the initial readiness event
    ws_url_stream_begin = (
        f"ws://{base.split('://',1)[1]}/session/{session.uuid}/stream?start=1"
    )
    asyncio.run(f_wait_for_play_ready(ws_url_stream_begin, timeout=300.0))

    r = httpx.post(f"{base}/session/{session.uuid}/stop", timeout=5.0)
    assert r.status_code == 200

    # archive via client API, but wait actively until non-empty
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{session.uuid}.zip"
        f_wait_client_archive_nonempty(session, out, timeout_s=60.0, interval_s=1.0)
        assert out.exists() and out.stat().st_size > 0

    # cleanup
    assert client.session.delete(session) is None
    r = httpx.get(f"{base}/session/{session.uuid}", timeout=5.0)
    assert r.status_code == 404
