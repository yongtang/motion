import json
import pathlib
import tempfile
import time
import uuid
import zipfile

import httpx

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

    # direct REST check for metadata (now includes runner)
    base = f"http://{docker_compose['motion']}:8080"
    r = httpx.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": str(scene.uuid), "runner": runner}

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

    # fetch runner for the scene so we can construct motion.Scene
    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200, r.text
    runner = r.json()["runner"]

    # Build a Scene with uuid + runner (constructor requires runner)
    scene_obj = motion.Scene(base, scene, runner, timeout=5.0)

    # --- helpers using /status and archive (no websockets) ---

    def f_wait_status(
        session_id: str, want: str, timeout_s: float = 60.0, interval_s: float = 0.25
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
            r2 = httpx.get(f"{base}/session/{session_id}/status", timeout=5.0)
            assert r2.status_code == 200, r2.text
            state = r2.json()["state"]
            last = state
            if want == "play":
                if state == "stop":
                    raise AssertionError(
                        "cannot wait for play: session already stopped"
                    )
                if state == "play":
                    return
            else:  # want == "stop"
                if state == "stop":
                    return
            time.sleep(interval_s)
        raise AssertionError(f"Timed out waiting for state={want}. last_state={last}")

    def f_archive_contains_data_json(session_obj) -> list[str]:
        """Download archive once and return non-empty JSONL lines from data.json."""
        with tempfile.TemporaryDirectory() as tdir:
            out = pathlib.Path(tdir) / f"{session_obj.uuid}.zip"
            client.session.archive(session_obj, out)
            assert out.exists() and out.stat().st_size > 0

            with zipfile.ZipFile(out, "r") as z:
                names = set(z.namelist())
                assert "session.json" in names, "archive missing session.json"
                assert "data.json" in names, "archive missing data.json"
                with z.open("data.json") as f:
                    content = f.read().decode("utf-8", errors="ignore")
                    lines = [ln for ln in content.splitlines() if ln.strip()]
                    assert lines, "data.json empty"
                    for ln in lines:
                        json.loads(ln)
                    return lines

    # explicit stop flow with status-based readiness + stop waits
    session = client.session.create(scene_obj)
    assert isinstance(session, motion.Session)

    # play
    r = httpx.post(f"{base}/session/{session.uuid}/play", timeout=5.0)
    assert r.status_code == 200

    # wait for play using /status
    f_wait_status(str(session.uuid), want="play", timeout_s=300.0, interval_s=0.25)

    # stop
    r = httpx.post(f"{base}/session/{session.uuid}/stop", timeout=5.0)
    assert r.status_code == 200

    # wait for stop using /status, then download archive once and validate data.json
    f_wait_status(str(session.uuid), want="stop", timeout_s=60.0, interval_s=0.5)
    lines = f_archive_contains_data_json(session)
    assert lines

    # cleanup
    assert client.session.delete(session) is None
    r = httpx.get(f"{base}/session/{session.uuid}", timeout=5.0)
    assert r.status_code == 404
