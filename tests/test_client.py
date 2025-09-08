import pathlib
import tempfile
import time
import uuid

import requests

import motion


def test_client_scene(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    client = motion.client(base=base, timeout=5.0)

    # create from USD + runtime (client zips internally and uploads)
    with tempfile.TemporaryDirectory() as tdir:
        tdir = pathlib.Path(tdir)
        usd_path = tdir / "scene.usd"
        usd_contents = "#usda 1.0\ndef X {\n}\n"
        usd_path.write_text(usd_contents, encoding="utf-8")

        runtime = "echo"
        scene = client.scene.create(usd_path, runtime)
    assert scene

    # search should find the scene
    assert client.scene.search(str(scene.uuid)) == [scene]

    # direct REST check for minimal metadata
    r = requests.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
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
    r = requests.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
    assert r.status_code == 404


def test_client_session(scene_on_server):
    base, scene = scene_on_server
    client = motion.client(base=base, timeout=5.0)
    scene_obj = motion.Scene(base, scene, timeout=5.0)

    # explicit stop flow only (keep long sleeps)
    session = client.session.create(scene_obj)
    assert isinstance(session, motion.Session)

    # play -> wait 120s -> stop -> wait 30s
    r = requests.post(f"{base}/session/{session.uuid}/play", timeout=5.0)
    assert r.status_code == 200
    time.sleep(30)

    r = requests.post(f"{base}/session/{session.uuid}/stop", timeout=5.0)
    assert r.status_code == 200
    time.sleep(30)

    # archive via client API and confirm file exists and is non-empty
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{session.uuid}.zip"
        client.session.archive(session, out)
        assert out.exists() and out.stat().st_size > 0

    # cleanup
    assert client.session.delete(session) is None
    r = requests.get(f"{base}/session/{session.uuid}", timeout=5.0)
    assert r.status_code == 404
