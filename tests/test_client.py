import json
import pathlib
import tempfile
import uuid
import zipfile

import pytest
import requests

import motion


def test_client_scene(docker_compose):
    base = f"http://{docker_compose['motion']}:8080"
    client = motion.client(base=base, timeout=5.0)

    # ---- create from USD + runtime (client zips internally and uploads)
    with tempfile.TemporaryDirectory() as tdir:
        tdir = pathlib.Path(tdir)
        usd_path = tdir / "scene.usd"
        usd_contents = "#usda 1.0\ndef X {\n}\n"
        usd_path.write_text(usd_contents, encoding="utf-8")

        runtime = "ros2"
        scene = client.scene.create(usd_path, runtime)
    assert scene

    # ---- search: should find the scene
    assert client.scene.search(str(scene.uuid)) == [scene]

    # ---- direct REST check for minimal metadata
    r = requests.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": str(scene.uuid)}

    # ---- archive (download) and check contents: expect USD + meta.json
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{scene.uuid}.zip"
        client.scene.archive(scene, out)
        with zipfile.ZipFile(out) as z:
            names = set(z.namelist())
            assert "scene.usd" in names
            assert "meta.json" in names

            # meta.json should contain the chosen runtime
            with z.open("meta.json") as f:
                meta = json.loads(f.read().decode("utf-8"))
                assert meta.get("runtime") == runtime

            # USD file should match what we uploaded
            with z.open("scene.usd") as f:
                assert f.read().decode("utf-8") == usd_contents

    # ---- search for a random uuid should be empty
    bogus = str(uuid.uuid4())
    assert client.scene.search(bogus) == []

    # ---- delete
    assert client.scene.delete(scene) == {"status": "deleted", "uuid": str(scene.uuid)}

    # ---- search after delete should be empty
    assert client.scene.search(str(scene.uuid)) == []

    # ---- after delete, REST lookup should 404
    r = requests.get(f"{base}/scene/{scene.uuid}", timeout=5.0)
    assert r.status_code == 404


def test_client_session(scene_on_server):
    base, scene = scene_on_server
    client = motion.client(base=base, timeout=5.0)

    # Build a Scene object for the existing scene id from the fixture
    scene_obj = motion.Scene(base, scene, timeout=5.0)

    # ---- CREATE: session from a real scene -> returns Session
    session = client.session.create(scene_obj)
    assert isinstance(session, motion.Session) and session

    # ---- verify mapping via REST
    r = requests.get(f"{base}/session/{session.uuid}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": str(session.uuid), "scene": scene}

    # ---- DELETE: then REST lookup should 404
    assert client.session.delete(session) == {
        "status": "deleted",
        "uuid": str(session.uuid),
    }
    r = requests.get(f"{base}/session/{session.uuid}", timeout=5.0)
    assert r.status_code == 404

    # ---- NEGATIVE CREATE: nonexistent scene -> 404
    bogus_scene = str(uuid.uuid4())
    # Create a lightweight dummy with a .uuid attribute to avoid fetching via Scene(...)
    DummyScene = type("DummyScene", (), {})
    dummy = DummyScene()
    dummy.uuid = bogus_scene
    with pytest.raises(requests.HTTPError) as ei2:
        client.session.create(dummy)
    assert ei2.value.response is not None
    assert ei2.value.response.status_code == 404
    assert ei2.value.response.json().get("detail") == "scene not found"
