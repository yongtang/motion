import pathlib
import tempfile
import time
import uuid
import zipfile

import pytest
import requests

import motion


def test_client_scene(server_container):
    base = f"http://{server_container['addr']}:{server_container['port']}"
    client = motion.client(base=base, timeout=5.0)

    # ---- create (+ upload) in one step: returns a uuid str
    with tempfile.TemporaryDirectory() as tdir:
        zip_path = pathlib.Path(tdir) / "scene.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("hello.txt", "world")
        scene = client.scene.create(zip_path)
    assert scene

    # ---- search: should find the scene
    assert client.scene.search(scene) == [scene]

    # ---- direct REST check for minimal metadata
    r = requests.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": scene}

    # ---- archive (download) and check contents
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{scene}.zip"
        client.scene.archive(scene, out)
        with zipfile.ZipFile(out) as z:
            with z.open("hello.txt") as f:
                assert f.read().decode("utf-8") == "world"

    # ---- search for a random uuid should be empty
    bogus = str(uuid.uuid4())
    assert client.scene.search(bogus) == []

    # ---- delete
    assert client.scene.delete(scene) == {"status": "deleted", "uuid": scene}

    # ---- search after delete should be empty
    assert client.scene.search(scene) == []

    # ---- after delete, REST lookup should 404
    r = requests.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 404


def test_client_session(scene_on_server):
    base, scene_uuid = scene_on_server
    client = motion.client(base=base, timeout=5.0)

    # ---- CREATE: session from a real scene -> returns session uuid str
    session_uuid = client.session.create(scene_uuid)
    assert isinstance(session_uuid, str) and session_uuid

    # ---- verify mapping via REST
    r = requests.get(f"{base}/session/{session_uuid}", timeout=5.0)
    assert r.status_code == 200
    assert r.json() == {"uuid": session_uuid, "scene": scene_uuid}

    # ---- DELETE: then REST lookup should 404
    assert client.session.delete(session_uuid) == {
        "status": "deleting",
        "uuid": session_uuid,
    }
    time.sleep(20)
    r = requests.get(f"{base}/session/{session_uuid}", timeout=5.0)
    assert r.status_code == 404

    # ---- NEGATIVE CREATE: nonexistent scene -> 404
    bogus_scene = str(uuid.uuid4())
    with pytest.raises(requests.HTTPError) as ei2:
        client.session.create(bogus_scene)
    assert ei2.value.response is not None
    assert ei2.value.response.status_code == 404
    assert ei2.value.response.json().get("detail") == "scene not found"
