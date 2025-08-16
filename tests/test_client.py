import io
import pathlib
import tempfile
import uuid
import zipfile

import requests

import motion


def test_client_scene(server_container):
    base = f"http://{server_container['addr']}:{server_container['port']}"
    client = motion.client(url=base, timeout=5.0)

    # create: make a tiny zip file on disk
    with tempfile.TemporaryDirectory() as tdir:
        zip_path = pathlib.Path(tdir) / "scene.zip"
        with zipfile.ZipFile(zip_path, "w") as z:
            z.writestr("hello.txt", "world")
        scene = client.scene.create(zip_path)
        assert scene

    # search: should find the scene
    assert client.scene.search(scene) == [scene]

    # lookup: download and check contents
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{scene}.zip"
        client.scene.lookup(scene, out)
        with zipfile.ZipFile(out) as z:
            with z.open("hello.txt") as f:
                assert f.read().decode("utf-8") == "world"

    # search for a random uuid should be empty
    bogus = str(uuid.uuid4())
    assert client.scene.search(bogus) == []

    # delete
    assert client.scene.delete(scene) == {"status": "deleted", "uuid": scene}

    # search after delete should be empty
    assert client.scene.search(scene) == []

    # lookup after delete should raise HTTPError (404)
    with tempfile.TemporaryDirectory() as tdir:
        out = pathlib.Path(tdir) / f"{scene}.zip"
        try:
            client.scene.lookup(scene, out)
            assert False, "expected HTTPError for deleted scene"
        except requests.HTTPError as e:
            assert e.response is not None
            assert e.response.status_code == 404
