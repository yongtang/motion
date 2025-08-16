import io
import uuid
import zipfile

import pytest
import requests

import motion


def test_scene_round_trip(scene_on_server):
    base, scene = scene_on_server

    s = motion.Scene(base, scene)
    assert s.uuid == scene


def test_scene_init_raises_on_404(server_container):
    base = f"http://{server_container['addr']}:{server_container['port']}"
    bogus = str(uuid.uuid4())

    with pytest.raises(requests.HTTPError):
        motion.Scene(base, bogus)
