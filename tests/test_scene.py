import io
import uuid
import zipfile

import pytest
import requests

import motion


def test_scene(scene_on_server):
    base, scene = scene_on_server

    s = motion.Scene(base, scene)
    assert str(s.uuid) == scene

    bogus = str(uuid.uuid4())

    with pytest.raises(requests.HTTPError):
        motion.Scene(base, bogus)
