import uuid

import pytest
import requests

import motion


def test_session(capsys, session_on_server):
    # Fixture uploads a scene, creates a session, and yields (base, session_uuid, scene_uuid)
    base, session_uuid, scene_uuid = session_on_server

    # construct client model (fetch-on-init)
    sess = motion.Session(base, session_uuid)

    # optional sanity on ids
    assert str(sess.uuid) == session_uuid

    # drive behavior that prints
    sess.step()
    sess.play()

    out = capsys.readouterr().out
    assert f"Step scene uuid='{scene_uuid}'" in out
    assert f"Play scene uuid='{scene_uuid}'" in out

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(requests.HTTPError):
        motion.Session(base, bogus)
