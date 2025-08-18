import uuid

import pytest
import requests

import motion


def test_session(capsys, session_on_server):
    # Fixture uploads a scene, creates a session, and yields (base, session, scene)
    base, session, scene = session_on_server

    # construct client model (fetch-on-init)
    sess = motion.Session(base, session)

    # optional sanity on ids
    assert str(sess.uuid) == session

    # drive behavior that prints
    sess.step()
    sess.play()

    out = capsys.readouterr().out
    assert f"Step scene uuid='{scene}'" in out
    assert f"Play scene uuid='{scene}'" in out

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(requests.HTTPError):
        motion.Session(base, bogus)
