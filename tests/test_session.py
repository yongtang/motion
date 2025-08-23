import time
import uuid

import pytest
import requests

import motion


def test_session(capsys, session_on_server):
    # Fixture uploads a scene, creates a session, and yields (base, session, scene)
    base, session, scene = session_on_server

    # construct client model (fetch-on-init)
    session = motion.Session(base, session)

    # optional sanity on ids
    assert str(session.uuid) == session_on_server[1]

    # drive behavior that prints
    # (updated: invoke play, wait, then stop)
    session.play()
    time.sleep(60)
    session.stop()

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(requests.HTTPError):
        motion.Session(base, bogus)
