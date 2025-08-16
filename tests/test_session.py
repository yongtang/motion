import motion


def test_session(capsys, scene_on_server):
    base, scene = scene_on_server

    s = motion.Scene(base, scene)
    session = motion.Session(s)

    session.step()
    session.play()

    out = capsys.readouterr().out
    assert f"Step scene uuid='{scene}'" in out
    assert f"Play scene uuid='{scene}'" in out
    assert s.uuid == scene
