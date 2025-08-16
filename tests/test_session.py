import motion


def test_session(capsys):
    scene = motion.Scene({"robot": "atlas", "world": "warehouse"})
    session = motion.Session(scene)

    session.step()
    captured = capsys.readouterr()
    assert "Step scene" in captured.out

    session.play()
    captured = capsys.readouterr()
    assert "Play scene" in captured.out
