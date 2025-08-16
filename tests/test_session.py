import motion


def test_session(capsys):
    scene = motion.Scene(name="warehouse")

    print(scene.name)
    out = capsys.readouterr().out
    assert "warehouse" in out and scene.name == "warehouse"
