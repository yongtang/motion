import motion


def test_scene(scene_on_server):
    base, scene = scene_on_server

    s = motion.Scene(base, scene)
    assert str(s.uuid) == scene
