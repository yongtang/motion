import motion


def test_scene(scene_on_server):
    base, scene = scene_on_server

    # scene is already a motion.Scene from the fixture
    assert isinstance(scene, motion.Scene)
    assert scene.base == base.rstrip("/")
    assert scene.uuid
    assert scene.runner == "counter"
