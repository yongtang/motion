import motion


def test_scene_minimal():
    scene = motion.Scene(name="Warehouse")
    assert scene.name == "Warehouse"
    assert scene.usd is None


def test_scene_with_usd():
    scene = motion.Scene(name="Factory", usd="factory.usd")
    assert scene.name == "Factory"
    assert scene.usd == "factory.usd"
