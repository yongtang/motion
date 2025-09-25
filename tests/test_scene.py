import httpx

import motion


def test_scene(scene_on_server):
    base, scene = scene_on_server

    # fetch runner from server
    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200, r.text
    runner = motion.scene.SceneRunnerSpec.parse_obj(r.json()["runner"])

    # construct Scene with uuid + runner
    s = motion.Scene(base, scene, runner, timeout=5.0)
    assert str(s.uuid) == scene
    assert s.runner == runner
