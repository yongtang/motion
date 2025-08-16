import json

import motion


def test_config_from_dict():
    s = motion.Scene({"robot": "ur5"})
    assert s.config["robot"] == "ur5"


def test_config_from_json_string():
    cfg = {"world": "warehouse"}
    s = motion.Scene(json.dumps(cfg))
    assert s.config["world"] == "warehouse"
