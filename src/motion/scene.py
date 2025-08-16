import json


class Scene:
    def __init__(self, config):
        if isinstance(config, dict):
            self.config = config
        elif isinstance(config, str):
            try:
                self.config = json.loads(config)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON string: {e}") from e
        else:
            raise TypeError("config must be dict or JSON string")
