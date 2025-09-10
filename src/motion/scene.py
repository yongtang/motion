import pydantic
import requests

from .motionclass import motionclass


class SceneBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4

    def __eq__(self, other):
        if not isinstance(other, SceneBaseModel):
            return NotImplemented
        return self.uuid == other.uuid


@motionclass
class Scene(SceneBaseModel):
    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        base = base.rstrip("/")

        s = requests.Session()
        r = s.request("GET", f"{base}/scene/{uuid}")
        r.raise_for_status()

        super().__init__(**r.json())

        # set private attrs after
        object.__setattr__(self, "_base_", base)
        object.__setattr__(self, "_timeout_", timeout)
        object.__setattr__(self, "_session_", s)
