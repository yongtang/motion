import pydantic
import requests

from .motionclass import motionclass


class SceneBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4


@motionclass
class Scene(SceneBaseModel):
    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        base = base.rstrip("/")
        r = requests.get(f"{base}/scene/{uuid}", timeout=timeout)
        r.raise_for_status()

        super().__init__(**r.json())

        # bypass Pydantic field validation for decorator-added private attrs
        object.__setattr__(self, "_base_", base)
        object.__setattr__(self, "_timeout_", timeout)
