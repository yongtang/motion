import pydantic
import requests

from .motionclass import motionclass


class SceneBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4


@motionclass
class Scene(SceneBaseModel):
    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        url = f"{base.rstrip('/')}/scene/{uuid}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        super().__init__(**r.json())
        # bypass Pydantic field validation for decorator-added private attrs
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
