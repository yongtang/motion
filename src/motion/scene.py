import httpx
import pydantic

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

        r = httpx.request("GET", f"{base}/scene/{uuid}", timeout=timeout)
        r.raise_for_status()

        super().__init__(**r.json())

        # set private attrs
        object.__setattr__(self, "_base_", base)
        object.__setattr__(self, "_timeout_", timeout)
        object.__setattr__(self, "_httpx_", httpx)
