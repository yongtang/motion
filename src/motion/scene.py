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
        super().__init__(uuid=uuid)
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
