import enum

import pydantic

from .motionclass import motionclass


class SceneRunnerSpec(str, enum.Enum):
    echo = "echo"
    isaac = "isaac"


class SceneSpecModel(pydantic.BaseModel):
    runner: SceneRunnerSpec


class SceneBaseModel(SceneSpecModel):
    uuid: pydantic.UUID4

    def __eq__(self, other):
        if not isinstance(other, SceneBaseModel):
            return NotImplemented
        return self.uuid == other.uuid


@motionclass
class Scene(SceneBaseModel):
    def __init__(
        self,
        base: str,
        uuid: pydantic.UUID4,
        runner: SceneRunnerSpec,
        timeout: float = 5.0,
    ):
        super().__init__(uuid=uuid, runner=runner)
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
