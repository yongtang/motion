import enum

import pydantic

from .motionclass import motionclass


class RunnerSpec(str, enum.Enum):
    ros = "ros"
    isaac = "isaac"
    counter = "counter"


class SceneSpec(pydantic.BaseModel):
    runner: RunnerSpec


class SceneBase(SceneSpec):
    uuid: pydantic.UUID4

    def __eq__(self, other):
        if not isinstance(other, SceneBase):
            return NotImplemented
        return self.uuid == other.uuid


@motionclass
class Scene(SceneBase):
    def __init__(
        self,
        base: str,
        uuid: pydantic.UUID4,
        runner: RunnerSpec,
        timeout: float = 5.0,
    ):
        super().__init__(uuid=uuid, runner=runner)
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
