import enum

import pydantic

from .motionclass import motionclass


class SceneRunnerImageSpec(str, enum.Enum):
    relay = "relay"
    isaac = "isaac"


class SceneRunnerDeviceSpec(str, enum.Enum):
    cpu = "cpu"
    cuda = "cuda"


class SceneRunnerSpec(pydantic.BaseModel):
    image: SceneRunnerImageSpec
    device: SceneRunnerDeviceSpec


class SceneSpec(pydantic.BaseModel):
    runner: SceneRunnerSpec


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
        runner: SceneRunnerSpec,
        timeout: float = 5.0,
    ):
        super().__init__(uuid=uuid, runner=runner)
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
