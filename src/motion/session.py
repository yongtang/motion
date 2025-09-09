import pydantic
import requests

from .motionclass import motionclass
from .scene import Scene


class SessionBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4
    scene: pydantic.UUID4
    camera: list[str] = []


@motionclass
class Session(SessionBaseModel):
    # Store the resolved Scene model (instead of the UUID)
    scene: Scene

    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        base = base.rstrip("/")
        r = requests.get(f"{base}/session/{uuid}", timeout=timeout)
        r.raise_for_status()

        session = SessionBaseModel.parse_obj(r.json())
        scene = Scene(base, session.scene, timeout=timeout)
        super().__init__(uuid=session.uuid, scene=scene, camera=session.camera)

        # bypass Pydantic validation for decorator-added private attrs
        object.__setattr__(self, "_base_", base)
        object.__setattr__(self, "_timeout_", timeout)

    def play(self):
        r = requests.post(f"{self.base}/session/{self.uuid}/play", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def stop(self):
        r = requests.post(f"{self.base}/session/{self.uuid}/stop", timeout=self.timeout)
        r.raise_for_status()
        return r.json()
