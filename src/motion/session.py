import pydantic
import requests

from .motionclass import motionclass
from .scene import Scene


class SessionBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4
    scene: pydantic.UUID4


@motionclass
class Session(SessionBaseModel):
    scene: Scene

    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        url = f"{base.rstrip('/')}/session/{uuid}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        wire = r.json()
        scene = Scene(base, wire["scene"], timeout=timeout)
        super().__init__(uuid=wire["uuid"], scene=scene)
        # bypass Pydantic field validation for decorator-added private attrs
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)

    def play(self):
        r = requests.post(f"{self.base}/session/{self.uuid}/play", timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def stop(self):
        r = requests.post(f"{self.base}/session/{self.uuid}/stop", timeout=self.timeout)
        r.raise_for_status()
        return r.json()
