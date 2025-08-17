import pydantic
import requests

from .scene import Scene


class SessionBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4
    scene: pydantic.UUID4


class Session(SessionBaseModel):
    scene: Scene

    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        url = f"{base.rstrip('/')}/session/{uuid}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        wire = r.json()
        scene = Scene(base, wire["scene"], timeout=timeout)
        super().__init__(uuid=wire["uuid"], scene=scene)

    def step(self):
        print(f"Step scene uuid='{self.scene.uuid}'")

    def play(self):
        print(f"Play scene uuid='{self.scene.uuid}'")
