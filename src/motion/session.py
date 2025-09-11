import httpx
import pydantic

from .motionclass import motionclass
from .scene import Scene


class CameraSpec(pydantic.BaseModel):
    width: pydantic.PositiveInt
    height: pydantic.PositiveInt


class SessionSpecModel(pydantic.BaseModel):
    scene: pydantic.UUID4
    joint: list[str] = pydantic.Field(default_factory=lambda: ["*"])
    camera: dict[str, CameraSpec] = pydantic.Field(
        default_factory=lambda: {"*": CameraSpec(width=1280, height=720)}
    )
    link: list[str] = pydantic.Field(default_factory=lambda: ["*"])


class SessionBaseModel(SessionSpecModel):
    uuid: pydantic.UUID4

    def __eq__(self, other):
        if not isinstance(other, SessionBaseModel):
            return NotImplemented
        return self.uuid == other.uuid


@motionclass
class Session(SessionBaseModel):
    scene: Scene

    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        base = base.rstrip("/")

        r = httpx.request("GET", f"{base}/session/{uuid}", timeout=timeout)
        r.raise_for_status()

        session = SessionBaseModel.parse_obj(r.json())
        scene = Scene(base, session.scene, timeout=timeout)

        super().__init__(
            uuid=session.uuid,
            scene=scene,
            joint=session.joint,
            camera=session.camera,
            link=session.link,
        )

        # set private attrs
        object.__setattr__(self, "_base_", base)
        object.__setattr__(self, "_timeout_", timeout)
        object.__setattr__(self, "_httpx_", httpx)

    def play(self):
        r = self._request_("POST", f"session/{self.uuid}/play")
        return r.json()

    def stop(self):
        r = self._request_("POST", f"session/{self.uuid}/stop")
        return r.json()
