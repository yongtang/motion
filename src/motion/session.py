import asyncio
import json

import httpx
import pydantic
import websockets

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


class SessionStream:
    def __init__(
        self,
        base: str,
        session_uuid: pydantic.UUID4,
        *,
        timeout: float,
        start: int | None,
    ):
        self._base_ = base.rstrip("/")
        self._uuid_ = session_uuid
        self._timeout_ = float(timeout)
        self._start_ = start if (isinstance(start, int) and start > 0) else None

    async def __aenter__(self):
        ws_base = self._base_.replace("https://", "wss://").replace("http://", "ws://")
        url = f"{ws_base}/session/{self._uuid_}/stream" + (
            f"?start={self._start_}" if self._start_ is not None else ""
        )

        self._socket_ = await websockets.connect(
            url,
            open_timeout=self._timeout_,
            close_timeout=self._timeout_,
            ping_interval=None,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._socket_.close()
        self._socket_ = None
        return False

    async def step(self, payload: dict) -> None:
        """Send one JSON step command."""
        await self._socket_.send(json.dumps(payload))

    async def data(self, *, timeout: float | None = None):
        """Receive one message; JSON-decode if possible."""
        to = self._timeout_ if timeout is None else timeout
        msg = await asyncio.wait_for(self._socket_.recv(), timeout=to)
        if isinstance(msg, (bytes, bytearray)):
            try:
                return json.loads(msg)
            except Exception:
                return msg
        try:
            return json.loads(msg)
        except Exception:
            return msg


@motionclass
class Session(SessionBaseModel):
    """
    Usage:
        async with Session(base, uuid, timeout=5.0) as session:
            await session.play()
            async with session.stream(start=None) as stream:
                await stream.step({...})
                data = await stream.data()
            await session.stop()
    """

    scene: Scene

    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        base = base.rstrip("/")

        # one-time sync fetch to populate fields
        r = httpx.request("GET", f"{base}/session/{uuid}", timeout=timeout)
        r.raise_for_status()
        data = SessionBaseModel.parse_obj(r.json())

        scene = Scene(base, data.scene, timeout=timeout)

        super().__init__(
            uuid=data.uuid,
            scene=scene,
            joint=data.joint,
            camera=data.camera,
            link=data.link,
        )

        object.__setattr__(self, "_base_", base)
        object.__setattr__(self, "_timeout_", timeout)

    def stream(self, start: int | None = None) -> SessionStream:
        return SessionStream(
            self._base_, self.uuid, timeout=self._timeout_, start=start
        )

    async def play(self):
        r = await self._request_("POST", f"session/{self.uuid}/play")
        return r.json()

    async def stop(self):
        r = await self._request_("POST", f"session/{self.uuid}/stop")
        return r.json()
