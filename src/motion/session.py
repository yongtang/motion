import asyncio
import contextlib
import datetime
import enum
import itertools
import json
import random
import typing
import urllib

import httpx
import pydantic
import websockets

from .motionclass import motionclass
from .scene import RunnerSpec, Scene, SceneBase


class DeviceSpec(str, enum.Enum):
    cpu = "cpu"
    cuda = "cuda"


class ModelSpec(str, enum.Enum):
    model = "model"
    bounce = "bounce"
    remote = "remote"


class RosNodeSpec(pydantic.BaseModel):
    session: pydantic.UUID4
    runner: typing.Literal[RunnerSpec.ros]
    device: DeviceSpec = DeviceSpec.cpu
    model: ModelSpec
    tick: typing.Literal[False] = False


class IsaacNodeSpec(pydantic.BaseModel):
    session: pydantic.UUID4
    runner: typing.Literal[RunnerSpec.isaac]
    device: typing.Literal[DeviceSpec.cuda] = DeviceSpec.cuda
    model: ModelSpec
    tick: bool = True


class CounterNodeSpec(pydantic.BaseModel):
    session: pydantic.UUID4
    runner: typing.Literal[RunnerSpec.counter]
    device: DeviceSpec = DeviceSpec.cpu
    model: ModelSpec
    tick: bool = True


SessionNodeSpec = typing.Annotated[
    RosNodeSpec | IsaacNodeSpec | CounterNodeSpec,
    pydantic.Field(discriminator="runner"),
]


class SessionStatusSpec(str, enum.Enum):
    pending = "pending"
    play = "play"
    stop = "stop"


class SessionStatus(pydantic.BaseModel):
    uuid: pydantic.UUID4
    state: SessionStatusSpec
    update: datetime.datetime


class CameraSpec(pydantic.BaseModel):
    width: pydantic.PositiveInt
    height: pydantic.PositiveInt


class PointSpec(pydantic.BaseModel):
    x: float
    y: float
    z: float


class QuaternionSpec(pydantic.BaseModel):
    x: float
    y: float
    z: float
    w: float


class PoseSpec(pydantic.BaseModel):
    position: PointSpec
    orientation: QuaternionSpec


class SessionStepSpec(pydantic.BaseModel):
    joint: dict[str, float] | None = None
    gamepad: dict[str, list[tuple[str, int]]] | None = None
    keyboard: dict[str, list[str]] | None = None
    metadata: str | None = None

    def __init__(self, **data):
        super().__init__(**data)
        if sum(v is not None for v in (self.joint, self.gamepad, self.keyboard)) != 1:
            raise ValueError(
                f"Must define exactly one from joint={self.joint} gamepad={self.gamepad} keyboard={self.keyboard}"
            )
        if self.gamepad is not None:
            for name, value in itertools.chain.from_iterable(self.gamepad.values()):
                if name.startswith("AXIS_"):
                    assert -32768 <= value <= 32767, f"{name} must be int16: {value}"
                elif name.startswith("BUTTON_"):
                    assert value in (0, 1), f"{name} must be 0|1: {value}"
                else:
                    assert False, f"{name}: {value}"
        if self.keyboard is not None:
            for entry in itertools.chain.from_iterable(self.keyboard.values()):
                assert entry in (
                    "K",
                    "W",
                    "S",
                    "A",
                    "D",
                    "Q",
                    "E",
                    "Z",
                    "X",
                    "T",
                    "G",
                    "C",
                    "V",
                ), f"{entry}"


class SessionSpec(pydantic.BaseModel):
    scene: pydantic.UUID4
    joint: list[str] = pydantic.Field(default_factory=lambda: ["*"])
    camera: dict[str, CameraSpec] = pydantic.Field(default_factory=dict)
    link: list[str] = pydantic.Field(default_factory=lambda: ["*"])


class SessionBase(SessionSpec):
    uuid: pydantic.UUID4

    def __eq__(self, other):
        if not isinstance(other, SessionBase):
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
        # allow -1 (tail: start from LAST) or any positive integer; else None => NEW
        if isinstance(start, int) and (start == -1 or start > 0):
            self._start_ = start
        else:
            self._start_ = None
        self._socket_ = None

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
        # be tolerant if already closed/None
        with contextlib.suppress(Exception):
            if self._socket_ is not None:
                await self._socket_.close()
        self._socket_ = None
        return False

    async def _reconnect_(self):
        # best-effort close old socket
        with contextlib.suppress(Exception):
            if self._socket_ is not None:
                await self._socket_.close()

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

    async def step(self, payload: dict, *, timeout: float | None = None) -> None:
        """
        Send one JSON step command. If the websocket was closed with an expected
        Close (ClosedError/ClosedOK), transparently reconnect and retry until
        the per-call timeout expires.
        """
        if self._socket_ is None:
            await self._reconnect_()

        to = self._timeout_ if timeout is None else float(timeout)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + to

        payload = (
            SessionStepSpec.parse_obj(payload)
            if not isinstance(payload, SessionStepSpec)
            else payload
        )
        msg = payload.json()

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()

            try:
                # keep the single await short to allow retries
                await asyncio.wait_for(
                    self._socket_.send(msg),
                    timeout=min(1.5, remaining),
                )
                return None  # success

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
            ):
                # reconnect and retry until deadline
                await self._reconnect_()
                continue

    async def data(self, *, timeout: float | None = None):
        """Receive one message; JSON-decode if possible."""
        to = self._timeout_ if timeout is None else float(timeout)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + to

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()

            try:
                msg = await asyncio.wait_for(
                    self._socket_.recv(), timeout=min(1.5, remaining)
                )
                if isinstance(msg, (bytes, bytearray)):
                    try:
                        return json.loads(msg)
                    except Exception:
                        return msg
                try:
                    return json.loads(msg)
                except Exception:
                    return msg

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
            ):
                await self._reconnect_()
                continue


@motionclass
class Session(SessionBase):
    """
    Usage:
        async with Session(base, uuid, timeout=5.0) as session:
            await session.play()
            async with session.stream(start=None) as stream:
                await stream.step({...})
                data = await stream.data()
            await session.stop()
    """

    scene: Scene  # overridden to the richer Scene type

    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        base = base.rstrip("/")

        # One-time sync fetch to populate fields
        r = httpx.request("GET", f"{base}/session/{uuid}", timeout=timeout)
        r.raise_for_status()
        data = SessionBase.parse_obj(r.json())

        # Fetch scene metadata to obtain runner
        r = httpx.request("GET", f"{base}/scene/{data.scene}", timeout=timeout)
        r.raise_for_status()
        scene = SceneBase.parse_obj(r.json())
        scene = Scene(base, scene.uuid, scene.runner, timeout=timeout)

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

    async def play(
        self,
        device: str | None = None,
        model: str | None = None,
        tick: bool | None = None,
    ):
        params = {
            k: v
            for k, v in {
                "device": device,
                "model": model,
                "tick": str(tick).lower() if tick is not None else None,
            }.items()
            if v is not None
        }
        query = f"?{urllib.parse.urlencode(params)}" if params else ""

        r = await self._request_("POST", f"session/{self.uuid}/play{query}")
        return r.json()

    async def stop(self):
        r = await self._request_("POST", f"session/{self.uuid}/stop")
        return r.json()

    async def wait(self, status: str, timeout: float) -> None:
        """
        Block until the session reaches the requested status.
        Valid targets: "play" or "stop".

        Returns:
          None on success.
        Raises:
          ValueError for invalid status values
          RuntimeError if waiting for "play" but the session is already stopped
          asyncio.TimeoutError on timeout
        """
        status = (status or "").strip().lower()
        if status not in ("play", "stop"):
            raise ValueError("wait(status, ...) supports only 'play' or 'stop'")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(timeout)

        # fast path
        r = await self._request_("GET", f"session/{self.uuid}/status")
        state = SessionStatus.parse_obj(r.json())
        if status == "play":
            if state.state is SessionStatusSpec.stop:
                raise RuntimeError("cannot wait for play: session already stopped")
            if state.state is SessionStatusSpec.play:
                return None
        else:  # status == "stop"
            if state.state is SessionStatusSpec.stop:
                return None

        # poll with exponential backoff and jitter
        delay = 0.1
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError()

            r = await self._request_(
                "GET",
                f"session/{self.uuid}/status",
                timeout=min(self._timeout_, remaining),
            )
            state = SessionStatus.parse_obj(r.json())
            if status == "play":
                if state.state is SessionStatusSpec.stop:
                    raise RuntimeError(
                        "cannot wait for play: session stopped during wait"
                    )
                if state.state is SessionStatusSpec.play:
                    return None
            else:  # status == "stop"
                if state.state is SessionStatusSpec.stop:
                    return None

            await asyncio.sleep(
                min(delay * (0.75 + 0.5 * random.random()), 2.0, remaining)
            )
            delay = min(delay * 2.0, 2.0)
