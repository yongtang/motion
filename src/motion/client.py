import json
import pathlib
import tempfile
import zipfile

import httpx

from .scene import Scene, SceneBase, SceneSpec
from .session import Session, SessionBase


class BaseClient:

    def __init__(self, base: str, *, timeout: float):
        self._base_ = base.rstrip("/")
        self._timeout_ = float(timeout)

    def _request_(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self._base_}/{path.lstrip('/')}"
        r = httpx.request(method, url, timeout=self._timeout_, **kwargs)
        r.raise_for_status()
        return r

    def _download_(self, path: str, file: str | pathlib.Path) -> pathlib.Path:
        file = pathlib.Path(file)
        file.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self._base_}/{path.lstrip('/')}"
        with httpx.stream("GET", url, timeout=self._timeout_) as r:
            r.raise_for_status()
            with file.open("wb") as f:
                for chunk in r.iter_bytes():
                    f.write(chunk)
        return file


class SceneClient(BaseClient):
    def create(self, file: str | pathlib.Path, runner: str) -> Scene:
        file = pathlib.Path(file)
        if not file.is_file():
            raise FileNotFoundError(f"Input file not found: {file}")

        # Validate + normalize via SceneSpec (uses enums under the hood)
        spec = SceneSpec(
            runner=runner,
        )

        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            zipf = directory.joinpath(f"{file.stem}.zip")
            meta = directory.joinpath("meta.json")

            # Keep empty meta.json for forward compatibility
            with meta.open("w", encoding="utf-8") as mf:
                json.dump({}, mf)

            with zipfile.ZipFile(zipf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.write(file, arcname="scene.usd")
                zf.write(meta, arcname="meta.json")

            with zipf.open("rb") as f:
                files = {"file": (zipf.name, f, "application/zip")}
                # Server expects FLAT form fields for multipart
                data = {"runner": spec.runner}
                r = self._request_("POST", "scene", files=files, data=data)

        scene = SceneBase.parse_obj(r.json())
        return Scene(self._base_, scene.uuid, scene.runner, timeout=self._timeout_)

    def archive(self, scene: Scene, file: str | pathlib.Path) -> pathlib.Path:
        return self._download_(f"scene/{scene.uuid}/archive", file)

    def search(self, q: str | None = None) -> list[Scene]:
        params = None if not q else {"q": q}
        r = httpx.get(f"{self._base_}/scene", params=params, timeout=self._timeout_)
        if r.status_code == 422:
            return []
        r.raise_for_status()
        items = [SceneBase.parse_obj(item) for item in r.json()]
        return [
            Scene(self._base_, s.uuid, s.runner, timeout=self._timeout_) for s in items
        ]

    def delete(self, scene: Scene) -> None:
        r = self._request_("DELETE", f"scene/{scene.uuid}")
        SceneBase.parse_obj(r.json())
        return None


class SessionClient(BaseClient):
    def create(
        self,
        scene: Scene,
        *,
        joint: list[str] | None = None,
        camera: dict | None = None,
        link: list[str] | None = None,
    ) -> Session:
        payload = (
            {"scene": str(scene.uuid)}
            | ({"joint": joint} if joint is not None else {})
            | ({"camera": camera} if camera is not None else {})
            | ({"link": link} if link is not None else {})
        )
        r = self._request_("POST", "session", json=payload)
        session = SessionBase.parse_obj(r.json())
        return Session(self._base_, session.uuid, timeout=self._timeout_)

    def archive(self, session: Session, file: str | pathlib.Path) -> pathlib.Path:
        return self._download_(f"session/{session.uuid}/archive", file)

    def search(self, q: str | None = None) -> list[Session]:
        params = None if not q else {"q": q}
        r = httpx.get(f"{self._base_}/session", params=params, timeout=self._timeout_)
        if r.status_code == 422:
            return []
        r.raise_for_status()
        items = [SessionBase.parse_obj(item) for item in r.json()]
        return [Session(self._base_, s.uuid, timeout=self._timeout_) for s in items]

    def delete(self, session: Session) -> None:
        r = self._request_("DELETE", f"session/{session.uuid}")
        SessionBase.parse_obj(r.json())
        return None


class Client:
    def __init__(self, base: str, *, timeout: float = 30.0):
        self._base_ = base.rstrip("/")
        self._timeout_ = timeout
        self.scene = SceneClient(self._base_, timeout=timeout)
        self.session = SessionClient(self._base_, timeout=timeout)


def client(base: str, *, timeout: float = 30.0) -> Client:
    return Client(base, timeout=timeout)
