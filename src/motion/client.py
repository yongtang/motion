import json
import pathlib
import tempfile
import zipfile

import requests

from .motionclass import motionclass
from .scene import Scene, SceneBaseModel
from .session import Session, SessionBaseModel


@motionclass
class SceneClient:
    def __init__(self, base: str, *, timeout: float):
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
        object.__setattr__(self, "_session_", requests.Session())

    def create(self, file: str | pathlib.Path, runtime: str) -> Scene:
        file = pathlib.Path(file)
        if not file.is_file():
            raise FileNotFoundError(f"Input file not found: {file}")

        with tempfile.TemporaryDirectory() as directory:
            directory = pathlib.Path(directory)
            zip_path = directory / f"{file.stem}.zip"
            meta = directory / "meta.json"

            # Serialize runtime into meta.json
            with meta.open("w", encoding="utf-8") as mf:
                json.dump({"runtime": runtime}, mf, ensure_ascii=False)

            # Zip USD file + meta.json
            with zipfile.ZipFile(
                zip_path, mode="w", compression=zipfile.ZIP_DEFLATED
            ) as zf:
                zf.write(file, arcname="scene.usd")
                zf.write(meta, arcname="meta.json")

            # Upload the zip
            with zip_path.open("rb") as f:
                files = {"file": (zip_path.name, f, "application/zip")}
                r = self._request_("POST", "scene", files=files)

        scene = SceneBaseModel.parse_obj(r.json())
        return Scene(self._base_, scene.uuid, timeout=self._timeout_)

    def archive(self, scene: Scene, file: str | pathlib.Path) -> pathlib.Path:
        return self._download_(f"scene/{scene.uuid}/archive", file)

    def search(self, q: str) -> list[Scene]:
        # preserve special 422 -> [] behavior (check before raising)
        r = self._session_.get(
            f"{self._base_}/scene", params={"q": q}, timeout=self._timeout_
        )
        if r.status_code == 422:
            return []
        r.raise_for_status()
        scenes = [SceneBaseModel.parse_obj(item) for item in r.json()]
        return [Scene(self._base_, s.uuid, timeout=self._timeout_) for s in scenes]

    def delete(self, scene: Scene) -> None:
        r = self._request_("DELETE", f"scene/{scene.uuid}")
        SceneBaseModel.parse_obj(r.json())  # validate, discard
        return None


@motionclass
class SessionClient:
    def __init__(self, base: str, *, timeout: float):
        object.__setattr__(self, "_base_", base.rstrip("/"))
        object.__setattr__(self, "_timeout_", timeout)
        object.__setattr__(self, "_session_", requests.Session())

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
        session = SessionBaseModel.parse_obj(r.json())
        return Session(self._base_, session.uuid, timeout=self._timeout_)

    def archive(self, session: Session, file: str | pathlib.Path) -> pathlib.Path:
        return self._download_(f"session/{session.uuid}/archive", file)

    def search(self, q: str) -> list[Session]:
        # preserve special 404 -> [] behavior (check before raising)
        r = self._session_.get(f"{self._base_}/session/{q}", timeout=self._timeout_)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        session = SessionBaseModel.parse_obj(r.json())
        return [Session(self._base_, session.uuid, timeout=self._timeout_)]

    def delete(self, session: Session) -> None:
        r = self._request_("DELETE", f"session/{session.uuid}")
        SessionBaseModel.parse_obj(r.json())  # validate, discard
        return None


class Client:
    def __init__(self, base: str, *, timeout: float = 30.0):
        self.scene = SceneClient(base, timeout=timeout)
        self.session = SessionClient(base, timeout=timeout)


def client(base: str, *, timeout: float = 30.0) -> Client:
    return Client(base, timeout=timeout)
