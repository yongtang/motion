import json
import pathlib
import tempfile
import zipfile

import requests

from .scene import Scene, SceneBaseModel
from .session import Session, SessionBaseModel


class SceneClient:
    def __init__(self, base: str, *, timeout: float):
        self._base_ = base.rstrip("/")
        self._timeout_ = timeout

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
                r = requests.post(
                    f"{self._base_}/scene", files=files, timeout=self._timeout_
                )

        r.raise_for_status()
        scene = SceneBaseModel.parse_obj(r.json())
        return Scene(self._base_, scene.uuid, timeout=self._timeout_)

    def archive(self, scene: Scene, file: str | pathlib.Path) -> pathlib.Path:
        r = requests.get(
            f"{self._base_}/scene/{scene.uuid}/archive",
            stream=True,
            timeout=self._timeout_,
        )
        r.raise_for_status()
        file = pathlib.Path(file)
        file.parent.mkdir(parents=True, exist_ok=True)
        with file.open("wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return file

    def search(self, q: str) -> list[Scene]:
        r = requests.get(
            f"{self._base_}/scene", params={"q": q}, timeout=self._timeout_
        )
        if r.status_code == 422:
            return []
        r.raise_for_status()
        scenes = [SceneBaseModel.parse_obj(item) for item in r.json()]
        return [Scene(self._base_, s.uuid, timeout=self._timeout_) for s in scenes]

    def delete(self, scene: Scene) -> None:
        r = requests.delete(f"{self._base_}/scene/{scene.uuid}", timeout=self._timeout_)
        r.raise_for_status()
        SceneBaseModel.parse_obj(r.json())  # validate, discard
        return None


class SessionClient:
    def __init__(self, base: str, *, timeout: float):
        self._base_ = base.rstrip("/")
        self._timeout_ = timeout

    def create(self, scene: Scene, *, camera: list[str] = ["*"]) -> Session:
        payload = {"scene": str(scene.uuid), "camera": camera}
        r = requests.post(
            f"{self._base_}/session",
            json=payload,
            timeout=self._timeout_,
        )
        r.raise_for_status()
        session = SessionBaseModel.parse_obj(r.json())
        return Session(self._base_, session.uuid, timeout=self._timeout_)

    def archive(self, session: Session, file: str | pathlib.Path) -> pathlib.Path:
        r = requests.get(
            f"{self._base_}/session/{session.uuid}/archive",
            stream=True,
            timeout=self._timeout_,
        )
        r.raise_for_status()
        file = pathlib.Path(file)
        file.parent.mkdir(parents=True, exist_ok=True)
        with file.open("wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return file

    def search(self, q: str) -> list[Session]:
        r = requests.get(f"{self._base_}/session/{q}", timeout=self._timeout_)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        session = SessionBaseModel.parse_obj(r.json())
        return [Session(self._base_, session.uuid, timeout=self._timeout_)]

    def delete(self, session: Session) -> None:
        r = requests.delete(
            f"{self._base_}/session/{session.uuid}", timeout=self._timeout_
        )
        r.raise_for_status()
        SessionBaseModel.parse_obj(r.json())  # validate, discard
        return None


class Client:
    def __init__(self, base: str, *, timeout: float = 30.0):
        self.scene = SceneClient(base, timeout=timeout)
        self.session = SessionClient(base, timeout=timeout)


def client(base: str, *, timeout: float = 30.0) -> Client:
    return Client(base, timeout=timeout)
