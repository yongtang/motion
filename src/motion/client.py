import json
import pathlib
import tempfile
import zipfile

import requests

from .scene import Scene
from .session import Session


class SceneClient:
    def __init__(self, base: str, *, timeout: float):
        self._base_ = base.rstrip("/")
        self._timeout_ = timeout

    def create(self, file: str | pathlib.Path, runtime: str) -> Scene:

        file = pathlib.Path(file)
        if not file.is_file():
            raise FileNotFoundError(f"Input file not found: {file}")

        # Create a temp directory that will be auto-deleted on exit
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
        return Scene(self._base_, r.json()["uuid"], timeout=self._timeout_)

    def archive(self, scene: Scene, file: str | pathlib.Path) -> pathlib.Path:
        r = requests.get(
            f"{self._base_}/scene/{scene.uuid}/archive",
            stream=True,
            timeout=self._timeout_,
        )
        r.raise_for_status()
        out = pathlib.Path(file)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return out

    def search(self, q: str) -> list[Scene]:
        r = requests.get(
            f"{self._base_}/scene", params={"q": q}, timeout=self._timeout_
        )
        r.raise_for_status()
        return [Scene(self._base_, sid, timeout=self._timeout_) for sid in r.json()]

    def delete(self, scene: Scene) -> dict:
        r = requests.delete(f"{self._base_}/scene/{scene.uuid}", timeout=self._timeout_)
        r.raise_for_status()
        return r.json()


class SessionClient:
    def __init__(self, base: str, *, timeout: float):
        self._base_ = base.rstrip("/")
        self._timeout_ = timeout

    def create(self, scene: Scene) -> Session:
        r = requests.post(
            f"{self._base_}/session",
            json={"scene": str(scene.uuid)},
            timeout=self._timeout_,
        )
        r.raise_for_status()
        return Session(self._base_, r.json()["uuid"], timeout=self._timeout_)

    def archive(self, session: Session, file: str | pathlib.Path) -> pathlib.Path:
        """Download /session/{uuid}/archive to a local zip file.

        Raises:
            requests.HTTPError on non-2xx (e.g., 404 if session not found).
        """
        r = requests.get(
            f"{self._base_}/session/{session.uuid}/archive",
            stream=True,
            timeout=self._timeout_,
        )
        r.raise_for_status()
        out = pathlib.Path(file)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return out

    def search(self, q: str) -> list[Session]:
        # simple existence check by id
        r = requests.get(f"{self._base_}/session/{q}", timeout=self._timeout_)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return [Session(self._base_, q, timeout=self._timeout_)]

    def delete(self, session: Session) -> dict:
        r = requests.delete(
            f"{self._base_}/session/{session.uuid}", timeout=self._timeout_
        )
        r.raise_for_status()
        return r.json()


class Client:
    def __init__(self, base: str, *, timeout: float = 30.0):
        self.scene = SceneClient(base, timeout=timeout)
        self.session = SessionClient(base, timeout=timeout)


def client(base: str, *, timeout: float = 30.0) -> Client:
    return Client(base, timeout=timeout)
