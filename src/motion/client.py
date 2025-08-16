import pathlib
import uuid

import requests


class Scene:
    def __init__(self, base_url: str, timeout: float):
        self._base_ = base_url.rstrip("/")
        self._timeout_ = timeout

    def create(self, zip_path: str | pathlib.Path) -> str:
        p = pathlib.Path(zip_path)
        with p.open("rb") as f:
            files = {"file": (p.name, f, "application/zip")}
            r = requests.post(
                f"{self._base_}/scene", files=files, timeout=self._timeout_
            )
        r.raise_for_status()
        return r.json()["uuid"]

    def delete(self, scene_id: str | uuid.UUID) -> dict:
        r = requests.delete(f"{self._base_}/scene/{scene_id}", timeout=self._timeout_)
        r.raise_for_status()
        return r.json()

    def lookup(
        self, scene_id: str | uuid.UUID, out_path: str | pathlib.Path
    ) -> pathlib.Path:
        r = requests.get(
            f"{self._base_}/scene/{scene_id}", stream=True, timeout=self._timeout_
        )
        r.raise_for_status()
        out = pathlib.Path(out_path)
        with out.open("wb") as f:
            for chunk in r.iter_content(8192):
                if chunk:
                    f.write(chunk)
        return out

    def search(self, q: str) -> list[str]:
        r = requests.get(
            f"{self._base_}/scene", params={"q": q}, timeout=self._timeout_
        )
        r.raise_for_status()
        return r.json()


class Client:
    def __init__(self, url: str, *, timeout: float = 30.0):
        self.scene = Scene(url, timeout)


def client(url: str, *, timeout: float = 30.0) -> Client:
    return Client(url, timeout=timeout)
