import pathlib
import uuid

import requests


class Scene:
    def __init__(self, base: str, timeout: float):
        self._base_ = base.rstrip("/")
        self._timeout_ = timeout

    def create(self, file: str | pathlib.Path) -> str:
        p = pathlib.Path(file)
        with p.open("rb") as f:
            files = {"file": (p.name, f, "application/zip")}
            r = requests.post(
                f"{self._base_}/scene", files=files, timeout=self._timeout_
            )
        r.raise_for_status()
        return r.json()["uuid"]

    def delete(self, scene: str | uuid.UUID) -> dict:
        r = requests.delete(f"{self._base_}/scene/{scene}", timeout=self._timeout_)
        r.raise_for_status()
        return r.json()

    def lookup(self, scene: str | uuid.UUID) -> dict:
        r = requests.get(f"{self._base_}/scene/{scene}", timeout=self._timeout_)
        r.raise_for_status()
        return r.json()

    def export(self, scene: str | uuid.UUID, file: str | pathlib.Path) -> pathlib.Path:
        r = requests.get(
            f"{self._base_}/scene/{scene}/export",
            stream=True,
            timeout=self._timeout_,
        )
        r.raise_for_status()
        out = pathlib.Path(file)
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
    def __init__(self, base: str, *, timeout: float = 30.0):
        self.scene = Scene(base, timeout)


def client(base: str, *, timeout: float = 30.0) -> Client:
    return Client(base, timeout=timeout)
