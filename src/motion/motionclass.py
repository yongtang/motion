import pathlib

import httpx
import pydantic


def motionclass(cls):
    cls._base_ = pydantic.PrivateAttr()
    cls._timeout_ = pydantic.PrivateAttr()
    cls._httpx_ = pydantic.PrivateAttr()

    @property
    def base(self) -> str:
        return self._base_

    @property
    def timeout(self) -> float:
        return self._timeout_

    def _request_(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self._base_}/{path.lstrip('/')}"
        r = self._httpx_.request(method, url, timeout=self._timeout_, **kwargs)
        r.raise_for_status()
        return r

    def _download_(self, path: str, file: str | pathlib.Path) -> pathlib.Path:
        file = pathlib.Path(file)
        file.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self._base_}/{path.lstrip('/')}"
        with self._httpx_.stream("GET", url, timeout=self._timeout_) as r:
            r.raise_for_status()
            with file.open("wb") as f:
                for chunk in r.iter_bytes():
                    if chunk:
                        f.write(chunk)
        return file

    cls.base = base
    cls.timeout = timeout
    cls._request_ = _request_
    cls._download_ = _download_
    return cls
