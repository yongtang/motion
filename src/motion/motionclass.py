import pathlib

import pydantic
import requests


def motionclass(cls):
    cls._base_: str = pydantic.PrivateAttr("")
    cls._timeout_: float = pydantic.PrivateAttr(5.0)
    cls._session_: requests.Session = pydantic.PrivateAttr(
        default_factory=requests.Session
    )

    @property
    def base(self) -> str:
        return self._base_

    @property
    def timeout(self) -> float:
        return self._timeout_

    def _request_(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._base_}/{path.lstrip('/')}"
        r = self._session_.request(method, url, timeout=self._timeout_, **kwargs)
        r.raise_for_status()
        return r

    def _download_(self, path: str, dest: str | pathlib.Path) -> pathlib.Path:
        dest = pathlib.Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self._base_}/{path.lstrip('/')}"
        with self._session_.get(url, stream=True, timeout=self._timeout_) as r:
            r.raise_for_status()
            with dest.open("wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)
        return dest

    cls.base = base
    cls.timeout = timeout
    cls._request_ = _request_
    cls._download_ = _download_
    return cls
