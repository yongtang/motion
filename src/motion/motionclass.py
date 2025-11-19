import pathlib

import httpx
import pydantic


def motionclass(cls):
    cls._base_ = pydantic.PrivateAttr()
    cls._timeout_ = pydantic.PrivateAttr()
    cls._client_ = pydantic.PrivateAttr()

    @property
    def base(self) -> str:
        return self._base_

    @property
    def timeout(self) -> float:
        return self._timeout_

    async def _request_(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self._base_}/{path.lstrip('/')}"
        timeout = kwargs.pop("timeout", self._timeout_)
        r = await self._client_.request(method, url, timeout=timeout, **kwargs)
        r.raise_for_status()
        return r

    async def _download_(
        self, path: str, file: str | pathlib.Path, **kwargs
    ) -> pathlib.Path:
        file = pathlib.Path(file)
        file.parent.mkdir(parents=True, exist_ok=True)
        url = f"{self._base_}/{path.lstrip('/')}"
        timeout = kwargs.pop("timeout", self._timeout_)
        async with self._client_.stream("GET", url, timeout=timeout, **kwargs) as r:
            r.raise_for_status()
            with file.open("wb") as f:
                async for chunk in r.aiter_bytes():
                    if chunk:
                        f.write(chunk)
        return file

    async def __aenter__(self):
        object.__setattr__(self, "_client_", httpx.AsyncClient())
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self._client_.aclose()
        object.__delattr__(self, "_client_")
        return False

    cls.base = base
    cls.timeout = timeout
    cls._request_ = _request_
    cls._download_ = _download_
    cls.__aenter__ = __aenter__
    cls.__aexit__ = __aexit__
    return cls
