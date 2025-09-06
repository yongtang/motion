import pydantic


def motionclass(cls):
    cls._base_: str = pydantic.PrivateAttr("")
    cls._timeout_: float = pydantic.PrivateAttr(5.0)

    @property
    def base(self):
        return self._base_

    @property
    def timeout(self):
        return self._timeout_

    cls.base = base
    cls.timeout = timeout
    return cls
