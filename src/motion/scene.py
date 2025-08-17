import pydantic
import requests


class SceneBaseModel(pydantic.BaseModel):
    uuid: pydantic.UUID4


class Scene(SceneBaseModel):
    def __init__(self, base: str, uuid: pydantic.UUID4, timeout: float = 5.0):
        url = f"{base.rstrip('/')}/scene/{uuid}"
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        super().__init__(**r.json())
