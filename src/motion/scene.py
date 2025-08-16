import pydantic
import requests


class Scene(pydantic.BaseModel):
    uuid: str

    def __init__(self, base: str, uuid: str, timeout: float = 5.0):
        url = f"{base.rstrip('/')}/scene/{uuid}"
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()

        super().__init__(**data)
