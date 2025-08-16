from typing import Optional

import pydantic


class Scene(pydantic.BaseModel):
    name: str
    usd: Optional[str] = None
