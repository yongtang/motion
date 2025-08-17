from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI


@asynccontextmanager
async def lifespan(app: FastAPI, prefix: str) -> AsyncIterator[None]:
    print(f"Lifespan start[{prefix}]")
    try:
        yield
    finally:
        print(f"Lifespan final[{prefix}]")
