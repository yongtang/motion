import asyncio
import json

import omni.ext
import omni.kit
import omni.usd
from omni.isaac.core.articulations import Articulation

from .node import run_http, run_link


def f_prim(metadata, stage):
    print(f"[motion.extension] prim: {metadta} {stage}")
    prim = (
        stage.GetPrimAtPath(metadata["prim"])
        if "prim" in metadata
        else stage.GetDefaultPrim()
    )
    assert prim and prim.IsValid()
    return prim.GetPath().pathString


async def main():
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())

    print("[motion.extension] stage")
    ctx = omni.usd.get_context()
    if ctx.get_stage():
        await ctx.close_stage_async()
    await asyncio.wait_for(
        ctx.open_stage_async(e, load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL),
        timeout=120.0,
    )
    stage = ctx.get_stage()
    assert stage
    print("[motion.extension] loaded")

    prim = f_prim(metadata, stage)
    articulation = Articulation(prim)
    articulation.initialize()

    session = metadata["session"]
    async with run_http():
        async with run_link() as channel:
            await asyncio.Event().wait()


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        print("[motion.extension] startup")
        omni.kit.app.get_app().get_async_event_loop().create_task(main())

    def on_shutdown(self):
        print("[motion.extension] shutdown")
        self.task.cancel() if self.task else None
