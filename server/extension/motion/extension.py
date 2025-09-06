import asyncio
import json
import logging
import pathlib
import sys

import omni.ext
import omni.kit
import omni.usd

logging.basicConfig(
    level=logging.INFO,
    format="[%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


async def main():
    log.info(f"Load stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    log.info(f"Loaded metadata: {metadata}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        log.info("Closing existing stage…")
        await ctx.close_stage_async()

    log.info("Opening stage…")
    await asyncio.wait_for(
        ctx.open_stage_async(
            "file:///storage/node/scene/scene.usd",
            load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
        ),
        timeout=120.0,
    )

    # let Kit process one update tick
    await omni.kit.app.get_app().next_update_async()

    stage = ctx.get_stage()
    if not stage:
        log.error("Failed to open stage")
        raise RuntimeError("stage is None after open")

    log.info("Stage loaded")


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.task = None

    def on_startup(self, ext_id):
        log.info("Startup")
        self.task = omni.kit.async_engine.run_coroutine(main())

    def on_shutdown(self):
        log.info("Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
