import asyncio
import json

import omni.ext
import omni.kit
import omni.usd

import carb
import carb.settings


async def main():
    carb.log_info("Load stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.load(f)
    carb.log_info(f"Loaded metadata: {metadata}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        carb.log_info("Closing existing stage…")
        await ctx.close_stage_async()

    carb.log_info("Opening stage…")
    await asyncio.wait_for(
        ctx.open_stage_async(
            "file:///storage/node/scene/scene.usd",
            load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
        ),
        timeout=120.0,
    )

    # Let Kit process one update tick
    await omni.kit.app.get_app().next_update_async()

    stage = ctx.get_stage()
    if not stage:
        carb.log_error("Failed to open stage")
        raise RuntimeError("stage is None after open")

    carb.log_info("Stage loaded")


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None

    def on_startup(self, ext_id):
        settings = carb.settings.get_settings()
        settings.set("/log/enabled", True)
        settings.set("/log/level", "info")
        settings.set("/log/enableStandardStreamOutput", True)
        settings.set("/log/outputStream", "stdout")
        settings.set("/log/outputStreamLevel", "info")
        settings.set("/log/flushStandardStreamOutput", True)
        settings.set("/log/file", "")  # disable file logging; stdout only

        carb.log_info(f"[{ext_id}] Startup")

        # Run main(); always terminate Kit when done (0=ok, 1=error)
        self.task = omni.kit.async_engine.run_coroutine(main())
        self.task.add_done_callback(
            lambda e: omni.kit.app.get_app().post_quit(0 if not e.exception() else 1)
        )

    def on_shutdown(self):
        carb.log_info("Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
