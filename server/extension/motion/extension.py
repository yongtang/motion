import asyncio
import json
import sys

import omni.ext
import omni.kit
import omni.usd
import carb.settings


async def main():
    print("[motion.extension] Load stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    print(f"[motion.extension] Loaded metadata: {metadata}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        print("[motion.extension] Closing existing stage...")
        await ctx.close_stage_async()

    print("[motion.extension] Opening stage…")
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
        print("[motion.extension] Failed to open stage")
        raise RuntimeError("stage is None after open")

    print("[motion.extension] Stage loaded")


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        print(f"[motion.extension] [{ext_id}] Startup")
        self.task = asyncio.create_task(main())

        # When main finishes, exit Kit (0=success, 1=error)
        self.task.add_done_callback(
            lambda e: omni.kit.app.get_app().post_quit(0 if not e.exception() else 1)
        )

    def on_shutdown(self):
        print("[motion.extension] Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
