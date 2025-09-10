import asyncio
import functools
import json
import sys

import omni.ext
import omni.usd


async def main():
    print("[motion.extension] Loading stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    print(f"[motion.extension] Loaded metadata: {metadata}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        print("[motion.extension] Closing existing stage...")
        await ctx.close_stage_async()
        print("[motion.extension] Existing stage closed")

    def on_stage_event(event, e):
        print(f"[motion.extension] Stage event {omni.usd.StageEventType(e.type)}")
        if omni.usd.StageEventType(e.type) == omni.usd.StageEventType.OPENED:
            print("[motion.extension] Stage opened")
            event.set()

    event = asyncio.Event()
    subscription = ctx.get_stage_event_stream().create_subscription_to_push(
        functools.partial(on_stage_event, event)
    )
    try:
        print("[motion.extension] Opening stage...")

        await ctx.open_stage_async(
            "file:///storage/node/scene/scene.usd",
            load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
        )

        print("[motion.extension] Waiting stage...")
        await event.wait()
    finally:
        subscription.unsubscribe()

    while ctx.is_stage_loading():
        print("[motion.extension] Waiting loading...")
        await omni.kit.app.get_app().next_update_async()

    stage = ctx.get_stage()
    if not stage:
        print("[motion.extension] Failed to load stage")
        raise RuntimeError("stage is None after open")

    print("[motion.extension] Stage loaded")


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        print(f"[motion.extension] Startup [{ext_id}]")
        self.task = asyncio.create_task(main())
        self.task.add_done_callback(lambda e: e.exception() and sys.exit(1))

    def on_shutdown(self):
        print("[motion.extension] Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
