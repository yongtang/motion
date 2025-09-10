import asyncio

import carb
import omni.ext
import omni.usd


async def main(self, ctx):
    print("[motion.extension] Load stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    print(f"[motion.extension] Loaded metadata: {metadata}")

    if ctx.get_stage():
        print("[motion.extension] Closing existing stage...")
        await ctx.close_stage_async()

    self.e_stage_event.clear()
    await ctx.open_stage_async(
        "file:///storage/node/scene/scene.usd",
        load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,  # ensure full load
    )

    # Wait for StageEventType.OPENED (signaled in on_stage_event)
    await self.e_stage_event.wait()

    # Wait until the stage finishes loading all assets
    while ctx.is_stage_loading():
        await asyncio.sleep(0.05)

    carb.log_info("[motion.extension] Stage OPENED and fully loaded")


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.e_stage_task = None
        self.e_stage_event = None
        self.e_stage_subscription = None

    def on_startup(self, ext_id):
        self.e_stage_event = asyncio.Event()

        ctx = omni.usd.get_context()
        self.e_stage_subscription = (
            ctx.get_stage_event_stream().create_subscription_to_pop(self.on_stage_event)
        )

        # Schedule the task (exceptions bubble up; we just log them)
        self.e_stage_task = asyncio.create_task(
            main(self, ctx)
        )

        # Done-callback to surface any unhandled exceptions prominently
        def f_done(t: asyncio.Task):
            exc = t.exception()
            if exc is not None:
                carb.log_error(f"[motion.extension] Stage task failed: {exc!r}")

        self.e_stage_task.add_done_callback(f_done)

    def on_shutdown(self):
        # Cancel stage-open task first
        if self.e_stage_task:
            try:
                self.e_stage_task.cancel()
            except Exception:
                pass
            self.e_stage_task = None

        # Unsubscribe from stage events
        if self.e_stage_subscription:
            try:
                self.e_stage_subscription.unsubscribe()
            except Exception:
                pass
            self.e_stage_subscription = None

        self.e_stage_event = None

    # ---------- Stage events ----------

    def on_stage_event(self, e):
        if e.type == omni.usd.StageEventType.OPENED:
            carb.log_info("[motion.extension] Stage OPENED")
            if self.e_stage_event and not self.e_stage_event.is_set():
                self.e_stage_event.set()
