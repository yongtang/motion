import asyncio

import carb
import omni.ext
import omni.usd
import omni.timeline


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.e_stage_task = None
        self.e_stage_event = None
        self.e_stage_subscription = None
        self.e_timeline = None

    def on_startup(self, ext_id):
        carb.log_info("[motion.extension] Startup")

        self.e_stage_event = asyncio.Event()

        ctx = omni.usd.get_context()
        self.e_stage_subscription = (
            ctx.get_stage_event_stream().create_subscription_to_pop(self.on_stage_event)
        )

        self.e_timeline = omni.timeline.get_timeline_interface()

        async def f_stage(url: str):
            carb.log_info(f"[motion.extension] Opening stage: {url}")
            # Close any existing stage, then open with full load
            if ctx.get_stage():
                carb.log_info("[motion.extension] Closing existing stage")
                await ctx.close_stage_async()

            self.e_stage_event.clear()
            await ctx.open_stage_async(
                url,
                load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
            )

            # Wait for StageEventType.OPENED
            carb.log_info("[motion.extension] Waiting for OPENED event...")
            await self.e_stage_event.wait()

            # Wait until the stage finishes loading all assets
            while ctx.is_stage_loading():
                carb.log_info("[motion.extension] Stage still loading...")
                await asyncio.sleep(0.05)

            carb.log_info("[motion.extension] Stage OPENED and fully loaded")

            # Example: pause and single-step
            carb.log_info("[motion.extension] Pausing simulation")
            self.e_timeline.pause()

            await asyncio.sleep(2.0)

            carb.log_info("[motion.extension] Stepping simulation one tick")
            self.e_timeline.step()

            await asyncio.sleep(2.0)

            carb.log_info("[motion.extension] Resuming simulation")
            self.e_timeline.play()

        # Schedule the task
        self.e_stage_task = asyncio.create_task(
            f_stage("file:///storage/node/scene.usd")
        )

        def f_done(t: asyncio.Task):
            exc = t.exception()
            if exc is not None:
                carb.log_error(f"[motion.extension] Stage task failed: {exc!r}")

        self.e_stage_task.add_done_callback(f_done)

    def on_shutdown(self):
        carb.log_info("[motion.extension] Shutdown")

        if self.e_stage_task:
            try:
                self.e_stage_task.cancel()
            except Exception:
                pass
            self.e_stage_task = None

        if self.e_stage_subscription:
            try:
                self.e_stage_subscription.unsubscribe()
            except Exception:
                pass
            self.e_stage_subscription = None

        self.e_stage_event = None
        self.e_timeline = None

    def on_stage_event(self, e):
        if e.type == omni.usd.StageEventType.OPENED:
            carb.log_info("[motion.extension] Stage OPENED event received")
            if self.e_stage_event and not self.e_stage_event.is_set():
                self.e_stage_event.set()
