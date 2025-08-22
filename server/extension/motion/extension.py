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
        carb.log_info("[motion.extension] on_startup")
        self.e_stage_event = asyncio.Event()
        self.e_timeline = omni.timeline.get_timeline_interface()

        ctx = omni.usd.get_context()
        stream = ctx.get_stage_event_stream()

        # IMPORTANT: give the subscription a name in 5.0.0
        try:
            self.e_stage_subscription = stream.create_subscription_to_pop(
                self.on_stage_event, name="motion.stage.subscription"
            )
            carb.log_info("[motion.extension] stage event subscription created")
        except Exception as ex:
            carb.log_error(
                f"[motion.extension] failed to subscribe to stage events: {ex!r}"
            )
            raise

        async def f_stage(url: str):
            # Close any existing stage, then open with full load
            if ctx.get_stage():
                carb.log_info("[motion.extension] closing existing stage")
                await ctx.close_stage_async()

            carb.log_info(f"[motion.extension] opening stage: {url!s}")
            self.e_stage_event.clear()
            await ctx.open_stage_async(
                url,
                load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
            )

            # Wait for StageEventType.OPENED (signaled in on_stage_event)
            carb.log_info("[motion.extension] waiting for OPENED event")
            await self.e_stage_event.wait()

            # Wait until the stage finishes loading all assets
            while ctx.is_stage_loading():
                carb.log_info("[motion.extension] stage still loading...")
                await asyncio.sleep(0.05)

            carb.log_info("[motion.extension] Stage OPENED and fully loaded")

            # SAFE ZONE: timeline/physics calls are now safe
            carb.log_info("[motion.extension] pausing simulation")
            self.pause_simulation()

            carb.log_info("[motion.extension] sleeping 2s (real time) before step")
            await asyncio.sleep(2.0)

            carb.log_info("[motion.extension] stepping simulation by one tick")
            self.step_simulation()

            carb.log_info("[motion.extension] sleeping 2s (real time) before resume")
            await asyncio.sleep(2.0)

            carb.log_info("[motion.extension] resuming simulation")
            self.resume_simulation()

        # Schedule the task (exceptions bubble up; we just log them)
        self.e_stage_task = asyncio.create_task(
            f_stage("file:///storage/node/scene.usd")
        )

        # Done-callback to surface any unhandled exceptions prominently
        def f_done(t: asyncio.Task):
            try:
                exc = t.exception()
            except asyncio.CancelledError:
                exc = None
            if exc is not None:
                carb.log_error(f"[motion.extension] Stage task failed: {exc!r}")

        self.e_stage_task.add_done_callback(f_done)

    def on_shutdown(self):
        carb.log_info("[motion.extension] on_shutdown")
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
        self.e_timeline = None

    # ---------- Stage events ----------

    def on_stage_event(self, e):
        # Log first for visibility
        carb.log_info(f"[motion.extension] stage event: {getattr(e, 'type', None)}")
        if e.type == omni.usd.StageEventType.OPENED:
            carb.log_info("[motion.extension] Stage OPENED")
            if self.e_stage_event and not self.e_stage_event.is_set():
                self.e_stage_event.set()

    # ---------- Timeline helpers ----------

    def pause_simulation(self):
        if not self.e_timeline:
            carb.log_warn("[motion.extension] pause_simulation: timeline not ready")
            return
        self.e_timeline.pause()
        carb.log_info("[motion.extension] timeline paused")

    def resume_simulation(self):
        if not self.e_timeline:
            carb.log_warn("[motion.extension] resume_simulation: timeline not ready")
            return
        self.e_timeline.play()
        carb.log_info("[motion.extension] timeline playing")

    def step_simulation(self, dt: float = 1.0 / 60.0):
        if not self.e_timeline:
            carb.log_warn("[motion.extension] step_simulation: timeline not ready")
            return
        # In Kit 105 / Isaac 5.0.0, step(dt) advances one frame of length dt.
        self.e_timeline.step(dt)
        carb.log_info(f"[motion.extension] timeline stepped by {dt} sec")
