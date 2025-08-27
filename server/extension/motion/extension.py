import asyncio

import carb
import carb.events
import omni.ext
import omni.usd
import omni.timeline


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.stage_task = None
        self.stage_event = None
        self.stage_subscription = None
        self.timeline = None
        self.timeline_event_stream = None
        self.timeline_event_stream_subscription = None

    def on_startup(self, ext_id):
        carb.log_info("[motion.extension] startup")

        self.timeline = omni.timeline.get_timeline_interface()
        self.timeline_event_stream = self.timeline.get_timeline_event_stream()
        self.timeline_event_stream_subscription = (
            self.timeline_event_stream.create_subscription_to_pop(
                self.on_timeline_event, name="motion.extension.timeline"
            )
        )

        self.stage_event = asyncio.Event()

        ctx = omni.usd.get_context()
        self.stage_subscription = (
            ctx.get_stage_event_stream().create_subscription_to_pop(
                self.on_stage_event, name="motion.extension.stage"
            )
        )

        async def f_stage(url: str):
            # Close any existing stage, then open with full load
            if ctx.get_stage():
                await ctx.closstage_async()

            self.stage_event.clear()
            await ctx.open_stage_async(
                url,
                load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,  # ensure full load
            )

            # Wait for StageEventType.OPENED (signaled in on_stage_event)
            await self.stage_event.wait()

            # Wait until the stage finishes loading all assets
            while ctx.is_stage_loading():
                await asyncio.sleep(0.05)

            carb.log_info("[motion.extension] Stage OPENED and fully loaded")

        # Schedule the task (exceptions bubble up; we just log them)
        self.stage_task = asyncio.create_task(
            f_stage("file:///storage/node/scene/scene.usd")
        )

        # Done-callback to surface any unhandled exceptions prominently
        def f_done(t: asyncio.Task):
            exc = t.exception()
            if exc is not None:
                carb.log_error(f"[motion.extension] Stage task failed: {exc!r}")

        self.stage_task.add_done_callback(f_done)

    def on_shutdown(self):

        carb.log_info("[motion.extension] shutdown")

        # Cancel stage-open task first
        if self.stage_task:
            try:
                self.stage_task.cancel()
            except Exception:
                pass
            self.stage_task = None

        # Unsubscribe from stage events
        if self.stage_subscription:
            try:
                self.stage_subscription.unsubscribe()
            except Exception:
                pass
            self.stage_subscription = None

        self.stage_event = None

    def on_timeline_event(self, e):
        name = omni.timeline.TimelineEventType(e.type).name
        carb.log_info(f"[my.timeline.control] timeline {name}")
        if e.type == omni.timeline.TimelineEventType.PLAY.value:
            carb.log_info("[my.timeline.control] timeline PLAY")

        # STOP: tear down per-tick subscription and world refs
        elif e.type == omni.timeline.TimelineEventType.STOP.value:
            carb.log_info("[my.timeline.control] timeline STOP")

    def on_play(self):
        carb.log_info("[my.timeline.logger] timeline started")

    def on_stop(self):
        carb.log_info("[my.timeline.logger] timeline stopped")

    # ---------- Stage events ----------

    def on_stage_event(self, e):
        if e.type == omni.usd.StageEventType.OPENED:
            carb.log_info("[motion.extension] Stage OPENED")
            if self.stage_event and not self.stage_event.is_set():
                self.stage_event.set()
