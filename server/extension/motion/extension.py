import asyncio

import carb
import carb.events
import omni.ext
import omni.usd
import omni.timeline


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.e_stage_task = None
        self.e_stage_event = None
        self.e_stage_subscription = None

    def on_startup(self, ext_id):
        print("[my.timeline.logger] startup")

        self.timeline = omni.timeline.get_timeline_interface()
        self.timeline_event_stream = self.timeline.get_timeline_event_stream()
        self.timeline_event_stream_subscription = self.timeline_event_stream.create_subscription_to_pop(
            self.on_timeline_event, name="my.timeline.control"
        )


        print("[my.timeline.logger] startup2")
        self.e_stage_event = asyncio.Event()

        ctx = omni.usd.get_context()
        self.e_stage_subscription = (
            ctx.get_stage_event_stream().create_subscription_to_pop(self.on_stage_event)
        )

        async def f_stage(url: str):
            # Close any existing stage, then open with full load
            if ctx.get_stage():
                await ctx.close_stage_async()

            self.e_stage_event.clear()
            await ctx.open_stage_async(
                url,
                load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,  # ensure full load
            )

            # Wait for StageEventType.OPENED (signaled in on_stage_event)
            await self.e_stage_event.wait()

            # Wait until the stage finishes loading all assets
            while ctx.is_stage_loading():
                await asyncio.sleep(0.05)

            carb.log_info("[motion.extension] Stage OPENED and fully loaded")

        # Schedule the task (exceptions bubble up; we just log them)
        self.e_stage_task = asyncio.create_task(
            f_stage("file:///storage/node/scene/scene.usd")
        )

        # Done-callback to surface any unhandled exceptions prominently
        def f_done(t: asyncio.Task):
            exc = t.exception()
            if exc is not None:
                carb.log_error(f"[motion.extension] Stage task failed: {exc!r}")

        self.e_stage_task.add_done_callback(f_done)

    def on_shutdown(self):

        carb.log_info("[my.timeline.logger] shutdown")


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

    def on_timeline_event(self, e):
        name = carb.events.Type(e.type)
        print(f"[my.timeline.control] timeline {name} {e.type}")
        et = e.type
        # PLAY: set up control loop and subscribe to per-tick
        if et == omni.timeline.TimelineEventType.PLAY.value:
            print("[my.timeline.control] timeline PLAY")
            #self._on_play()

        # STOP: tear down per-tick subscription and world refs
        elif et == omni.timeline.TimelineEventType.STOP.value:
            print("[my.timeline.control] timeline STOP")
            #self._on_stop()


    def on_play(self):
        carb.log_info("[my.timeline.logger] timeline started")

    def on_stop(self):
        carb.log_info("[my.timeline.logger] timeline stopped")

    # ---------- Stage events ----------

    def on_stage_event(self, e):
        if e.type == omni.usd.StageEventType.OPENED:
            carb.log_info("[motion.extension] Stage OPENED")
            if self.e_stage_event and not self.e_stage_event.is_set():
                self.e_stage_event.set()
