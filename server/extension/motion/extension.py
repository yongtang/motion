import asyncio
import contextlib

import carb
import carb.events
import omni.ext
import omni.timeline
import omni.usd


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.stage_subscription = None
        self.stage_task = None

        self.timeline = None
        self.timeline_subscription = None

    def on_startup(self, ext_id):
        carb.log_info("[motion.extension] startup")

        ctx = omni.usd.get_context()

        async def f_stage(url: str):
            # Close any existing stage, then open with full load
            if ctx.get_stage():
                await ctx.close_stage_async()

            await ctx.open_stage_async(
                url,
                load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,  # ensure full load
            )

        self.stage_subscription = (
            ctx.get_stage_event_stream().create_subscription_to_pop(
                self.on_stage_event, name="motion.extension.stage"
            )
        )
        self.stage_task = asyncio.create_task(
            f_stage("file:///storage/node/scene/scene.usd")
        )

        self.timeline = omni.timeline.get_timeline_interface()
        self.timeline_subscription = (
            self.timeline.get_timeline_event_stream().create_subscription_to_pop(
                self.on_timeline_event, name="motion.extension.timeline"
            )
        )

    def on_shutdown(self):
        carb.log_info("[motion.extension] shutdown")
        with contextlib.suppress(Exception):
            if self.timeline_subscription:
                self.timeline_subscription.unsubscribe()
        with contextlib.suppress(Exception):
            if self.stage_subscription:
                self.stage_subscription.unsubscribe()
        with contextlib.suppress(Exception):
            if self.stage_task:
                self.stage_task.cancel()
        self.timeline_subscription = None
        self.timeline = None
        self.stage_task = None
        self.stage_subscription = None

    def on_stage_event(self, e):
        name = omni.usd.StageEventType(e.type).name
        carb.log_info(f"[motion.extension] stage {name}")

    def on_timeline_event(self, e):
        name = omni.timeline.TimelineEventType(e.type).name
        carb.log_info(f"[motion.extension] timeline {name}")
        if e.type == omni.timeline.TimelineEventType.PLAY.value:
            carb.log_info("[motion.extension] timeline PLAY")
        elif e.type == omni.timeline.TimelineEventType.STOP.value:
            carb.log_info("[motion.extension] timeline STOP")
