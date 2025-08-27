import asyncio
import contextlib

import omni.ext
import omni.timeline
import omni.usd


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.stage = None
        self.timeline = None
        self.timeline_subscription = None

    def on_startup(self, ext_id):
        print("[motion.extension] startup")

        self.timeline = omni.timeline.get_timeline_interface()
        self.timeline_subscription = (
            self.timeline.get_timeline_event_stream().create_subscription_to_pop(
                self.on_timeline_event, name="motion.extension.timeline"
            )
        )

        async def f_stage(self, e):
            ctx = omni.usd.get_context()
            if ctx.get_stage():
                await ctx.close_stage_async()
            self.stage = await asyncio.wait_for(
                usd_ctx.open_stage_async(
                    e, load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL
                ),
                timeout=120.0,
            )

        loop = omni.kit.app.get_app().get_async_event_loop()
        loop.create_task(f_stage(self, "file:///storage/node/scene/scene.usd"))

    def on_shutdown(self):
        print("[motion.extension] shutdown")

        with contextlib.suppress(Exception):
            if self.timeline_subscription:
                self.timeline_subscription.unsubscribe()
        self.timeline_subscription = None
        self.timeline = None

    def on_timeline_event(self, e):
        name = omni.timeline.TimelineEventType(e.type).name
        print(f"[motion.extension] timeline {name}")
        if e.type == omni.timeline.TimelineEventType.PLAY.value:
            print("[motion.extension] timeline PLAY")
        elif e.type == omni.timeline.TimelineEventType.STOP.value:
            print("[motion.extension] timeline STOP")
