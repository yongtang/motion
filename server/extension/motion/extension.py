import asyncio

import carb
import carb.events
import omni.ext
import omni.usd
import omni.timeline


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
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

    def on_shutdown(self):
        carb.log_info("[motion.extension] shutdown")

    def on_timeline_event(self, e):
        name = omni.timeline.TimelineEventType(e.type).name
        carb.log_info(f"[motion.extension] timeline {name}")
        if e.type == omni.timeline.TimelineEventType.PLAY.value:
            carb.log_info("[motion.extension] timeline PLAY")
        elif e.type == omni.timeline.TimelineEventType.STOP.value:
            carb.log_info("[motion.extension] timeline STOP")
