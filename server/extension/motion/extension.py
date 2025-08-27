import asyncio
import contextlib
import json

import omni.ext
import omni.kit
import omni.timeline
import omni.usd

from .channel import Channel
from .node import run_data, run_http


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.timeline = None
        self.timeline_subscription = None

        self.session = None
        self.run_http = None
        self.run_data = None
        self.channel = None

        self.stage = None

    def on_startup(self, ext_id):
        print("[motion.extension] startup")

        with open("/storage/node/session.json", "r") as f:
            self.session = json.loads(f.read())["session"]

        self.timeline = omni.timeline.get_timeline_interface()
        self.timeline_subscription = (
            self.timeline.get_timeline_event_stream().create_subscription_to_pop(
                self.on_timeline_event, name="motion.extension.timeline"
            )
        )

        async def run_isaac(session: str, channel: Channel):
            sub = await channel.subscribe_step(session)
            print(f"[motion.extension] subscribed step for session={session}")

            try:
                while True:
                    payload = json.dumps({"session": session})
                    await channel.publish_data(session, payload)
                    await asyncio.sleep(1.0)
            finally:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
                print(f"[motion.extension] unsubscribed step for session={session}")

        async def f_stage(self, e):
            print("[motion.extension] stage")
            ctx = omni.usd.get_context()
            if ctx.get_stage():
                await ctx.close_stage_async()
            stage = await asyncio.wait_for(
                ctx.open_stage_async(
                    e, load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL
                ),
                timeout=120.0,
            )
            print("[motion.extension] loaded")

            self.run_http = run_http()
            await self.run_http.__aenter__()

            print("[motion.extension] http up")
            self.run_data = run_data()
            self.channel = await self.run_data.__aenter__()

            print("[motion.extension] data up")

            self.stage = stage

            print("[motion.extension] stage up")

            await run_isaac(self.session, self.channel)

        omni.kit.async_engine.run_coroutine(
            f_stage(self, "file:///storage/node/scene/scene.usd")
        )

    def on_shutdown(self):
        print("[motion.extension] shutdown")
        with contextlib.suppress(Exception):
            if self.run_data:
                self.run_data.__aexit__(None, None, None)
            if self.run_http:
                self.run_http.__aexit__(None, None, None)

        with contextlib.suppress(Exception):
            if self.timeline_subscription:
                self.timeline_subscription.unsubscribe()
        self.timeline_subscription = None
        self.timeline = None

    def on_timeline_event(self, e):
        name = omni.timeline.TimelineEventType(e.type).name
        print(f"[motion.extension] timeline {name}")
        if e.type == omni.timeline.TimelineEventType.PLAY.value:
            print("[motion.extension] timeline play")
        elif e.type == omni.timeline.TimelineEventType.STOP.value:
            print("[motion.extension] timeline stop")
