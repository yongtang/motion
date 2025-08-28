import asyncio
import contextlib
import json
import threading

import omni.ext
import omni.kit
import omni.timeline
import omni.usd

from .node import run_data, run_http


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        super().__init__()
        self.timeline = None
        self.timeline_subscription = None

        self.session = None
        self.run_http = None
        self.stage = None

        self.loop = None
        self.thread = None

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

        async def run_isaac(session: str):
            # NATS lives entirely on the background loop/thread.
            async with run_data() as channel:
                while True:
                    # Kit loop only when needed
                    info = self.call(self.get_stage_info)
                    payload = json.dumps({"session": session, "info": info})
                    await channel.publish_data(session, payload)
                    await asyncio.sleep(0.1)

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

            # dedicated asyncio loop in its own OS thread
            self.loop = asyncio.new_event_loop()
            self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
            self.thread.start()

            # fire-and-forget publisher
            asyncio.run_coroutine_threadsafe(run_isaac(self.session), self.loop)

            self.stage = stage
            print("[motion.extension] stage up")

        omni.kit.async_engine.run_coroutine(
            f_stage(self, "file:///storage/node/scene/scene.usd")
        )

    # ---------- cross-loop helpers ----------
    def call(self, f):
        """Run f() on Kit's loop and block until it returns."""
        loop = omni.kit.app.get_app().get_async_event_loop()
        return asyncio.run_coroutine_threadsafe(f(), loop).result()

    async def get_stage_info(self):
        return {"root": "root"}


    # ----------------------------------------

    def on_shutdown(self):
        print("[motion.extension] shutdown")
        with contextlib.suppress(Exception):
            if self.loop:
                self.loop.call_soon_threadsafe(self.loop.stop)
            if self.thread:
                self.thread.join(timeout=2.0)

        with contextlib.suppress(Exception):
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
