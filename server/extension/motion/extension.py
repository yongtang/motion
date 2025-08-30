import asyncio
import contextlib
import json
import time

import omni.ext
import omni.kit
import omni.timeline
import omni.usd
from omni.isaac.core.articulations import ArticulationView

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

        async def run_isaac(self, session: str, channel: Channel):
            sub = await channel.subscribe_step(session)
            print(f"[motion.extension] subscribed step for session={session}")

            articulation = ArticulationView(prim_paths_expr="/World/tracking/Franka")
            articulation.initialize()

            self.timeline.set_ticks_per_frame(1)
            self.timeline.forward_one_frame()

            print(f"[motion.extension] articulation dof: {articulation.dof_names}")
            assert articulation.dof_names is not None

            try:
                while True:
                    try:
                        msgs = await sub.fetch(128, timeout=1)
                    except asyncio.TimeoutError:
                        continue
                    for msg in msgs:
                        # no ack
                        print(
                            f"[motion.extension] received message for session={session}: {msg}"
                        )

                        data = json.loads(msg.data)
                        targets = list(target.values())
                        indices = list(
                            articulation.dof_names.index(e) for e in target.keys()
                        )
                        print(
                            f"[motion.extension] articulation for session={session}: {targets}, {indices}"
                        )

                        articulation.set_joint_position_targets(targets, indices)

                        self.timeline.forward_one_frame()

                        payload = json.dumps({**data, "session": session})
                        await channel.publish_data(session, payload)
            finally:
                with contextlib.suppress(Exception):
                    await sub.unsubscribe()
                print(f"[motion.extension] unsubscribed step for session={session}")

        async def f_stage(self, e):
            print("[motion.extension] stage")
            ctx = omni.usd.get_context()
            if ctx.get_stage():
                await ctx.close_stage_async()
            await asyncio.wait_for(
                ctx.open_stage_async(
                    e, load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL
                ),
                timeout=120.0,
            )
            stage = ctx.get_stage()
            assert stage
            print("[motion.extension] loaded")

            self.run_http = run_http()
            await self.run_http.__aenter__()

            print("[motion.extension] http up")
            self.run_data = run_data()
            self.channel = await self.run_data.__aenter__()

            print("[motion.extension] data up")

            self.stage = stage

            print("[motion.extension] stage up")

            await run_isaac(self, self.session, self.channel)

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
        elif e.type == omni.timeline.TimelineEventType.STEP.value:
            print("[motion.extension] timeline step")
