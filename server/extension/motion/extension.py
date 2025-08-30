import asyncio
import contextlib
import json
import time

import omni.ext
import omni.kit
import omni.timeline
import omni.usd

from omni.isaac.core.articulations import Articulation

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

        def ff(robot):
            joint_targets = {}  # joint_name -> Usd.Attribute (targetPosition)
            joint_states = {}  # joint_name -> Usd.Attribute (position)

            def scan_prim(p):
                for attr in p.GetAttributes():
                    name = attr.GetName()
                    # drive:<joint>:targetPosition
                    if name.startswith("drive:") and name.endswith(":targetPosition"):
                        joint = name.split(":")[1]
                        joint_targets[joint] = attr
                    # state:<joint>:position
                    if name.startswith("state:") and name.endswith(":position"):
                        joint = name.split(":")[1]
                        joint_states[joint] = attr

                for child in p.GetChildren():
                    scan_prim(child)

            scan_prim(robot)
            return joint_targets, joint_states

        async def run_isaac(self, session: str, channel: Channel):
            sub = await channel.subscribe_step(session)
            print(f"[motion.extension] subscribed step for session={session}")

            last = time.perf_counter()

            self.timeline.set_ticks_per_frame(1)
            try:
                while True:
                    # ---- drift probe (inline) ----
                    now = time.perf_counter()
                    drift = now - last - 1.0  # expected ~1.0s
                    if drift > 0.05:
                        print(f"[motion.extension][DRIFT] loop delayed by {drift:.3f}s")
                    last = now
                    # -----------------------------

                    print(f"[motion.extension] stage ={self.stage}")

                    articulation = Articulation(
                        prim_paths_expr="/World/tracking/Franka"
                    )

                    positions = articulation.data.joint_pos 

                    # names = articulation.get_joints_state().names
                    names = articulation.joint_names 
                    print(f"[motion.extension] names={names} positions={positions}")

                    self.timeline.forward_one_frame()

                    """
                    robot = self.stage.GetPrimAtPath("/World/tracking/Franka")
                    assert robot.IsValid()
                    joint_targets, joint_states = ff(robot)

                    print(f"[motion.extension][robot] joint_targets={joint_targets}, joint_states={joint_states}")

                    joint_targets_position = {k:v.Get() for k, v in joint_targets.items()}
                    joint_states_position = {k:v.Get() for k, v in joint_states.items()}

                    print(f"[motion.extension][robot] joint_targets_position={joint_targets_position}, joint_states_position={joint_states_position}")


                    #joint_targets_position = {k:v.Set(v.Get() + 0.1) for k, v in joint_targets.items()}
                    #joint_states_position = {k:v.Set(v.Get() + 0.1) for k, v in joint_states.items()}


                    self.timeline.forward_one_frame()

                    joint_targets_position = {k:v.Get() for k, v in joint_targets.items()}
                    joint_states_position = {k:v.Get() for k, v in joint_states.items()}

                    print(f"[motion.extension][robot] xxxx - joint_targets_position={joint_targets_position}, joint_states_position={joint_states_position}")
                    """

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
