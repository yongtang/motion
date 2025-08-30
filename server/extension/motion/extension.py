import asyncio
import contextlib
import json
import time

import omni.ext
import omni.kit
import omni.timeline
import omni.usd
from omni.isaac.core.articulations import Articulation
from omni.isaac.core.prims import XFormPrim

from .channel import Channel
from .node import run_data, run_http, run_step


def f_sync(metadata):
    return bool(metadata["sync"]) if "sync" in metadata else True


def f_prim(metadata, stage):
    print(f"[motion.extension] prim: {metadta} {stage}")
    prim = (
        stage.GetPrimAtPath(metadata["prim"])
        if "prim" in metadata
        else stage.GetDefaultPrim()
    )
    assert prim and prim.IsValid()
    return prim.GetPath().pathString


def f_call(metadata, channel, articulation):
    print(f"[motion.extension] call: {metadta}")
    if f_sync(metadata):
        return None
    session = metadata["session"]
    link = tuple(
        XFormPrim(e)
        for e in sorted(set(metadata["link"] if "link" in metadata else []))
    )

    def on_timeline_event(e):
        name = omni.timeline.TimelineEventType(e.type).name
        print(f"[motion.extension] timeline {name}")
        if not (e.type == omni.timeline.TimelineEventType.STEP.value):
            return
        print("[motion.extension] timeline step")

        omni.kit.async_engine.run_coroutin(
            channel.publish_data(session, f_data(session, articulation, link))
        )

    return on_timeline_event


def f_step(metadata, channel, articulation):
    print(f"[motion.extension] call: {metadta}")
    session = metadata["session"]
    link = tuple(
        XFormPrim(e)
        for e in sorted(set(metadata["link"] if "link" in metadata else []))
    )

    async def run_isaac(msg):
        print(f"[motion.extension] step session={session}: {msg}")
        step = json.loads(msg.data)
        targets = list(target.values())
        indices = list(articulation.dof_names.index(e) for e in target.keys())
        print(
            f"[motion.extension] articulation for session={session}: {targets}, {indices}"
        )
        articulation.set_joint_position_targets(targets, indices)

        if not f_sync(metadata):
            return None
        timeline.forward_one_frame()

        await channel.publish_data(session, f_data(session, articulation, link))

    return run_isaac


def f_data(session, articulation, link):
    time = omni.timeline.get_timeline_interface().get_current_time()
    joint = dict(zip(articulation.dof_names, articulation.get_joint_position()))

    data = {"session": session, "joint": joint, "time": time}
    # orientation: quotanion - xyzw
    pose = {
        name: {"position": position, "orientation": orientation}
        for name, position, orientation in list(
            (e.prim_link, *e.get_world_pose()) for e in link
        )
    }
    data = {**data, **({"pose": pose} if len(pose) else {})}

    data = json.dumps(data)
    print(f"[motion.extension] step->data session={session}: {data[:120]!r}")
    return data


@contextlib.asynccontextmanager
async def run_call(call):
    subscription = (
        (
            omni.timeline.get_timeline_interface()
            .get_timeline_event_stream()
            .create_subscription_to_pop(call, name="motion.extension.timeline")
        )
        if call
        else None
    )
    print("Timeline subscribed")

    try:
        yield subscription
    finally:
        with contextlib.suppress(Exception):
            subscription.unsubscribe() if call else None
            print("Timeline unsubscribed")


async def main():
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())

    print("[motion.extension] stage")
    ctx = omni.usd.get_context()
    if ctx.get_stage():
        await ctx.close_stage_async()
    await asyncio.wait_for(
        ctx.open_stage_async(e, load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL),
        timeout=120.0,
    )
    stage = ctx.get_stage()
    assert stage
    print("[motion.extension] loaded")

    prim = f_prim(metadata, stage)
    articulation = Articulation(prim)
    articulation.initialize()

    session = metadata["session"]
    async with run_http():
        async with run_data() as channel:
            async with run_step(
                session=session,
                channel=channel,
                callback=f_step(metadata, channel, articulation),
            ) as subscribe:
                async with run_call(
                    f_call(metadata, channel, articulation)
                ) as subscription:
                    omni.timeline.get_timeline_interface().set_ticks_per_frame(1)
                    omni.timeline.get_timeline_interface().forward_one_frame()
                    print(
                        f"[motion.extension] articulation dof: {articulation.dof_names}"
                    )
                    assert articulation.dof_names is not None

                    (
                        omni.timeline.get_timeline_interface().play()
                        if subscription
                        else None
                    )

                    await asyncio.Event().wait()


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        print("[motion.extension] startup")
        omni.kit.app.get_app().get_async_event_loop().create_task(main())

    def on_shutdown(self):
        print("[motion.extension] shutdown")
        self.task.cancel() if self.task else None
