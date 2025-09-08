import asyncio
import contextlib
import io
import json

import omni.ext
import omni.kit
import omni.replicator
import omni.timeline
import omni.usd
import PIL.Image
import pxr
from omni.isaac.core import articulations
from omni.isaac.core import prims

# from omni.isaac.core.articulations import Articulation
# from omni.isaac.core.prims import XFormPrim

from .node import run_http, run_link, run_step


def f_sync(metadata):
    return bool(metadata["sync"]) if "sync" in metadata else True


def f_prim(metadata, stage):
    print(f"[motion.extension] prim: {metadata} {stage}")
    metadata["prim"] = "/World/tracking/Franka"
    prim = (
        stage.GetPrimAtPath(metadata["prim"])
        if "prim" in metadata
        else stage.GetDefaultPrim()
    )
    assert prim and prim.IsValid()
    return prim.GetPath().pathString


def f_rend(metadata, stage):
    print(f"[motion.extension] rend: {metadata} {stage}")
    if "camera" not in metadata or len(metadata["camera"]) == 0:
        return None
    camera = list(stage.GetPrimAtPath(e) for e in metadata["camera"])
    assert all((e and e.IsValid() and e.IsA(pxr.UsdGeom.Camera)) for e in camera)
    camera = list(e.GetPath().pathString for e in camera)
    return {
        e: omni.replicator.core.create.render_product(
            e, (metadata["camera"][e]["weight"], metadata["camera"][e]["height"])
        )
        for e in camera
    }


def f_call(metadata, channel, articulation, annotator, state):
    print(f"[motion.extension] call: {metadata}")
    if f_sync(metadata):
        return None
    session = metadata["uuid"]
    link = tuple(
        prims.XFormPrim(e)
        for e in sorted(set(metadata["link"] if "link" in metadata else []))
    )

    def on_timeline_event(e):
        name = omni.timeline.TimelineEventType(e.type).name
        print(f"[motion.extension] timeline {name}")
        if not (e.type == omni.timeline.TimelineEventType.STEP.value):
            return

        frame = state["frame"]
        print(f"[motion.extension] timeline step {frame} enter")

        data, post = f_data(
            session=session,
            frame=frame,
            articulation=articulation,
            annotator=annotator,
            link=link,
        )
        omni.kit.async_engine.run_coroutine(
            channel.publish_data(session=session, data=data)
        )
        omni.kit.async_engine.run_coroutine(post())

        print(f"[motion.extension] timeline step {frame} leave")

        state["frame"] += 1

    return on_timeline_event


def f_step(metadata, channel, articulation, annotator, state):
    print(f"[motion.extension] step: {metadata}")
    session = metadata["uuid"]
    link = tuple(
        XFormPrim(e)
        for e in sorted(set(metadata["link"] if "link" in metadata else []))
    )

    async def run_isaac(msg):
        if f_sync(metadata):
            frame = state["frame"]
            print(f"[motion.extension] timeline step {frame} enter")

            omni.timeline.get_timeline_interface().forward_one_frame()

            data, post = f_data(
                session=session,
                frame=frame,
                articulation=articulation,
                annotator=annotator,
                link=link,
            )
            await channel.publish_data(session=session, data=data)
            await post()

            print(f"[motion.extension] timeline step {frame} enter")

            state["frame"] += 1

        print(f"[motion.extension] step session={session}: {msg}")
        step = json.loads(msg.data)
        targets = list(target.values())
        indices = list(articulation.dof_names.index(e) for e in target.keys())
        print(
            f"[motion.extension] articulation for session={session}: {targets}, {indices}"
        )
        articulation.set_joint_position_targets(targets, indices)

    return run_isaac


def f_data(session, frame, articulation, annotator, link):
    time = omni.timeline.get_timeline_interface().get_current_time()
    joint = dict(zip(articulation.dof_names, articulation.get_joint_position()))

    # orientation: quotanion - xyzw
    pose = {
        name: {"position": position, "orientation": orientation}
        for name, position, orientation in list(
            (e.prim_link, *e.get_world_pose()) for e in link
        )
    }
    camera = {k: v.get_data().get("data", None) for k, v in annotator.items()}
    camera = {k: v for k, v in camera.items() if v}

    data = json.dumps(
        {
            "session": session,
            "frame": frame,
            "joint": joint,
            **({"pose": pose} if len(pose) else {}),
            "time": time,
        }
    )
    print(f"[motion.extension] step->data session={session}: {data}")

    async def f(key, value):
        image = PIL.Image.from_arra(value)
        buffer = io.BytesIO()
        image.save(buffer, format="JPEG", quality=90)
        storage_kv_set("image", f"{session}-{frame}-{key}.jpg", buffer.getvalue())

    async def post():
        for key, value in camera.items():
            omni.kit.async_engine.run_coroutine(f(key, value))

    return data, post


@contextlib.asynccontextmanager
async def run_rend(rend):
    annotator = None
    if rend:
        writer = omni.replicator.core.WriterRegistry.get("RTSPWriter")
        writer.initialize(
            annotator="rgb", output_dir="rtsp://127.0.0.1:8554/RTSPWriter"
        )
        writer.attach(list(rend.values()))

        annotator = {
            e: omni.replicator.core.AnnotatorRegistry.get_annotator("rgb") for e in rend
        }
        for i, e in annotator.items():
            e.attach(rend[i])
        print("RTSP Writer attached")
    try:
        yield annotator
    finally:
        if rend:
            with contextlib.suppress(Exception):
                for i, e in annotator.items():
                    e.detach(rend[i])
            with contextlib.suppress(Exception):
                writer.detach(list(rend.values()))
            print("RTSP Writer detached")


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
        ctx.open_stage_async(
            "file:///storage/node/scene/scene.usd",
            load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
        ),
        timeout=120.0,
    )
    stage = ctx.get_stage()
    assert stage
    print("[motion.extension] loaded")

    # sim = core.SimulationContext()
    # await omni.kit.app.get_app().next_update_async()
    # sim.step(render=False)

    omni.timeline.get_timeline_interface().set_ticks_per_frame(1)
    omni.timeline.get_timeline_interface().forward_one_frame()
    await omni.kit.app.get_app().next_update_async()

    prim = f_prim(metadata, stage)
    articulation = articulations.Articulation(prim)
    articulation.initialize()

    await omni.kit.app.get_app().next_update_async()
    print("[motion.extension] dofs:", articulation.dof_names)

    state = {"frame": 0}
    session = metadata["uuid"]
    async with run_http():
        async with run_link() as channel:
            async with run_rend(f_rend(metadata, stage)) as annotator:
                async with run_step(
                    session=session,
                    channel=channel,
                    callback=f_step(metadata, channel, articulation, annotator, state),
                ) as subscribe:
                    async with run_call(
                        f_call(metadata, channel, articulation, annotator, state)
                    ) as subscription:
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
        omni.kit.async_engine.run_coroutine(main())

    def on_shutdown(self):
        print("[motion.extension] shutdown")
        self.task.cancel() if self.task else None
