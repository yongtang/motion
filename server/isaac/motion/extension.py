import asyncio
import contextlib
import functools
import itertools
import json
import logging
import traceback

import isaacsim.core.experimental.prims
import isaacsim.replicator.agent.core.data_generation.writers.rtsp  # pylint: disable=W0611
import numpy
import omni.ext
import omni.kit
import omni.replicator.core
import omni.timeline
import omni.usd
import pxr

from .channel import Channel
from .interface import Interface


def f_data(e, session, interface, channel, articulation, joint, link, annotator):
    print(f"[motion.extension] [run_call] Annotator callback")
    entries = {n: numpy.asarray(e.get_data()) for n, e in annotator.items()}
    # Expect numpy.uint8 or numpy.float64
    assert all(e.dtype in (numpy.uint8, numpy.float64) for e in entries.values()), {
        n: e.dtype for n, e in entries.items()
    }

    # Expect H×W×C with C = 3 (BGR) or 4 (BGRA).
    assert all(
        (
            (e.ndim == 1 and e.shape[0] == 0)
            or (
                e.ndim == 3
                and e.shape[0] > 0
                and e.shape[1] > 0
                and e.shape[2] in (3, 4)
            )
        )
        for e in entries.values()
    ), {n: e.shape for n, e in entries.items()}
    # BGRA/BGR -> RGBA/RGB
    entries = {
        n: (
            e
            if e.size == 0
            else (e[..., [2, 1, 0]] if e.shape[2] == 3 else e[..., [2, 1, 0, 3]])
        )
        for n, e in entries.items()
    }
    # numpy.uint8
    entries = {
        n: (
            (numpy.clip(e, 0.0, 1.0) * 255).astype(numpy.uint8)
            if e.dtype is not numpy.uint8
            else e
        )
        for n, e in entries.items()
    }
    entries = {n: numpy.ascontiguousarray(e) for n, e in entries.items()}
    entries = {
        n: {
            "dtype": str(e.dtype),
            "shape": str(e.shape),
        }
        for n, e in entries.items()
    }
    print(f"[motion.extension] [run_call] Annotator callback - camera: {entries}")

    print(f"[motion.extension] [run_call] Articulation callback")
    state = dict(
        zip(
            list(itertools.chain.from_iterable(articulation.dof_paths)),
            list(
                itertools.chain.from_iterable(
                    numpy.asarray(articulation.get_dof_positions()).tolist()
                )
            ),
        )
    )
    state = state if "*" in joint else {k: v for k, v in state.items() if k in joint}
    print(f"[motion.extension] [run_call] Articulation callback - state: {state}")

    print(f"[motion.extension] [run_call] Link callback")
    position, quaternion = link.get_world_poses()  # quaternion: w, x, y, z
    position, quaternion = (
        numpy.asarray(position),
        numpy.asarray(quaternion)[:, [1, 2, 3, 0]],
    )  # quaternion: x, y, z, w
    pose = {
        e: {
            "position": {
                "x": float(p[0]),
                "y": float(p[1]),
                "z": float(p[2]),
            },
            "orientation": {
                "x": float(q[0]),
                "y": float(q[1]),
                "z": float(q[2]),
                "w": float(q[3]),
            },
        }
        for e, p, q in zip(link.paths, position, quaternion)
    }
    print(f"[motion.extension] [run_call] Link callback - pose: {pose}")
    try:
        data = json.dumps(
            {
                "joint": state,
                "camera": entries,
                "pose": pose,
            },
            sort_keys=True,
        ).encode()
        omni.kit.async_engine.run_coroutine(channel.publish_data(session, data))
        print(f"[motion.extension] [run_call] Channel callback: done - {data}")
    except Exception as e:
        print(f"[motion.extension] [run_call] Channel callback: {e}")
        raise


async def run_tick(session, interface, channel, articulation, joint, link, annotator):
    print(f"[motion.extension] [run_call] [run_tick] subscription")
    subscription = (
        omni.kit.app.get_app()
        .get_update_event_stream()
        .create_subscription_to_pop(
            functools.partial(
                f_data,
                session=session,
                interface=interface,
                channel=channel,
                articulation=articulation,
                joint=joint,
                link=link,
                annotator=annotator,
            )
        )
    )
    print(f"[motion.extension] [run_call] [run_tick] Timeline playing")
    omni.timeline.get_timeline_interface().play()
    print(f"[motion.extension] [run_call] [run_tick] Timeline in play")
    await asyncio.sleep(float("inf"))


async def run_norm(session, interface, channel, articulation, joint, link, annotator):
    print(f"[motion.extension] [run_call] [run_norm] subscription")
    subscription = (
        omni.kit.app.get_app()
        .get_update_event_stream()
        .create_subscription_to_pop(
            functools.partial(
                f_data,
                session=session,
                interface=interface,
                channel=channel,
                articulation=articulation,
                joint=joint,
                link=link,
                annotator=annotator,
            )
        )
    )
    print(f"[motion.extension] [run_call] [run_norm] Timeline playing")
    omni.timeline.get_timeline_interface().play()
    print(f"[motion.extension] [run_call] [run_norm] Timeline in play")


async def run_call(session, call):
    print(f"[motion.extension] [run_call] Loaded session {session}")

    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    print(f"[motion.extension] [run_call] Loaded metadata {metadata}")

    camera = metadata["camera"]
    print(f"[motion.extension] [run_call] Loaded camera {camera}")

    joint = metadata["joint"]
    print(f"[motion.extension] [run_call] Loaded joint {joint}")

    link = metadata["link"]
    print(f"[motion.extension] [run_call] Loaded link {link}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        print("[motion.extension] [run_call] Closing existing stage...")
        await ctx.close_stage_async()
        print("[motion.extension] [run_call] Existing stage closed")
        await omni.kit.app.get_app().next_update_async()

    print("[motion.extension] [run_call] Opening stage...")
    await ctx.open_stage_async(
        "file:///storage/node/scene/scene.usd",
        load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
    )

    print("[motion.extension] [run_call] Waiting stage...")
    stage = ctx.get_stage()
    while stage is None:
        print("[motion.extension] [run_call] Waiting loading...")
        await omni.kit.app.get_app().next_update_async()
        stage = ctx.get_stage()
    assert stage

    print(f"[motion.extension] [run_call] Stage loaded")

    articulation = [
        str(e.GetPath())
        for e in stage.Traverse()
        if e.HasAPI(pxr.UsdPhysics.ArticulationRootAPI)
    ]
    print(f"[motion.extension] [run_call] Articulation: {articulation}")

    articulation = isaacsim.core.experimental.prims.Articulation(articulation)
    print(f"[motion.extension] [run_call] Articulation Joint: {articulation.dof_paths}")

    link = isaacsim.core.experimental.prims.XformPrim(
        paths=(
            list(itertools.chain.from_iterable(articulation.link_paths))
            if "*" in link
            else link
        )
    )
    print(f"[motion.extension] [run_call] Link: {link.paths}")

    camera = (
        {
            str(pxr.UsdGeom.Camera(e).GetPrim().GetPath()): camera["*"]
            for e in stage.Traverse()
            if e.IsA(pxr.UsdGeom.Camera) and e.IsActive()
        }
        if "*" in camera
        else camera
    )
    print(f"[motion.extension] [run_call] Camera: {camera}")

    with open("/run/motion/camera.json", "w") as f:
        f.write(json.dumps(camera))
    print(f"[motion.extension] [run_call] Camera: /run/motion/camera.json")

    render = {
        e: omni.replicator.core.create.render_product(e, (v["width"], v["height"]))
        for e, v in camera.items()
    }
    print(f"[motion.extension] [run_call] Render: {render}")

    writer = omni.replicator.core.WriterRegistry.get("RTSPWriter")
    writer.initialize(
        rtsp_stream_url="rtsp://127.0.0.1:8554/RTSPWriter",
        rtsp_rgb=True,
    )
    print(f"[motion.extension] [run_call] Writer: {writer}")

    annotator = {
        e: omni.replicator.core.AnnotatorRegistry.get_annotator("rgb")
        for e, v in camera.items()
    }
    print(f"[motion.extension] [run_call] Annotator: {annotator}")

    writer.attach(list(render.values()))
    print(f"[motion.extension] [run_call] Writer attached")

    for k, v in render.items():
        annotator[k].attach(v)
    print(f"[motion.extension] [run_call] Annotator attached")

    try:
        print(f"[motion.extension] [run_call] Callback call")
        await call(
            articulation=articulation, joint=joint, link=link, annotator=annotator
        )
        print(f"[motion.extension] [run_call] Callback done")

    except Exception as e:
        print(f"[motion.extension] [run_call] [Exception]: {e}")
        traceback.print_exec()
    finally:
        with contextlib.suppress(Exception):
            for k, v in camera.items():
                annotator[k].detach(v)
        print(f"[motion.extension] [run_call] Camera annotator detached")
        with contextlib.suppress(Exception):
            writer.detach(list(camera.values()))
        print(f"[motion.extension] [run_call] Camera detached")


async def run_node(session: str, tick: bool):
    print(f"[motion.extension] [run_node] session={session} tick={tick}")

    # ZMQ DEALER (encapsulated by Interface)
    interface = Interface(tick=tick, sync=False)

    # Channel
    channel = Channel()
    await channel.start()
    print(f"[motion.extension] [run_node] channel start")

    # Wait for ROUTER to be ready (server has __PING__/__PONG__ built-in)
    # Send mode exactly once; runner requires it before first real payload
    await interface.ready(timeout=2.0, max=300)
    print(f"[motion.extension] [run_node] ready")

    try:
        await run_call(
            session,
            (
                functools.partial(
                    run_tick, session=session, interface=interface, channel=channel
                )
                if tick
                else functools.partial(
                    run_norm, session=session, interface=interface, channel=channel
                )
            ),
        )
    except Exception as e:
        print(f"[motion.extension] [run_node] [Exception]: {e}")
        traceback.print_exec()
    finally:
        print(f"[motion.extension] [run_node] channel close")
        await channel.close()
        print(f"[motion.extension] [run_node] close")
        await interface.close()


async def main():
    with open("/storage/node/node.json", "r") as f:
        meta = json.loads(f.read())
    print(f"[motion.extension] [main] meta={meta}")

    session, tick = meta["session"], meta["tick"]
    await run_node(session, tick)


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        print(f"[motion.extension] Startup [{ext_id}]")

        self.task = asyncio.create_task(main())

    def on_shutdown(self):
        print("[motion.extension] Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
