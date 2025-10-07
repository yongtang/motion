import asyncio
import contextlib
import functools
import itertools
import json
import logging
import traceback

import isaacsim.replicator.agent.core.data_generation.writers.rtsp  # pylint: disable=W0611
import omni.ext
import omni.kit
import omni.replicator.core
import omni.timeline
import omni.usd
import pxr

from .channel import Channel
from .interface import Interface


async def run_tick(session, interface, channel):
    assert False


async def run_norm(session, interface, channel):
    assert False


async def run_call(session, call):
    print(f"[motion.extension] [run_call] Loaded session {session}")

    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    print(f"[motion.extension] [run_call] Loaded metadata {metadata}")

    camera = metadata["camera"]
    print(f"[motion.extension] [run_call] Loaded camera {camera}")

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

    print(f"[motion.extension] [run_call] Timeline playing")
    omni.timeline.get_timeline_interface().play()
    print(f"[motion.extension] [run_call] Timeline in play")

    try:
        await asyncio.sleep(float("inf"))
        print("[motion.extension] [run_call] Running")
        await call()
        print("[motion.extension] [run_call] Stopped")
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
                functools.partial(run_tick, session, interface, channel)
                if tick
                else functools.partial(run_norm, session, interface, channel)
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
