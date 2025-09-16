import asyncio
import contextlib
import json
import traceback

import isaacsim.replicator.agent.core.data_generation.writers.rtsp  # pylint: disable=W0611
import omni.ext
import omni.kit
import omni.replicator.core
import omni.usd
import pxr

from .channel import Channel


async def run_node(session):
    channel = Channel()
    await channel.start()
    print("[run_node] Channel started")

    await channel.publish_data(session, json.dumps({"op": "none"}))
    print(f"[run_node] Sent none to {session}")

    async def f(msg):
        print(f"[callback] [Echo {session}] Step: {msg}")
        step = json.loads(msg.data)
        data = json.dumps(step)
        print(f"[callback] [Echo {session}] Step->data ({len(data)} bytes): {data}")

        await channel.publish_data(session, data)
        print(f"[callback] [Echo {session}] Published")

    subscribe = await channel.subscribe_step(session, f)
    print(f"[run_node] Subscribed for {session}")

    try:
        print("[run_node] Waiting for events")
        await asyncio.Future()
    finally:
        await subscribe.unsubscribe()
        print(f"[run_node] Unsubscribed for {session}")
        await channel.close()
        print("[run_node] Channel closed")


async def main():
    print("[motion.extension] Loading stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    session = metadata["uuid"]
    print(f"[motion.extension] Loaded session {session}")

    ctx = omni.usd.get_context()
    if ctx.get_stage():
        print("[motion.extension] Closing existing stage...")
        await ctx.close_stage_async()
        print("[motion.extension] Existing stage closed")

    def f_event(event, e):
        print(f"[motion.extension] Stage event {omni.usd.StageEventType(e.type)}")
        if omni.usd.StageEventType(e.type) == omni.usd.StageEventType.OPENED:
            print("[motion.extension] Stage opened")
            event.set()

    print("[motion.extension] Opening stage...")
    await ctx.open_stage_async(
        "file:///storage/node/scene/scene.usd",
        load_set=omni.usd.UsdContextInitialLoadSet.LOAD_ALL,
    )

    print("[motion.extension] Waiting stage...")
    stage = ctx.get_stage()
    while stage is None:
        print("[motion.extension] Waiting loading...")
        await omni.kit.app.get_app().next_update_async()
        stage = ctx.get_stage()
    assert stage

    print(f"[motion.extension] Stage loaded")

    camera = (
        {
            str(pxr.UsdGeom.Camera(e).GetPath()): camera["*"]
            for e in stage.Traverse()
            if e.IsA(pxr.UsdGeom.Camera) and e.IsActive()
        }
        if "*" in camera
        else camera
    )
    print(f"[motion.extension] Camera: {camera}")

    camera = {
        e: omni.replicator.core.create.render_product(e, (v["width"], v["height"]))
        for e, v in camera.items()
    }

    writer = omni.replicator.core.WriterRegistry.get("RTSPWriter")
    writer.initialize(
        rtsp_stream_url="rtsp://127.0.0.1:8554/RTSPWriter",
        rtsp_rgb=True,
    )
    writer.attach(list(camera.values()))
    print(f"[motion.extension] Camera attached")

    annotator = omni.replicator.core.AnnotatorRegistry.get_annotator("rgb")
    print(f"[motion.extension] Camera annotator attached")
    annotator.attach(list(camera.values()))

    try:
        print("[motion.extension] [Node] Running")
        await run_node(session)
        print("[motion.extension] [Node] Stopped")
    except Exception as e:
        print(f"[motion.extension] [Exception]: {e}")
        traceback.print_exec()
    finally:
        with contextlib.suppress(Exception):
            annotator.detach(list(camera.values()))
        print(f"[motion.extension] Camera annotator detached")
        with contextlib.suppress(Exception):
            writer.detach(list(camera.values()))
        print(f"[motion.extension] Camera detached")


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
