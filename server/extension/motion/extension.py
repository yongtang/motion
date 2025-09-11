import asyncio
import contextlib
import functools
import json
import sys

import omni.ext
import omni.kit
import omni.replicator.core
import omni.usd
import pxr

from .node import run_http, run_link, run_step


def f_rend(metadata, stage):
    camera = [
        pxr.UsdGeom.Camera(e)
        for e in stage.Traverse()
        if e.IsA(pxr.UsdGeom.Camera) and e.IsActive()
    ]
    print(f"[motion.extension] Camera available: {[str(e.GetPath()) for e in camera]}")

    import omni.kit.app, omni.replicator.core as rep

    em = omni.kit.app.get_app().get_extension_manager()
    em.set_extension_enabled_immediate("isaacsim.replicator.agent.core", True)
    em.set_extension_enabled_immediate(
        "isaacsim.replicator.agent.ui", True
    )  # harmless if headless
    import isaacsim.replicator.agent.core.data_generation.writers.rtsp

    omni.kit.app.get_app().update()
    print(f"[motion.extension] REGISTRY: {rep.WriterRegistry.get_writers().keys()}")

    camera = {
        "/World/Scene/CameraA": {
            "width": 1280,
            "height": 720,
        }
    }
    print(f"[motion.extension] Camera rend: {camera}")
    return {
        e: omni.replicator.core.create.render_product(e, (v["width"], v["height"]))
        for e, v in camera.items()
    }


@contextlib.asynccontextmanager
async def run_rend(rend):
    print("[motion.extension] rend start")
    annotator = None

    if rend:
        print("[motion.extension] rend writer")
        writer = rep.WriterRegistry.get("RTSPWriter")
        print("[motion.extension] rend writer {writer}")
        writer.initialize(
            rtsp_stream_url="rtsp://127.0.0.1:8554/RTSPWriter",
            rtsp_rgb=True,
        )
        print(f"[motion.extension] rend writer={writer} attach{rend.values()}")
        writer.attach(list(rend.values()))

        print("[motion.extension] rend annotator")
        annotator = rep.AnnotatorRegistry.get_annotator("rgb")
        print(f"[motion.extension] rend annotator={annotator} attach{rend.values()}")
        annotator.attach(list(rend.values()))

        print("[motion.extension] rend ready")

    try:
        yield annotator
    finally:
        if rend:
            print("[motion.extension] rend annotator detach")
            with contextlib.suppress(Exception):
                annotator.detach(list(rend.values()))
            print("[motion.extension] rend writer detach")
            with contextlib.suppress(Exception):
                writer.detach(list(rend.values()))
    print("[motion.extension] rend complete")


async def main():
    print("[motion.extension] Loading stage")
    with open("/storage/node/session.json", "r") as f:
        metadata = json.loads(f.read())
    print(f"[motion.extension] Loaded metadata: {metadata}")

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

    print("[motion.extension] Stage loaded")

    session = metadata["uuid"]
    async with run_http():
        async with run_link() as channel:
            async with run_rend(f_rend(metadata, stage)) as annotator:
                await asyncio.Event().wait()


class MotionExtension(omni.ext.IExt):
    def __init__(self):
        self.task = None
        super().__init__()

    def on_startup(self, ext_id):
        print(f"[motion.extension] Startup [{ext_id}]")

        self.task = omni.kit.async_engine.run_coroutine(main())

        def f_done(e: asyncio.Task):
            if e.exception() is not None:
                print(f"[motion.extension] Task failed: {e.exception()}")
                sys.exit(1)

        self.task.add_done_callback(f_done)

    def on_shutdown(self):
        print("[motion.extension] Shutdown")
        if self.task and not self.task.done():
            self.task.cancel()
