import asyncio
import contextlib
import datetime
import io
import json
import os
import traceback

import isaacsim.replicator.agent.core.data_generation.writers.rtsp  # pylint: disable=W0611
import omni.ext
import omni.kit
import omni.replicator.core
import omni.timeline
import omni.usd
import PIL.Image
import pxr
import numpy as np

from .channel import Channel
from .storage import storage_kv_set


async def run_node(session, annotator):
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

    os.makedirs("/tmp/image", exist_ok=True)

    def on_update(e):
        print(f"[motion.extension] Writer on_update")
        for k, v in annotator.items():
            data = v.get_data()
            print(f"[motion.extension] Writer on_update data - {k} - {getattr(data, 'shape', None)} - {getattr(data, 'dtype', None)}")

            # Ensure numpy array
            arr = np.asarray(data)

            # Basic validation
            if arr.ndim != 3 or arr.shape[2] not in (3, 4):
                print(f"Unexpected image shape {arr.shape}; expected (H, W, 3/4)")
                continue

            # Ensure uint8
            if arr.dtype != np.uint8:
                # If it’s float in [0,1] or [0,255], this keeps it safe
                if np.issubdtype(arr.dtype, np.floating):
                    arr = np.clip(arr * (255.0 if arr.max() <= 1.0 else 1.0), 0, 255).astype(np.uint8)
                else:
                    arr = np.clip(arr, 0, 255).astype(np.uint8)

            # Make memory C-contiguous to avoid stride/tile issues
            arr = np.ascontiguousarray(arr)

            # Channel order: many Omniverse/Isaac feeds are BGRA/BGR
            if arr.shape[2] == 4:
                # Treat input as BGRA → convert to RGBA
                arr = arr[..., [2, 1, 0, 3]]
                mode = "RGBA"
            else:  # 3 channels
                # Treat input as BGR → convert to RGB
                arr = arr[..., ::-1]
                mode = "RGB"

            H, W = arr.shape[:2]
            if H <= 0 or W <= 0:
                raise ValueError(f"Bad size: W={W}, H={H}")

            kk = k.replace("/", "_")
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{session}_{kk}_{ts}.png"
            path = os.path.join("/tmp/image", filename)

            # Construct via fromarray (avoids raw/stride pitfalls)
            Image.fromarray(arr, mode).save(path)
            print(f"[motion.extension] Saved {path} ({W}x{H}, {mode})")

            #storage_kv_set("image", f"{session}_{k}_{ts}", image_bytes)

    sub = (
        omni.kit.app.get_app()
        .get_update_event_stream()
        .create_subscription_to_pop(on_update)
    )

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
    print(f"[motion.extension] Loaded metadata {metadata}")

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

    camera = metadata["camera"]
    camera = (
        {
            str(pxr.UsdGeom.Camera(e).GetPrim().GetPath()): camera["*"]
            for e in stage.Traverse()
            if e.IsA(pxr.UsdGeom.Camera) and e.IsActive()
        }
        if "*" in camera
        else camera
    )
    print(f"[motion.extension] Camera: {camera}")

    # gst-launch-1.0 -e   rtspsrc location="rtsp://127.0.0.1:8554/RTSPWriter_World_Scene_CameraA_rgb" protocols=tcp latency=200 name=src     src. ! application/x-rtp,media=video,encoding-name=H265 !       rtph265depay ! h265parse config-interval=-1 !       mp4mux faststart=true streamable=true !       filesink location=out_hevc.mp4
    # gst-launch-1.0 -e \
    # rtspsrc location="rtsp://127.0.0.1:8554/RTSPWriter_World_Scene_CameraA_rgb" \
    #      protocols=tcp latency=200 retry=TRUE tcp-timeout=0 name=src \
    # src. ! application/x-rtp,media=video,encoding-name=H265 ! \
    #  rtph265depay ! h265parse config-interval=-1 ! \
    #  mp4mux faststart=true streamable=true ! \
    #  filesink location=out_hevc.mp4
    #
    # timeout in ms

    render = {
        e: omni.replicator.core.create.render_product(e, (v["width"], v["height"]))
        for e, v in camera.items()
    }
    print(f"[motion.extension] Render: {render}")

    writer = omni.replicator.core.WriterRegistry.get("RTSPWriter")
    writer.initialize(
        rtsp_stream_url="rtsp://127.0.0.1:8554/RTSPWriter",
        rtsp_rgb=True,
    )
    print(f"[motion.extension] Writer: {writer}")

    writer.attach(list(render.values()))
    print(f"[motion.extension] Writer attached")

    annotator = {
        e: omni.replicator.core.AnnotatorRegistry.get_annotator("rgb")
        for e, v in camera.items()
    }
    print(f"[motion.extension] Annotator: {annotator}")

    for k, v in render.items():
        annotator[k].attach(v)
    print(f"[motion.extension] Annotator attached")

    omni.timeline.get_timeline_interface().play()

    try:
        print("[motion.extension] [Node] Running")
        await run_node(session, annotator)
        print("[motion.extension] [Node] Stopped")
    except Exception as e:
        print(f"[motion.extension] [Exception]: {e}")
        traceback.print_exec()
    finally:
        with contextlib.suppress(Exception):
            for k, v in camera.items():
                annotator[k].detach(v)
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
