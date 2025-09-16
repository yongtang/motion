import asyncio
import json
import sys
import traceback

import omni.ext
import omni.kit
import omni.usd

from .channel import Channel
from .node import run_http


async def run_node():
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


async def main():
    try:
        print("[motion.extension] [HTTP] Starting on :8899")
        async with run_http(8899):
            print("[motion.extension] [Node] Running")
            await run_node()
            print("[motion.extension] [Node] Stopped")
        print("[motion.extension] [HTTP] Closed")
    except Exception as e:
        print(f"[motion.extension] [Exception]: {e}")
        traceback.print_exec()


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
