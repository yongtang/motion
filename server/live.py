import asyncio
import json
import logging
import os
import pathlib

import aiohttp

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def main():
    async def f_file(file):
        while not pathlib.Path(file).exists():
            log.info(f"File {file} not available. Retrying in 1 seconds...")
            await asyncio.sleep(1)

    async def f_live(data):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(
                        "http://127.0.0.1:9997/v3/paths/list"
                    ) as response:
                        response.raise_for_status()
                        live = await response.json()
                        log.info(f"Live: {data} vs. {live}")
                        entries = list(item["name"] for item in live["items"])
                        if all(entry in entries for entry in data.keys()):
                            return
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    log.exception(f"Request failed: {e}. Retrying in 1 seconds...")
                    await asyncio.sleep(1)

    file = os.environ.get("RUNNER_LIVE", "/run/motion/camera.json")
    await asyncio.wait_for(f_file(file), timeout=300)

    with open(file, "r") as f:
        data = json.loads(f.read())
        data = {
            "RTSPWriter_{}_rgb".format(n.removeprefix("/").replace("/", "_")): e
            for n, e in data.items()
        }

    await asyncio.wait_for(f_live(data), timeout=300)

    argv = [
        "parallel",
        "--line-buffer",
        "--tag",
        "--halt",
        "now,done=1",
        "--halt",
        "now,fail=1",
        "-j",
        "0",
        ":::",
    ]
    for name, entry in data.items():
        width, height = entry["width"], entry["height"]
        argv.append(
            f'gst-launch-1.0 -e rtspsrc location="rtsp://127.0.0.1:8554/{name}" protocols=tcp latency=200 tcp-timeout=0 name=src  src. ! application/x-rtp,media=video,encoding-name=H265 !  rtph265depay ! h265parse config-interval=-1 !  splitmuxsink location="/{name}-%05d.mp4"  max-size-time=$((5*60*1000000000))  muxer=mp4mux 2>&1'
        )
        # gst-launch-1.0 -e rtspsrc location="rtsp://127.0.0.1:8554/RTSPWriter_World_Scene_CameraA_rgb" protocols=tcp latency=200 tcp-timeout=0 name=src  src. ! application/x-rtp,media=video,encoding-name=H265 !  rtph265depay ! h265parse config-interval=-1 !  mp4mux faststart=true streamable=true !  filesink location=/file.mp4

    log.info(f"Process: {' '.join(argv)}")
    os.execvp(argv[0], argv)


if __name__ == "__main__":
    asyncio.run(main())
