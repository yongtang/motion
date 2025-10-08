import asyncio
import json
import os
import pathlib


async def main():
    async def f_file(file):
        while not pathlib.Path(file).exists():
            await asyncio.sleep(1)

    file = os.environ.get("RUNNER_LIVE", "/run/motion/camera.json")
    await asyncio.wait_for(f_file(file), timeout=300)

    with open(file, "r") as f:
        camera = json.loads(f.read())

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
    vv = ["gst-launch-1.0"]
    for name, entry in camera.items():
        width, height = entry["width"], entry["height"]
        location = name.removeprefix("/").replace("/", "_")
        argv.append(
            f"gst-launch-1.0 -e rtspsrc location=rtsp://127.0.0.1:8554/RTSPWriter_{location}_rgb protocols=tcp latency=200 tcp-timeout=0 name=src src. ! application/x-rtp,media=video,encoding-name=H265 ! rtph265depay ! h265parse config-interval=-1 ! mp4mux faststart=true streamable=true ! filesink location=/{location}.mp4 2>&1"
        )
        vv.extend(
            [
                "-e",
                "rtspsrc",
                f"location=rtsp://127.0.0.1:8554/RTSPWriter_{location}_rgb",
                "protocols=tcp",
                "latency=200",
                "tcp-timeout=0",
                "name=src",
                "src.",
                "!",
                "application/x-rtp,media=video,encoding-name=H265",
                "!",
                "rtph265depay",
                "!",
                "h265parse",
                "config-interval=-1",
                "!",
                "mp4mux",
                "faststart=true",
                "streamable=true",
                "!",
                "filesink",
                f"location=/{location}.mp4",
            ]
        )
        break

    print("ARGS: ", vv)

    os.execvp(vv[0], vv)


if __name__ == "__main__":
    asyncio.run(main())
