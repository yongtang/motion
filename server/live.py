import json
import shlex
import pathlib

import asyncio
import os


async def main():
    async def f_file(file):
        while not pathlib.Path(file).exists():
            await asyncio.sleep(1)

    file = os.environ.get("RUNNER_LIVE", "/run/motion/camera.json")
    await asyncio.wait_for(f_file(file), timeout=300)

    with open(file, "r") as f:
        camera = json.loads(f.read())

    live = ["parallel --halt now,done=1 -j 0 :::"]
    for name, entry in camera.items():
        width, height = entry["width"], entry["height"]
        live.append(shlex.quote(f"echo {name} {width} {height}"))

    print(" ".join(live))


if __name__ == "__main__":
    asyncio.run(main())
