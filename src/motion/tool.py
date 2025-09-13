import argparse
import asyncio
import logging
import pathlib

from .client import client as motion_client

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(description="Run USD motion")
    parser.add_argument("file", help="Path to USD file")
    parser.add_argument("--base", default="http://127.0.0.1:8080")
    parser.add_argument("--runtime", default="isaac")
    args = parser.parse_args()

    client = motion_client(args.base)

    log.info(f"[Scene] Creating from {args.file} (runtime={args.runtime!r}) …")
    scene = client.scene.create(pathlib.Path(args.file), args.runtime)
    log.info(f"[Scene {scene.uuid}] Created")

    log.info("[Session] Creating...")
    async with client.session.create(scene) as session:
        log.info(f"[Session {session.uuid}] Starting playback...")
        await session.play()
        log.info(f"[Session {session.uuid}] Playing (stream-driven)")

        async with session.stream(start=-1) as stream:
            for seq in range(15):
                data = await stream.data(timeout=300.0)
                data["seq"] = seq
                await stream.step(data)
                log.info(f"[Session {session.uuid}] Sent step with seq={seq}")

        log.info(f"[Session {session.uuid}] Stopping …")
        await session.stop()
        log.info(f"[Session {session.uuid}] Stopped")

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
