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
    parser.add_argument("--duration", type=float, default=150.0)
    args = parser.parse_args()

    client = motion_client(args.base)

    log.info(f"[Scene] Creating from {args.file} (runtime={args.runtime!r}) …")
    scene = client.scene.create(pathlib.Path(args.file), args.runtime)
    log.info(f"[Scene {scene.uuid}] Created")

    log.info("[Session] Creating …")
    session = client.session.create(scene)
    log.info(f"[Session {session.uuid}] Created")

    log.info(f"[Session {session.uuid}] Starting playback …")
    session.play()
    log.info(f"[Session {session.uuid}] Playing for {args.duration:.1f}s")
    await asyncio.sleep(args.duration)

    log.info(f"[Session {session.uuid}] Stopping …")
    session.stop()
    log.info(f"[Session {session.uuid}] Stopped")

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
