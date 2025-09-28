import argparse
import asyncio
import logging
import pathlib

import motion

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def f_live(client, file, image, device, duration):
    log.info(f"[Scene] Creating from {file} (image={image}, device={device}) ...")
    scene = client.scene.create(pathlib.Path(file), image, device)
    log.info(f"[Scene {scene.uuid}] Created")

    log.info("[Session] Creating...")
    async with client.session.create(scene) as session:
        log.info(f"[Session {session.uuid}] Starting playback...")
        await session.play()

        log.info(f"[Session {session.uuid}] Waiting for play ...")
        await session.wait("play", timeout=300.0)
        log.info(f"[Session {session.uuid}] Playing (status-confirmed)")

        interval = 15
        for count in range(0, duration, interval):
            await asyncio.sleep(interval)
            log.info(f"[Session {session.uuid}] Elapsed {count}/{duration} ...")

        log.info(f"[Session {session.uuid}] Stopping ...")
        await session.stop()

        log.info(f"[Session {session.uuid}] Waiting for stop ...")
        await session.wait("stop", timeout=60.0)
        log.info(f"[Session {session.uuid}] Stopped (status-confirmed)")

        client.session.delete(session)

    client.scene.delete(scene)


async def main():
    parser = argparse.ArgumentParser()

    mode = parser.add_subparsers(dest="mode", required=True)

    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_parser.add_argument("--base", default="http://127.0.0.1:8080")

    live_parser = mode.add_parser("live", parents=[mode_parser])
    live_parser.add_argument("--duration", type=int, default=3600)
    live_parser.add_argument("--file", required=True)
    live_parser.add_argument("--image", default="count")
    live_parser.add_argument("--device", default="cpu")

    args = parser.parse_args()

    client = motion.client(args.base)

    match args.mode:
        case "live":
            await f_live(client, args.file, args.image, args.device, args.duration)
        case other:
            raise ValueError(f"Unsupported mode {other}")

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
