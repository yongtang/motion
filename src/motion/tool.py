import argparse
import asyncio
import logging
import pathlib

import motion

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def main():
    parser = argparse.ArgumentParser(prog="tool", description="Run motion stream")

    mode = parser.add_subparsers(dest="mode", required=True)

    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_parser.add_argument("--file", help="path to USD file")
    mode_parser.add_argument("--base", default="http://127.0.0.1:8080")
    mode_parser.add_argument("--timeout", type=float, default=None)
    mode_parser.add_argument("--iteration", type=int, default=15)
    mode_parser.add_argument("--runner", default="isaac")

    read_parser = mode.add_parser("read", parents=[mode_parser], help="read data only")

    # example: 'import sys,json;[sys.stdout.write(json.dumps(dict(json.loads(l),seq=i))+"\n") for i,l in enumerate(sys.stdin) if l.strip()]'
    tick_parser = mode.add_parser(
        "tick", parents=[mode_parser], help="read data and transform to step"
    )
    tick_parser.add_argument(
        "--model",
        required=True,
        help="""python one-liner with stdin json and stdout json""",
    )

    args = parser.parse_args()

    client = motion.client(args.base)

    log.info(f"[Scene] Creating from {args.file} (runner={args.runner!r}) …")
    scene = client.scene.create(pathlib.Path(args.file), args.runner)
    log.info(f"[Scene {scene.uuid}] Created")

    log.info("[Session] Creating...")
    async with client.session.create(scene) as session:
        log.info(f"[Session {session.uuid}] Starting playback...")
        await session.play()
        log.info(f"[Session {session.uuid}] Playing (stream-driven)")

        async with session.stream(start=-1) as stream:
            for seq in range(args.iteration):
                data = await stream.data(timeout=args.timeout)
                match args.mode:
                    case "tick":
                        data["seq"] = seq
                        await stream.step(data)
                        log.info(f"[Session {session.uuid}] Sent step with seq={seq}")
                    case "read":
                        log.info(f"[Session {session.uuid}] Read frame {seq} (no step)")

        log.info(f"[Session {session.uuid}] Stopping …")
        await session.stop()
        log.info(f"[Session {session.uuid}] Stopped")

        client.session.delete(session)

    client.scene.delete(scene)

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
