import argparse
import asyncio
import contextlib
import itertools
import json
import logging
import pathlib

import motion

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

model_read = """
import sys
for _ in sys.stdin:
    pass
"""


async def f_producer(stream, proc, iteration, timeout):
    for i in range(iteration):
        log.info(f"[Producer] Iteration {i}")
        data = await stream.data(timeout=timeout)
        proc.stdin.write((json.dumps(data) + "\n").encode())
        await proc.stdin.drain()


async def f_consumer(proc, stream):
    for i in itertools.count():
        log.info(f"[Consumer] Iteration {i}")
        line = await proc.stdout.readline()
        if not line:
            log.info(f"[Consumer] EOF")
            return
        await stream.step(json.loads(line))


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

            model = model_read if args.mode == "read" else args.model
            proc = await asyncio.create_subprocess_exec(
                "python3",
                "-u",
                "-c",
                model,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            log.info(f"[Session {session.uuid}] Process model {model}")
            try:
                producer = asyncio.create_task(
                    f_producer(
                        stream,
                        proc,
                        args.iteration,
                        args.timeout,
                    )
                )
                consumer = asyncio.create_task(
                    f_consumer(
                        proc,
                        stream,
                    )
                )
                await producer
                log.info(f"[Session {session.uuid}] Producer done")

                proc.stdin.close()
                log.info(f"[Session {session.uuid}] Process close")

                await consumer
                log.info(f"[Session {session.uuid}] Consumer done")

            finally:
                log.info(f"[Session {session.uuid}] Process done")
                with contextlib.suppress(Exception):
                    if proc.stdin and not proc.stdin.is_closing():
                        proc.stdin.close()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=1.0)
                    except asyncio.TimeoutError:
                        proc.kill()
                        await proc.wait()

        log.info(f"[Session {session.uuid}] Stopping …")
        await session.stop()
        log.info(f"[Session {session.uuid}] Stopped")

        client.session.delete(session)

    client.scene.delete(scene)

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
