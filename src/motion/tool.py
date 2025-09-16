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

model_sink = r"""
import sys, json, asyncio, itertools


async def main():
    # Block until one line is read
    sys.stdin.readline()

    # Background task to consume and discard the rest forever
    async def drain():
        for _ in sys.stdin:
            pass

    asyncio.create_task(asyncio.to_thread(drain))

    # Start producing after the first line
    for i in range(15):
        print(json.dumps({"seq": i}), flush=True)
        await asyncio.sleep(1.0)


asyncio.run(main())
"""


async def f_producer(stream, proc, iteration, timeout):
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout

    for i in range(iteration):

        # compute remaining total budget
        if deadline is not None:
            remaining = deadline - loop.time()
            if remaining <= 0:
                log.info("[Producer] Total timeout reached; stopping producer")
                break
        else:
            remaining = None  # unlimited

        data = await stream.data(timeout=remaining)
        log.info(f"[Producer] Iteration {i}: {json.dumps(data)}")
        proc.stdin.write((json.dumps(data) + "\n").encode())
        await proc.stdin.drain()


async def f_consumer(proc, stream):
    for i in itertools.count():
        line = await proc.stdout.readline()
        if not line:
            log.info(f"[Consumer] EOF")
            return
        log.info(f"[Consumer] Iteration {i}: {line.rstrip().decode()}")
        await stream.step(json.loads(line))


async def f_observer(proc):
    while True:
        line = await proc.stderr.readline()
        if not line:
            log.info(f"[Observer] EOF")
            return
        log.info(f"[Observer] {line.rstrip().decode()}")


async def main():
    parser = argparse.ArgumentParser(prog="tool", description="Run motion stream")

    mode = parser.add_subparsers(dest="mode", required=True)

    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_parser.add_argument("--file", help="path to USD file", required=True)
    mode_parser.add_argument("--base", default="http://127.0.0.1:8080")
    mode_parser.add_argument("--timeout", type=float, default=None)
    mode_parser.add_argument("--iteration", type=int, default=15)
    mode_parser.add_argument("--runner", default="isaac")

    read_parser = mode.add_parser("read", parents=[mode_parser], help="read data only")
    sink_parser = mode.add_parser("sink", parents=[mode_parser], help="sink step only")

    # example: 'import sys,json;[sys.stdout.write(json.dumps(dict(json.loads(l),seq=i))+"\n") for i,l in enumerate(sys.stdin) if l.strip()]'
    tick_parser = mode.add_parser(
        "tick", parents=[mode_parser], help="read data and transform to step"
    )
    tick_parser.add_argument(
        "--model",
        required=True,
        help="python one-liner with stdin json and stdout json",
    )

    args = parser.parse_args()

    client = motion.client(args.base)

    log.info(f"[Scene] Creating from {args.file} (runner={args.runner!r}) ...")
    scene = client.scene.create(pathlib.Path(args.file), args.runner)
    log.info(f"[Scene {scene.uuid}] Created")

    log.info("[Session] Creating...")
    async with client.session.create(scene) as session:
        log.info(f"[Session {session.uuid}] Starting playback...")
        await session.play()

        log.info(f"[Session {session.uuid}] Waiting for play ...")
        await session.wait("play", timeout=300.0)
        log.info(f"[Session {session.uuid}] Playing (status-confirmed)")

        async with session.stream(start=-1) as stream:
            match args.mode:
                case "tick":
                    model = args.model
                case "read":
                    model = model_read
                case "sink":
                    model = model_sink
                case _:
                    raise ValueError(f"Unknown mode {args.mode!r}")
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
            observer = asyncio.create_task(f_observer(proc))
            producer = asyncio.create_task(
                f_producer(stream, proc, args.iteration, args.timeout)
            )
            consumer = asyncio.create_task(f_consumer(proc, stream))

            await producer
            log.info(f"[Session {session.uuid}] Producer done")

            proc.stdin.close()
            log.info(f"[Session {session.uuid}] Process close")

            await consumer
            log.info(f"[Session {session.uuid}] Consumer done")

            await observer
            log.info(f"[Session {session.uuid}] Observer done")

            with contextlib.suppress(Exception):
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
            log.info(f"[Session {session.uuid}] Process done")

        log.info(f"[Session {session.uuid}] Stopping ...")
        await session.stop()

        log.info(f"[Session {session.uuid}] Waiting for stop ...")
        await session.wait("stop", timeout=60.0)
        log.info(f"[Session {session.uuid}] Stopped (status-confirmed)")

        client.session.delete(session)

    client.scene.delete(scene)

    log.info("[Tool] Done")


if __name__ == "__main__":
    asyncio.run(main())
