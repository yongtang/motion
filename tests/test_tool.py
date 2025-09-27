import asyncio
import contextlib
import os
import sys

import pytest


@pytest.mark.parametrize(
    "mode, timeout, iteration, image, device, entries",
    [
        pytest.param(
            "read",
            150.0,
            1,
            "count",
            "cpu",
            [
                '[Producer] Iteration 0: {"op": "none"}',
            ],
            id="read",
        ),
        pytest.param(
            "sink",
            150.0,
            15,
            "count",
            "cpu",
            [
                '"key": "Q"',
            ],
            id="sink",
        ),
        pytest.param(
            "tick",
            300.0,
            15,
            "count",
            "cpu",
            [
                '[Consumer] Iteration 14: {"op": "none", "seq": 14}',
            ],
            id="tick",
        ),
    ],
)
@pytest.mark.asyncio
async def test_tool(
    docker_compose,
    monkeypatch,
    tmp_path,
    mode,
    timeout,
    iteration,
    image,
    device,
    entries,
):
    monkeypatch.setenv("PYTHONPATH", "src")

    base = f"http://{docker_compose['motion']}:8080"

    file = tmp_path.joinpath("scene.usd")
    file.write_text("# usd")

    node = (
        [
            sys.executable,
            "-m",
            "motion.tool",
            str(mode),
            "--file",
            str(file),
            "--base",
            str(base),
        ]
        + ([] if timeout is None else ["--timeout", str(timeout)])
        + [
            "--iteration",
            str(iteration),
            "--image",
            str(image),
            "--device",
            str(device),
            "--camera",
            "/world/camera_A:1024:768",
            "/world/camera_B:1280:720",
        ]
        + (
            [
                "--model",
                r'import sys,json;[print(json.dumps({**json.loads(l),"seq":i}),flush=True) for i,l in enumerate(sys.stdin) if l.strip()]',
            ]
            if mode == "tick"
            else []
        )
    )
    print(f"\n==== {' '.join(node)} ====")

    # Always create a pseudo-tty
    fd_man, fd_work = os.openpty()

    proc = await asyncio.create_subprocess_exec(
        *node,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ, "SINK_TTY_PATH": os.ttyname(fd_work)},
    )

    proc_stdout: list[str] = []
    proc_stderr: list[str] = []

    async def f_send(fd: int):
        await asyncio.sleep(0.02)
        os.write(fd, b"a")
        os.write(fd, b"b")
        os.write(fd, b"3")
        os.write(fd, b"Q")
        await asyncio.sleep(0.05)
        os.close(fd)

    tasks = []

    async def f_task(stream, sink):
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode(errors="replace")
            print(text, end="", flush=True)
            sink.append(text)
            # if sink mode and not already added teleop
            if (
                (mode == "sink")
                and ('[Sink] Ready: {"op": "none"}' in text)
                and (len(tasks) < 3)
            ):
                tasks.append(asyncio.create_task(f_send(fd_man)))

    tasks.append(asyncio.create_task(f_task(proc.stdout, proc_stdout)))
    tasks.append(asyncio.create_task(f_task(proc.stderr, proc_stderr)))

    try:
        await asyncio.wait_for(proc.wait(), timeout=300)
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        with contextlib.suppress(Exception):
            proc.kill()
        with contextlib.suppress(Exception):
            os.close(fd_man)
        with contextlib.suppress(Exception):
            os.close(fd_work)

    text = "".join(proc_stdout + proc_stderr)

    assert proc.returncode == 0, text
    assert "[Tool] Done" in text, text
    for entry in entries:
        assert entry in text, f"{entry} vs. {text}"
