import asyncio
import contextlib
import os
import sys

import pytest


@pytest.mark.parametrize(
    "mode, duration, image, device, entries",
    [
        pytest.param(
            "live",
            15,
            "count",
            "cpu",
            [],
            id="live",
        ),
    ],
)
@pytest.mark.asyncio
async def test_tool(
    docker_compose,
    monkeypatch,
    tmp_path,
    mode,
    duration,
    image,
    device,
    entries,
):
    monkeypatch.setenv("PYTHONPATH", "src")

    base = f"http://{docker_compose['motion']}:8080"

    file = tmp_path.joinpath("scene.usd")
    file.write_text("# usd")

    node = [
        sys.executable,
        "-m",
        "motion.tool",
        str(mode),
        "--file",
        str(file),
        "--base",
        str(base),
    ] + [
        "--duration",
        str(duration),
        "--image",
        str(image),
        "--device",
        str(device),
    ]
    print(f"\n==== {' '.join(node)} ====")

    proc = await asyncio.create_subprocess_exec(
        *node,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={**os.environ},
    )

    proc_stdout: list[str] = []
    proc_stderr: list[str] = []

    tasks = []

    async def f_task(stream, sink):
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode(errors="replace")
            print(text, end="", flush=True)
            sink.append(text)

    tasks.append(asyncio.create_task(f_task(proc.stdout, proc_stdout)))
    tasks.append(asyncio.create_task(f_task(proc.stderr, proc_stderr)))

    try:
        await asyncio.wait_for(proc.wait(), timeout=300)
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        with contextlib.suppress(Exception):
            proc.kill()
    text = "".join(proc_stdout + proc_stderr)

    assert proc.returncode == 0, text
    assert "[Tool] Done" in text, text
    for entry in entries:
        assert entry in text, f"{entry} vs. {text}"
