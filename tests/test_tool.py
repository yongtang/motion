import asyncio
import contextlib
import sys

import pytest


@pytest.mark.parametrize(
    "mode, timeout, iteration, runner, entries",
    [
        pytest.param(
            "read",
            150.0,
            1,
            "echo",
            [
                "[Tool] Done",
                '[Producer] Iteration 0: {"op": "none"}',
            ],
            id="read",
        ),
        pytest.param(
            "sink",
            150.0,
            15,
            "echo",
            [
                "[Tool] Done",
                '[Consumer] Iteration 14: {"seq": 14}',
            ],
            id="sink",
        ),
        pytest.param(
            "tick",
            300.0,
            15,
            "echo",
            [
                "[Tool] Done",
                '[Consumer] Iteration 14: {"op": "none", "seq": 14}',
            ],
            id="tick",
        ),
    ],
)
@pytest.mark.asyncio
async def test_tool(
    docker_compose, monkeypatch, tmp_path, mode, timeout, iteration, runner, entries
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
            "--runner",
            str(runner),
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

    proc = await asyncio.create_subprocess_exec(
        *node,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    proc_stdout: list[str] = []
    proc_stderr: list[str] = []

    async def f_task(stream, sink):
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode(errors="replace")
            print(text, end="", flush=True)
            sink.append(text)

    task_stdout = asyncio.create_task(f_task(proc.stdout, proc_stdout))
    task_stderr = asyncio.create_task(f_task(proc.stderr, proc_stderr))

    try:
        await asyncio.wait_for(proc.wait(), timeout=300)
    finally:
        with contextlib.suppress(Exception):
            proc.kill()
        with contextlib.suppress(Exception):
            await asyncio.gather(task_stdout, task_stderr)

    text = "".join(proc_stdout + proc_stderr)

    assert proc.returncode == 0, text
    for entry in entries:
        assert entry in text, f"{entry} vs. {text}"
