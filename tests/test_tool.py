import subprocess
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
def test_tool(
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
    try:
        proc = subprocess.run(
            node,
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired as e:
        proc = e

    print(
        f"\n==== {' '.join(node)} ===="
        + (f"\n{proc.stdout}" if proc.stdout else "")
        + (f"\n{proc.stderr}" if proc.stderr else "")
        + f"==== {' '.join(node)} ===="
    )
    assert not isinstance(proc, subprocess.TimeoutExpired), proc
    assert proc.returncode == 0, proc.stdout
    for entry in entries:
        assert entry in proc.stdout, f"{entry} vs. {proc.stdout} - {proc.stderr}"
