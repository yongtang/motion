import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "mode, timeout, iteration, runner",
    [
        pytest.param("read", 150.0, 1, "echo", id="read"),
        pytest.param("sink", 150.0, 15, "echo", id="sink"),
        pytest.param("tick", 300.0, 15, "echo", id="tick"),
    ],
)
def test_tool(docker_compose, monkeypatch, tmp_path, mode, timeout, iteration, runner):
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
    proc = subprocess.run(
        node,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
        text=True,
        timeout=300,
    )
    print(f"\n==== {' '.join(node)} ====\n{proc.stdout}\n==== {' '.join(node)} ====")

    assert proc.returncode == 0, proc.stdout
    assert "[Tool] Done" in proc.stdout
