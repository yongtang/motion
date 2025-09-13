import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "mode, timeout, iteration, runtime",
    [
        pytest.param("read", 150.0, 1, "echo", id="read"),
        pytest.param("incr", 300.0, 15, "echo", id="indr"),
    ],
)
def test_tool(docker_compose, monkeypatch, tmp_path, mode, timeout, iteration, runtime):
    monkeypatch.setenv("PYTHONPATH", "src")

    base = f"http://{docker_compose['motion']}:8080"

    file = tmp_path.joinpath("scene.usd")
    file.write_text("# usd")

    node = (
        [
            sys.executable,
            "-m",
            "motion.tool",
            "--mode",
            str(mode),
        ]
        + ([] if timeout is None else ["--timeout", str(timeout)])
        + [
            "--iteration",
            str(iteration),
            "--base",
            str(base),
            "--runtime",
            str(runtime),
            str(file),
        ]
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
