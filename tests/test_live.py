import asyncio
import json
import sys

import pytest


@pytest.mark.asyncio
async def test_live(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "src")

    file = tmp_path.joinpath("camera1.json")
    monkeypatch.setenv("RUNNER_LIVE", str(file))
    file.write_text(json.dumps({"camera1": {"width": 100, "height": 200}}))

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "server.live",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
    stdout = stdout.decode().strip()
    print(stdout, stderr)
    assert stdout.startswith("parallel --halt now,done=1 -j 0 :::")
    assert "'echo camera1 100 200'" in stdout

    file = tmp_path.joinpath("camera2.json")
    monkeypatch.setenv("RUNNER_LIVE", str(file))

    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "server.live",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    await asyncio.sleep(5)
    file.write_text(
        json.dumps(
            {
                "camera1": {"width": 100, "height": 200},
                "camera2": {"width": 300, "height": 400},
            }
        )
    )

    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15)
    stdout = stdout.decode().strip()
    print(stdout, stderr)
    assert stdout.startswith("parallel --halt now,done=1 -j 0 :::")
    assert "'echo camera1 100 200'" in stdout
    assert "'echo camera2 300 400'" in stdout
