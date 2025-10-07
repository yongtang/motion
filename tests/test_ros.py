import asyncio
import json
import zipfile

import pytest

import motion


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", ["ros"])
@pytest.mark.parametrize("model", ["bounce"])
async def test_ros(scope, docker_compose, runner, model, tmp_path):
    async def f():
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "exec",
            "-i",
            f"{scope}-runner",
            "/ros_entrypoint.sh",
            "ros2",
            "topic",
            "echo",
            "--once",
            "/joint_trajectory",
            "--full-length",
            "--timeout",
            "3",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        return stdout.decode()

    base = f"http://{docker_compose['motion']}:8080"
    client = motion.client(base=base, timeout=5.0)

    usd_path = tmp_path.joinpath("scene.usd")
    usd_path.write_text("#usda 1.0\ndef X {\n}\n", encoding="utf-8")

    scene = client.scene.create(usd_path, runner)

    assert scene
    assert scene.runner == runner

    async with client.session.create(scene=scene) as session:
        # sanity on the typed object we got back
        assert isinstance(session, motion.Session)
        assert session.scene.uuid == scene.uuid
        assert session.joint == ["*"]
        assert isinstance(session.camera, dict)
        assert session.link == ["*"]

        # drive lifecycle via the async API
        await session.play(device="cpu", model=model, tick=False)
        stdout = []
        for i in range(120):  # retry up to 120 times
            stdout.append(await f())
            print("\n".join(stdout))
            if "panda_finger_joint1" in "\n".join(stdout):
                break
            await asyncio.sleep(2)
        else:
            assert (
                False
            ), "Did not see 'panda_finger_joint1' after {i} retries: {stdout}"
        await session.wait("play", timeout=300.0)

        await session.stop()
        await session.wait("stop", timeout=60.0)

        # archive and validate JSON
        out = tmp_path.joinpath(f"{session.uuid}.zip")
        client.session.archive(session, out)  # sync call is fine inside async block
        assert out.exists() and out.stat().st_size > 0

        with zipfile.ZipFile(out, "r") as z:
            names = set(z.namelist())
            assert "session.json" in names, "archive missing session.json"
            assert "data.json" in names, "archive missing data.json"
            content = z.read("data.json").decode("utf-8", errors="ignore")
            lines = [line for line in content.splitlines() if line.strip()]
            assert lines, "data.json empty"
            for line in lines:
                json.loads(line)  # each line parses

    # delete and verify search is empty after
    assert client.session.delete(session) is None
    assert client.session.search(str(session.uuid)) == []

    # delete via client API
    assert client.scene.delete(scene) is None
    assert client.scene.search(str(scene.uuid)) == []
