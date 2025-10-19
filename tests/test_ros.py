import asyncio
import contextlib
import json
import zipfile

import pytest

import motion


async def f_bounce_joint(session):
    return


async def f_remote_joint(session):
    async with session.stream(start=1) as stream:
        for i in range(150):
            await stream.step(
                {
                    "joint": {
                        "panda_finger_joint1": 0.1,
                        "panda_finger_joint2": 0.1,
                        "panda_joint1": 0.1,
                        "panda_joint2": 0.1,
                        "panda_joint3": 0.1,
                        "panda_joint4": 0.1,
                        "panda_joint5": 0.1,
                        "panda_joint6": 0.1,
                        "panda_joint7": 0.1,
                    }
                }
            )
            await asyncio.sleep(2)


async def f_remote_twist(session):
    async with session.stream(start=1) as stream:
        for i in range(150):
            await stream.step(
                {
                    "twist": {
                        "panda_hand": {
                            "linear": {
                                "x": 0.1,
                                "y": 0.1,
                                "z": 0.1,
                            },
                            "angular": {
                                "x": 0.1,
                                "y": 0.1,
                                "z": 0.1,
                            },
                        },
                    }
                }
            )
            await asyncio.sleep(2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model, call",
    [
        pytest.param("bounce", f_bounce_joint, id="bounce-joint"),
        pytest.param("remote", f_remote_joint, id="remote-joint"),
        pytest.param("remote", f_remote_twist, id="remote-twist"),
    ],
)
@pytest.mark.parametrize("runner", ["ros"])
async def test_ros(scope, docker_compose, runner, model, call, tmp_path):
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
            "/panda_arm_controller/joint_trajectory",
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
        task = asyncio.create_task(call(session))

        stdout = []
        for i in range(150):  # retry up to 120 times
            line = await f()
            if line.strip():
                stdout.append(line)
                print(line)
            if "panda_joint1" in line:
                break
            await asyncio.sleep(2)
        else:
            assert (
                False
            ), "Did not see 'panda_joint1' after {i} retries: {'\n'.join(stdout)}"
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError):
            await task
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
