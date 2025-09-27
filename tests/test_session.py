import uuid

import httpx
import pytest

import motion


@pytest.mark.parametrize(
    "model",
    [
        pytest.param(
            "bounce",
            id="bounce",
        ),
        pytest.param(
            "remote",
            id="remote",
        ),
    ],
)
@pytest.mark.asyncio
async def test_session(scene_on_server, model):
    base, scene = scene_on_server

    # fetch runner from server
    r = httpx.get(f"{base}/scene/{scene}", timeout=5.0)
    assert r.status_code == 200, r.text
    runner = motion.scene.SceneRunnerSpec.parse_obj(r.json()["runner"])

    client = motion.client(base=base, timeout=5.0)

    # Build a Scene with uuid + runner (constructor requires runner)
    scene_obj = motion.Scene(base, scene, runner, timeout=5.0)

    # construct client model (fetch-on-init)
    async with client.session.create(scene_obj) as session:
        # start playback
        await session.play()

        # wait until playing via status waiter (no WS-based readiness)
        await session.wait("play", timeout=300.0)

        # duplex stream: receive one data message, send one step
        async with session.stream(start=1) as stream:
            msg = await stream.data(timeout=30.0)
            assert msg is not None  # JSON or text/bytes, validated by client
            await stream.step({"k": "v", "i": 1})

        # stop playback
        await session.stop()

        # wait until fully stopped before finishing
        await session.wait("stop", timeout=60.0)

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(httpx.HTTPError):
        motion.Session(base, bogus, timeout=5.0)
