import json
import uuid

import httpx
import pytest

import motion


@pytest.mark.parametrize("model", ["bounce", "remote"])
@pytest.mark.asyncio
async def test_session(scene_on_server, model):
    base, scene = scene_on_server  # scene is already a motion.Scene
    client = motion.client(base=base, timeout=5.0)

    async with client.session.create(scene=scene) as session:
        # typed object sanity
        assert isinstance(session, motion.Session)
        assert session.scene.uuid == scene.uuid
        assert session.joint == ["*"]
        assert isinstance(session.camera, dict)
        assert session.link == ["*"]

        # start -> wait until playing
        await session.play(
            model=model
        )  # if play(model=...) not merged yet, call await session.play()
        await session.wait("play", timeout=300.0)

        # duplex stream: receive one message, then send one step
        async with session.stream(start=1) as stream:
            msg = await stream.data(timeout=30.0)
            assert msg is not None
            # best-effort: if bytes, try to JSON-decode like the client does
            if isinstance(msg, (bytes, bytearray)):
                try:
                    msg = json.loads(msg)
                except Exception:
                    pass
            await stream.step({"joint": {"j0": 1.0}})

        # stop -> wait until stopped
        await session.stop()
        await session.wait("stop", timeout=60.0)

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(httpx.HTTPError):
        motion.Session(base, bogus, timeout=5.0)
