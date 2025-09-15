import uuid

import httpx
import pytest

import motion


@pytest.mark.asyncio
async def test_session(session_on_server):
    base, session_id, scene = session_on_server

    # construct client model (fetch-on-init)
    async with motion.Session(base, session_id, timeout=5.0) as session:
        # start playback
        await session.play()

        # wait until playing via status waiter (no WS-based readiness)
        await session.wait("play", timeout=300.0)

        # tail: ensure we can get something quickly from the end
        async with session.stream(start=-1) as tail:
            tail_msg = await tail.data(timeout=30.0)
            assert tail_msg is not None

        # duplex stream: send one step, receive one data message
        async with session.stream(start=None) as stream:
            await stream.step({"k": "v", "i": 1})
            msg = await stream.data(timeout=30.0)
            assert msg is not None  # JSON or text/bytes, validated by client

        # stop playback
        await session.stop()

        # wait until fully stopped before finishing
        await session.wait("stop", timeout=60.0)

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(httpx.HTTPError):
        motion.Session(base, bogus, timeout=5.0)
