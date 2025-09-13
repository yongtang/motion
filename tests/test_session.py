import asyncio
import json
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

        async def wait_for_play_ready(sess: motion.Session, timeout: float = 300.0):
            """
            Wait until the runtime signals readiness after play.
            IMPORTANT: subscribe from the beginning (start=1) so we don't miss the initial {"op":"none"}.
            Returns early once ready; retries until `timeout`.
            """
            loop = asyncio.get_running_loop()
            deadline = loop.time() + timeout
            last_err = None
            while loop.time() < deadline:
                try:
                    # Subscribe from the beginning to catch the very first readiness message.
                    async with sess.stream(start=1) as stream:
                        while loop.time() < deadline:
                            remaining = max(0.1, deadline - loop.time())
                            try:
                                msg = await stream.data(timeout=min(1.5, remaining))
                            except Exception as e:
                                last_err = e
                                break  # reconnect by reopening the stream
                            if not msg:
                                continue
                            # Try to parse JSON and check for {"op":"none"}; otherwise any data implies readiness.
                            try:
                                if isinstance(msg, (bytes, bytearray)):
                                    data = json.loads(
                                        msg.decode("utf-8", errors="ignore")
                                    )
                                elif isinstance(msg, str):
                                    data = json.loads(msg)
                                else:
                                    data = msg
                            except Exception:
                                return True
                            if isinstance(data, dict) and data.get("op") == "none":
                                return True
                            # Any other well-formed data also indicates the stream is live.
                            return True
                except Exception as e:
                    last_err = e
                    await asyncio.sleep(0.5)
            raise AssertionError(
                f"Timed out waiting for play readiness via WS. Last error={last_err!r}"
            )

        # actively wait for readiness (from the beginning) instead of a fixed sleep
        await wait_for_play_ready(session, timeout=300.0)

        # ADDITIONAL CHECK: tail the latest message (start=-1) and ensure we get something quickly.
        # Note: if the stream were empty, -1 behaves like NEW and would wait for the first future message.
        async with session.stream(start=-1) as tail:
            tail_msg = await tail.data(timeout=30.0)
            assert tail_msg is not None

        # open duplex stream, send one step, receive one data message
        async with session.stream(start=None) as stream:
            await stream.step({"k": "v", "i": 1})
            msg = await stream.data(timeout=30.0)
            assert msg is not None  # JSON or text/bytes, validated by client

        # stop playback
        await session.stop()

    # negative: constructing with a bogus session should raise
    bogus = str(uuid.uuid4())
    with pytest.raises(httpx.HTTPError):
        motion.Session(base, bogus, timeout=5.0)
