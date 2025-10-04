import asyncio
import functools
import json
import threading

import pytest

import server.interface
import server.runner


@pytest.mark.parametrize(
    "tick",
    [
        pytest.param(True, id="tick-async"),
        pytest.param(False, id="norm-async"),
    ],
)
def test_runner(tmp_path, monkeypatch, tick):
    # Point server to a temp socket file (no addr setting needed)
    sockfile = tmp_path / "runner.sock"
    monkeypatch.setenv("RUNNER_SOCK", str(sockfile))

    # Server: handle exactly one real request (dict in, dict out)
    def run_server(n):
        with server.runner.context() as ctx:
            for _ in range(n):
                obj = ctx.data()  # dict from JSON header
                msg = obj.get("msg", "")
                ctx.step({"echo": msg[:32].upper()})

    payload = {"msg": "hello"}

    # tick => 1 request, norm => 2
    n = 1 if tick else 2
    t = threading.Thread(target=(functools.partial(run_server, n)), daemon=True)
    t.start()

    if tick:

        async def client():
            interface = server.interface.Interface(tick=True)
            await interface.ready(timeout=2.0, max=60)

            reply_b = await interface.tick(json.dumps(payload).encode("utf-8"))
            reply = json.loads(reply_b.decode("utf-8"))
            assert reply["echo"] == payload["msg"][:32].upper()

            await interface.close()

        asyncio.run(client())

    else:

        async def client():
            interface = server.interface.Interface(tick=False)
            await interface.ready(timeout=2.0, max=60)

            data = json.dumps(payload).encode("utf-8")

            async def sender():
                # wait before first send so recv is already polling
                await asyncio.sleep(3.0)
                await interface.send(data)
                await asyncio.sleep(3.0)
                await interface.send(data)

            async def receiver():
                for _ in range(2):
                    reply_b = await interface.recv()
                    reply = json.loads(reply_b.decode("utf-8"))
                    assert reply["echo"] == payload["msg"][:32].upper()

            await asyncio.gather(receiver(), sender())
            await interface.close()

        asyncio.run(client())

    t.join(timeout=3)
    assert not sockfile.exists()
