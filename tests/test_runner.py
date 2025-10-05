import asyncio
import functools
import json
import select
import threading
import time

import pytest

import server.interface
import server.runner


@pytest.mark.parametrize(
    "tick, sync",
    [
        pytest.param(True, False, id="tick-async"),
        pytest.param(False, False, id="norm-async"),
        pytest.param(False, True, id="norm-sync"),
    ],
)
def test_runner(tmp_path, monkeypatch, tick, sync):
    sockfile = tmp_path / "runner.sock"
    monkeypatch.setenv("RUNNER_SOCK", str(sockfile))

    def run_server(n):
        with server.runner.context() as ctx:
            for _ in range(n):
                obj = ctx.data()
                msg = obj.get("msg", "")
                ctx.step({"echo": msg[:32].upper()})

    payload = {"msg": "hello"}
    n = 1 if tick else 2
    t = threading.Thread(target=(functools.partial(run_server, n)), daemon=True)
    t.start()

    if tick:

        async def client():
            interface = server.interface.Interface(tick=True, sync=False)
            await interface.ready(timeout=2.0, max=60)

            reply_b = await interface.tick(json.dumps(payload).encode("utf-8"))
            reply = json.loads(reply_b.decode("utf-8"))
            assert reply["echo"] == payload["msg"][:32].upper()

            await interface.close()

        asyncio.run(client())

    else:
        if sync:
            interface = server.interface.Interface(tick=False, sync=True)
            interface.ready(timeout=2.0, max=60)

            data = json.dumps(payload).encode("utf-8")

            # --- stdlib poll for a POSIX socket ---
            poller = select.poll()
            fd = interface._sock_.fileno()
            poller.register(fd, select.POLLOUT | select.POLLIN)

            def send_and_expect():
                # wait until writable (up to ~2s)
                deadline = time.monotonic() + 2.0
                while time.monotonic() < deadline:
                    ev = dict(poller.poll(50))  # ms
                    mask = ev.get(fd, 0)
                    if mask & select.POLLOUT:
                        break
                else:
                    assert False, "socket not writable before send"

                # best-effort send (your Interface drops if busy)
                interface.send(data)

                # wait for reply (up to ~3s)
                deadline = time.monotonic() + 3.0
                reply = None
                while time.monotonic() < deadline:
                    ev = dict(poller.poll(50))
                    mask = ev.get(fd, 0)
                    if mask & select.POLLIN:
                        step = interface.recv()  # bytes | None
                        if step is not None:
                            reply = json.loads(step.decode("utf-8"))
                            break
                assert reply is not None, "no reply within timeout"
                assert reply["echo"] == payload["msg"][:32].upper()

            send_and_expect()
            time.sleep(3.0)
            send_and_expect()

            interface.close()

        else:

            async def client():
                interface = server.interface.Interface(tick=False, sync=False)
                await interface.ready(timeout=2.0, max=60)

                data = json.dumps(payload).encode("utf-8")

                async def sender():
                    await asyncio.sleep(2.0)
                    await interface.send(data)
                    await asyncio.sleep(2.0)
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
