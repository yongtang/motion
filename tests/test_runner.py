import asyncio
import json
import threading

import server.interface
import server.runner


def test_runner(tmp_path, monkeypatch):
    # Point server to a temp socket file (no addr setting needed)
    sockfile = tmp_path / "runner.sock"
    monkeypatch.setenv("RUNNER_SOCK", str(sockfile))

    # Server: handle exactly one real request (dict in, dict out)
    def run_server_once():
        with server.runner.context() as ctx:
            obj = ctx.data()  # dict from JSON header
            msg = obj.get("msg", "")
            ctx.step({"echo": msg[:32].upper()})

    t = threading.Thread(target=run_server_once, daemon=True)
    t.start()

    async def client():
        # Client via Interface (tick mode)
        interface = server.interface.Interface(tick=True)

        # Handshake: Ping/Pong until ready, then send mode once (inside ready())
        await interface.ready(timeout=2.0, max=60)

        # Real roundtrip (JSON header only)
        payload = {"msg": "hello"}
        reply_b = await interface.tick(json.dumps(payload).encode("utf-8"))
        reply = json.loads(reply_b.decode("utf-8"))
        assert reply["echo"] == payload["msg"][:32].upper()

        await interface.close()

    asyncio.run(client())

    t.join(timeout=3)
    assert not sockfile.exists()  # server cleaned up
