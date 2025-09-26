import json
import threading
import time

import pytest
import zmq

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

    # Client
    addr = f"ipc://{sockfile}"
    zctx = zmq.Context.instance()
    sock = zctx.socket(zmq.DEALER)
    sock.connect(addr)

    def recv(timeout_ms=2000):
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        ev = dict(poller.poll(timeout_ms))
        if sock not in ev:
            pytest.fail("timeout waiting for server reply")
        return sock.recv_multipart()

    # Handshake: Ping/Pong until ready
    for _ in range(30):
        sock.send_multipart([b"", b"__PING__"])
        try:
            _, pong = recv(500)
            if pong == b"__PONG__":
                break
        except Exception:
            time.sleep(1)
    else:
        pytest.fail("server not ready for PING/PONG")

    # Send mode once (mandatory before payload)
    sock.send_multipart([b"", b"__MODE__", b"TICK"])

    # Real roundtrip (JSON header only)
    payload = {"msg": "hello"}
    sock.send_multipart([b"", json.dumps(payload).encode("utf-8")])

    _, reply_b = recv()
    reply = json.loads(reply_b.decode("utf-8"))
    assert reply["echo"] == payload["msg"][:32].upper()

    sock.close(0)
    zctx.term()
    t.join(timeout=3)
    assert not sockfile.exists()  # server cleaned up
