import threading

import pytest
import zmq

import server.runner


def test_runner(tmp_path, monkeypatch):
    # Point server to a temp socket file (no addr setting needed)
    sockfile = tmp_path / "runner.sock"
    monkeypatch.setenv("RUNNER_SOCK", str(sockfile))

    # Server: handle exactly one real request
    def run_server_once():
        with server.runner.context() as ctx:
            data = ctx.data()
            ctx.step(b"OK:" + data[:32].upper())

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

    # Ping/Pong (invisible to server loop)
    sock.send_multipart([b"", b"__PING__"])
    _, pong = recv()
    assert pong == b"__PONG__"

    # Real roundtrip
    payload = b"hello"
    sock.send_multipart([b"", payload])
    _, reply = recv()
    assert reply == b"OK:" + payload.upper()

    sock.close(0)
    zctx.term()
    t.join(timeout=3)
    assert not sockfile.exists()  # server cleaned up
