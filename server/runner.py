import contextlib
import json
import os
import socket


@contextlib.contextmanager
def context():
    file = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
    with contextlib.suppress(FileNotFoundError):
        os.unlink(file)

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    # keep buffers modest so backpressure is visible quickly
    with contextlib.suppress(Exception):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 64 * 1024)
    with contextlib.suppress(Exception):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024)

    sock.bind(file)
    sock.listen(1)

    conn = None
    try:
        conn, _ = sock.accept()

        class Context:
            def __init__(self):
                pass  # no identity/mode needed with a single connection

            def data(self) -> dict:
                # Blocking read of one packet (a JSON bytes payload)
                buf = conn.recv(1024 * 1024)  # 1 MiB cap for control
                if not buf:
                    return {}
                return json.loads(buf.decode("utf-8"))

            def step(self, o):
                # Always reply (blocking). Client-side policy handles congestion.
                payload = json.dumps(o).encode("utf-8")
                conn.sendall(payload)

        yield Context()

    finally:
        with contextlib.suppress(Exception):
            if conn is not None:
                conn.close()
        with contextlib.suppress(Exception):
            sock.close()
        with contextlib.suppress(FileNotFoundError):
            os.unlink(file)
