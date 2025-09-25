import contextlib
import os

import zmq


@contextlib.contextmanager
def context():
    file = os.environ.get("RUNNER_SOCK", "/tmp/runner.sock")
    with contextlib.suppress(FileNotFoundError):
        os.unlink(file)

    queue = 16
    context = zmq.Context()
    sock = context.socket(zmq.ROUTER)
    sock.setsockopt(zmq.SNDHWM, queue)
    sock.setsockopt(zmq.RCVHWM, queue)
    sock.setsockopt(zmq.LINGER, 500)
    sock.bind(f"ipc://{file}")

    class Context:
        def __init__(self):
            self.identity = None

        def data(self) -> bytes:
            while True:
                ident, empty, payload = sock.recv_multipart()
                if payload == b"__PING__":
                    sock.send_multipart([ident, b"", b"__PONG__"])
                    continue
                self.identity = ident
                return payload

        def step(self, reply: bytes):
            if self.identity is None:
                raise RuntimeError("step() called before data()")
            sock.send_multipart([self.identity, b"", reply])
            self.identity = None

    try:
        yield Context()
    finally:
        with contextlib.suppress(Exception):
            sock.close()
        with contextlib.suppress(Exception):
            context.term()
        with contextlib.suppress(FileNotFoundError):
            os.unlink(file)
