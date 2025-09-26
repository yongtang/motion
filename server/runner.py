import contextlib
import json
import os

import numpy as np
import zmq


@contextlib.contextmanager
def context():
    file = os.environ.get("RUNNER_SOCK", "/run/motion/runner.sock")
    with contextlib.suppress(FileNotFoundError):
        os.unlink(file)

    queue = 16
    context = zmq.Context()
    sock = context.socket(zmq.ROUTER)
    sock.setsockopt(zmq.SNDHWM, queue)
    sock.setsockopt(zmq.RCVHWM, queue)
    sock.setsockopt(zmq.LINGER, -1)
    sock.bind(f"ipc://{file}")

    class Context:
        def __init__(self):
            self.identity = None

        def data(self) -> dict:
            while True:
                identity, empty, *payload = sock.recv_multipart()
                assert (
                    empty == b""
                ), "protocol error: expected empty delimiter frame b''"

                if len(payload) == 1 and payload[0] == b"__PING__":
                    sock.send_multipart([identity, b"", b"__PONG__"])
                    continue

                self.identity = identity
                if not payload:
                    return {}

                header = payload[0]  # JSON bytes
                frame = payload[1:]  # list[bytes]

                o = json.loads(header.decode("utf-8"))

                camera = o.get("camera")
                if camera is not None:
                    for name, meta in list(camera.items()):
                        index = int(meta["frame"])
                        dtype = np.dtype(meta["dtype"])
                        shape = tuple(meta["shape"])
                        assert (
                            0 <= index < len(frame)
                        ), f"camera frame index out of range: {index}"
                        camera[name] = np.frombuffer(frame[index], dtype=dtype).reshape(
                            shape
                        )
                        with contextlib.suppress(Exception):
                            camera[name].setflags(write=False)

                return o

        def step(self, o):
            assert self.identity is not None, f"step() called before data()"
            sock.send_multipart([self.identity, b"", json.dumps(o).encode()])
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
