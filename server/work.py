import asyncio
import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import nats

from .task import session_play, session_stop


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        return


async def run(stop_event: asyncio.Event):
    nc = await nats.connect("nats://127.0.0.1:4222")

    async def on_play(msg):
        try:
            data = json.loads(msg.data.decode())
            await session_play(data["session"])
        except Exception:
            return

    async def on_stop(msg):
        try:
            data = json.loads(msg.data.decode())
            await session_stop(data["session"])
        except Exception:
            return

    await nc.subscribe("motion.session.play", cb=on_play)
    await nc.subscribe("motion.session.stop", cb=on_stop)

    try:
        await stop_event.wait()
    finally:
        await nc.drain()


async def main():
    httpd = HTTPServer(("0.0.0.0", 9999), Health)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await run(stop_event)
    finally:
        httpd.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    asyncio.run(main())
