import asyncio
import logging
import signal
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import nats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("work")


async def session_play(session: str):
    logger.info(f"Received play session: {session}")


async def session_stop(session: str):
    logger.info(f"Received stop session: {session}")


class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            logger.debug("Health check request")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
        else:
            logger.warning("Invalid health path requested: %s", self.path)
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_):
        # silence default HTTP server logs, use logger instead
        return


async def run(stop_event: asyncio.Event):
    logger.info("Connecting to NATS at nats://127.0.0.1:4222")
    nc = await nats.connect("nats://127.0.0.1:4222")

    async def cb(msg):
        try:
            call, session = msg.data.decode().split(" ", 1)
            if call == "play":
                await session_play(session)
            elif call == "stop":
                await session_stop(session)
            else:
                assert False, f"{call} {session}"
        except Exception as e:
            logger.exception(f"Error handling message {msg}: {e}")

    await nc.subscribe("motion.session", cb=cb)
    logger.info("Subscribed to NATS topics: motion.session")

    try:
        await stop_event.wait()
    finally:
        logger.info("Shutting down NATS connection...")
        await nc.drain()
        logger.info("NATS connection closed")


async def main():
    httpd = HTTPServer(("0.0.0.0", 9999), Health)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    logger.info("HTTP health server started on port 9999")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            logger.debug("Signal handlers not implemented on this platform")

    try:
        await run(stop_event)
    finally:
        logger.info("Shutting down HTTP server...")
        httpd.shutdown()
        httpd.server_close()
        logger.info("HTTP server stopped")


if __name__ == "__main__":
    asyncio.run(main())
