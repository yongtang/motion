import functools
import logging

import nats


class Channel:
    def __init__(self, servers: str = "nats://127.0.0.1:4222"):
        self.servers = servers
        self.nc: nats.NATS | None = None
        self.js: nats.js.JetStreamContext | None = None
        self.log = logging.getLogger("channel")

    async def start(self) -> None:
        self.log.info("Connecting to NATS at %s ...", self.servers)

        async def cb(event: str, *args, **kwargs):
            log.info("NATS %s: %s %s", event, args, kwargs)

        self.nc = await nats.connect(
            servers=self.servers,
            error_cb=functools.partial(cb, "error"),
            reconnected_cb=functools.partial(cb, "reconnected"),
            disconnected_cb=functools.partial(cb, "disconnected"),
            closed_cb=functools.partial(cb, "closed"),
        )
        self.js = self.nc.jetstream()
        await self.nc.flush()

        try:
            await self.js.add_stream(name="motion", subjects=["motion.session"])
            self.log.info(
                "Created/verified stream 'motion' (subjects: ['motion.session'])"
            )
        except nats.js.errors.APIError:
            pass

        self.log.info("Connected to NATS at %s", self.servers)

    async def close(self) -> None:
        if self.nc and not self.nc.is_closed:
            self.log.info("Draining NATS connection...")
            await self.nc.drain()
            self.log.info("NATS connection closed")
        self.nc = None
        self.js = None

    async def publish(self, payload: str) -> None:
        self.log.info("Publish motion.session: %s", payload)
        ack = await self.js.publish("motion.session", payload.encode())
        self.log.info("Ack %s %s", ack.stream, ack.seq)
