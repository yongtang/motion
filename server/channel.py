import functools
import logging

import nats


class NodeChannel:
    def __init__(self, servers: str = "nats://127.0.0.1:4222"):
        self.servers = servers
        self.nc: nats.NATS | None = None
        self.js: nats.js.JetStreamContext | None = None
        self.log = logging.getLogger("channel")

    async def start(self) -> None:
        assert self.nc is None and self.js is None, "NodeChannel already started"
        self.log.info(f"Connecting to NATS at {self.servers} ...")

        async def cb(event: str, *args, **kwargs):
            self.log.info(f"NATS {event}: args={args} kwargs={kwargs}")

        self.nc = await nats.connect(
            servers=self.servers,
            error_cb=functools.partial(cb, "error"),
            reconnected_cb=functools.partial(cb, "reconnected"),
            disconnected_cb=functools.partial(cb, "disconnected"),
            closed_cb=functools.partial(cb, "closed"),
        )
        self.js = self.nc.jetstream()
        await self.nc.flush()

        # Single stream for node-scoped commands (play/stop on separate subjects)
        config = nats.js.api.StreamConfig(
            name="motion",
            subjects=["motion.node.*.play", "motion.node.*.stop"],
        )
        try:
            await self.js.add_stream(config=config)
            self.log.info(f"Created stream 'motion' with subjects {config.subjects}")
        except nats.js.errors.APIError:
            await self.js.update_stream(config=config)
            self.log.info(f"Updated stream 'motion' with subjects {config.subjects}")

        self.log.info(f"Connected to NATS at {self.servers}")

    async def close(self) -> None:
        if self.nc and not self.nc.is_closed:
            self.log.info("Draining NATS connection...")
            await self.nc.drain()
            self.log.info("NATS connection closed")
        self.nc = None
        self.js = None

    async def publish(self, node: str, session: str, op: str) -> None:
        assert self.js is not None, "NodeChannel not started"
        assert op in ("play", "stop"), f"Unsupported op: {op}"
        subject = f"motion.node.{node}.{op}"
        payload = f"{session}"
        self.log.info(f"Publish {subject}: {payload}")
        ack = await self.js.publish(subject, payload.encode())
        self.log.info(f"Ack {ack.stream} {ack.seq}")

    async def subscribe(self, node: str, op: str):
        assert self.js is not None, "NodeChannel not started"
        assert op in ("play", "stop"), f"Unsupported op: {op}"
        subject = f"motion.node.{node}.{op}"
        durable = f"motion-node-{node}-{op}"
        config = nats.js.api.ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            ack_policy=nats.js.api.AckPolicy.EXPLICIT,
            max_ack_pending=1,
        )
        try:
            await self.js.add_consumer(stream="motion", config=config)
        except nats.js.errors.APIError:
            # Consumer already exists; continue
            pass
        sub = await self.js.pull_subscribe(
            subject,
            durable=durable,
            stream="motion",
        )
        self.log.info(f"Subscribed to {subject} with durable {durable}")
        return sub

    async def publish_play(self, node: str, session: str) -> None:
        await self.publish(node=node, session=session, op="play")

    async def publish_stop(self, node: str, session: str) -> None:
        await self.publish(node=node, session=session, op="stop")

    async def subscribe_play(self, node: str):
        return await self.subscribe(node=node, op="play")

    async def subscribe_stop(self, node: str):
        return await self.subscribe(node=node, op="stop")
