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
        assert self.nc is None and self.js is None, "Channel already started"
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

        # Single stream for node commands and data
        config = nats.js.api.StreamConfig(
            name="motion",
            subjects=["motion.node.*.play", "motion.node.*.stop", "motion.data.>"],
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

    # ----- Node (play/stop) -----

    async def publish_node(self, node: str, session: str, op: str) -> None:
        assert self.js is not None, "Channel not started"
        assert op in ("play", "stop"), f"Unsupported op: {op}"
        subject = f"motion.node.{node}.{op}"
        payload = f"{session}"
        self.log.info(f"Publish {subject}: {payload}")
        ack = await self.js.publish(subject, payload.encode())
        self.log.info(f"Ack {ack.stream} {ack.seq}")

    async def subscribe_node(self, node: str, op: str):
        assert self.js is not None, "Channel not started"
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
            pass
        sub = await self.js.pull_subscribe(
            subject,
            durable=durable,
            stream="motion",
        )
        self.log.info(f"Subscribed to {subject} with durable {durable}")
        return sub

    async def publish_play(self, node: str, session: str) -> None:
        await self.publish_node(node=node, session=session, op="play")

    async def publish_stop(self, node: str, session: str) -> None:
        await self.publish_node(node=node, session=session, op="stop")

    async def subscribe_play(self, node: str):
        return await self.subscribe_node(node=node, op="play")

    async def subscribe_stop(self, node: str):
        return await self.subscribe_node(node=node, op="stop")

    # ----- Data (motion.data.<session>) -----

    async def publish_data(self, session: str, payload: str) -> None:
        assert self.js is not None, "Channel not started"
        subject = f"motion.data.{session}"
        self.log.info(f"Publish {subject}: {payload[:80]!r}")
        ack = await self.js.publish(subject, payload.encode())
        self.log.info(f"Ack {ack.stream} {ack.seq}")

    async def subscribe_archive(self, session: str):
        # durable pull (ALL) on motion.data.{session}
        assert self.js is not None, "Channel not started"
        subject = f"motion.data.{session}"
        durable = f"motion-data-{session}"
        config = nats.js.api.ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            deliver_policy=nats.js.api.DeliverPolicy.ALL,
            ack_policy=nats.js.api.AckPolicy.EXPLICIT,
            max_ack_pending=10_000,
        )
        try:
            await self.js.add_consumer(stream="motion", config=config)
        except nats.js.errors.APIError:
            pass
        sub = await self.js.pull_subscribe(subject, durable=durable, stream="motion")
        self.log.info(f"[archive] pull_subscribed {subject} durable={durable}")
        return sub

    async def subscribe_stream(self, session: str):
        # ephemeral push (NEW) on motion.data.{session}
        assert self.js is not None, "Channel not started"
        subject = f"motion.data.{session}"
        config = nats.js.api.ConsumerConfig(
            filter_subject=subject,
            deliver_policy=nats.js.api.DeliverPolicy.NEW,
            replay_policy=nats.js.api.ReplayPolicy.INSTANT,
            ack_policy=nats.js.api.AckPolicy.NONE,
        )
        sub = await self.js.subscribe(subject, stream="motion", config=config)
        self.log.info(f"[stream] subscribed {subject} (ephemeral, NEW)")
        return sub
