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

        config = nats.js.api.StreamConfig(
            name="motion",
            subjects=["motion.session", "motion.session.*.stop"],
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

    # play: subject "motion.session", payload "<session>.play <data>"
    async def publish_play(self, session: str, data: str) -> None:
        assert self.js is not None, "Channel not started"
        subject = f"motion.session"
        payload = f"{session}.play {data}"
        self.log.info(f"Publish {subject}: {payload}")
        ack = await self.js.publish(subject, payload.encode())
        self.log.info(f"Ack {ack.stream} {ack.seq}")

    # stop: subject "motion.session.<session>.stop", payload "<data>"
    async def publish_stop(self, session: str, data: str) -> None:
        assert self.js is not None, "Channel not started"
        subject = f"motion.session.{session}.stop"
        payload = f"{data}"
        self.log.info(f"Publish {subject}: {payload}")
        ack = await self.js.publish(subject, payload.encode())
        self.log.info(f"Ack {ack.stream} {ack.seq}")

    # Subscribe to plays (shared durable, pull).
    async def subscribe_play(self):
        assert self.js is not None, "Channel not started"
        subject = "motion.session"
        durable = "motion-session"
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

    # Subscribe to stops for a session (per-session durable, DeliverPolicy.All).
    async def subscribe_stop(self, session: str):
        assert self.js is not None, "Channel not started"
        subject = f"motion.session.{session}.stop"
        durable = f"motion-session-{session}"
        config = nats.js.api.ConsumerConfig(
            durable_name=durable,
            filter_subject=subject,
            deliver_policy=nats.js.api.DeliverPolicy.ALL,
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

    # Cleanup per-session stop consumer and purge lingering stop messages.
    async def subscribe_done(self, session: str) -> None:
        assert self.js is not None, "Channel not started"
        subject = f"motion.session.{session}.stop"
        durable = f"motion-session-{session}"

        # Delete the per-session consumer
        try:
            await self.js.delete_consumer("motion", durable)
            self.log.info(f"Deleted consumer {durable}")
        except nats.js.errors.APIError:
            pass

        # Purge any remaining stop messages for this session
        try:
            try:
                await self.js.purge_stream("motion", filter_subject=subject)
            except TypeError:
                await self.js.purge_stream("motion", subject=subject)
            self.log.info(f"Purged stream 'motion' for subject {subject}")
        except Exception:
            pass
