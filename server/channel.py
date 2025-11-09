import datetime
import functools
import logging

import nats

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class Channel:
    def __init__(self, servers: str = "nats://127.0.0.1:4222"):
        self.servers = servers
        self.nc: nats.NATS | None = None
        self.js: nats.js.JetStreamContext | None = None
        log.info(f"[Channel.__init__] servers={servers}")

    async def start(self) -> None:
        log.info("[Channel.start] called")
        assert self.nc is None and self.js is None, "Channel already started"
        log.info(f"[Channel.start] Connecting to NATS at {self.servers} ...")

        async def cb(event: str, *args, **kwargs):
            log.info(f"[Channel.start.cb] NATS {event}: args={args} kwargs={kwargs}")

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
            subjects=[
                "motion.data.*",
                "motion.step.*",
            ],
            allow_rollup_hdrs=True,
            storage=nats.js.api.StorageType.FILE,
            retention=nats.js.api.RetentionPolicy.LIMITS,
            max_msgs=-1,
            max_bytes=-1,
        )
        try:
            await self.js.add_stream(config=config)
            log.info(
                f"[Channel.start] Created stream 'motion' with subjects={config.subjects}"
            )
        except nats.js.errors.APIError:
            await self.js.update_stream(config=config)
            log.info(
                f"[Channel.start] Updated stream 'motion' with subjects={config.subjects}"
            )

        log.info(f"[Channel.start] Connected to NATS at {self.servers}")

    async def close(self) -> None:
        log.info("[Channel.close] called")
        if self.nc and not self.nc.is_closed:
            log.info("[Channel.close] Draining NATS connection...")
            await self.nc.drain()
            log.info("[Channel.close] NATS connection closed")
        self.nc = None
        self.js = None

    async def publish_data(self, session: str, payload: bytes) -> None:
        log.info(f"[Channel.publish_data] session={session}, size={len(payload)}")
        assert self.nc is not None, "Channel not started"
        subject = f"motion.data.{session}"
        log.info(f"[Channel.publish_data] Publish (core) {subject}: {payload[:80]!r}")
        await self.nc.publish(subject, payload)

    async def subscribe_data(self, session: str, start: int | None = None):
        log.info(f"[Channel.subscribe_data] session={session}, start={start}")
        assert self.js is not None, "Channel not started"
        subject = f"motion.data.{session}"

        match start:
            case None:
                config = nats.js.api.ConsumerConfig(
                    filter_subject=subject,
                    deliver_policy=nats.js.api.DeliverPolicy.NEW,
                    replay_policy=nats.js.api.ReplayPolicy.INSTANT,
                    ack_policy=nats.js.api.AckPolicy.NONE,
                    inactive_threshold=datetime.timedelta(hours=1).total_seconds(),
                )
                note = "NEW"
            case -1:
                # tail the last available message, then follow new ones
                config = nats.js.api.ConsumerConfig(
                    filter_subject=subject,
                    deliver_policy=nats.js.api.DeliverPolicy.LAST,
                    replay_policy=nats.js.api.ReplayPolicy.INSTANT,
                    ack_policy=nats.js.api.AckPolicy.NONE,
                    inactive_threshold=datetime.timedelta(hours=1).total_seconds(),
                )
                note = "LAST"
            case seq if isinstance(seq, int) and seq > 0:
                config = nats.js.api.ConsumerConfig(
                    filter_subject=subject,
                    deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
                    opt_start_seq=seq,
                    replay_policy=nats.js.api.ReplayPolicy.INSTANT,
                    ack_policy=nats.js.api.AckPolicy.NONE,
                    inactive_threshold=datetime.timedelta(hours=1).total_seconds(),
                )
                note = f"START_SEQUENCE {seq}"
            case other:
                log.error(f"[Channel.subscribe_data] Invalid start={other!r}")
                raise ValueError(f"Invalid start sequence: {other!r}")

        sub = await self.js.subscribe(subject, stream="motion", config=config)
        log.info(
            f"[Channel.subscribe_data] Subscribed (JS) {subject} (ephemeral, {note})"
        )
        return sub

    async def publish_step(self, session: str, payload: bytes) -> None:
        log.info(f"[Channel.publish_step] session={session}, size={len(payload)}")
        assert self.nc is not None, "Channel not started"
        subject = f"motion.step.{session}"
        log.info(f"[Channel.publish_step] Publish (core) {subject}: {payload[:80]!r}")
        await self.nc.publish(subject, payload)

    async def subscribe_step(self, session: str, callback):
        log.info(f"[Channel.subscribe_step] session={session}")
        assert self.js is not None, "Channel not started"
        subject = f"motion.step.{session}"
        config = nats.js.api.ConsumerConfig(
            filter_subject=subject,
            deliver_policy=nats.js.api.DeliverPolicy.ALL,
            ack_policy=nats.js.api.AckPolicy.NONE,
            inactive_threshold=datetime.timedelta(hours=1).total_seconds(),
        )
        sub = await self.js.subscribe(
            subject, config=config, stream="motion", cb=callback
        )
        log.info(f"[Channel.subscribe_step] Subscribed (JS) {subject}")
        return sub
