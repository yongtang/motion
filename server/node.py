import asyncio
import datetime
import json
import logging
import os

import aiohttp.web

from .channel import Channel
from .storage import storage_kv_set

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("node")


async def node_http():
    app = aiohttp.web.Application()
    app.add_routes(
        [
            aiohttp.web.get(
                "/health", lambda _: aiohttp.web.json_response({"status": "ok"})
            )
        ]
    )
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", 8888)
    await site.start()
    log.info("HTTP health at http://0.0.0.0:8888/health")

    try:
        await asyncio.Future()
    finally:
        await runner.cleanup()
        log.info("HTTP server stopped")


async def node_main(channel: Channel, session: str):
    async with asyncio.timeout(30):
        for i in range(60):
            data = json.dumps({"session": session, "count": i})
            log.info(f"Publish {data}...")
            await channel.publish_data(session, data)
            await asyncio.sleep(1)


async def node_data(channel: Channel, session: str):
    sub = await channel.subscribe_archive(session)

    while True:
        batch = []
        start = asyncio.get_event_loop().time()

        while True:
            elapsed = asyncio.get_event_loop().time() - start
            remaining = max(0.1, 5.0 - elapsed)  # 5s max window

            try:
                msgs = await sub.fetch(1000 - len(batch), timeout=remaining)
            except asyncio.TimeoutError:
                msgs = []

            batch.extend(msgs)

            if len(batch) >= 1000:
                break
            if (asyncio.get_event_loop().time() - start) >= 5.0:
                break
            if not msgs:
                continue

        if not batch:
            continue

        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        key = f"{session}-{ts}.json"

        payload = b"\n".join(m.data for m in batch) + b"\n"

        etag = storage_kv_set("data", key, payload)
        log.info(f"Uploaded {len(batch)} messages to s3://data/{key} etag={etag}")

        for m in batch:
            await m.ack()


async def main():
    with open(os.path.join("/storage/node", "session.json"), "rb") as f:
        data = json.loads(f.read())
    session = data["session"]

    channel = Channel()
    await channel.start()
    try:
        await asyncio.gather(
            node_http(),
            node_main(channel, session),
            node_data(channel, session),
        )
    finally:
        await channel.close()


if __name__ == "__main__":
    asyncio.run(main())
