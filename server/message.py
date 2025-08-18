import websockets


async def message_pub(sub, data):
    async with websockets.connect("ws://127.0.0.1:8081") as websocket:
        message = f"PUB {sub} {len(data)}\r\n{data}\r\n"
        await websocket.send(message)
