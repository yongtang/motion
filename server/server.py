from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

app = FastAPI()


@app.get("/health")
async def health():
    return JSONResponse({"status": "ok"})


@app.websocket("/ws")
async def ws(ws: WebSocket):
    await ws.accept()
    try:
        await ws.send_text("hello from server")
        while True:
            msg = await ws.receive_text()
            await ws.send_text(f"echo: {msg}")
    except WebSocketDisconnect:
        pass
