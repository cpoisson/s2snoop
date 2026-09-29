"""End to end: fake Realtime server + fake SSE LLM behind the real proxies."""

import asyncio
import base64
import json
import socket
import time

import httpx
import numpy as np
import pytest
import uvicorn
import websockets
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from s2snoop.hub import Hub
from s2snoop.llm_tap import create_llm_tap_app
from s2snoop.proxy import Router, create_proxy_app
from s2snoop.session import LATENCY_KEY
from s2snoop.web import create_web_app


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def start(app, port):
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on"))
    task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.02)
    return server, task


async def fake_realtime(ws):
    """Scripted server: answers after 10 audio chunks with one full turn."""
    assert ws.request.path.startswith("/v1/realtime")
    chunks = 0
    await ws.send(json.dumps({"type": "session.created",
                              "session": {"audio": {"input": {"format": {"type": "audio/pcm", "rate": 24000}},
                                                    "output": {"format": {"type": "audio/pcm", "rate": 24000}}}}}))
    async for raw in ws:
        ev = json.loads(raw)
        if ev["type"] == "input_audio_buffer.append":
            chunks += 1
            if chunks == 10:
                tts = base64.b64encode(np.full(2400, 500, dtype="<i2").tobytes()).decode()
                script = [
                    {"type": "input_audio_buffer.speech_started", "audio_start_ms": 100, "item_id": "u1"},
                    {"type": "input_audio_buffer.speech_stopped", "audio_end_ms": 900, "item_id": "u1"},
                    {"type": "conversation.item.input_audio_transcription.completed", "item_id": "u1",
                     "transcript": "hello"},
                    {"type": "response.created", "response": {"id": "r1"}},
                    {"type": "response.output_item.added", "response_id": "r1", "item": {"id": "a1", "type": "message"}},
                    {"type": "response.output_audio.delta", "response_id": "r1", "item_id": "a1", "delta": tts},
                    {"type": "response.output_audio.delta", "response_id": "r1", "item_id": "a1", "delta": tts},
                    {"type": "response.output_audio_transcript.delta", "response_id": "r1", "delta": "Hi"},
                    {"type": "response.done", "response": {
                        "id": "r1", "status": "completed", "usage": {"input_tokens": 42, "output_tokens": 3},
                        "metadata": {LATENCY_KEY: json.dumps({"e2e_s": 0.5, "llm_ttft_s": 0.1})}}},
                ]
                for msg in script:
                    await ws.send(json.dumps(msg))
        elif ev["type"] == "conversation.item.create":
            await ws.send(json.dumps({"type": "conversation.item.created", "item": ev["item"]}))


def fake_llm_app() -> FastAPI:
    app = FastAPI()

    @app.post("/v1/responses")
    async def responses():
        async def gen():
            yield b'data: {"type":"response.created"}\n\n'
            await asyncio.sleep(0.05)
            yield b'data: {"type":"response.output_text.delta","delta":"Hel"}\n\n'
            yield b'data: {"type":"response.output_text.delta","delta":"lo"}\n\n'
            yield (b'data: {"type":"response.completed","response":{"model":"gemma-local","usage":{"input_tokens":12,'
                   b'"output_tokens":2},"timings":{"predicted_per_second":41.5}}}\n\n')
        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/health")
    async def health():
        return {"ok": True}

    return app


@pytest.fixture
async def stack(tmp_path):
    hub = Hub(tmp_path / "data")
    up_port, px_port, llm_port, tap_port, ui_port = (free_port() for _ in range(5))
    upstream = await websockets.serve(fake_realtime, "127.0.0.1", up_port)
    servers = [
        await start(create_proxy_app(hub, Router(f"ws://127.0.0.1:{up_port}", {})), px_port),
        await start(fake_llm_app(), llm_port),
        await start(create_llm_tap_app(hub, f"http://127.0.0.1:{llm_port}"), tap_port),
        await start(create_web_app(hub, {}), ui_port),
    ]
    yield hub, px_port, tap_port, ui_port
    for server, task in servers:
        server.should_exit = True
        await task
    upstream.close()
    await upstream.wait_closed()


async def test_proxy_llm_tap_and_web(stack):
    hub, px_port, tap_port, ui_port = stack
    chunk = base64.b64encode(np.full(2400, 1000, dtype="<i2").tobytes()).decode()  # 100 ms
    received = []
    async with websockets.connect(f"ws://127.0.0.1:{px_port}/v1/realtime?client=robot&model=x") as ws:
        received.append(json.loads(await ws.recv()))
        await ws.send(json.dumps({"type": "conversation.item.create", "item": {
            "type": "message", "role": "user",
            "content": [{"type": "input_image", "image_url": "data:image/png;base64,iVBORw0KGgo="}]}}))
        # LLM call while the session is live
        async with httpx.AsyncClient() as http:
            r = await http.post(f"http://127.0.0.1:{tap_port}/v1/responses",
                                json={"model": "gemma", "stream": True, "input": [{"role": "user", "content": "hi"}],
                                      "tools": [{"type": "function", "name": "camera"}]})
            assert "Hel" in r.text and "lo" in r.text
        for _ in range(10):
            await ws.send(json.dumps({"type": "input_audio_buffer.append", "audio": chunk}))
            await asyncio.sleep(0.1)
        deadline = time.time() + 3
        while time.time() < deadline:
            received.append(json.loads(await asyncio.wait_for(ws.recv(), 3)))
            if received[-1]["type"] == "response.done":
                break
    # relay is verbatim: audio deltas reached the client untouched
    deltas = [e for e in received if e["type"] == "response.output_audio.delta"]
    assert len(deltas) == 2 and len(base64.b64decode(deltas[0]["delta"])) == 4800
    await asyncio.sleep(0.2)
    hub.flush()

    sid = hub.list_sessions()[0]["id"]
    assert hub.live[sid].closed, "client disconnect must close the session"
    assert not hub.list_sessions()[0]["live"]
    snap = hub.snapshot(sid)
    assert snap["meta"]["client"] == "robot"
    assert "client=" not in snap["meta"]["upstream"]
    turn = snap["turns"][-1]
    assert turn["transcript"] == "hello"
    assert turn["responses"][0]["usage"]["input_tokens"] == 42
    assert turn["proxy_e2e"] is not None
    assert snap["placements"][0]["audio_s"] == pytest.approx(0.2)
    img = snap["turns"][0]["images"][0]["src"]
    assert img.startswith("file:images/") and (hub.session_dir(sid) / img[5:]).exists()
    # LLM tap captured the call
    calls = [c for t in snap["turns"] for c in t["llm_calls"]]
    assert calls and calls[0]["usage"]["input_tokens"] == 12
    assert calls[0]["timings"]["predicted_per_second"] == 41.5
    assert calls[0]["output_text"] == "Hello" and calls[0]["tool_names"] == ["camera"]
    assert calls[0]["model"] == "gemma-local" and calls[0]["requested_model"] == "gemma"
    assert calls[0]["t_first_token"] >= calls[0]["t_start"]
    # mic audio: 10 x 100 ms
    assert (hub.session_dir(sid) / "mic.pcm").stat().st_size == 10 * 4800

    async with httpx.AsyncClient() as http:
        base = f"http://127.0.0.1:{ui_port}"
        assert (await http.get(f"{base}/api/sessions")).json()[0]["id"] == sid
        wav = await http.get(f"{base}/api/sessions/{sid}/audio/assistant.wav")
        assert wav.status_code == 200 and wav.content[:4] == b"RIFF"
        mic = await http.get(f"{base}/api/sessions/{sid}/audio/mic.wav")
        assert mic.status_code == 200 and len(mic.content) > 48000
        peaks = (await http.get(f"{base}/api/sessions/{sid}/peaks")).json()
        assert len(peaks["mic"]["peaks"]) == 50 and "r1" in peaks["responses"]
        ev = (await http.get(f"{base}/api/sessions/{sid}/events", params={"type": "response.done"})).json()
        assert ev["total"] == 1
        # persisted session replays to the same turns once it is no longer live
        del hub.live[sid]
        replay = (await http.get(f"{base}/api/sessions/{sid}")).json()
        assert replay["turns"][-1]["transcript"] == "hello"
        assert replay["placements"][0]["audio_s"] == pytest.approx(0.2)
        assert replay["stats"]["llm_calls"] == 1


async def test_http_passthrough(tmp_path):
    hub = Hub(tmp_path / "data")
    llm_port, px_port = free_port(), free_port()
    s1 = await start(fake_llm_app(), llm_port)
    s2 = await start(create_proxy_app(hub, Router(f"ws://127.0.0.1:{llm_port}", {})), px_port)
    async with httpx.AsyncClient() as http:
        r = await http.get(f"http://127.0.0.1:{px_port}/health")
        assert r.json() == {"ok": True}
    for server, task in (s1, s2):
        server.should_exit = True
        await task


def test_probe_series_attribution(tmp_path):
    hub = Hub(tmp_path / "data")
    sid = hub.open_session({"client": "t"})
    t0 = hub.live[sid].t0
    hub.ingest_probe({"kind": "vad", "t": t0 + 1, "frames": [[t0 + 1, 0.9, 0.6, 1, 0]]})
    hub.ingest_probe({"kind": "queues", "t": t0 + 1.2, "depths": {"TTSHandler": 2}})
    hub.ingest_probe({"kind": "status", "hooks": {"vad": "on"}})
    rows = hub.series_since(sid, 0)
    assert [r["kind"] for r in rows] == ["vad", "queues"]
    assert rows[0]["data"]["frames"][0][0] == pytest.approx(1.0)
    assert hub.status()["probe"]["status"]["hooks"] == {"vad": "on"}


def test_clear_sessions_keeps_live_ones(tmp_path):
    from fastapi.testclient import TestClient

    from s2snoop.web import create_web_app

    hub = Hub(tmp_path / "data")
    ended = [hub.open_session({"client": f"old{i}"}) for i in range(2)]
    for sid in ended:
        (hub.session_dir(sid) / "images").mkdir(parents=True)
        hub.close_session(sid, "done")
    live = hub.open_session({"client": "live"})
    hub.flush()
    client = TestClient(create_web_app(hub, {}))
    assert client.delete("/api/sessions").json() == {"deleted": 2}
    assert [s["id"] for s in client.get("/api/sessions").json()] == [live]
    assert not any(hub.session_dir(sid).exists() for sid in ended)
    assert client.delete(f"/api/sessions/{live}").status_code == 409


def test_clear_command(tmp_path):
    import subprocess
    import sys

    hub = Hub(tmp_path / "data")
    hub.close_session(hub.open_session({}), "done")
    hub.flush()
    out = subprocess.run([sys.executable, "-m", "s2snoop.cli", "clear", "--data", str(tmp_path / "data"), "--yes"],
                         capture_output=True, text=True, check=True).stdout
    assert "Deleted 1 session" in out
    assert Hub(tmp_path / "data").store.list_sessions() == []
