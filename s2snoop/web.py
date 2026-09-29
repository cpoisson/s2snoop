"""Dashboard: JSON API, audio rendering, live WebSocket and static front end."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles

from s2snoop import audio as A
from s2snoop.hub import Hub

logger = logging.getLogger("s2snoop.web")
STATIC = Path(__file__).parent / "static"


def create_web_app(hub: Hub, info: dict) -> FastAPI:
    app = FastAPI(title="s2snoop", docs_url=None, redoc_url=None)

    def _snap(sid: str) -> dict:
        snap = hub.snapshot(sid)
        if snap is None:
            raise HTTPException(404, "unknown session")
        return snap

    @app.get("/")
    async def index():
        # Version the asset URLs with their mtime: a browser can never keep running an old app.js.
        html = (STATIC / "index.html").read_text()
        for name in ("app.js", "style.css"):
            version = int((STATIC / name).stat().st_mtime)
            html = html.replace(f"/static/{name}\"", f"/static/{name}?v={version}\"")
        return HTMLResponse(html, headers={"Cache-Control": "no-store"})

    @app.get("/api/status")
    async def status():
        return {**hub.status(), **info}

    @app.get("/api/sessions")
    async def sessions():
        return hub.list_sessions()

    @app.get("/api/sessions/{sid}")
    async def session(sid: str):
        return _snap(sid)

    @app.delete("/api/sessions/{sid}")
    async def delete(sid: str):
        if not hub.delete_session(sid):
            raise HTTPException(409, "session is live")
        return {"ok": True}

    @app.delete("/api/sessions")
    async def clear():
        """Delete all ended sessions (live ones are kept)."""
        return {"deleted": hub.clear_sessions()}

    @app.get("/api/sessions/{sid}/events")
    async def events(sid: str, offset: int = 0, limit: int = 200, type: str | None = None,
                     source: str | None = None):
        hub.flush()
        return hub.store.events_page(sid, offset, min(limit, 1000), type, source)

    @app.get("/api/sessions/{sid}/series")
    async def series(sid: str, after: int = 0):
        return hub.series_since(sid, after)

    @app.get("/api/sessions/{sid}/peaks")
    async def peaks(sid: str, mic_from: int = 0):
        snap = _snap(sid)
        return _peaks(hub, sid, snap, mic_from, None)

    @app.get("/api/sessions/{sid}/audio/{track}.wav")
    async def track_audio(sid: str, track: str):
        snap = _snap(sid)
        d = hub.session_dir(sid)
        _flush_audio(hub, sid)
        if track == "mic":
            mic = A.MicMap(rate=snap["mic"]["rate"], start_s=snap["mic"]["start_s"])
            pcm, rate = A.render_mic(d, mic)
        elif track == "assistant":
            pcm, rate = A.render_assistant(d, snap["placements"])
        else:
            raise HTTPException(404)
        return Response(A.wav_bytes(pcm, rate), media_type="audio/wav", headers={"Cache-Control": "no-store"})

    @app.get("/api/sessions/{sid}/audio/response/{rid}.wav")
    async def response_audio(sid: str, rid: str):
        snap = _snap(sid)
        rate = next((p["rate"] for p in snap["placements"] if p["response_id"] == rid and p.get("rate")), None)
        _flush_audio(hub, sid)
        pcm = A.render_response(hub.session_dir(sid), rid, rate or 24000)
        return Response(A.wav_bytes(pcm, rate or 24000), media_type="audio/wav", headers={"Cache-Control": "no-store"})

    @app.get("/api/sessions/{sid}/files/{path:path}")
    async def files(sid: str, path: str):
        base = hub.session_dir(sid).resolve()
        target = (base / path).resolve()
        if base not in target.parents or not target.is_file():
            raise HTTPException(404)
        return FileResponse(target)

    @app.websocket("/api/live")
    async def live(ws: WebSocket):
        await ws.accept()
        q = hub.listen()
        state = {"sid": None, "series": 0, "mic": 0, "resp": {}, "version": -1}
        last_list = 0.0

        async def push_session(force: bool = False) -> None:
            sid = state["sid"]
            if not sid:
                return
            snap = hub.snapshot(sid)
            if snap is None:
                return
            await ws.send_text(json.dumps({"type": "snapshot", "session": snap}))
            rows = hub.series_since(sid, state["series"])
            if rows:
                state["series"] = rows[-1]["seq"]
                await ws.send_text(json.dumps({"type": "series", "rows": rows}))
            pk = _peaks(hub, sid, snap, state["mic"], state["resp"])
            if pk["mic"]["peaks"] or pk["responses"]:
                win = max(1, (pk["mic"]["rate"] or 24000) // A.PEAKS_PER_S)
                state["mic"] = pk["mic"]["from"] + len(pk["mic"]["peaks"]) * win
                await ws.send_text(json.dumps({"type": "peaks", **pk}))

        async def reader() -> None:
            while True:
                msg = json.loads(await ws.receive_text())
                if "subscribe" in msg:
                    state.update({"sid": msg["subscribe"], "series": 0, "mic": 0, "resp": {}})
                    await push_session(force=True)

        read_task = asyncio.create_task(reader())
        try:
            await ws.send_text(json.dumps({"type": "sessions", "sessions": hub.list_sessions(),
                                           "status": {**hub.status(), **info}}))
            while not read_task.done():
                try:
                    dirty = await asyncio.wait_for(q.get(), timeout=2.0)
                except asyncio.TimeoutError:
                    dirty = set()
                loop_t = asyncio.get_running_loop().time()
                if (dirty and loop_t - last_list > 1.0) or loop_t - last_list > 5.0:
                    last_list = loop_t
                    await ws.send_text(json.dumps({"type": "sessions", "sessions": hub.list_sessions(),
                                                   "status": {**hub.status(), **info}}))
                if state["sid"] in dirty:
                    await push_session()
        except (WebSocketDisconnect, RuntimeError):
            pass
        finally:
            hub.unlisten(q)
            read_task.cancel()

    @app.middleware("http")
    async def revalidate_static(request, call_next):
        # Front-end files change with s2snoop updates: always revalidate (ETag), never serve a stale copy.
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-cache"
        return response

    app.mount("/static", StaticFiles(directory=STATIC), name="static")
    return app


def _flush_audio(hub: Hub, sid: str) -> None:
    rt = hub.live.get(sid)
    if rt:
        if rt.mic:
            rt.mic.flush()
        if rt.responses:
            rt.responses.flush()


def _peaks(hub: Hub, sid: str, snap: dict, mic_from: int, sent_resp: dict | None) -> dict:
    d = hub.session_dir(sid)
    _flush_audio(hub, sid)
    rate = snap["mic"]["rate"] or 24000
    start, mic = A.mic_peaks_from(d, mic_from, rate) if snap["mic"]["rate"] else (0, [])
    responses = {}
    for p in snap["placements"]:
        rid = p["response_id"]
        size = (d / "responses" / f"{A._safe(rid)}.pcm")
        n = size.stat().st_size if size.exists() else 0
        if sent_resp is not None and sent_resp.get(rid) == n:
            continue
        pcm = A.read_pcm(size)
        responses[rid] = A.peaks(pcm, p.get("rate") or 24000)
        if sent_resp is not None:
            sent_resp[rid] = n
    return {"mic": {"from": start, "rate": rate, "start_s": snap["mic"]["start_s"], "peaks": mic},
            "responses": responses, "per_s": A.PEAKS_PER_S}
