"""B · LLM tap: HTTP reverse proxy between the voice server and the LLM server.

Captures, for every generation request (``/responses``, ``/chat/completions``,
``/completions``): the exact request body, time to first byte / first token,
total time, streamed output text, tool calls, ``usage`` and llama.cpp
``timings`` when the server includes them. Other requests are relayed silently.
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager
import time

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import Response, StreamingResponse

from s2snoop.hub import Hub
from s2snoop.proxy import HOP_HEADERS

logger = logging.getLogger("s2snoop.llm_tap")

GEN_SUFFIXES = ("responses", "chat/completions", "completions")
MAX_STR = 4000


def _shrink(obj):
    """Replace data URLs / huge strings (images, audio) so requests stay storable."""
    if isinstance(obj, str):
        if obj.startswith("data:") and len(obj) > 200:
            return f"<data url, {len(obj)} chars>"
        if len(obj) > 50_000:
            return obj[:MAX_STR] + f"… <{len(obj)} chars>"
        return obj
    if isinstance(obj, list):
        return [_shrink(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _shrink(v) for k, v in obj.items()}
    return obj


def _find(obj, key: str):
    if isinstance(obj, dict):
        if key in obj and obj[key]:
            return obj[key]
        for v in obj.values():
            found = _find(v, key)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find(v, key)
            if found:
                return found
    return None


class CallRecorder:
    def __init__(self, endpoint: str, request: dict | None, t_start: float) -> None:
        self.endpoint = endpoint
        self.request = request
        self.t_start = t_start
        self.t_first_byte: float | None = None
        self.t_first_token: float | None = None
        self.text: list[str] = []
        self.tool_calls: list[dict] = []
        self.usage = None
        self.timings = None
        self.served_model: str | None = None
        self.status: int | None = None
        self.error: str | None = None
        self._buf = b""

    def feed(self, chunk: bytes, streaming: bool) -> None:
        if self.t_first_byte is None:
            self.t_first_byte = time.time()
        if not streaming:
            self._buf += chunk
            return
        self._buf += chunk
        while b"\n" in self._buf:
            line, self._buf = self._buf.split(b"\n", 1)
            line = line.strip()
            if not line.startswith(b"data:"):
                continue
            payload = line[5:].strip()
            if not payload or payload == b"[DONE]":
                continue
            try:
                self._event(json.loads(payload))
            except ValueError:
                continue

    def finish(self, streaming: bool) -> None:
        if not streaming and self._buf:
            try:
                body = json.loads(self._buf)
            except ValueError:
                return
            self._final(body)
            text = _find(body, "output_text")
            if isinstance(text, str):
                self.text.append(text)
            else:
                for choice in body.get("choices") or []:
                    msg = choice.get("message") or {}
                    if msg.get("content"):
                        self.text.append(msg["content"])
                    for tc in msg.get("tool_calls") or []:
                        fn = tc.get("function") or {}
                        self.tool_calls.append({"name": fn.get("name"), "arguments": fn.get("arguments")})
                for item in body.get("output") or []:
                    if item.get("type") == "function_call":
                        self.tool_calls.append({"name": item.get("name"), "arguments": item.get("arguments")})
                    for c in item.get("content") or []:
                        if c.get("type") == "output_text" and c.get("text"):
                            self.text.append(c["text"])

    def _token(self, text: str | None) -> None:
        if text and text.strip() and self.t_first_token is None:
            self.t_first_token = time.time()
        if text:
            self.text.append(text)

    def _event(self, ev: dict) -> None:
        kind = ev.get("type", "")
        if kind == "response.output_text.delta":
            self._token(ev.get("delta"))
        elif kind == "response.output_item.done":
            item = ev.get("item") or {}
            if item.get("type") == "function_call":
                if self.t_first_token is None:
                    self.t_first_token = time.time()
                self.tool_calls.append({"name": item.get("name"), "arguments": item.get("arguments")})
        elif kind in ("response.completed", "response.done", "response.incomplete", "response.failed"):
            self._final(ev.get("response") or ev)
        elif "choices" in ev:  # chat completions chunk
            for choice in ev.get("choices") or []:
                delta = choice.get("delta") or {}
                self._token(delta.get("content"))
                for tc in delta.get("tool_calls") or []:
                    fn = tc.get("function") or {}
                    if fn.get("name"):
                        self.tool_calls.append({"name": fn.get("name"), "arguments": fn.get("arguments") or ""})
                    elif self.tool_calls and fn.get("arguments"):
                        self.tool_calls[-1]["arguments"] = (self.tool_calls[-1]["arguments"] or "") + fn["arguments"]
            self._final(ev)
        else:
            self._final(ev)

    def _final(self, body: dict) -> None:
        # The model that actually answered: llama.cpp ignores the requested name.
        model = body.get("model") or (body.get("response") or {}).get("model")
        if isinstance(model, str) and model:
            self.served_model = model
        usage = body.get("usage")
        if usage:
            self.usage = usage
        timings = body.get("timings") or _find(body, "timings")
        if timings:
            self.timings = timings

    def to_record(self) -> dict:
        req = self.request or {}
        tools = req.get("tools") or []
        return {
            "endpoint": self.endpoint,
            "model": self.served_model or req.get("model"),
            "requested_model": req.get("model"),
            "served_model": self.served_model,
            "stream": bool(req.get("stream")),
            "t_start": self.t_start,
            "t_first_byte": self.t_first_byte,
            "t_first_token": self.t_first_token,
            "t_end": time.time(),
            "status": self.status,
            "error": self.error,
            "request": _shrink(req),
            "tool_names": [t.get("name") or (t.get("function") or {}).get("name") for t in tools],
            "n_input_items": len(req.get("input") or req.get("messages") or []) if isinstance(
                req.get("input") or req.get("messages"), list) else 1,
            "output_text": "".join(self.text)[:20000],
            "tool_calls": self.tool_calls,
            "usage": self.usage,
            "timings": self.timings,
        }


def create_llm_tap_app(hub: Hub, upstream: str) -> FastAPI:
    upstream = upstream.rstrip("/")
    http = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0))

    @asynccontextmanager
    async def lifespan(_app):
        yield
        await http.aclose()

    app = FastAPI(title="s2snoop llm tap", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
    async def tap(request: Request, path: str):
        body = await request.body()
        url = upstream + "/" + path + (("?" + request.url.query) if request.url.query else "")
        headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS]
        is_gen = request.method == "POST" and path.rstrip("/").endswith(GEN_SUFFIXES)
        rec = None
        if is_gen:
            try:
                parsed = json.loads(body) if body else None
            except ValueError:
                parsed = None
            rec = CallRecorder(path.rstrip("/").split("/")[-1] if not path.endswith("chat/completions")
                               else "chat/completions", parsed, time.time())
        try:
            resp = await http.send(http.build_request(request.method, url, headers=headers, content=body),
                                   stream=True)
        except httpx.HTTPError as exc:
            if rec:
                rec.error = f"{type(exc).__name__}: {exc}"
                hub.ingest_llm(rec.to_record())
            return Response(f"upstream error: {exc}", status_code=502)
        out_headers = {k: v for k, v in resp.headers.items()
                       if k.lower() not in HOP_HEADERS and k.lower() != "content-encoding"}
        if rec is None:
            async def plain():
                try:
                    async for chunk in resp.aiter_bytes():
                        yield chunk
                finally:
                    await resp.aclose()
            return StreamingResponse(plain(), status_code=resp.status_code, headers=out_headers)

        rec.status = resp.status_code
        streaming = "text/event-stream" in resp.headers.get("content-type", "")

        async def relay():
            try:
                async for chunk in resp.aiter_bytes():
                    yield chunk
                    rec.feed(chunk, streaming)
            except Exception as exc:  # noqa: BLE001
                rec.error = f"{type(exc).__name__}: {exc}"
                raise
            finally:
                await resp.aclose()
                rec.finish(streaming)
                if resp.status_code >= 400 and not rec.error:
                    rec.error = f"HTTP {resp.status_code}"
                hub.ingest_llm(rec.to_record())

        return StreamingResponse(relay(), status_code=resp.status_code, headers=out_headers)

    return app
