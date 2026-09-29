"""A · transparent Realtime proxy.

WebSocket messages are relayed verbatim in both directions; each one is handed
to the hub *after* it has been forwarded. Plain HTTP requests (health checks,
``/v1/realtime/calls`` SDP offers, ``/v1/usage``…) are reverse-proxied as-is so
any client keeps working unchanged.

Routing: ``--upstream`` is the default target. Named routes
(``--route openai=wss://api.openai.com``) are selected by the first path
segment: ``ws://proxy:8765/openai/v1/realtime`` → ``wss://api.openai.com/v1/realtime``.
An optional ``?client=<label>`` query parameter tags the session and is removed
before relaying.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
import time
from urllib.parse import parse_qsl, urlencode, urlsplit

import httpx
import websockets
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import Response, StreamingResponse
from starlette.websockets import WebSocketDisconnect, WebSocketState

from s2snoop.hub import Hub

logger = logging.getLogger("s2snoop.proxy")

HOP_HEADERS = {
    "host", "connection", "upgrade", "keep-alive", "proxy-connection", "transfer-encoding", "te", "trailer",
    "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions", "sec-websocket-protocol",
    "sec-websocket-accept", "content-length", "accept-encoding",
}


def _ws_to_http(url: str) -> str:
    if url.startswith("wss://"):
        return "https://" + url[6:]
    if url.startswith("ws://"):
        return "http://" + url[5:]
    return url


def _http_to_ws(url: str) -> str:
    if url.startswith("https://"):
        return "wss://" + url[8:]
    if url.startswith("http://"):
        return "ws://" + url[7:]
    return url


class Router:
    def __init__(self, default: str, routes: dict[str, str]) -> None:
        self.default = default.rstrip("/")
        self.routes = {k: v.rstrip("/") for k, v in routes.items()}

    def resolve(self, path: str) -> tuple[str, str, str]:
        """Return (route name, base url, remaining path)."""
        path = path.lstrip("/")
        head, _, rest = path.partition("/")
        if head in self.routes:
            return head, self.routes[head], rest
        return "default", self.default, path


def create_proxy_app(hub: Hub, router: Router) -> FastAPI:
    http = httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=False)

    @asynccontextmanager
    async def lifespan(_app):
        yield
        await http.aclose()

    app = FastAPI(title="s2snoop proxy", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    async def ws_proxy(ws: WebSocket, path: str) -> None:
        route, base, rest = router.resolve(path)
        query = [(k, v) for k, v in parse_qsl(ws.url.query, keep_blank_values=True)]
        label = next((v for k, v in query if k == "client"), None)
        query = [(k, v) for k, v in query if k != "client"]
        target = _http_to_ws(base) + "/" + rest + (("?" + urlencode(query)) if query else "")
        headers = [(k, v) for k, v in ws.headers.items() if k.lower() not in HOP_HEADERS
                   and k.lower() not in ("origin", "cookie")]
        subprotocols = ws.scope.get("subprotocols") or None
        try:
            upstream = await websockets.connect(
                target, additional_headers=headers, subprotocols=subprotocols,
                max_size=None, open_timeout=15, ping_interval=20, ping_timeout=60,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("upstream connect failed %s: %s", target, exc)
            await ws.close(code=1011, reason=f"upstream unavailable: {type(exc).__name__}"[:120])
            return
        await ws.accept(subprotocol=upstream.subprotocol)
        client = ws.client
        sid = hub.open_session({
            "client": label or _guess_client(ws.headers.get("user-agent", "")),
            "label": label,
            "remote": f"{client.host}:{client.port}" if client else None,
            "user_agent": ws.headers.get("user-agent"),
            "route": route,
            "upstream": _redact(target),
            "subprotocol": upstream.subprotocol,
        })
        reason = "closed"

        async def client_to_upstream() -> str:
            while True:
                msg = await ws.receive()
                if msg["type"] == "websocket.disconnect":
                    return f"client disconnected ({msg.get('code')})"
                data = msg.get("text")
                if data is None:
                    data = msg.get("bytes")
                if data is None:
                    continue
                await upstream.send(data)
                _record(hub, sid, "c2s", data)

        async def upstream_to_client() -> str:
            async for data in upstream:
                if isinstance(data, str):
                    await ws.send_text(data)
                else:
                    await ws.send_bytes(data)
                _record(hub, sid, "s2c", data)
            return f"upstream closed ({upstream.close_code})"

        tasks = [asyncio.create_task(client_to_upstream()), asyncio.create_task(upstream_to_client())]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                try:
                    reason = task.result()
                except (WebSocketDisconnect, websockets.ConnectionClosed) as exc:
                    reason = f"{type(exc).__name__}"
                except Exception as exc:  # noqa: BLE001
                    reason = f"error: {exc}"
                    logger.exception("relay error")
            for task in pending:
                task.cancel()
        finally:
            hub.close_session(sid, reason)
            try:
                await upstream.close()
            except Exception:  # noqa: BLE001
                pass
            if ws.application_state == WebSocketState.CONNECTED:
                try:
                    await ws.close()
                except (RuntimeError, WebSocketDisconnect):
                    pass

    app.add_api_websocket_route("/{path:path}", ws_proxy)
    # Reused by the dashboard's same-origin /talk route (browser mic over the dashboard's own http(s)).
    app.state.ws_relay = ws_proxy

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
    async def http_proxy(request: Request, path: str):
        _, base, rest = router.resolve(path)
        query = [(k, v) for k, v in request.query_params.multi_items() if k != "client"]
        url = _ws_to_http(base) + "/" + rest + (("?" + urlencode(query)) if query else "")
        headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS]
        body = await request.body()
        try:
            req = http.build_request(request.method, url, headers=headers, content=body)
            resp = await http.send(req, stream=True)
        except httpx.HTTPError as exc:
            return Response(f"upstream error: {exc}", status_code=502)
        out_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_HEADERS
                       and k.lower() != "content-encoding"}
        return StreamingResponse(resp.aiter_bytes(), status_code=resp.status_code, headers=out_headers,
                                 background=_Closer(resp))

    return app


class _Closer:
    def __init__(self, resp: httpx.Response) -> None:
        self.resp = resp

    async def __call__(self) -> None:
        await self.resp.aclose()


def _record(hub: Hub, sid: str, direction: str, data) -> None:
    # Recording happens after forwarding and must never break the relay.
    try:
        hub.ingest_realtime(sid, time.time(), direction, data)
    except Exception:  # noqa: BLE001
        logger.exception("recording failed (%s), relay continues", direction)


def _guess_client(user_agent: str) -> str:
    ua = user_agent.lower()
    if "reachy" in ua:
        return "reachy"
    if "python" in ua or "websockets" in ua:
        return "python"
    if "mozilla" in ua:
        return "browser"
    return user_agent.split("/")[0][:24] or "client"


def _redact(url: str) -> str:
    parts = urlsplit(url)
    query = [(k, "***" if "key" in k.lower() or "token" in k.lower() else v) for k, v in parse_qsl(parts.query)]
    return parts._replace(query=urlencode(query)).geturl()
