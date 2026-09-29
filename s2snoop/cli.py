"""Command line: run the proxies + dashboard, or launch speech-to-speech with the probe.

    s2snoop [--listen 0.0.0.0:8765] [--upstream ws://127.0.0.1:8766] \
                   [--llm-listen 127.0.0.1:8081 --llm-upstream http://host:8080] \
                   [--ui 127.0.0.1:8767] [--route name=wss://host] [--no-audio]

    s2snoop s2s [--s2s-python PATH] -- serve --port 8766 ...
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import socket
import sys
from pathlib import Path

import uvicorn

from s2snoop.hub import Hub
from s2snoop.llm_tap import create_llm_tap_app
from s2snoop.proxy import Router, create_proxy_app
from s2snoop.web import create_web_app

logger = logging.getLogger("s2snoop")


def _hostport(value: str) -> tuple[str, int]:
    host, _, port = value.rpartition(":")
    return host or "127.0.0.1", int(port)


class ProbeProtocol(asyncio.DatagramProtocol):
    def __init__(self, hub: Hub) -> None:
        self.hub = hub

    def datagram_received(self, data: bytes, addr) -> None:
        try:
            ev = json.loads(data)
        except ValueError:
            return
        if isinstance(ev, dict):
            self.hub.ingest_probe(ev)


async def serve(args: argparse.Namespace) -> None:
    hub = Hub(Path(args.data), record_audio=not args.no_audio, retention_days=args.retention_days)
    routes = dict(r.split("=", 1) for r in args.route)
    info = {"listen": args.listen, "upstream": args.upstream, "routes": routes, "ui": args.ui,
            "llm_listen": args.llm_listen, "llm_upstream": args.llm_upstream, "probe_port": args.probe_port,
            "tls": bool(args.tls_cert)}
    servers = []

    def add(app, hostport: str, tls: bool = False) -> None:
        host, port = _hostport(hostport)
        ssl = {"ssl_certfile": args.tls_cert, "ssl_keyfile": args.tls_key} if tls and args.tls_cert else {}
        config = uvicorn.Config(app, host=host, port=port, log_level="warning", ws_max_size=64 * 1024 * 1024,
                                ws_ping_interval=None, lifespan="on", **ssl)
        servers.append(uvicorn.Server(config))

    proxy_app = create_proxy_app(hub, Router(args.upstream, routes))
    add(proxy_app, args.listen)
    add(create_web_app(hub, info, proxy_app.state.ws_relay), args.ui, tls=True)
    if args.llm_upstream:
        add(create_llm_tap_app(hub, args.llm_upstream), args.llm_listen)

    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(lambda: ProbeProtocol(hub), local_addr=("127.0.0.1", args.probe_port))

    ui_host, ui_port = _hostport(args.ui)
    lan = _lan_ip()
    print("\ns2snoop")
    print(f"  proxy Realtime  ws://{args.listen}  →  {args.upstream}")
    for name, url in routes.items():
        print(f"    route {name:<8}  /{name}/…  →  {url}")
    if args.llm_upstream:
        print(f"  proxy LLM       http://{args.llm_listen}  →  {args.llm_upstream}")
    print(f"  probe (UDP)     127.0.0.1:{args.probe_port}")
    scheme = "https" if args.tls_cert else "http"
    print(f"  dashboard       {scheme}://localhost:{ui_port}" + (f"   ·   {scheme}://{lan}:{ui_port}" if lan and ui_host
                                                                 in ("0.0.0.0", "::") else ""))
    if not args.tls_cert and ui_host not in ("127.0.0.1", "localhost", "::1"):
        print("                  (browser mic works on localhost only: add --tls-cert/--tls-key for other devices)")
    print(f"  data            {Path(args.data).resolve()}" + ("  (audio recording off)" if args.no_audio else ""))
    print(flush=True)

    hub_task = asyncio.create_task(hub.run())
    try:
        await asyncio.gather(*(s.serve() for s in servers))
    finally:
        hub_task.cancel()
        for rt in list(hub.live.values()):
            hub.close_session(rt.session.id, "s2snoop stopped")
        hub.flush()


def _lan_ip() -> str | None:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("192.0.2.1", 9))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return None


def _s2s_python(explicit: str | None) -> str:
    if explicit:
        return explicit
    exe = shutil.which("speech-to-speech")
    if exe:
        with open(exe, "rb") as fh:
            first = fh.readline().decode(errors="ignore").strip()
        if first.startswith("#!"):
            return first[2:].strip().split()[0]
    sys.exit("speech-to-speech not found on PATH: pass --s2s-python /path/to/.venv/bin/python")


def run_s2s(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(prog="s2snoop s2s",
                                     description="Run speech-to-speech with the s2snoop probe.")
    parser.add_argument("--s2s-python", help="speech-to-speech interpreter (default: the one on PATH)")
    parser.add_argument("--probe", default="127.0.0.1:8799", help="s2snoop UDP address")
    if "--" in argv:
        i = argv.index("--")
        own, rest = argv[:i], argv[i + 1:]
    else:
        own, rest = [], argv
    args = parser.parse_args(own)
    python = _s2s_python(args.s2s_python)
    env = dict(os.environ)
    pkg_parent = str(Path(__file__).resolve().parent.parent)
    env["PYTHONPATH"] = pkg_parent + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["S2SNOOP_PROBE"] = args.probe
    print(f"[s2snoop] speech-to-speech via {python} + probe → udp://{args.probe}", flush=True)
    os.execve(python, [python, "-m", "s2snoop.probe", *rest], env)


def run_fix_rate(argv: list[str]) -> None:
    from s2snoop.fix_rate import fix_rate

    parser = argparse.ArgumentParser(prog="s2snoop fix-rate",
                                     description="Repair a session recorded with the wrong sample rate.")
    parser.add_argument("session", help="session id (see the dashboard or data/sessions/)")
    parser.add_argument("--input", type=int, default=16000, help="real microphone rate (default 16000)")
    parser.add_argument("--output", type=int, default=16000, help="real assistant audio rate (default 16000)")
    parser.add_argument("--data", default=str(Path.cwd() / "data"), help="data directory")
    args = parser.parse_args(argv)
    print(json.dumps(fix_rate(Path(args.data), args.session, args.input, args.output), indent=2))
    print("Restart s2snoop if it is running so it reloads the session.")


def run_clear(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(prog="s2snoop clear",
                                     description="Delete all recorded sessions (database rows, audio, images).")
    parser.add_argument("--data", default=str(Path.cwd() / "data"), help="data directory")
    parser.add_argument("-y", "--yes", action="store_true", help="do not ask for confirmation")
    args = parser.parse_args(argv)
    hub = Hub(Path(args.data))
    n = len(hub.store.list_sessions(limit=1_000_000))
    if n == 0:
        print("No session to delete.")
        return
    if not args.yes and input(f"Delete {n} session(s) in {Path(args.data).resolve()}? [y/N] ").strip().lower() != "y":
        print("Cancelled.")
        return
    print(f"Deleted {hub.clear_sessions()} session(s).")
    print("If s2snoop is running, a session that is live right now is kept only by the running instance: "
          "prefer the dashboard button while it runs.")


def main() -> None:
    if len(sys.argv) > 1 and sys.argv[1] in ("--version", "-V"):
        from s2snoop import __version__
        print(f"s2snoop {__version__}")
        return
    if len(sys.argv) > 1 and sys.argv[1] == "clear":
        run_clear(sys.argv[2:])
        return
    if len(sys.argv) > 1 and sys.argv[1] == "s2s":
        run_s2s(sys.argv[2:])
        return
    if len(sys.argv) > 1 and sys.argv[1] == "fix-rate":
        run_fix_rate(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(prog="s2snoop",
                                     description="Observe OpenAI Realtime sessions live.")
    parser.add_argument("--listen", default="0.0.0.0:8765", help="Realtime proxy address (default 0.0.0.0:8765)")
    parser.add_argument("--upstream", default="ws://127.0.0.1:8766", help="default Realtime server")
    parser.add_argument("--route", action="append", default=[], metavar="NAME=URL",
                        help="named route: ws://proxy/NAME/v1/realtime → URL/v1/realtime (repeatable)")
    parser.add_argument("--llm-listen", default="127.0.0.1:8081", help="LLM proxy address")
    parser.add_argument("--llm-upstream", default=None, help="OpenAI-compatible LLM server to proxy (e.g. http://llm-host:8080)")
    parser.add_argument("--ui", default="127.0.0.1:8767", help="dashboard address (default 127.0.0.1:8767; no auth, bind wider with care)")
    parser.add_argument("--tls-cert", default=None, help="serve the dashboard over https (needed for the browser mic "
                        "on other devices)")
    parser.add_argument("--tls-key", default=None, help="private key for --tls-cert")
    parser.add_argument("--probe-port", type=int, default=8799, help="probe UDP port")
    parser.add_argument("--data", default=str(Path.cwd() / "data"), help="data directory")
    parser.add_argument("--no-audio", action="store_true", help="do not record audio")
    parser.add_argument("--retention-days", type=float, default=None, help="delete sessions older than N days")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    if bool(args.tls_cert) != bool(args.tls_key):
        parser.error("--tls-cert and --tls-key go together")
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
