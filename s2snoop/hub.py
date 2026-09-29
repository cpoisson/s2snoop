"""Hub: receives the three streams (Realtime proxy, LLM tap, probe), keeps live
sessions, writes audio and events, and notifies live subscribers.

Everything runs on the asyncio loop thread; producers call the ``ingest_*``
methods from that loop (the proxy relays first, then hands the raw message
over, so recording stays off the relay path).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import mimetypes
import secrets
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from s2snoop import audio as A
from s2snoop.session import AUDIO_DELTA, Session
from s2snoop.store import Store

logger = logging.getLogger("s2snoop.hub")

SERIES_KINDS = {"vad", "span", "queues"}


@dataclass
class Runtime:
    session: Session
    t0: float
    dir: Path
    record_audio: bool
    in_fmt: A.AudioFormat = A.DEFAULT_FORMAT
    out_fmt: A.AudioFormat = A.DEFAULT_FORMAT
    mic: A.MicWriter | None = None
    responses: A.ResponseAudioWriter | None = None
    seen_audio: set[str] = field(default_factory=set)
    series: list[dict] = field(default_factory=list)
    series_seq: int = 0
    image_n: int = 0
    closed: bool = False
    closed_at: float | None = None
    version: int = 0


class Hub:
    def __init__(self, data_dir: Path, record_audio: bool = True, retention_days: float | None = None) -> None:
        self.data_dir = data_dir
        self.store = Store(data_dir / "snoop.db")
        self.record_audio = record_audio
        self.retention_days = retention_days
        self.live: dict[str, Runtime] = {}
        self._events: list[tuple] = []
        self._series: list[tuple] = []
        self._dirty: set[str] = set()
        self._listeners: set[asyncio.Queue] = set()
        self._cache: dict[str, tuple[float, dict, Session]] = {}
        self.orphans = {"llm": 0, "probe": 0}
        self.probe_status: dict | None = None
        self.probe_seen: float | None = None
        self.llm_seen: float | None = None

    # ------------------------------------------------------------ sessions
    def session_dir(self, sid: str) -> Path:
        return self.data_dir / "sessions" / sid

    def open_session(self, meta: dict) -> str:
        now = time.time()
        sid = datetime.fromtimestamp(now).strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(2)
        meta = {**meta, "started_at": now}
        d = self.session_dir(sid)
        rt = Runtime(session=Session(sid, meta), t0=now, dir=d, record_audio=self.record_audio)
        if self.record_audio:
            rt.mic = A.MicWriter(d / "mic.pcm")
            rt.responses = A.ResponseAudioWriter(d / "responses")
        self.live[sid] = rt
        self.store.create_session(sid, now, meta)
        self._touch(sid)
        logger.info("session %s opened (%s)", sid, meta.get("client") or meta.get("remote"))
        return sid

    def close_session(self, sid: str, reason: str) -> None:
        rt = self.live.get(sid)
        if not rt or rt.closed:
            return
        t = time.time() - rt.t0
        if rt.responses:
            for ev in rt.responses.close():
                self._apply(rt, t, "snoop", ev)
        self._apply(rt, t, "snoop", {"type": "snoop.connection_closed", "reason": reason})
        if rt.mic:
            for ev in rt.mic.close():
                self._apply(rt, t, "snoop", ev)
        rt.closed = True
        rt.closed_at = time.time()
        self._persist_session(rt)
        logger.info("session %s closed (%s)", sid, reason)

    # ------------------------------------------------------------ realtime
    def ingest_realtime(self, sid: str, epoch: float, direction: str, raw: str | bytes) -> None:
        rt = self.live.get(sid)
        if rt is None or isinstance(raw, bytes):
            return
        try:
            ev = json.loads(raw)
        except ValueError:
            return
        if not isinstance(ev, dict):
            return
        t = epoch - rt.t0
        kind = ev.get("type", "")
        if direction == "c2s":
            if kind == "input_audio_buffer.append":
                self._mic(rt, t, ev)
                return
            if kind == "session.update":
                self._update_formats(rt, ev.get("session") or {}, provisional=True)
            if kind == "conversation.item.create":
                ev = self._strip_item(rt, ev)
        else:
            if kind in ("session.created", "session.updated"):
                self._update_formats(rt, ev.get("session") or {}, provisional=False)
            if kind in AUDIO_DELTA:
                self._assistant_audio(rt, t, ev)
                return
        self._apply(rt, t, direction, ev)
        if direction == "s2c" and kind == "response.done" and rt.responses:
            rid = (ev.get("response") or {}).get("id")
            done = rt.responses.finish(rid) if rid else None
            if done:
                self._apply(rt, t, "snoop", done)

    def _mic(self, rt: Runtime, t: float, ev: dict) -> None:
        if not rt.mic:
            return
        try:
            raw = base64.b64decode(ev.get("audio") or "")
        except (binascii.Error, ValueError):
            return
        for sev in rt.mic.append(A.decode(raw, rt.in_fmt), rt.in_fmt, t):
            self._apply(rt, t, "snoop", sev)
        self._touch(rt.session.id)

    def _assistant_audio(self, rt: Runtime, t: float, ev: dict) -> None:
        rid = ev.get("response_id")
        samples = 0
        if rid and rt.responses:
            try:
                pcm = A.decode(base64.b64decode(ev.get("delta") or ""), rt.out_fmt)
            except (binascii.Error, ValueError):
                pcm = None
            if pcm is not None:
                rt.responses.append(rid, pcm, rt.out_fmt)
                samples = len(pcm)
        stripped = {k: v for k, v in ev.items() if k != "delta"}
        stripped["_samples"] = samples
        stripped["_rate"] = rt.out_fmt.rate
        first = rid not in rt.seen_audio
        rt.seen_audio.add(rid)
        # Only the first delta per response is persisted (gives t_first_audio on replay);
        # totals are persisted by snoop.response_audio at response.done.
        self._apply(rt, t, "s2c", stripped, persist=first)

    def _update_formats(self, rt: Runtime, sess: dict, provisional: bool) -> None:
        audio = sess.get("audio") or {}
        inp = A.parse_format((audio.get("input") or {}).get("format") or sess.get("input_audio_format"))
        out = A.parse_format((audio.get("output") or {}).get("format") or sess.get("output_audio_format"))
        mic_started = rt.mic is not None and rt.mic.started
        if inp and not mic_started:
            rt.in_fmt = inp
        if out:
            rt.out_fmt = out

    def _strip_item(self, rt: Runtime, ev: dict) -> dict:
        item = ev.get("item") or {}
        content = item.get("content")
        if not isinstance(content, list):
            return ev
        ev = json.loads(json.dumps(ev))
        for part in ev["item"]["content"]:
            if part.get("type") == "input_image":
                url = part.get("image_url") or ""
                if isinstance(url, str) and url.startswith("data:"):
                    part["image_url"] = self._save_image(rt, url)
            if part.get("type") == "input_audio" and part.get("audio"):
                part["audio"] = f"<{len(part['audio']) * 3 // 4} bytes>"
        return ev

    def _save_image(self, rt: Runtime, data_url: str) -> str:
        header, _, b64 = data_url.partition(",")
        mime = header[5:].split(";")[0] or "image/jpeg"
        ext = mimetypes.guess_extension(mime) or ".bin"
        if ext == ".jpe":
            ext = ".jpg"
        rt.image_n += 1
        name = f"images/{rt.image_n:04d}{ext}"
        path = rt.dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            path.write_bytes(base64.b64decode(b64))
        except (binascii.Error, ValueError):
            return "<invalid image>"
        return f"file:{name}"

    # ------------------------------------------------------------ llm / probe
    def _attribute(self, epoch: float) -> Runtime | None:
        best = None
        for rt in self.live.values():
            if rt.t0 - 0.5 <= epoch and (not rt.closed or (rt.closed_at and epoch <= rt.closed_at + 2)):
                if best is None or rt.t0 > best.t0:
                    best = rt
        return best

    def ingest_llm(self, call: dict) -> None:
        self.llm_seen = time.time()
        rt = self._attribute(call.get("t_start", time.time()))
        if rt is None:
            self.orphans["llm"] += 1
            return
        rel = dict(call)
        for k in ("t_start", "t_first_byte", "t_first_token", "t_end"):
            if rel.get(k) is not None:
                rel[k] = rel[k] - rt.t0
        self._apply(rt, rel["t_start"], "llm", rel)

    def ingest_probe(self, ev: dict) -> None:
        self.probe_seen = time.time()
        kind = ev.get("kind")
        if kind == "status":
            self.probe_status = ev
            return
        rt = self._attribute(ev.get("t", time.time()))
        if rt is None:
            self.orphans["probe"] += 1
            return
        if kind in SERIES_KINDS:
            self._add_series(rt, ev)
            return
        rel = dict(ev)
        rel["t"] = ev.get("t", time.time()) - rt.t0
        self._apply(rt, rel["t"], "probe", rel)

    def _add_series(self, rt: Runtime, ev: dict) -> None:
        t0 = rt.t0
        if ev["kind"] == "vad":
            data = {"pipeline": ev.get("pipeline"),
                    "frames": [[round(f[0] - t0, 4), *f[1:]] for f in ev.get("frames", [])]}
            t = data["frames"][0][0] if data["frames"] else ev.get("t", time.time()) - t0
        elif ev["kind"] == "span":
            data = {k: v for k, v in ev.items() if k not in ("kind", "t")}
            for k in ("t_start", "t_first", "t_end"):
                if data.get(k) is not None:
                    data[k] = round(data[k] - t0, 4)
            t = data.get("t_start", 0.0)
        else:
            data = {"depths": ev.get("depths", {}), "pipeline": ev.get("pipeline")}
            t = ev.get("t", time.time()) - t0
        rt.series_seq += 1
        row = {"seq": rt.series_seq, "t": t, "kind": ev["kind"], "data": data}
        rt.series.append(row)
        if len(rt.series) > 20000:
            rt.series = rt.series[-20000:]
        self._series.append((rt.session.id, row["seq"], t, row["kind"], json.dumps(data)))
        self._touch(rt.session.id)

    # ------------------------------------------------------------ core
    def _apply(self, rt: Runtime, t: float, source: str, ev: dict, persist: bool = True) -> None:
        rt.session.apply(t, source, ev)
        rt.version += 1
        if persist:
            kind = ev.get("type") or (f"llm.{ev.get('endpoint', 'call')}" if source == "llm" else ev.get("kind"))
            self._events.append((rt.session.id, t, source, kind, json.dumps(ev)))
        self._touch(rt.session.id)

    def _touch(self, sid: str) -> None:
        self._dirty.add(sid)

    def _persist_session(self, rt: Runtime) -> None:
        s = rt.session
        meta = {**s.meta, "input_format": rt.in_fmt.to_json(), "output_format": rt.out_fmt.to_json(),
                "mic": {"rate": s.mic.rate, "start_s": s.mic.start_s, **s.mic_info}}
        self.store.update_session(s.id, meta, s.stats(), rt.closed_at)

    async def run(self) -> None:
        """Flush loop: persist every 0.5 s, notify listeners, drop closed sessions."""
        last_retention = 0.0
        while True:
            await asyncio.sleep(0.5)
            try:
                self.flush()
                if self.retention_days and time.time() - last_retention > 3600:
                    last_retention = time.time()
                    self.purge(self.retention_days)
            except Exception:  # noqa: BLE001
                logger.exception("flush failed")

    def flush(self) -> None:
        events, self._events = self._events, []
        series, self._series = self._series, []
        self.store.add_events(events)
        self.store.add_series(series)
        dirty, self._dirty = self._dirty, set()
        for sid in dirty:
            rt = self.live.get(sid)
            if rt:
                if rt.mic:
                    rt.mic.flush()
                if rt.responses:
                    rt.responses.flush()
                self._persist_session(rt)
        self.store.commit()
        for sid, rt in list(self.live.items()):
            if rt.closed and rt.closed_at and time.time() - rt.closed_at > 30:
                del self.live[sid]
        if dirty:
            for q in list(self._listeners):
                q.put_nowait(dirty)

    def is_live(self, sid: str) -> bool:
        rt = self.live.get(sid)
        return bool(rt and not rt.closed)

    def delete_session(self, sid: str) -> bool:
        """Delete an ended session: database rows, audio and images. Live sessions are kept."""
        if self.is_live(sid):
            return False
        self.live.pop(sid, None)
        self._cache.pop(sid, None)
        self.store.delete_session(sid)
        shutil.rmtree(self.session_dir(sid), ignore_errors=True)
        self._dirty.add("*")
        return True

    def clear_sessions(self) -> int:
        """Delete every ended session; return how many were deleted."""
        ids = [row["id"] for row in self.store.list_sessions(limit=1_000_000)]
        return sum(self.delete_session(sid) for sid in ids)

    def purge(self, days: float) -> int:
        old = self.store.sessions_older_than(time.time() - days * 86400)
        return sum(self.delete_session(sid) for sid in old)

    # ------------------------------------------------------------ reads
    def listen(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._listeners.add(q)
        return q

    def unlisten(self, q: asyncio.Queue) -> None:
        self._listeners.discard(q)

    def list_sessions(self) -> list[dict]:
        rows = self.store.list_sessions()
        for row in rows:
            rt = self.live.get(row["id"])
            row["live"] = bool(rt and not rt.closed)
            if rt:
                row["stats"] = rt.session.stats()
                row["meta"] = rt.session.meta
        return rows

    def session_obj(self, sid: str) -> tuple[Session, dict] | None:
        rt = self.live.get(sid)
        if rt:
            return rt.session, {"live": not rt.closed, "t0": rt.t0}
        row = self.store.get_session_row(sid)
        if not row:
            return None
        cached = self._cache.get(sid)
        if cached and cached[0] == (row["ended_at"] or 0):
            return cached[2], cached[1]
        s = Session(sid, row["meta"])
        for t, source, ev in self.store.iter_events(sid):
            s.apply(t, source, ev)
        extra = {"live": False, "t0": row["started_at"]}
        self._cache[sid] = ((row["ended_at"] or 0), extra, s)
        if len(self._cache) > 20:
            self._cache.pop(next(iter(self._cache)))
        return s, extra

    def snapshot(self, sid: str) -> dict | None:
        got = self.session_obj(sid)
        if not got:
            return None
        s, extra = got
        snap = s.snapshot()
        snap.update(extra)
        return snap

    def series_since(self, sid: str, after: int) -> list[dict]:
        rt = self.live.get(sid)
        if rt and (not rt.series or rt.series[0]["seq"] <= after + 1):
            return [r for r in rt.series if r["seq"] > after]
        self.flush_series_only()
        return self.store.series_since(sid, after)

    def flush_series_only(self) -> None:
        series, self._series = self._series, []
        self.store.add_series(series)
        self.store.commit()

    def status(self) -> dict:
        now = time.time()
        return {
            "live_sessions": sum(1 for rt in self.live.values() if not rt.closed),
            "probe": {"connected": bool(self.probe_seen and now - self.probe_seen < 5),
                      "status": self.probe_status},
            "llm_tap": {"last_call_s_ago": (now - self.llm_seen) if self.llm_seen else None},
            "orphans": self.orphans,
            "record_audio": self.record_audio,
        }
