"""Session model: rebuilds turns from an OpenAI Realtime event stream.

Pure and deterministic: the same event sequence (live, or replayed from the
store) gives the same snapshot. Inputs are ``(t, source, event)`` where ``t``
is seconds since the session connected and ``source`` is one of:

- ``c2s`` / ``s2c``: Realtime events from the client / server (audio payloads
  already stripped by the hub, replaced by ``_bytes``/``_samples``);
- ``snoop``: synthetic events from s2snoop (mic alignment, audio totals);
- ``llm``: one LLM call captured by the LLM tap;
- ``probe``: speech-to-speech internals from the in-process probe.
"""

from __future__ import annotations

import json
import statistics
from dataclasses import dataclass, field
from typing import Any

from s2snoop.audio import MicMap
from s2snoop.store import loads

LATENCY_KEY = "speech_to_speech.turn_latency"

AUDIO_DELTA = {"response.output_audio.delta", "response.audio.delta"}
TRANSCRIPT_DELTA = {"response.output_audio_transcript.delta", "response.audio_transcript.delta",
                    "response.output_text.delta", "response.text.delta"}
TRANSCRIPT_DONE = {"response.output_audio_transcript.done", "response.audio_transcript.done",
                   "response.output_text.done", "response.text.done"}


@dataclass
class Tool:
    name: str | None
    call_id: str | None
    arguments: str | None
    t_call: float
    output: str | None = None
    t_output: float | None = None

    def to_json(self) -> dict:
        return {"name": self.name, "call_id": self.call_id, "arguments": self.arguments,
                "t_call": self.t_call, "output": self.output, "t_output": self.t_output}


@dataclass
class Response:
    id: str
    t_created: float
    t_first_audio: float | None = None
    t_first_text: float | None = None
    t_done: float | None = None
    status: str | None = None
    text: str = ""
    item_ids: list[str] = field(default_factory=list)
    tools: list[Tool] = field(default_factory=list)
    usage: dict | None = None
    latency: dict | None = None
    audio_samples: int = 0
    audio_rate: int | None = None
    truncate_ms: int | None = None
    cut_t: float | None = None  # estimated cut (barge-in without truncate)

    def to_json(self) -> dict:
        return {"id": self.id, "t_created": self.t_created, "t_first_audio": self.t_first_audio,
                "t_first_text": self.t_first_text, "t_done": self.t_done, "status": self.status,
                "text": self.text, "tools": [t.to_json() for t in self.tools], "usage": self.usage,
                "latency": self.latency, "audio_s": self.audio_s, "truncate_ms": self.truncate_ms}

    @property
    def audio_s(self) -> float:
        return self.audio_samples / self.audio_rate if self.audio_rate else 0.0


@dataclass
class Turn:
    idx: int
    t_start: float
    source: str  # speech | text | image | response
    user_item_id: str | None = None
    speech_start: float | None = None
    speech_end: float | None = None
    t_speech_stopped_evt: float | None = None
    transcript: str = ""
    transcript_final: bool = False
    t_transcript: float | None = None
    texts: list[str] = field(default_factory=list)
    images: list[dict] = field(default_factory=list)
    responses: list[Response] = field(default_factory=list)
    llm_calls: list[dict] = field(default_factory=list)
    tts_segments: list[dict] = field(default_factory=list)
    smart_turn: dict | None = None
    smart_turns: list[dict] = field(default_factory=list)
    interrupted_at: float | None = None

    @property
    def first_audio(self) -> float | None:
        return min((r.t_first_audio for r in self.responses if r.t_first_audio is not None), default=None)

    @property
    def proxy_e2e(self) -> float | None:
        end = self.speech_end
        fa = self.first_audio
        if end is None or fa is None:
            return None
        return max(0.0, fa - end)

    def to_json(self) -> dict:
        usage_in = sum((r.usage or {}).get("input_tokens", 0) or 0 for r in self.responses)
        usage_out = sum((r.usage or {}).get("output_tokens", 0) or 0 for r in self.responses)
        return {
            "idx": self.idx, "t_start": self.t_start, "source": self.source,
            "user_item_id": self.user_item_id, "speech_start": self.speech_start, "speech_end": self.speech_end,
            "transcript": self.transcript, "transcript_final": self.transcript_final,
            "t_transcript": self.t_transcript, "texts": self.texts, "images": self.images,
            "responses": [r.to_json() for r in self.responses], "llm_calls": self.llm_calls,
            "tts_segments": self.tts_segments, "smart_turn": self.smart_turn, "smart_turns": self.smart_turns,
            "interrupted_at": self.interrupted_at, "first_audio": self.first_audio,
            "proxy_e2e": self.proxy_e2e, "tokens_in": usage_in, "tokens_out": usage_out,
        }


class Session:
    def __init__(self, session_id: str, meta: dict | None = None) -> None:
        self.id = session_id
        self.meta = dict(meta or {})
        self.turns: list[Turn] = []
        self.responses: dict[str, Response] = {}
        self.item_to_response: dict[str, str] = {}
        self.pending_calls: dict[str, Tool] = {}
        self.mic = MicMap()
        self.mic_info: dict = {}
        self.input_format: dict | None = None
        self.output_format: dict | None = None
        self.errors: list[dict] = []
        self.t_last = 0.0
        self.n_events = 0
        self.config: dict = {}
        # Backends and models of the speech-to-speech pipeline, from probe C.
        self.models: list[dict] = []

    # ------------------------------------------------------------ helpers
    def _turn(self, t: float, source: str) -> Turn:
        turn = Turn(idx=len(self.turns) + 1, t_start=t, source=source)
        self.turns.append(turn)
        return turn

    def _current_turn(self, t: float, source: str = "response") -> Turn:
        return self.turns[-1] if self.turns else self._turn(t, source)

    def _turn_at(self, t: float) -> Turn | None:
        cur = None
        for turn in self.turns:
            if turn.t_start <= t + 1e-6:
                cur = turn
        return cur

    def _response(self, rid: str | None, t: float) -> Response | None:
        if not rid:
            return None
        resp = self.responses.get(rid)
        if resp is None:
            resp = Response(id=rid, t_created=t)
            self.responses[rid] = resp
            self._current_turn(t).responses.append(resp)
        return resp

    # ------------------------------------------------------------ ingest
    def apply(self, t: float, source: str, ev: dict[str, Any]) -> None:
        self.t_last = max(self.t_last, t)
        self.n_events += 1
        handler = {
            "c2s": self._client, "s2c": self._server, "snoop": self._snoop,
            "llm": self._llm, "probe": self._probe,
        }.get(source)
        if handler:
            handler(t, ev)

    def _client(self, t: float, ev: dict) -> None:
        kind = ev.get("type")
        if kind == "session.update":
            sess = ev.get("session") or {}
            self._formats(sess, provisional=True)
            self.config.update({k: v for k, v in sess.items() if k in ("instructions", "voice", "tools", "model")})
        elif kind == "conversation.item.create":
            item = ev.get("item") or {}
            if item.get("type") == "function_call_output":
                call = self.pending_calls.get(item.get("call_id"))
                if call:
                    call.output = item.get("output")
                    call.t_output = t
            elif item.get("type") == "message" and item.get("role") in ("user", None):
                content = item.get("content") or []
                texts = [c.get("text", "") for c in content if c.get("type") in ("input_text", "text")]
                images = [c for c in content if c.get("type") == "input_image"]
                turn = self.turns[-1] if self.turns and not self.turns[-1].responses else None
                if texts and turn is None:
                    turn = self._turn(t, "text")
                if turn is None:
                    turn = self._current_turn(t, "image")
                turn.texts.extend(texts)
                for img in images:
                    turn.images.append({"t": t, "src": img.get("image_url")})
        elif kind == "conversation.item.truncate":
            rid = self.item_to_response.get(ev.get("item_id"))
            if rid and rid in self.responses:
                self.responses[rid].truncate_ms = int(ev.get("audio_end_ms") or 0)

    def _server(self, t: float, ev: dict) -> None:
        kind = ev.get("type", "")
        if kind in ("session.created", "session.updated"):
            self._formats(ev.get("session") or {}, provisional=False)
            sess = ev.get("session") or {}
            self.config.update({k: v for k, v in sess.items() if k in ("instructions", "voice", "tools", "model")})
        elif kind == "input_audio_buffer.speech_started":
            prev = self.turns[-1] if self.turns else None
            start = self.mic.server_ms_to_t(ev.get("audio_start_ms"))
            if prev and prev.user_item_id and prev.user_item_id == ev.get("item_id"):
                return
            if prev is not None:
                self._maybe_barge_in(prev, start if start is not None else t)
            turn = self._turn(start if start is not None else t, "speech")
            turn.user_item_id = ev.get("item_id")
            turn.speech_start = start if start is not None else t
        elif kind == "input_audio_buffer.speech_stopped":
            turn = self._turn_for_item(ev.get("item_id")) or self._current_turn(t, "speech")
            end = self.mic.server_ms_to_t(ev.get("audio_end_ms"))
            turn.speech_end = end if end is not None else t
            turn.t_speech_stopped_evt = t
        elif kind == "conversation.item.input_audio_transcription.delta":
            turn = self._turn_for_item(ev.get("item_id"))
            if turn and not turn.transcript_final:
                turn.transcript += ev.get("delta") or ""
        elif kind == "conversation.item.input_audio_transcription.completed":
            turn = self._turn_for_item(ev.get("item_id")) or self._current_turn(t, "speech")
            turn.transcript = ev.get("transcript") or ""
            turn.transcript_final = True
            turn.t_transcript = t
        elif kind == "response.created":
            self._response((ev.get("response") or {}).get("id"), t)
        elif kind == "response.output_item.added":
            resp = self._response(ev.get("response_id"), t)
            item = ev.get("item") or {}
            if resp and item.get("id"):
                resp.item_ids.append(item["id"])
                self.item_to_response[item["id"]] = resp.id
        elif kind == "response.output_item.done":
            resp = self._response(ev.get("response_id"), t)
            item = ev.get("item") or {}
            if resp and item.get("type") == "function_call":
                tool = Tool(item.get("name"), item.get("call_id"), item.get("arguments"), t)
                resp.tools.append(tool)
                if tool.call_id:
                    self.pending_calls[tool.call_id] = tool
        elif kind in AUDIO_DELTA:
            resp = self._response(ev.get("response_id"), t)
            if resp:
                if resp.t_first_audio is None:
                    resp.t_first_audio = t
                if ev.get("_samples"):
                    resp.audio_samples += int(ev["_samples"])
                    resp.audio_rate = ev.get("_rate") or resp.audio_rate
        elif kind in TRANSCRIPT_DELTA:
            resp = self._response(ev.get("response_id"), t)
            if resp:
                if resp.t_first_text is None:
                    resp.t_first_text = t
                resp.text += ev.get("delta") or ""
        elif kind in TRANSCRIPT_DONE:
            resp = self._response(ev.get("response_id"), t)
            final = ev.get("transcript") if "transcript" in ev else ev.get("text")
            if resp and final:
                resp.text = final
        elif kind == "response.done":
            body = ev.get("response") or {}
            resp = self._response(body.get("id"), t)
            if resp:
                resp.t_done = t
                resp.status = body.get("status")
                resp.usage = body.get("usage")
                meta = body.get("metadata") or {}
                raw = meta.get(LATENCY_KEY)
                if raw:
                    try:
                        resp.latency = loads(raw) if isinstance(raw, str) else raw
                    except ValueError:
                        pass
                if resp.status == "cancelled" and resp.cut_t is None:
                    turn = self._turn_of_response(resp.id)
                    nxt = self.turns[turn.idx] if turn and turn.idx < len(self.turns) else None
                    if nxt and nxt.speech_start is not None:
                        resp.cut_t = nxt.speech_start
        elif kind == "error":
            err = ev.get("error") or {}
            self.errors.append({"t": t, "message": err.get("message") or str(err), "code": err.get("code")})

    def _snoop(self, t: float, ev: dict) -> None:
        kind = ev.get("type")
        if kind == "snoop.mic_start":
            self.mic.rate = ev.get("rate")
            self.mic.start_s = ev.get("t_start")
            self.mic_info = {k: ev.get(k) for k in ("rate_source", "declared_rate", "measured_rate")}
        elif kind == "snoop.mic_gap":
            self.mic.gaps.append((int(ev["at"]), int(ev["pad"])))
        elif kind == "snoop.response_audio":
            resp = self.responses.get(ev.get("response_id"))
            if resp:
                resp.audio_samples = int(ev.get("samples") or 0)
                resp.audio_rate = ev.get("rate") or resp.audio_rate
        elif kind == "snoop.connection_closed":
            self.meta["ended"] = t
            self.meta["close_reason"] = ev.get("reason")

    def _llm(self, t: float, call: dict) -> None:
        turn = self._turn_at(call.get("t_start", t))
        if turn is None:
            turn = self._current_turn(t)
        turn.llm_calls.append(call)

    def _probe(self, t: float, ev: dict) -> None:
        kind = ev.get("kind")
        if kind == "models":
            self.models = list(ev.get("handlers") or [])
        elif kind == "smart_turn":
            # Smart Turn runs at the VAD pause, before the server emits speech_stopped:
            # it belongs to the latest turn whose speech started before it.
            turn = self._turn_at(t)
            if turn is not None:
                result = {"t": t, "probability": ev.get("probability"), "complete": ev.get("complete"),
                          "duration_s": ev.get("duration_s")}
                turn.smart_turns.append(result)
                turn.smart_turn = result
        elif kind == "tts_input":
            turn = self._turn_at(t)
            if turn is not None:
                turn.tts_segments.append({"t": t, "text": ev.get("text"), "turn_id": ev.get("turn_id")})

    # ------------------------------------------------------------ internals
    def _formats(self, sess: dict, provisional: bool) -> None:
        audio = sess.get("audio") or {}
        inp = (audio.get("input") or {}).get("format") or sess.get("input_audio_format")
        out = (audio.get("output") or {}).get("format") or sess.get("output_audio_format")
        if inp is not None and (not provisional or self.input_format is None):
            self.input_format = inp if isinstance(inp, dict) else {"type": inp}
        if out is not None and (not provisional or self.output_format is None):
            self.output_format = out if isinstance(out, dict) else {"type": out}

    def _turn_for_item(self, item_id: str | None) -> Turn | None:
        if not item_id:
            return None
        for turn in reversed(self.turns):
            if turn.user_item_id == item_id:
                return turn
        return None

    def _turn_of_response(self, rid: str) -> Turn | None:
        for turn in self.turns:
            if any(r.id == rid for r in turn.responses):
                return turn
        return None

    def _maybe_barge_in(self, prev: Turn, t: float) -> None:
        """New speech while the previous answer is still generating or playing.

        A response the server stops (cancelled, or still open) is cut where the
        new speech starts, unless the client later reports the exact
        ``audio_end_ms`` with ``conversation.item.truncate``. speech-to-speech
        may send ``response.done`` (cancelled) before or after ``speech_started``.
        """
        playing = {p["response_id"] for p in self.placements()
                   if p["play_start"] <= t < p["play_start"] + p["audio_s"]}
        for r in prev.responses:
            stopped = r.t_done is None or r.status in ("cancelled", "incomplete")
            if r.t_done is None or r.id in playing:
                prev.interrupted_at = t
            if stopped and r.cut_t is None:
                r.cut_t = t

    # ------------------------------------------------------------ outputs
    def placements(self) -> list[dict]:
        """Estimated playback of each response on the session clock."""
        out = []
        prev_end = 0.0
        ordered = sorted((r for r in self.responses.values() if r.t_first_audio is not None),
                         key=lambda r: r.t_first_audio)
        for r in ordered:
            start = max(r.t_first_audio, prev_end)
            dur = r.audio_s
            estimated = False
            if r.truncate_ms is not None:
                played = min(dur, r.truncate_ms / 1000.0)
            elif r.cut_t is not None and start < r.cut_t < start + dur:
                played = r.cut_t - start
                estimated = True
            else:
                played = dur
            out.append({"response_id": r.id, "play_start": start, "audio_s": dur, "played_s": played,
                        "rate": r.audio_rate, "cut": played < dur - 1e-3, "estimated_cut": estimated})
            prev_end = start + played
        return out

    def stats(self) -> dict:
        e2e = [x.proxy_e2e for x in self.turns if x.proxy_e2e is not None]
        server_e2e = [r.latency.get("e2e_s") for r in self.responses.values()
                      if r.latency and r.latency.get("e2e_s") is not None]
        ttft = [r.latency.get("llm_ttft_s") for r in self.responses.values()
                if r.latency and r.latency.get("llm_ttft_s") is not None]
        tin = sum((r.usage or {}).get("input_tokens", 0) or 0 for r in self.responses.values())
        tout = sum((r.usage or {}).get("output_tokens", 0) or 0 for r in self.responses.values())
        return {
            "turns": len(self.turns),
            "responses": len(self.responses),
            "e2e_p50": _pct(e2e, 50), "e2e_p95": _pct(e2e, 95),
            "server_e2e_p50": _pct(server_e2e, 50),
            "ttft_median": statistics.median(ttft) if ttft else None,
            "tokens_in": tin, "tokens_out": tout,
            "interruptions": sum(1 for x in self.turns if x.interrupted_at is not None),
            "tool_calls": sum(len(r.tools) for r in self.responses.values()),
            "images": sum(len(x.images) for x in self.turns),
            "llm_calls": sum(len(x.llm_calls) for x in self.turns),
            "errors": len(self.errors),
            "duration": self.t_last,
        }

    def snapshot(self) -> dict:
        return {
            "id": self.id, "meta": self.meta, "input_format": self.input_format,
            "output_format": self.output_format, "config": self.config, "models": self.models,
            "mic": {"rate": self.mic.rate, "start_s": self.mic.start_s, **self.mic_info},
            "turns": [x.to_json() for x in self.turns], "placements": self.placements(),
            "errors": self.errors, "stats": self.stats(),
        }


def _pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)
