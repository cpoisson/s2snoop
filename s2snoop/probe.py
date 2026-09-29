"""C · in-process probe for speech-to-speech (stdlib only).

Run with the speech-to-speech interpreter::

    python -m s2snoop.probe serve --port 8766 ...

It patches a few speech-to-speech internals at start-up (no file is modified),
then runs the normal ``speech-to-speech`` CLI. Captured data is sent as JSON
datagrams to s2snoop (``S2SNOOP_PROBE``, default 127.0.0.1:8799);
if nobody listens, datagrams are simply dropped.

Hooks (each one is checked and skipped with a warning if the target moved):

- ``vad``: Silero speech probability per frame, threshold, triggered state
  (``speech_to_speech.VAD.vad_iterator.VADIterator``);
- ``smart_turn``: end-of-turn probability (``speech_to_speech.VAD.smart_turn.SmartTurnAnalyzer.predict``);
- ``handlers``: per-handler processing spans and text sent to TTS
  (``speech_to_speech.baseHandler.BaseHandler``);
- ``queues``: input queue depth of every handler, sampled every 50 ms;
- ``models``: the setup configuration of every handler (backend class, model id, voice,
  language, VAD thresholds…), taken from ``BaseHandler`` ``setup_kwargs`` and sent every 2 s.
  Secret-looking keys are dropped and URLs keep only scheme, host and path.
"""

from __future__ import annotations

import json
import math
import os
import socket
import sys
import threading
import time
import weakref
from urllib.parse import urlsplit

TESTED_WITH = "speech-to-speech @ c60efc4 (2026-09)"


class Emitter:
    def __init__(self, target: str) -> None:
        host, _, port = target.rpartition(":")
        self.addr = (host or "127.0.0.1", int(port))
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)
        self.lock = threading.Lock()

    def send(self, obj: dict) -> None:
        try:
            data = json.dumps(obj, separators=(",", ":")).encode()
            if len(data) > 60000:
                return
            with self.lock:
                self.sock.sendto(data, self.addr)
        except (OSError, TypeError, ValueError):
            pass


EMIT: Emitter | None = None
HOOKS: dict[str, str] = {}


def _warn(msg: str) -> None:
    print(f"[s2snoop probe] {msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------- VAD


class _VadBatcher:
    """Batches VAD frames per iterator, flushed every 100 ms."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.frames: dict[int, list] = {}
        self.pipeline: dict[int, object] = {}

    def add(self, key: int, pipeline, frame: list) -> None:
        with self.lock:
            self.frames.setdefault(key, []).append(frame)
            self.pipeline[key] = pipeline

    def flush(self) -> None:
        with self.lock:
            batches, self.frames = self.frames, {}
        for key, frames in batches.items():
            if frames:
                EMIT.send({"kind": "vad", "t": frames[0][0], "pipeline": self.pipeline.get(key), "frames": frames})


VAD = _VadBatcher()


class _ModelTap:
    """Wraps the Silero model so the iterator's local ``speech_prob`` is observable."""

    def __init__(self, model) -> None:
        object.__setattr__(self, "_model", model)
        object.__setattr__(self, "last", None)

    def __call__(self, *args, **kwargs):
        out = self._model(*args, **kwargs)
        try:
            object.__setattr__(self, "last", float(out.item()))
        except Exception:  # noqa: BLE001
            pass
        return out

    def __getattr__(self, name):
        return getattr(self._model, name)

    def __setattr__(self, name, value):
        setattr(self._model, name, value)


def hook_vad() -> None:
    from speech_to_speech.VAD import vad_iterator

    cls = vad_iterator.VADIterator
    for attr in ("__init__", "__call__"):
        if not hasattr(cls, attr):
            raise AttributeError(f"VADIterator.{attr}")
    orig_init, orig_call = cls.__init__, cls.__call__

    def __init__(self, *args, **kwargs):
        orig_init(self, *args, **kwargs)
        if not hasattr(self, "model") or not hasattr(self, "threshold"):
            raise AttributeError("VADIterator.model/threshold")
        self.model = _ModelTap(self.model)

    def __call__(self, x, *args, **kwargs):
        result = orig_call(self, x, *args, **kwargs)
        tap = self.__dict__.get("model")
        prob = getattr(tap, "last", None) if isinstance(tap, _ModelTap) else None
        if prob is not None:
            pipeline = _current_pipeline()
            VAD.add(id(self), pipeline, [round(time.time(), 4), round(prob, 3), round(float(self.threshold), 3),
                                         1 if getattr(self, "triggered", False) else 0,
                                         1 if result is not None else 0])
        return result

    cls.__init__ = __init__
    cls.__call__ = __call__


def hook_smart_turn() -> None:
    from speech_to_speech.VAD import smart_turn

    cls = smart_turn.SmartTurnAnalyzer
    orig = cls.predict

    def predict(self, *args, **kwargs):
        t0 = time.time()
        result = orig(self, *args, **kwargs)
        EMIT.send({"kind": "smart_turn", "t": time.time(), "pipeline": _current_pipeline(),
                   "probability": round(float(getattr(result, "probability", -1)), 4),
                   "complete": bool(getattr(result, "complete", False)),
                   "duration_s": round(time.time() - t0, 4)})
        return result

    cls.predict = predict


# ---------------------------------------------------------------- handlers

HANDLERS: "weakref.WeakSet" = weakref.WeakSet()

_SECRET = ("key", "token", "secret", "password", "auth", "credential")
_STAGES = (("VAD", "vad"), ("STT", "stt"), ("ASR", "stt"), ("TTS", "tts"), ("LanguageModel", "llm"),
           ("Responses", "llm"), ("ChatCompletions", "llm"), ("LLM", "llm"))


def _stage(name: str) -> str:
    for needle, stage in _STAGES:
        if needle in name:
            return stage
    return "other"


def _describe(setup_kwargs) -> dict:
    """Keep what identifies a handler's model: scalar setup kwargs, minus secrets."""
    out: dict = {}
    if not isinstance(setup_kwargs, dict):
        return out
    for key, value in setup_kwargs.items():
        if not isinstance(key, str) or any(s in key.lower() for s in _SECRET):
            continue
        if isinstance(value, str) and "://" in value:
            parts = urlsplit(value)
            value = f"{parts.scheme}://{parts.hostname or ''}{f':{parts.port}' if parts.port else ''}{parts.path}"
        if isinstance(value, float) and not math.isfinite(value):
            value = str(value)  # "inf" / "nan": JSON has no such numbers
        if value is None or isinstance(value, (bool, int, float)) or (isinstance(value, str) and len(value) <= 200):
            out[key] = value
        if len(out) >= 40:
            break
    return out


def models_snapshot() -> list:
    rows = []
    for h in list(HANDLERS):
        config = h.__dict__.get("_snoop_config")
        if config is None:
            continue
        name = type(h).__name__
        rows.append({"handler": name, "stage": _stage(name), "pipeline": getattr(h, "pipeline_index", None),
                     "config": config})
    order = {"vad": 0, "stt": 1, "llm": 2, "tts": 3, "other": 4}
    rows.sort(key=lambda r: (str(r["pipeline"]), order[r["stage"]], r["handler"]))
    return rows


def _current_pipeline():
    try:
        from speech_to_speech.pipeline.log_context import pipeline_log_ctx

        return pipeline_log_ctx.get()
    except Exception:  # noqa: BLE001
        return None


def hook_handlers() -> None:
    from speech_to_speech import baseHandler

    cls = baseHandler.BaseHandler
    orig_init = cls.__init__

    def __init__(self, *args, **kwargs):
        # BaseHandler(stop_event, queue_in, queue_out, setup_args=(), setup_kwargs={})
        setup_kwargs = kwargs.get("setup_kwargs", args[4] if len(args) > 4 else None)
        orig_init(self, *args, **kwargs)
        try:
            self.__dict__["_snoop_config"] = _describe(setup_kwargs)
        except Exception:  # noqa: BLE001
            pass
        HANDLERS.add(self)
        process = self.process
        name = type(self).__name__

        def traced(item, _process=process, _name=name, _self=self):
            t_start = time.time()
            text = getattr(item, "text", None) if type(item).__name__ == "TTSInput" else None
            turn_id = getattr(item, "turn_id", None)
            if text:
                EMIT.send({"kind": "tts_input", "t": t_start, "text": text[:2000], "turn_id": turn_id,
                           "pipeline": getattr(_self, "pipeline_index", None)})
            t_first = None
            n = 0
            try:
                for out in _process(item):
                    if t_first is None:
                        t_first = time.time()
                    n += 1
                    yield out
            finally:
                t_end = time.time()
                # Skip no-output micro calls (e.g. VAD on every audio chunk).
                if n or t_end - t_start > 0.02:
                    EMIT.send({"kind": "span", "t": t_start, "handler": _name, "input": type(item).__name__,
                               "t_start": t_start, "t_first": t_first, "t_end": t_end, "n_out": n,
                               "turn_id": turn_id, "pipeline": getattr(_self, "pipeline_index", None)})

        self.process = traced

    cls.__init__ = __init__


def queue_sampler(stop: threading.Event) -> None:
    last: dict = {}
    last_sent = 0.0
    tick = 0
    while not stop.wait(0.05):
        tick += 1
        if tick % 2 == 0:
            VAD.flush()
        depths: dict[str, dict] = {}
        for h in list(HANDLERS):
            q = getattr(h, "queue_in", None)
            if q is None:
                continue
            try:
                size = q.qsize()
            except Exception:  # noqa: BLE001
                continue
            pipeline = str(getattr(h, "pipeline_index", None))
            depths.setdefault(pipeline, {})[type(h).__name__] = size
        now = time.time()
        if depths != last or now - last_sent > 1.0:
            for pipeline, d in depths.items():
                EMIT.send({"kind": "queues", "t": now, "pipeline": pipeline, "depths": d})
            last, last_sent = depths, now


def heartbeat(stop: threading.Event) -> None:
    while not stop.wait(2.0):
        EMIT.send({"kind": "status", "t": time.time(), "hooks": HOOKS, "tested_with": TESTED_WITH,
                   "pid": os.getpid()})
        if HOOKS.get("models") == "on":
            rows = models_snapshot()
            if rows:
                EMIT.send({"kind": "models", "t": time.time(), "handlers": rows})


# ---------------------------------------------------------------- main


def install() -> None:
    global EMIT
    EMIT = Emitter(os.environ.get("S2SNOOP_PROBE", "127.0.0.1:8799"))
    for name, fn in (("vad", hook_vad), ("smart_turn", hook_smart_turn), ("handlers", hook_handlers)):
        try:
            fn()
            HOOKS[name] = "on"
        except Exception as exc:  # noqa: BLE001
            HOOKS[name] = f"off: {type(exc).__name__}: {exc}"
            _warn(f"{name}: disabled ({type(exc).__name__}: {exc})")
    HOOKS["queues"] = HOOKS.get("handlers", "off")
    HOOKS["models"] = HOOKS.get("handlers", "off")
    stop = threading.Event()
    threading.Thread(target=queue_sampler, args=(stop,), daemon=True, name="snoop-queues").start()
    threading.Thread(target=heartbeat, args=(stop,), daemon=True, name="snoop-heartbeat").start()
    EMIT.send({"kind": "status", "t": time.time(), "hooks": HOOKS, "tested_with": TESTED_WITH, "pid": os.getpid()})
    _warn("hooks: " + ", ".join(f"{k}={v}" for k, v in HOOKS.items()))


def main() -> None:
    install()
    from speech_to_speech.cli import main as s2s_main

    sys.argv = ["speech-to-speech", *sys.argv[1:]]
    sys.exit(s2s_main())


if __name__ == "__main__":
    main()
