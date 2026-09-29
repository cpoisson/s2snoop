"""Audio capture, alignment and rendering.

Time axis: every track is placed on the session clock (seconds since the
client connected). The microphone track is written sample by sample; when the
client pauses its stream (gap in wall-clock arrival), silence is inserted so
the file stays aligned with the wall clock. The server's ``audio_start_ms`` /
``audio_end_ms`` count only received samples, so :class:`MicMap` keeps the
inserted gaps to convert server milliseconds to session time.

Sample rate: clients do not always declare it (``{"type": "audio/pcm", "rate":
null}``), and servers disagree on the default (OpenAI: 24 kHz, speech-to-speech:
16 kHz, see ``api/openai_realtime/handlers/audio.py``). A wrong rate replays the
voice too fast or too slow, so the microphone rate is measured from the stream
itself during the first seconds (clients stream in real time) and wins over a
declaration it contradicts.
"""

from __future__ import annotations

import io
import json
import wave
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

PEAKS_PER_S = 50
# Only real pauses of the client stream are filled with silence; shorter
# network jitter (late chunk, then a burst) must not shift the timeline.
GAP_TOLERANCE_S = 1.0
# ``audio/pcm`` without a rate: speech-to-speech uses its pipeline rate (16 kHz).
# OpenAI always states its rate explicitly, so this default only applies when
# the server itself leaves it unspecified.
UNDECLARED_PCM_RATE = 16000
STANDARD_RATES = (8000, 11025, 16000, 22050, 24000, 32000, 44100, 48000)
RATE_PROBE_S = 3.0

# ---------------------------------------------------------------- formats


@dataclass(frozen=True)
class AudioFormat:
    codec: str = "pcm16"  # pcm16 | ulaw | alaw
    rate: int = UNDECLARED_PCM_RATE
    declared: bool = False  # rate stated explicitly by the client or server

    def to_json(self) -> dict:
        return {"codec": self.codec, "rate": self.rate, "declared": self.declared}


DEFAULT_FORMAT = AudioFormat()


def parse_format(value) -> AudioFormat | None:
    """Parse GA (``{"type": "audio/pcm", "rate": 24000}``) and beta (``"pcm16"``) formats."""
    if value is None:
        return None
    if isinstance(value, str):
        v = value.lower()
        if v == "pcm16":  # beta schema: always 24 kHz
            return AudioFormat("pcm16", 24000, True)
        if v == "audio/pcm":
            return AudioFormat("pcm16", UNDECLARED_PCM_RATE, False)
        if v in ("g711_ulaw", "audio/pcmu"):
            return AudioFormat("ulaw", 8000, True)
        if v in ("g711_alaw", "audio/pcma"):
            return AudioFormat("alaw", 8000, True)
        return None
    if isinstance(value, dict):
        kind = str(value.get("type", "")).lower()
        if kind == "audio/pcm":
            rate = value.get("rate")
            return AudioFormat("pcm16", int(rate), True) if rate else AudioFormat("pcm16", UNDECLARED_PCM_RATE, False)
        if kind == "audio/pcmu":
            return AudioFormat("ulaw", 8000, True)
        if kind == "audio/pcma":
            return AudioFormat("alaw", 8000, True)
    return None


def measure_rate(arrivals: list[tuple[int, float]], window_s: float = 1.0) -> float | None:
    """Samples per second from ``(n_samples, arrival_time)`` of a real-time stream.

    Median over sliding windows of at least ``window_s``: a late chunk followed
    by a burst (network jitter) only skews the few windows that end on it.
    """
    if len(arrivals) < 3:
        return None
    rates = []
    j = 0
    received = 0  # samples that arrived after arrivals[j] and up to arrivals[k]
    for k in range(1, len(arrivals)):
        received += arrivals[k][0]
        while j < k - 1 and arrivals[k][1] - arrivals[j + 1][1] >= window_s:
            j += 1
            received -= arrivals[j][0]
        span = arrivals[k][1] - arrivals[j][1]
        if span >= window_s:
            rates.append(received / span)
    if not rates:
        span = arrivals[-1][1] - arrivals[0][1]
        return sum(n for n, _ in arrivals[1:]) / span if span > 0.3 else None
    rates.sort()
    return rates[len(rates) // 2]


def snap_rate(measured: float) -> int | None:
    """Nearest standard rate within 12 %, else None."""
    best = min(STANDARD_RATES, key=lambda r: abs(r - measured))
    return best if abs(best - measured) / best < 0.12 else None


def _ulaw_table() -> np.ndarray:
    u = np.arange(256, dtype=np.int32)
    u = ~u & 0xFF
    sign = u & 0x80
    exponent = (u >> 4) & 0x07
    mantissa = u & 0x0F
    sample = ((mantissa << 3) + 0x84) << exponent
    sample -= 0x84
    return np.where(sign != 0, -sample, sample).astype(np.int16)


def _alaw_table() -> np.ndarray:
    a = np.arange(256, dtype=np.int32) ^ 0x55
    sign = a & 0x80
    exponent = (a >> 4) & 0x07
    mantissa = a & 0x0F
    sample = np.where(exponent == 0, (mantissa << 4) + 8, ((mantissa << 4) + 0x108) << np.maximum(exponent - 1, 0))
    return np.where(sign != 0, sample, -sample).astype(np.int16)


_ULAW = _ulaw_table()
_ALAW = _alaw_table()


def decode(raw: bytes, fmt: AudioFormat) -> np.ndarray:
    """Decode wire audio to int16 samples."""
    if fmt.codec == "ulaw":
        return _ULAW[np.frombuffer(raw, dtype=np.uint8)]
    if fmt.codec == "alaw":
        return _ALAW[np.frombuffer(raw, dtype=np.uint8)]
    if len(raw) % 2:
        raw = raw[:-1]
    return np.frombuffer(raw, dtype="<i2")


# ---------------------------------------------------------------- mic map


@dataclass
class MicMap:
    """Maps server-side input milliseconds to session seconds."""

    rate: int | None = None
    start_s: float | None = None  # session time of the first microphone sample
    gaps: list[tuple[int, int]] = field(default_factory=list)  # (received_samples, inserted_samples)

    def server_ms_to_t(self, ms: float | None) -> float | None:
        if ms is None or self.rate is None or self.start_s is None:
            return None
        samples = ms * self.rate / 1000.0
        inserted = sum(pad for at, pad in self.gaps if at <= samples)
        return self.start_s + (samples + inserted) / self.rate


# ---------------------------------------------------------------- writers


class MicWriter:
    """Appends microphone audio with gap padding. Emits synthetic events for :class:`MicMap`.

    The first ``RATE_PROBE_S`` seconds are buffered to measure the real sample
    rate from arrival times before anything is written.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.fmt: AudioFormat | None = None
        self.received = 0
        self.written = 0
        self.start_s: float | None = None
        self._fh = None
        self._probe: list[tuple[np.ndarray, AudioFormat, float]] = []

    @property
    def started(self) -> bool:
        return self.fmt is not None

    def append(self, samples: np.ndarray, fmt: AudioFormat, t: float) -> list[dict]:
        """Write ``samples`` that arrived at session time ``t``; return synthetic events."""
        if len(samples) == 0:
            return []
        if self.fmt is None:
            self._probe.append((samples, fmt, t))
            if t - self._probe[0][2] < RATE_PROBE_S:
                return []
            return self._commit()
        return self._write(samples, t)

    def flush_probe(self) -> list[dict]:
        """Session ended before the probe window: commit with what we have."""
        return self._commit() if self._probe and self.fmt is None else []

    def _commit(self) -> list[dict]:
        chunks, self._probe = self._probe, []
        declared = chunks[-1][1]
        measured = measure_rate([(len(c[0]), c[2]) for c in chunks])
        rate, source = declared.rate, "declared" if declared.declared else "default"
        snapped = snap_rate(measured) if measured else None
        if snapped and snapped != declared.rate:
            rate, source = snapped, "measured"
        self.fmt = AudioFormat(declared.codec, rate, declared.declared)
        first, _, t_first = chunks[0]
        self.start_s = max(0.0, t_first - len(first) / rate)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "ab")
        events = [{"type": "snoop.mic_start", "t_start": self.start_s, "rate": rate, "codec": declared.codec,
                   "rate_source": source, "declared_rate": declared.rate if declared.declared else None,
                   "measured_rate": round(measured) if measured else None}]
        for samples, _, t in chunks:
            events += self._write(samples, t)
        return events

    def _write(self, samples: np.ndarray, t: float) -> list[dict]:
        events: list[dict] = []
        n = len(samples)
        rate = self.fmt.rate
        chunk_start = t - n / rate
        expected = self.start_s + self.written / rate
        late = chunk_start - expected
        if late > GAP_TOLERANCE_S:
            pad = int(round(late * rate))
            self._fh.write(b"\x00\x00" * pad)
            self.written += pad
            events.append({"type": "snoop.mic_gap", "at": self.received, "pad": pad})
        self._fh.write(samples.astype("<i2").tobytes())
        self.received += n
        self.written += n
        return events

    def flush(self) -> None:
        if self._fh:
            self._fh.flush()

    def close(self) -> list[dict]:
        events = self.flush_probe()
        if self._fh:
            self._fh.close()
            self._fh = None
        return events


class ResponseAudioWriter:
    """One raw PCM file per response: the full generated TTS."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._files: dict[str, object] = {}
        self.samples: dict[str, int] = {}
        self.rates: dict[str, int] = {}

    def append(self, response_id: str, samples: np.ndarray, fmt: AudioFormat) -> None:
        fh = self._files.get(response_id)
        if fh is None:
            self.directory.mkdir(parents=True, exist_ok=True)
            fh = open(self.directory / f"{_safe(response_id)}.pcm", "ab")
            self._files[response_id] = fh
            self.rates[response_id] = fmt.rate
            self.samples[response_id] = 0
        fh.write(samples.astype("<i2").tobytes())
        self.samples[response_id] += len(samples)

    def finish(self, response_id: str) -> dict | None:
        fh = self._files.pop(response_id, None)
        if fh is None:
            return None
        fh.close()
        return {"type": "snoop.response_audio", "response_id": response_id,
                "samples": self.samples.get(response_id, 0), "rate": self.rates.get(response_id, 24000)}

    def flush(self) -> None:
        for fh in self._files.values():
            fh.flush()

    def close(self) -> list[dict]:
        return [e for rid in list(self._files) if (e := self.finish(rid))]


def _safe(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in name)


# ---------------------------------------------------------------- rendering


def read_pcm(path: Path) -> np.ndarray:
    if not path.exists():
        return np.zeros(0, dtype=np.int16)
    data = path.read_bytes()
    if len(data) % 2:
        data = data[:-1]
    return np.frombuffer(data, dtype="<i2")


def wav_bytes(samples: np.ndarray, rate: int) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(samples.astype("<i2").tobytes())
    return buf.getvalue()


def render_mic(session_dir: Path, mic: MicMap) -> tuple[np.ndarray, int]:
    """Microphone track on the session clock (leading silence up to the first sample)."""
    rate = mic.rate or 24000
    pcm = read_pcm(session_dir / "mic.pcm")
    lead = int(round((mic.start_s or 0.0) * rate))
    return np.concatenate([np.zeros(lead, dtype=np.int16), pcm]), rate


def render_assistant(session_dir: Path, placements: list[dict], rate: int | None = None) -> tuple[np.ndarray, int]:
    """Assistant track as heard: each response placed at ``play_start`` and cut at ``played_s``."""
    rates = [p["rate"] for p in placements if p.get("rate")]
    rate = rate or (rates[0] if rates else 24000)
    end = max((p["play_start"] + p["played_s"] for p in placements), default=0.0)
    out = np.zeros(int(round(end * rate)) + 1, dtype=np.int32)
    for p in placements:
        pcm = read_pcm(session_dir / "responses" / f"{_safe(p['response_id'])}.pcm")
        if p.get("rate") and p["rate"] != rate:
            pcm = resample(pcm, p["rate"], rate)
        n = min(len(pcm), int(round(p["played_s"] * rate)))
        start = int(round(p["play_start"] * rate))
        seg = pcm[:n].astype(np.int32)
        stop = min(len(out), start + len(seg))
        out[start:stop] += seg[: stop - start]
    return np.clip(out, -32768, 32767).astype(np.int16), rate


def render_response(session_dir: Path, response_id: str, rate: int) -> np.ndarray:
    return read_pcm(session_dir / "responses" / f"{_safe(response_id)}.pcm")


def resample(pcm: np.ndarray, src: int, dst: int) -> np.ndarray:
    if src == dst or len(pcm) == 0:
        return pcm
    n = int(round(len(pcm) * dst / src))
    x = np.linspace(0, len(pcm) - 1, n)
    return np.interp(x, np.arange(len(pcm)), pcm.astype(np.float32)).astype(np.int16)


def peaks(pcm: np.ndarray, rate: int, per_s: int = PEAKS_PER_S) -> list[int]:
    """Max absolute amplitude per window, scaled to 0..255."""
    if len(pcm) == 0:
        return []
    win = max(1, rate // per_s)
    n = len(pcm) // win
    if n == 0:
        return [int(min(255, np.abs(pcm.astype(np.int32)).max() * 255 // 32768))]
    blocks = np.abs(pcm[: n * win].astype(np.int32)).reshape(n, win).max(axis=1)
    return (np.minimum(blocks * 255 // 32768, 255)).astype(int).tolist()


def mic_peaks_from(session_dir: Path, from_sample: int, rate: int) -> tuple[int, list[int]]:
    """Peaks for the raw mic file starting at ``from_sample`` (window aligned)."""
    win = max(1, rate // PEAKS_PER_S)
    from_sample -= from_sample % win
    path = session_dir / "mic.pcm"
    if not path.exists():
        return from_sample, []
    with open(path, "rb") as fh:
        fh.seek(from_sample * 2)
        data = fh.read()
    usable = (len(data) // 2 // win) * win
    pcm = np.frombuffer(data[: usable * 2], dtype="<i2")
    return from_sample, peaks(pcm, rate)


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj))
    tmp.replace(path)
