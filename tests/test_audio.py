import io
import wave

import numpy as np

from s2snoop import audio as A


def test_g711_decode_reference_values():
    ulaw = A.decode(bytes([0xFF, 0x7F, 0x00, 0x80]), A.AudioFormat("ulaw", 8000))
    assert ulaw.tolist() == [0, 0, -32124, 32124]
    alaw = A.decode(bytes([0xD5, 0x55, 0x2A, 0xAA]), A.AudioFormat("alaw", 8000))
    assert alaw.tolist() == [8, -8, -32256, 32256]


def test_parse_formats_ga_and_beta():
    assert A.parse_format({"type": "audio/pcm", "rate": 24000}) == A.AudioFormat("pcm16", 24000, True)
    # speech-to-speech leaves the rate null and then runs at its 16 kHz pipeline rate
    assert A.parse_format({"type": "audio/pcm", "rate": None}) == A.AudioFormat("pcm16", 16000, False)
    assert A.parse_format("pcm16") == A.AudioFormat("pcm16", 24000, True)
    assert A.parse_format("g711_ulaw") == A.AudioFormat("ulaw", 8000, True)
    assert A.parse_format({"type": "audio/pcma"}) == A.AudioFormat("alaw", 8000, True)
    assert A.parse_format(None) is None


def stream(w, fmt, real_rate, start, seconds, chunk_s=0.1, jitter=None):
    """Chunks of ``real_rate`` audio arriving in real time from ``start``."""
    events = []
    n = int(real_rate * chunk_s)
    for i in range(int(round(seconds / chunk_s))):
        t = start + (i + 1) * chunk_s + (jitter(i) if jitter else 0.0)
        events += w.append(np.ones(n, dtype=np.int16), fmt, t)
    return events


def test_mic_writer_pads_real_pauses_and_maps_server_ms(tmp_path):
    fmt = A.AudioFormat("pcm16", 16000, True)
    w = A.MicWriter(tmp_path / "mic.pcm")
    events = stream(w, fmt, 16000, 1.0, 3.0)            # 3 s of audio from t=1.0
    events += stream(w, fmt, 16000, 5.5, 0.5)           # client paused 1.5 s
    events += w.close()
    start = events[0]
    assert start["type"] == "snoop.mic_start" and start["rate"] == 16000 and start["rate_source"] == "declared"
    assert abs(start["t_start"] - 1.0) < 1e-9
    gaps = [e for e in events if e["type"] == "snoop.mic_gap"]
    assert len(gaps) == 1 and gaps[0]["at"] == 48000 and abs(gaps[0]["pad"] - 24000) <= 1
    assert len(A.read_pcm(tmp_path / "mic.pcm")) == 48000 + gaps[0]["pad"] + 8000
    m = A.MicMap(rate=16000, start_s=1.0, gaps=[(48000, 24000)])
    assert m.server_ms_to_t(1000) == 2.0                 # before the pause
    assert abs(m.server_ms_to_t(3100) - 5.6) < 1e-9     # after it: shifted by 1.5 s


def test_mic_rate_is_measured_when_undeclared_or_wrong(tmp_path):
    """Undeclared-rate case: audio/pcm with rate null, really 16 kHz, with Wi-Fi jitter."""
    for declared in (A.AudioFormat("pcm16", 16000, False), A.AudioFormat("pcm16", 24000, True)):
        w = A.MicWriter(tmp_path / f"mic{declared.rate}.pcm")
        jitter = lambda i: 0.3 if i % 7 == 3 else 0.0   # a late chunk now and then, then a burst
        events = stream(w, declared, 16000, 0.0, 10.0, chunk_s=0.064, jitter=jitter) + w.close()
        start = events[0]
        assert start["rate"] == 16000
        assert start["rate_source"] == ("default" if not declared.declared else "measured")
        assert not [e for e in events if e["type"] == "snoop.mic_gap"], "jitter must not insert silence"


def test_short_session_commits_on_close(tmp_path):
    w = A.MicWriter(tmp_path / "mic.pcm")
    events = stream(w, A.AudioFormat("pcm16", 24000, True), 24000, 0.0, 0.3)
    assert events == []
    events = w.close()
    assert events[0]["rate"] == 24000 and len(A.read_pcm(tmp_path / "mic.pcm")) == 7200


def test_render_assistant_places_and_cuts(tmp_path):
    d = tmp_path
    rw = A.ResponseAudioWriter(d / "responses")
    rw.append("r1", np.full(1000, 1000, dtype=np.int16), A.AudioFormat("pcm16", 1000))
    rw.finish("r1")
    placements = [{"response_id": "r1", "play_start": 0.5, "audio_s": 1.0, "played_s": 0.4, "rate": 1000}]
    pcm, rate = A.render_assistant(d, placements)
    assert rate == 1000
    assert pcm[:500].max() == 0
    assert (pcm[500:900] == 1000).all()
    assert pcm[900:].max() == 0


def test_wav_and_peaks():
    pcm = np.concatenate([np.zeros(480, dtype=np.int16), np.full(480, 16384, dtype=np.int16)])
    data = A.wav_bytes(pcm, 24000)
    with wave.open(io.BytesIO(data)) as w:
        assert w.getframerate() == 24000 and w.getnframes() == 960
    assert A.peaks(pcm, 24000) == [0, 127]


def test_fix_rate_strips_wrong_pads_and_sets_rates(tmp_path):
    import json

    from s2snoop.fix_rate import fix_rate
    from s2snoop.store import Store

    store = Store(tmp_path / "snoop.db")
    store.create_session("s", 0.0, {})
    sdir = tmp_path / "sessions" / "s"
    sdir.mkdir(parents=True)
    # 2 s of real 16 kHz audio, recorded as if 24 kHz: a 0.3 s pad inserted after each 0.5 s
    real = np.arange(32000, dtype=np.int16)
    parts, rows = [], [("s", 0.0, "snoop", "snoop.mic_start",
                        json.dumps({"type": "snoop.mic_start", "t_start": 0.0, "rate": 24000}))]
    for k in range(4):
        parts.append(real[k * 8000:(k + 1) * 8000])
        if k < 3:
            parts.append(np.zeros(7200, dtype=np.int16))
            rows.append(("s", (k + 1) * 0.5, "snoop", "snoop.mic_gap",
                         json.dumps({"type": "snoop.mic_gap", "at": (k + 1) * 8000, "pad": 7200})))
    (sdir / "mic.pcm").write_bytes(np.concatenate(parts).tobytes())
    rows.append(("s", 1.0, "snoop", "snoop.response_audio",
                 json.dumps({"type": "snoop.response_audio", "response_id": "r", "samples": 10, "rate": 24000})))
    store.add_events(rows)
    store.commit()

    report = fix_rate(tmp_path, "s", 16000, 16000)
    assert report["mic"] == {"pads_removed": 3, "pads_kept": 0, "seconds": 2.0, "received_seconds": 2.0}
    assert (A.read_pcm(sdir / "mic.pcm") == real).all()
    events = {ev["type"]: ev for _, _, ev in store.iter_events("s")}
    assert events["snoop.mic_start"]["rate"] == 16000 and "snoop.mic_gap" not in events
    assert events["snoop.response_audio"]["rate"] == 16000
