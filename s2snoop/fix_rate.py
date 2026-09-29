"""Repair a session recorded with the wrong sample rate.

Before the rate was measured from the stream, an undeclared ``audio/pcm`` rate
was taken as 24 kHz. For a 16 kHz client (e.g. a robot talking to speech-to-speech)
this replayed both voices 1.5x too fast, and made every chunk look late, so
silence was inserted all along the microphone track.

This rewrites ``mic.pcm`` without those pads (real pauses are re-detected with
the right rate from the stored arrival times), then fixes the rates stored in
the events and the session row. The original file is kept as ``mic.orig.pcm``.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import numpy as np

from s2snoop import audio as A


def fix_rate(data_dir: Path, sid: str, input_rate: int | None, output_rate: int | None) -> dict:
    db = sqlite3.connect(str(data_dir / "snoop.db"))
    row = db.execute("SELECT meta FROM sessions WHERE id=?", (sid,)).fetchone()
    if not row:
        raise SystemExit(f"unknown session {sid}")
    meta = json.loads(row[0])
    sdir = data_dir / "sessions" / sid
    report: dict = {"session": sid}

    if input_rate:
        start = db.execute("SELECT id, data FROM events WHERE session_id=? AND type='snoop.mic_start'",
                           (sid,)).fetchone()
        gaps = db.execute("SELECT id, t, data FROM events WHERE session_id=? AND type='snoop.mic_gap' ORDER BY id",
                          (sid,)).fetchall()
        if start and json.loads(start[1]).get("rate_source") == "fixed":
            raise SystemExit(f"{sid}: microphone already fixed (original kept in mic.orig.pcm)")
        mic_path = sdir / "mic.pcm"
        orig = sdir / "mic.orig.pcm"
        if not orig.exists() and mic_path.exists():
            shutil.copyfile(mic_path, orig)
        padded = A.read_pcm(orig)
        # 1. strip every pad that was inserted at the wrong rate
        pieces, pos, prev_at = [], 0, 0
        for _, _, data in gaps:
            g = json.loads(data)
            take = g["at"] - prev_at
            pieces.append(padded[pos:pos + take])
            pos += take + g["pad"]
            prev_at = g["at"]
        pieces.append(padded[pos:])
        raw = np.concatenate(pieces) if pieces else padded
        # 2. re-detect real pauses: each old gap event gives (received samples, arrival time)
        start_data = json.loads(start[1]) if start else {"t_start": 0.0}
        t0 = start_data.get("t_start") or 0.0
        new_gaps, out, pos, pad_total = [], [], 0, 0
        for _, t, data in gaps:
            at = json.loads(data)["at"]
            expected = t0 + (at + pad_total) / input_rate
            late = t - expected
            if late > A.GAP_TOLERANCE_S:
                pad = int(round(late * input_rate))
                out += [raw[pos:at], np.zeros(pad, dtype=np.int16)]
                pos = at
                pad_total += pad
                new_gaps.append((t, {"type": "snoop.mic_gap", "at": at, "pad": pad}))
        out.append(raw[pos:])
        fixed = np.concatenate(out)
        mic_path.write_bytes(fixed.astype("<i2").tobytes())
        db.execute("DELETE FROM events WHERE session_id=? AND type='snoop.mic_gap'", (sid,))
        for t, ev in new_gaps:
            db.execute("INSERT INTO events(session_id, t, source, type, data) VALUES (?,?,?,?,?)",
                       (sid, t, "snoop", "snoop.mic_gap", json.dumps(ev)))
        if start:
            start_data.update({"rate": input_rate, "rate_source": "fixed"})
            db.execute("UPDATE events SET data=? WHERE id=?", (json.dumps(start_data), start[0]))
        meta.setdefault("input_format", {})["rate"] = input_rate
        meta.setdefault("mic", {}).update({"rate": input_rate, "rate_source": "fixed"})
        report["mic"] = {"pads_removed": len(gaps), "pads_kept": len(new_gaps),
                         "seconds": round(len(fixed) / input_rate, 1),
                         "received_seconds": round(len(raw) / input_rate, 1)}

    if output_rate:
        n = 0
        for eid, data in db.execute("SELECT id, data FROM events WHERE session_id=? AND type IN "
                                    "('snoop.response_audio','response.output_audio.delta','response.audio.delta')",
                                    (sid,)).fetchall():
            ev = json.loads(data)
            ev["rate" if ev.get("type") == "snoop.response_audio" else "_rate"] = output_rate
            db.execute("UPDATE events SET data=? WHERE id=?", (json.dumps(ev), eid))
            n += 1
        meta.setdefault("output_format", {})["rate"] = output_rate
        report["responses"] = {"events_updated": n}

    db.execute("UPDATE sessions SET meta=? WHERE id=?", (json.dumps(meta), sid))
    db.commit()
    db.close()
    return report
