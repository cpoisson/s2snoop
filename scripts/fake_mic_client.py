"""Scripted Realtime client: streams speech files like a live microphone.

Speech is synthesised with macOS ``say`` (or given as WAV files), converted to
24 kHz PCM16 and streamed in real time (20 ms chunks) with silence between
utterances, so a real speech-to-speech server runs its VAD / STT / LLM / TTS
path end to end. Useful to exercise s2snoop without a robot.

    uv run python scripts/fake_mic_client.py --url ws://127.0.0.1:8765/v1/realtime?client=fake \
        "Hello, how are you?" "Tell me a short story."
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import subprocess
import tempfile
import time
import wave
from pathlib import Path

import numpy as np
import websockets

RATE = 24000
CHUNK = RATE // 50  # 20 ms


def synth(text: str, voice: str | None) -> np.ndarray:
    with tempfile.TemporaryDirectory() as d:
        aiff, wav = Path(d) / "s.aiff", Path(d) / "s.wav"
        cmd = ["say", "-o", str(aiff)] + (["-v", voice] if voice else []) + [text]
        subprocess.run(cmd, check=True)
        subprocess.run(["afconvert", "-f", "WAVE", "-d", f"LEI16@{RATE}", "-c", "1", str(aiff), str(wav)], check=True)
        return read_wav(wav)


def read_wav(path: Path) -> np.ndarray:
    with wave.open(str(path)) as w:
        assert w.getsampwidth() == 2 and w.getnchannels() == 1, "expected mono PCM16"
        pcm = np.frombuffer(w.readframes(w.getnframes()), dtype="<i2")
        if w.getframerate() != RATE:
            x = np.linspace(0, len(pcm) - 1, int(len(pcm) * RATE / w.getframerate()))
            pcm = np.interp(x, np.arange(len(pcm)), pcm).astype(np.int16)
        return pcm


async def run(args) -> None:
    utterances = [read_wav(Path(u)) if u.endswith(".wav") else synth(u, args.voice) for u in args.utterances]
    async with websockets.connect(args.url, max_size=None) as ws:
        await ws.send(json.dumps({"type": "session.update", "session": {
            "type": "realtime",
            "audio": {"input": {"format": {"type": "audio/pcm", "rate": RATE}},
                      "output": {"format": {"type": "audio/pcm", "rate": RATE}}},
            **({"instructions": args.instructions} if args.instructions else {}),
        }}))
        log: list[str] = []

        async def reader():
            async for raw in ws:
                ev = json.loads(raw)
                kind = ev.get("type", "")
                if kind in ("input_audio_buffer.speech_started", "input_audio_buffer.speech_stopped",
                            "conversation.item.input_audio_transcription.completed", "response.done", "error"):
                    extra = ev.get("transcript") or (ev.get("response") or {}).get("status") or ev.get("error") or ""
                    line = f"{time.strftime('%H:%M:%S')} {kind} {extra}"
                    log.append(line)
                    print(line, flush=True)

        read_task = asyncio.create_task(reader())
        silence = np.zeros(CHUNK, dtype=np.int16)
        start = time.monotonic()
        sent = 0

        async def stream(pcm: np.ndarray) -> None:
            nonlocal sent
            for i in range(0, len(pcm), CHUNK):
                chunk = pcm[i:i + CHUNK]
                await ws.send(json.dumps({"type": "input_audio_buffer.append",
                                          "audio": base64.b64encode(chunk.tobytes()).decode()}))
                sent += len(chunk)
                delay = start + sent / RATE - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)

        await stream(np.tile(silence, 50))  # 1 s lead-in
        for i, pcm in enumerate(utterances):
            await stream(pcm)
            gap = args.barge_in if (args.barge_in and i < len(utterances) - 1) else args.gap
            await stream(np.tile(silence, int(gap * 50)))
        read_task.cancel()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("utterances", nargs="+", help="text (synthesised with say) or a .wav file")
    p.add_argument("--url", default="ws://127.0.0.1:8765/v1/realtime?client=fake-mic")
    p.add_argument("--gap", type=float, default=8.0, help="silence after each utterance (s)")
    p.add_argument("--barge-in", type=float, default=None,
                   help="shorter silence between utterances, to interrupt the answer (s)")
    p.add_argument("--voice", default=None, help="macOS voice (e.g. Samantha)")
    p.add_argument("--instructions", default=None)
    asyncio.run(run(p.parse_args()))


if __name__ == "__main__":
    main()
