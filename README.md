# s2snoop

*Listen in on your voice agent.*

Watch an **OpenAI Realtime** voice session as it happens, then replay it with the audio lined up:
latency per stage, transcripts, tool calls, images, tokens, the exact LLM prompt, and (for
[speech-to-speech](https://github.com/huggingface/speech-to-speech)) the pipeline internals: VAD probability,
Smart Turn, TTS segments, queue depth.

It sits between your client and your server as a transparent proxy. You don't modify either one.
It works with any WebSocket Realtime client (a robot, a browser, the Agents SDK, a script) and any Realtime
server (speech-to-speech, OpenAI, or anything else that speaks the protocol).

```
client ──ws──▶ A · Realtime proxy ──ws──▶ Realtime server ──http──▶ B · LLM proxy ──▶ LLM server
                    │                           │ C · probe (optional, in-process, UDP)
                    └─────────────┬─────────────┘
                                  ▼
                 s2snoop: SQLite + audio files → dashboard
```

| Tap | What it sees | What you need |
|---|---|---|
| **A · Realtime proxy** | audio in and out, transcripts, tools, images, tokens, the server's own latency report, and the latency the proxy measures from end of speech to first audio | point the client at the proxy, or move the server to another port |
| **B · LLM proxy** *(optional)* | the exact request (instructions, history, tools), TTFT, duration, tokens, tok/s | a server that calls an OpenAI-compatible LLM over HTTP at a configurable base URL |
| **C · probe** *(optional, speech-to-speech only)* | Silero probability per frame and its threshold, Smart Turn probability, the text sent to TTS, per-handler spans, queue depth | launch speech-to-speech through `s2snoop s2s` |

If B or C isn't running, its lanes are hidden from the dashboard.

> [!WARNING]
> There's no authentication or TLS. The proxy listens on your network by default, and recordings are stored
> unencrypted. Read [Security](#security) before running this anywhere other than your own machine.

## Install

Requires Python ≥ 3.10 and [uv](https://docs.astral.sh/uv/).

```bash
git clone <this repo> && cd s2snoop
uv sync
```

## Quick start

### With speech-to-speech (all three taps)

Suppose your client usually connects to speech-to-speech on port `8765`. s2snoop takes over that port,
and speech-to-speech moves to `8766`, so the client doesn't need any change.

```bash
# terminal 1: proxies + dashboard
uv run s2snoop \
  --listen 0.0.0.0:8765 --upstream ws://127.0.0.1:8766 \
  --llm-listen 127.0.0.1:8081 --llm-upstream http://<llm-host>:<llm-port>

# terminal 2: speech-to-speech with the probe, its LLM calls going through proxy B
uv run s2snoop s2s -- serve --host 127.0.0.1 --port 8766 \
  --responses_api_base_url http://127.0.0.1:8081 --responses_api_api_key ""
```

Open <http://127.0.0.1:7860>.

`s2snoop s2s` uses the Python interpreter of the `speech-to-speech` found on your `PATH` (or the one
given with `--s2s-python /path/to/venv/bin/python`). It puts this package on `PYTHONPATH` and runs
`python -m s2snoop.probe <args>`. The probe uses only the standard library, so nothing is installed
into the speech-to-speech environment.

To skip the probe, start speech-to-speech as usual on `8766`. To skip proxy B, leave out `--llm-upstream`.

### With the OpenAI Realtime API

```bash
uv run s2snoop --listen 127.0.0.1:8765 --route openai=wss://api.openai.com
```

Point the client at `ws://127.0.0.1:8765/openai/v1/realtime?model=<model>`. The `Authorization` header and the
query string are passed through unchanged and never stored.

### Labelling clients

Add `?client=NAME` to the client's URL to label its sessions in the dashboard. The proxy removes it before
passing the request on. If there's no label, the proxy guesses one from the User-Agent.

### Try it without a real client (macOS)

```bash
uv run python scripts/fake_mic_client.py \
  --url "ws://127.0.0.1:8765/v1/realtime?client=fake" \
  "Hello! What is a robot?" "Tell me a long story." --barge-in 2.2
```

Speech is synthesised with macOS `say` (`--voice` picks the voice) and streamed in real time, like a microphone.
You can also pass `.wav` files instead of text; they work on any OS.

## Options

| Flag | Default | |
|---|---|---|
| `--listen` | `0.0.0.0:8765` | Realtime proxy address |
| `--upstream` | `ws://127.0.0.1:8766` | default Realtime server |
| `--route NAME=URL` | | named upstream: `ws://proxy/NAME/v1/realtime` → `URL/v1/realtime` (repeatable) |
| `--llm-listen` | `127.0.0.1:8081` | LLM proxy address |
| `--llm-upstream` | *off* | OpenAI-compatible LLM server to proxy |
| `--ui` | `127.0.0.1:7860` | dashboard address |
| `--probe-port` | `8799` | UDP port the probe sends to |
| `--data` | `./data` | SQLite database, audio and images |
| `--no-audio` | | do not record audio |
| `--retention-days N` | | delete sessions older than N days |

Subcommands:

```bash
uv run s2snoop clear [--yes]                 # delete every recorded session (prefer the dashboard button while running)
uv run s2snoop fix-rate <session-id> --input 16000 --output 16000
                                                    # re-tag a session recorded with the wrong sample rate
uv run s2snoop s2s [--s2s-python PATH] [--probe HOST:PORT] -- <speech-to-speech args>
```

## Dashboard

- **Sessions**: live and past, filterable by client. Delete them one at a time or all ended sessions at once.
- **Timeline** (zoom with ctrl/⌘ + wheel):
  - user waveform with VAD speech regions and transcripts
  - VAD probability and threshold, and Smart Turn
  - LLM calls (prefill and generation) and tool calls
  - TTS segments
  - assistant audio as the user heard it; audio that was generated but cut off is faded
  - queue depth
  - amber bands from end of speech to first audio, red lines at barge-ins
- **Replay**: both tracks on one Web Audio clock, so they stay aligned to the sample. Click anywhere to seek,
  space to play or pause, mute each track, play at 1×, 1.5× or 2×. You can also replay the full generated TTS
  of any response.
- **Turn inspector**: latency bars (end-to-end at the proxy, STT, LLM TTFT, LLM, TTS time to first audio, end-to-end at the server), Smart Turn, the full LLM request
  (instructions, history, tools, raw JSON), TTS segments.
- **Raw events**: every stored event, filterable by type and source.

## How the timing works

- **Session time 0** is when the client connects.
- **Mic track.** It's written sample by sample. When the client pauses its stream, silence is inserted, so
  the file keeps pace with the wall clock. The server's `audio_start_ms`/`audio_end_ms` count received samples.
  They're mapped through those gaps, so VAD segments land exactly on the waveform.
- **Assistant track.** Each response starts at its first `response.output_audio.delta` and plays in real time
  from there (TTS arrives in bursts).
  - If the client sends `conversation.item.truncate`, the track is cut at its `audio_end_ms`.
  - Otherwise it's cut at the next speech start, if the server stopped the response. That cut is marked
    *estimated*.
- **Sample rate.** Clients may not declare one (`{"type": "audio/pcm", "rate": null}`), and servers disagree
  on the default: OpenAI uses 24 kHz, speech-to-speech 16 kHz.
  - The mic rate is measured from the stream over the first 3 s: the median over 1 s windows, which holds up
    to network jitter. It overrides a declared rate that contradicts it.
  - An undeclared output rate is taken as 16 kHz. OpenAI always declares its rate.
  - The session header shows which rate was used and where it came from.
- **One clock.** Proxy A, proxy B and the probe run on one machine, so they share a clock.

## Security

> [!WARNING]
> s2snoop is a **local debugging tool**. It has **no authentication and no TLS**. Run it only on a
> machine and network you trust, and never expose its ports to the internet.

**What is exposed on the network**

| Port | Default bind | Authentication | Who can reach it |
|---|---|---|---|
| Realtime proxy (`--listen`) | `0.0.0.0:8765` | none | anyone on your network: they can open sessions against your upstream (speech-to-speech, or OpenAI with their own key) |
| Dashboard (`--ui`) | `127.0.0.1:7860` | none | this machine only. With `--ui 0.0.0.0:7860`, anyone on your network can **listen to every recording, see camera images and prompts, and delete sessions** |
| LLM proxy (`--llm-listen`) | `127.0.0.1:8081` | none | this machine only |
| Probe (UDP) | `127.0.0.1:8799` | none | this machine only |

If the client runs on the same machine, use `--listen 127.0.0.1:8765` so nothing is exposed at all.

**In transit.** The proxy speaks plain `ws://`. Audio, transcripts and images between the client and the
proxy are not encrypted. `wss://` upstreams (e.g. `--route openai=wss://api.openai.com`) stay encrypted
between the proxy and the upstream.

**At rest.** Everything is stored unencrypted in `--data` (SQLite plus raw audio and image files) and stays
on your machine. Nothing is sent anywhere except to the upstreams you configure. Recordings contain voices,
camera images, full prompts and tool arguments. Audio takes about 170 MB per hour per track at 24 kHz.
To limit what's kept, use `--no-audio`, `--retention-days N`, or `s2snoop clear`.

**Credentials.** Auth headers are relayed to the upstream and never written to disk. Query parameters whose
name contains `key` or `token` are masked in stored URLs. Message *bodies* are stored as they are, though. A
secret that your client puts in `instructions`, a tool call or an LLM request **will** be recorded.

## Known limits

- WebRTC clients (audio over RTP) bypass the WebSocket proxy.
- The probe patches speech-to-speech internals at runtime and has been tested with commit `c60efc4`. Each
  hook is checked at start-up. If its target has moved, the hook is disabled with a warning, and the
  dashboard shows which hooks are on.
- Probe events go to the most recent live session. With several clients at once on one pipeline pool, they
  can land in the wrong session.
- llama.cpp returns no `timings` on `/v1/responses`, so tok/s is computed from `usage` and the measured time.
- The client's own playback buffer is invisible from the network. The assistant track is an estimate, accurate
  to within a few tens of ms.

## Development

```bash
uv run pytest    # unit + end-to-end (fake Realtime server, fake SSE LLM, real proxies)
```

The store (`s2snoop/store.py`) doesn't depend on the web layer. Another front end can read the same
`data/snoop.db` and `data/sessions/<id>/` files.

## License

[Apache-2.0](LICENSE)

Not affiliated with Hugging Face or OpenAI. speech-to-speech is a Hugging Face project; this tool only observes it.
