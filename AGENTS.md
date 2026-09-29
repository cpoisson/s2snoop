# AGENTS.md

Guide for coding agents working on s2snoop. Read the [README](README.md) first for what the tool does.

## Layout

| Path | Role |
|---|---|
| `s2snoop/cli.py` | entry point: starts proxies, dashboard, probe listener; `s2s`, `clear`, `fix-rate` subcommands |
| `s2snoop/proxy.py` | **A** · Realtime WebSocket proxy (verbatim relay) + HTTP passthrough; `app.state.ws_relay` is reused by the dashboard's `/talk` |
| `s2snoop/llm_tap.py` | **B** · OpenAI-compatible HTTP/SSE proxy in front of the LLM |
| `s2snoop/probe.py` | **C** · in-process probe for speech-to-speech (monkeypatches, UDP out). **Standard library only**: it runs inside the speech-to-speech interpreter |
| `s2snoop/hub.py` | live sessions, ingestion from the three taps, broadcast to the dashboard |
| `s2snoop/session.py` | pure event → turns/latencies model (no I/O) |
| `s2snoop/audio.py` | mic/assistant track alignment, sample-rate detection, peaks, WAV rendering |
| `s2snoop/store.py` | SQLite + files; independent of the web layer |
| `s2snoop/web.py` | dashboard API, live WebSocket, `/talk` route, static files |
| `s2snoop/static/` | vanilla JS front end: `app.js` (dashboard, canvas timeline, replay), `talk.js` (browser mic client) |
| `scripts/fake_mic_client.py` | scripted Realtime client for manual runs |
| `tests/` | unit + end-to-end (fake Realtime server, fake SSE LLM, real proxies) |

## Commands

```bash
uv sync
uv run pytest -q          # must pass before any commit; CI runs it on Python 3.10 and 3.13
uv run s2snoop --help
```

For a manual run without a real Realtime server, start the fake one from `tests/test_e2e.py` (`fake_realtime`)
with `websockets.serve`, point `--upstream` at it, then use the dashboard's Talk button or
`scripts/fake_mic_client.py`.

## Rules

- **Observe, never alter.** The proxies relay every frame verbatim. Recording happens *after* forwarding, and
  a recording failure must never break the relay.
- **Don't modify speech-to-speech.** The probe patches it at runtime. Every hook checks its target at
  start-up and disables itself with a warning if the target moved. Keep `TESTED_WITH` in `probe.py` current.
- **Stay generic.** No client-, robot- or machine-specific code, defaults or docs: no hostnames, IPs or
  personal paths. Reachy Mini is one client among others.
- **No secrets at rest.** Auth headers are relayed but never stored. Query parameters named `*key*`/`*token*`
  are masked in stored URLs.
- **Safe defaults.** The dashboard binds to `127.0.0.1`. Anything that widens exposure is opt-in and
  documented in the README's Security section.
- **Ports:** proxy 8765, speech-to-speech 8766, dashboard 8007, LLM proxy 8081, probe UDP 8799. Keep the
  dashboard away from 7860–7959 (Gradio apps, e.g. the Reachy Mini conversation app) and 8888 (Jupyter).
- **English** everywhere: UI, logs, docs, tests.
- **Front end:** no build step, no framework, no CDN scripts. Asset URLs are versioned by mtime in `web.py`;
  add any new static file to that list.
- **Tests:** a behaviour change comes with a test. Protocol behaviour goes in `tests/test_e2e.py`, the
  timing model in `tests/test_session.py` / `tests/test_audio.py`.
- **Commits:** imperative subject, with the body explaining why. Don't mention AI agents or assistants in
  commit messages (no `Co-Authored-By` trailers for them).

## Versioning and releases

[Semantic Versioning](https://semver.org). While the version is 0.x, a minor bump may break compatibility;
say so under **Changed** with **Breaking**.

- Every user-visible change adds a line under `## [Unreleased]` in `CHANGELOG.md`, in the same commit.
- The version lives only in `pyproject.toml`. `s2snoop.__version__` reads it from the package metadata.
- To release `X.Y.Z`:
  1. Rename `[Unreleased]` to `[X.Y.Z] - YYYY-MM-DD`, add a fresh `[Unreleased]` above it, and update the
     compare links at the bottom.
  2. Set `version = "X.Y.Z"` in `pyproject.toml`, then run `uv lock`.
  3. Commit (`Release X.Y.Z`), tag `vX.Y.Z`, and push the commit and the tag.
  4. `.github/workflows/release.yml` checks that the tag matches the version, runs the tests, builds, and
     publishes a GitHub release using that version's changelog section.

## Protocol facts worth knowing

Checked against speech-to-speech `c60efc4` and the OpenAI Realtime API:

- Clients may send `{"type": "audio/pcm", "rate": null}`. Reachy Mini does, and actually sends 16 kHz. The mic
  rate is measured from the stream (see `audio.py`). speech-to-speech defaults both directions to 16 kHz,
  OpenAI to 24 kHz.
- On barge-in, speech-to-speech sends `response.done` (status `cancelled`) **before**
  `input_audio_buffer.speech_started`.
- llama.cpp returns no `timings` on `/v1/responses`, so tok/s is derived from `usage` and the measured time.
- speech-to-speech accepts `conversation.item.truncate` and resamples each session to the declared rates.
