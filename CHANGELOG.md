# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Until 1.0.0, a minor version may include changes
that break compatibility; they are listed under **Changed** and marked **Breaking**.

## [Unreleased]

### Added

- **Pipeline models (probe C).** The session header shows the backend and model of each speech-to-speech stage
  (VAD settings, STT and TTS model ids, voice, language), and the Config tab shows each handler's full setup.
  The probe reads the handlers' `setup_kwargs`, drops secret-looking keys, and keeps only scheme, host and
  path of URLs. A session records the list once per pipeline change.
- The turn inspector reads latency records v2 (speech-to-speech `3a638ff`): `vad decision` and `hold` bars,
  and the record version in the heading.

### Fixed

- A latency field the server reports as `null` (a stage not measured for that backend) is shown as **n/a**
  instead of being hidden. Fields the record doesn't carry, such as `llm_ttft_s` in v2, stay hidden.
- Session pages no longer fail with a 500 error ("Out of range float values are not JSON compliant") when an event holds
  a non-finite number, such as the VAD's `max_speech_ms = inf`. The probe sends it as `"inf"`, and values
  already stored as `Infinity` / `NaN` are read back as `null`. The same strict parsing now applies to relayed
  Realtime frames and to the server's latency record, where a Python server may also write `NaN` or `Infinity`.

### Changed

- README: the architecture schematic is now a Mermaid diagram, rendered by GitHub.

## [0.3.0] - 2026-09-29

### Changed

- **Breaking:** the dashboard's default address is now `127.0.0.1:8007` (was `127.0.0.1:8767`). 8007 is a nod to
  007, it's unassigned, and it stays clear of Gradio (7860–7959) and Jupyter (8888). Pass `--ui 127.0.0.1:8767`
  to keep the 0.2.0 address.

## [0.2.0] - 2026-09-29

### Added

- **Talk from the browser.** A Talk button in the dashboard header turns the browser into a Realtime client:
  - The mic streams at 24 kHz, with echo cancellation and noise suppression.
  - Answers play in the browser. A barge-in sends `conversation.item.truncate`, so cut points are exact.
  - The new session opens live in the timeline.
  - A call bar shows the mic level, the state, and Mute and Hang up buttons.
- A same-origin `/talk/…` WebSocket on the dashboard, which reuses the proxy relay, so browser calls are
  recorded like any other client.
- `--tls-cert` / `--tls-key` to serve the dashboard over HTTPS, which browsers require for the mic on other
  devices.
- `s2snoop --version`.
- `AGENTS.md` (layout, commands, project rules), imported by `CLAUDE.md`.
- This changelog, and a release workflow that publishes a GitHub release for each `v*` tag.

### Changed

- **Breaking:** the dashboard's default address is now `127.0.0.1:8767` (was `127.0.0.1:7860`). The range
  7860–7959 belongs to Gradio apps, and the Reachy Mini conversation app opened s2snoop instead of its own UI.
  Pass `--ui 127.0.0.1:7860` to keep the old address.

### Fixed

- An error while recording an event no longer closes the live connection: the relay keeps going and the
  error is logged.

## [0.1.0] - 2026-09-29

### Added

- First public release.
- **A** · a transparent Realtime WebSocket proxy that records audio, transcripts, tools, images, tokens and
  latencies. It supports named routes (`--route NAME=URL`) and `?client=` labels, and relays auth headers
  without storing them.
- **B** · an OpenAI-compatible LLM proxy that records the exact request, TTFT, duration, tokens and tok/s.
- **C** · an in-process probe for speech-to-speech (`s2snoop s2s -- …`) that records the VAD probability,
  Smart Turn, TTS input, handler spans and queue depth. It uses only the standard library.
- A dashboard with the session list, a zoomable timeline, replay with aligned audio, a turn inspector, raw
  events and session config.
- Sample-rate detection for clients that don't declare one, and mic/assistant tracks aligned to the wall
  clock.
- `clear`, `fix-rate`, `--no-audio` and `--retention-days`.
- CI on Python 3.10 and 3.13.

[Unreleased]: https://github.com/cpoisson/s2snoop/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/cpoisson/s2snoop/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/cpoisson/s2snoop/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cpoisson/s2snoop/releases/tag/v0.1.0
