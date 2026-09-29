"use strict";

// ------------------------------------------------------------------ browser mic client
// A plain Realtime client living in the dashboard: mic → 24 kHz PCM16 → /talk (same origin, relayed by
// the proxy like any other client, so the session is recorded and shows up live), assistant audio played
// back on a Web Audio clock, barge-in answered with conversation.item.truncate so the cut point is exact.
const Talk = (() => {
  const RATE = 24000, CHUNK = RATE / 50;  // 20 ms
  const WORKLET = `registerProcessor("s2snoop-mic", class extends AudioWorkletProcessor {
    process(inputs) { const ch = inputs[0] && inputs[0][0]; if (ch) this.port.postMessage(ch.slice(0)); return true; }
  });`;
  const T = {
    ws: null, ctx: null, stream: null, node: null, src: null, muted: false, state: "idle",
    outRate: RATE, pending: [], pendingLen: 0, resample: null, level: 0, known: null, sid: null,
    playHead: 0, chunks: [], resp: null, meterRaf: 0,
  };

  const available = () => !!(window.isSecureContext && navigator.mediaDevices && navigator.mediaDevices.getUserMedia);

  function setState(state, info) {
    T.state = state;
    const label = { connecting: "connecting…", listening: "listening", hearing: "hearing you", thinking: "thinking…",
      speaking: "speaking", ended: "call ended", error: "error" }[state] || state;
    $("cb-state").textContent = label;
    $("cb-state").dataset.state = state;
    if (info != null) $("cb-info").textContent = info;
  }

  // Linear resampler that carries its fractional position and last sample across blocks.
  function makeResampler(src, dst) {
    const step = src / dst;
    let pos = 0, prev = 0;
    return (x) => {
      const out = [];
      const n = x.length;
      while (pos <= n - 1) {
        const i = Math.floor(pos), f = pos - i;
        const a = i < 0 ? prev : x[i];
        const b = i + 1 < 0 ? prev : x[Math.min(i + 1, n - 1)];
        out.push(a + (b - a) * f);
        pos += step;
      }
      pos -= n;
      prev = x[n - 1];
      return out;
    };
  }

  function b64FromInt16(pcm) {
    const bytes = new Uint8Array(pcm.buffer, pcm.byteOffset, pcm.byteLength);
    let s = "";
    for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
    return btoa(s);
  }

  function int16FromB64(b64) {
    const bin = atob(b64);
    const out = new Int16Array(bin.length >> 1);
    for (let i = 0; i < out.length; i++) out[i] = (bin.charCodeAt(2 * i) | (bin.charCodeAt(2 * i + 1) << 8)) << 16 >> 16;
    return out;
  }

  function onMicBlock(block) {
    let sum = 0;
    for (let i = 0; i < block.length; i++) sum += block[i] * block[i];
    T.level = Math.max(Math.sqrt(sum / block.length), T.level * 0.85);
    const samples = T.resample(block);
    for (const v of samples) T.pending.push(T.muted ? 0 : Math.max(-32768, Math.min(32767, Math.round(v * 32767))));
    while (T.pending.length >= CHUNK) {
      const pcm = Int16Array.from(T.pending.splice(0, CHUNK));
      if (T.ws && T.ws.readyState === 1) T.ws.send(JSON.stringify({ type: "input_audio_buffer.append", audio: b64FromInt16(pcm) }));
    }
  }

  function drawMeter() {
    const pct = T.muted ? 0 : Math.min(100, Math.round(Math.sqrt(T.level) * 160));
    $("cb-level").style.width = `${pct}%`;
    T.level *= 0.92;
    T.meterRaf = requestAnimationFrame(drawMeter);
  }

  // ---- playback
  function playDelta(ev) {
    const pcm = int16FromB64(ev.delta || "");
    if (!pcm.length) return;
    const buf = T.ctx.createBuffer(1, pcm.length, T.outRate);
    const ch = buf.getChannelData(0);
    for (let i = 0; i < pcm.length; i++) ch[i] = pcm[i] / 32768;
    const src = T.ctx.createBufferSource();
    src.buffer = buf;
    src.connect(T.ctx.destination);
    const start = Math.max(T.ctx.currentTime + 0.04, T.playHead);
    src.start(start);
    T.playHead = start + buf.duration;
    if (!T.resp || T.resp.id !== ev.response_id) T.resp = { id: ev.response_id, itemId: ev.item_id, doneS: 0 };
    const resp = T.resp, chunk = { src, start, dur: buf.duration };
    T.chunks.push(chunk);
    src.onended = () => {
      resp.doneS += chunk.dur;  // fully played: keep it in the heard total
      T.chunks = T.chunks.filter((c) => c !== chunk);
      if (!T.chunks.length && T.state === "speaking") setState("listening");
    };
    if (T.state !== "speaking") setState("speaking");
  }

  function playedMs() {
    const now = T.ctx.currentTime;
    return Math.round(1000 * T.chunks.reduce((acc, c) => acc + Math.min(Math.max(now - c.start, 0), c.dur), 0));
  }

  // User spoke over the assistant: stop what is still queued and tell the server how much was heard.
  function bargeIn() {
    if (!T.chunks.length || !T.resp) return;
    const heard = Math.round(T.resp.doneS * 1000) + playedMs();
    for (const c of T.chunks) { try { c.src.onended = null; c.src.stop(); } catch (_) { /* not started */ } }
    T.chunks = [];
    T.playHead = 0;
    if (T.resp.itemId) {
      T.ws.send(JSON.stringify({ type: "conversation.item.truncate", item_id: T.resp.itemId, content_index: 0, audio_end_ms: heard }));
    }
    T.resp = null;
  }

  function onServer(ev) {
    switch (ev.type) {
      case "session.created":
      case "session.updated": {
        const out = ev.session && ev.session.audio && ev.session.audio.output && ev.session.audio.output.format;
        if (out && out.rate) T.outRate = out.rate;
        $("cb-info").textContent = `${RATE / 1000} kHz in · ${T.outRate / 1000} kHz out`;
        if (T.state === "connecting") setState("listening");
        break;
      }
      case "input_audio_buffer.speech_started": bargeIn(); setState("hearing"); break;
      case "input_audio_buffer.speech_stopped": setState("thinking"); break;
      case "response.output_audio.delta":
      case "response.audio.delta":
        playDelta(ev);
        break;
      case "response.done":
        if (!T.chunks.length && T.state !== "hearing") setState("listening");
        break;
      case "error": setState("error", (ev.error && ev.error.message) || "server error"); break;
    }
  }

  // ---- lifecycle
  async function start() {
    if (T.ws || !available()) return;
    $("callbar").hidden = false;
    document.body.classList.add("in-call");
    renderButton();
    setState("connecting", "");
    // Create/resume the context inside the click (autoplay policy), before any await.
    T.ctx = new AudioContext();
    T.ctx.resume();
    try {
      T.stream = await navigator.mediaDevices.getUserMedia({
        audio: { channelCount: 1, echoCancellation: true, noiseSuppression: true, autoGainControl: true },
      });
    } catch (e) {
      setState("error", e.name === "NotAllowedError" ? "microphone permission denied" : `microphone: ${e.message}`);
      cleanup(false);
      return;
    }
    await T.ctx.audioWorklet.addModule(URL.createObjectURL(new Blob([WORKLET], { type: "text/javascript" })));
    T.resample = makeResampler(T.ctx.sampleRate, RATE);
    T.src = T.ctx.createMediaStreamSource(T.stream);
    T.node = new AudioWorkletNode(T.ctx, "s2snoop-mic");
    T.node.port.onmessage = (m) => onMicBlock(m.data);
    T.src.connect(T.node);  // not connected to the destination: no local monitoring
    T.meterRaf = requestAnimationFrame(drawMeter);

    T.known = new Set(S.sessions.map((s) => s.id));
    T.sid = null;
    const url = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/talk/v1/realtime?client=browser`;
    const ws = new WebSocket(url);
    T.ws = ws;
    renderButton();
    ws.onopen = () => ws.send(JSON.stringify({
      type: "session.update",
      session: { type: "realtime", audio: {
        input: { format: { type: "audio/pcm", rate: RATE } },
        output: { format: { type: "audio/pcm", rate: RATE } },
      } },
    }));
    ws.onmessage = (m) => { try { onServer(JSON.parse(m.data)); } catch (e) { console.warn("talk", e); } };
    ws.onclose = (e) => {
      if (T.ws !== ws) return;
      const why = e.reason || (e.code === 1011 ? "Realtime server unavailable" : `closed (${e.code})`);
      setState(T.state === "error" ? "error" : "ended", T.state === "error" ? null : why);
      cleanup(false);
    };
  }

  function cleanup(hideBar) {
    cancelAnimationFrame(T.meterRaf);
    const ws = T.ws;
    T.ws = null;
    if (ws && ws.readyState <= 1) ws.close();
    for (const c of T.chunks) { try { c.src.stop(); } catch (_) { /* not started */ } }
    if (T.stream) T.stream.getTracks().forEach((t) => t.stop());
    if (T.ctx) T.ctx.close();
    Object.assign(T, { ctx: null, stream: null, node: null, src: null, chunks: [], resp: null, playHead: 0,
      pending: [], level: 0, muted: false });
    $("cb-mute").textContent = "Mute";
    $("cb-mute").setAttribute("aria-pressed", "false");
    $("cb-level").style.width = "0";
    if (hideBar) { $("callbar").hidden = true; document.body.classList.remove("in-call"); }
    renderButton();
  }

  function hangUp() {
    if (T.ws) cleanup(true);
    else { $("callbar").hidden = true; document.body.classList.remove("in-call"); }
  }

  // Open the session our connection just created, so the timeline fills in while you talk.
  function onSessions(sessions) {
    if (!T.ws || T.sid || !T.known) return;
    const mine = sessions.find((s) => s.live && !T.known.has(s.id) && s.meta && s.meta.client === "browser");
    if (mine) { T.sid = mine.id; openSession(mine.id); }
  }

  function renderButton() {
    const ok = available();
    for (const b of document.querySelectorAll("[data-talk]")) {
      b.disabled = !ok || !!T.ws;
      b.classList.toggle("on-call", !!T.ws);
      b.title = !ok ? "The microphone needs https or localhost: start s2snoop with --tls-cert/--tls-key, or use tailscale serve."
        : T.ws ? "On a call: hang up from the bar at the bottom" : "Talk to the Realtime server from this browser";
      const label = b.querySelector(".talk-label");
      if (label) label.textContent = T.ws ? "On call" : label.dataset.idle;
    }
  }

  document.addEventListener("click", (e) => { if (e.target.closest("[data-talk]")) start(); });
  $("cb-hangup").addEventListener("click", hangUp);
  $("cb-mute").addEventListener("click", () => {
    T.muted = !T.muted;
    $("cb-mute").textContent = T.muted ? "Unmute" : "Mute";
    $("cb-mute").setAttribute("aria-pressed", String(T.muted));
  });
  window.addEventListener("beforeunload", () => { if (T.ws) cleanup(false); });
  renderButton();

  return { onSessions, available };
})();
