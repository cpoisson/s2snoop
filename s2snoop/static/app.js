"use strict";

// ------------------------------------------------------------------ state
const S = {
  sessions: [], status: null, sid: null, snap: null,
  vad: [], spans: [], queues: [], seriesSeq: 0,
  mic: { rate: 24000, startS: 0, win: 480, peaks: [] }, resp: {}, perS: 50,
  selTurn: null, pxs: 60, follow: true, playing: false, pos: 0,
  audioVersion: null, evOffset: 0, tab: "conv", clientFilter: "",
};
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const num = (v, d = 2) => (v == null || Number.isNaN(v) ? "—" : Number(v).toFixed(d));
const sec = (v, d = 2) => (v == null ? "—" : `${num(v, d)} s`);
const ms = (v) => (v == null ? "—" : `${Math.round(v * 1000)} ms`);
const kfmt = (n) => (n == null ? "—" : n >= 10000 ? `${num(n / 1000, 1)} k` : String(n));
const clock = (epoch) => new Date(epoch * 1000).toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit" });
const tc = (s) => { s = Math.max(0, s || 0); const m = Math.floor(s / 60); return `${String(m).padStart(2, "0")}:${(s - m * 60).toFixed(1).padStart(4, "0")}`; };
const dur = (s) => (s == null ? "—" : s < 60 ? `${Math.round(s)} s` : `${Math.floor(s / 60)} min ${String(Math.round(s % 60)).padStart(2, "0")} s`);
const imgSrc = (src) => (src && src.startsWith("file:") ? `/api/sessions/${S.sid}/files/${src.slice(5)}` : src);

const COLORS = {
  user: "#22d3ee", asst: "#8b7dff", proc: "#f59e0b", prefill: "rgba(245,158,11,.45)", tool: "#ffd21e",
  probe: "#34d399", err: "#ff6a75", text: "#f5f6fa", dim: "rgba(245,246,250,.65)", faint: "rgba(245,246,250,.42)",
  grid: "rgba(255,255,255,.05)", sep: "rgba(255,255,255,.07)", bg: "#0a0b10",
};

// ------------------------------------------------------------------ live socket
let ws = null, wsBackoff = 500;
function connect() {
  ws = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/live`);
  ws.onopen = () => { wsBackoff = 500; if (S.sid) subscribe(S.sid, true); };
  ws.onclose = () => { setTimeout(connect, wsBackoff); wsBackoff = Math.min(wsBackoff * 2, 8000); };
  ws.onmessage = (m) => onMessage(JSON.parse(m.data));
}
function subscribe(sid, keep = false) {
  if (!keep) resetSessionData();
  if (ws && ws.readyState === 1) ws.send(JSON.stringify({ subscribe: sid }));
}
function resetSessionData() {
  Object.assign(S, { snap: null, vad: [], spans: [], queues: [], seriesSeq: 0, resp: {}, selTurn: null, evOffset: 0, audioVersion: null });
  S.mic = { rate: 24000, startS: 0, win: 480, peaks: [] };
}

function onMessage(msg) {
  if (msg.type === "sessions") {
    S.sessions = msg.sessions; S.status = msg.status; renderSessions(); renderChips(); renderSetup();
    if (typeof Talk !== "undefined") Talk.onSessions(S.sessions);
    return;
  }
  if (!S.sid) return;
  if (msg.type === "snapshot" && msg.session.id === S.sid) {
    const first = !S.snap;
    S.snap = msg.session;
    scheduleRender(first);
  } else if (msg.type === "series") {
    for (const r of msg.rows) {
      if (r.seq <= S.seriesSeq) continue;
      S.seriesSeq = r.seq;
      if (r.kind === "vad") for (const f of r.data.frames) S.vad.push(f);
      else if (r.kind === "span") S.spans.push(r.data);
      else if (r.kind === "queues") S.queues.push({ t: r.t, depths: r.data.depths });
    }
    drawTimeline();
  } else if (msg.type === "peaks") {
    const m = msg.mic;
    S.perS = msg.per_s;
    S.mic.rate = m.rate; S.mic.startS = m.start_s || 0; S.mic.win = Math.max(1, Math.floor(m.rate / msg.per_s));
    const idx = Math.floor(m.from / S.mic.win);
    for (let i = 0; i < m.peaks.length; i++) S.mic.peaks[idx + i] = m.peaks[i];
    Object.assign(S.resp, msg.responses);
    drawTimeline();
  }
}

let renderPending = false;
function scheduleRender(first) {
  if (renderPending) return;
  renderPending = true;
  // rAF is frozen in hidden tabs: fall back to a timer so a background dashboard stays current.
  const run = () => { renderPending = false; renderSession(first); };
  if (document.hidden) setTimeout(run, 50); else requestAnimationFrame(run);
}

// ------------------------------------------------------------------ chips / sidebar
function renderChips() {
  const st = S.status || {};
  const chips = [];
  chips.push(`<span class="chip ok">A · proxy <b>${esc(st.listen || "")}</b> → ${esc(st.upstream || "")}</span>`);
  if (st.llm_upstream) {
    const ago = st.llm_tap && st.llm_tap.last_call_s_ago;
    chips.push(`<span class="chip ${ago == null ? "warn" : "ok"}">B · LLM <b>${ago == null ? "no call yet" : "seen " + dur(ago) + " ago"}</b></span>`);
  } else chips.push(`<span class="chip off">B · LLM <b>not configured</b></span>`);
  const p = st.probe || {};
  if (p.connected) {
    const hooks = (p.status && p.status.hooks) || {};
    const on = Object.entries(hooks).filter(([, v]) => v === "on").map(([k]) => k);
    const off = Object.entries(hooks).filter(([, v]) => v !== "on").map(([k]) => k);
    chips.push(`<span class="chip ${off.length ? "warn" : "ok"}" title="${esc(JSON.stringify(hooks))}">C · probe <b>${esc(on.join(", ") || "connected")}${off.length ? " · off: " + esc(off.join(", ")) : ""}</b></span>`);
  } else chips.push(`<span class="chip off">C · probe <b>not connected</b></span>`);
  if (st.record_audio === false) chips.push(`<span class="chip warn">audio <b>not recorded</b></span>`);
  $("chips").innerHTML = chips.join("");
}

function renderSetup() {
  const st = S.status || {};
  const port = (st.listen || ":8765").split(":").pop();
  $("setup").innerHTML = `<div class="setup mono">
    <div>client → <b>ws://${esc(location.hostname)}:${esc(port)}/v1/realtime?client=NAME</b></div>
    ${Object.keys(st.routes || {}).map((r) => `<div>route <b>${esc(r)}</b> → ws://${esc(location.hostname)}:${esc(port)}/${esc(r)}/v1/realtime</div>`).join("")}
    ${st.llm_upstream ? `<div>speech-to-speech → <b>--responses_api_base_url http://${esc(st.llm_listen)}</b></div>` : ""}
  </div>`;
}

function renderSessions() {
  const clients = [...new Set(S.sessions.map((s) => s.meta.client).filter(Boolean))];
  const sel = $("client-filter");
  const cur = sel.value;
  sel.innerHTML = `<option value="">all clients</option>` + clients.map((c) => `<option ${c === cur ? "selected" : ""}>${esc(c)}</option>`).join("");
  const list = S.sessions.filter((s) => !S.clientFilter || s.meta.client === S.clientFilter);
  $("no-sessions").hidden = list.length > 0;
  $("clear-btn").hidden = !S.sessions.some((x) => !x.live);
  $("sessions").innerHTML = list.map((s) => {
    const st = s.stats || {};
    return `<li data-sid="${esc(s.id)}" class="${s.id === S.sid ? "active" : ""}">
      <div class="row1"><span class="dot ${s.live ? "live" : ""}"></span><span class="client">${esc(s.meta.client || "client")}</span>
      <span class="when">${esc(new Date(s.started_at * 1000).toLocaleString("en-GB", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" }))}</span></div>
      <div class="row2"><span>${dur(st.duration)}</span><span>${st.turns ?? 0} turns</span><span>e2e ${sec(st.e2e_p50)}</span><span>${kfmt((st.tokens_in || 0) + (st.tokens_out || 0))} tok</span></div>
    </li>`;
  }).join("");
}

$("sessions").addEventListener("click", (e) => {
  const li = e.target.closest("li[data-sid]");
  if (!li) return;
  openSession(li.dataset.sid);
  $("sidebar").classList.remove("open");
});
$("client-filter").addEventListener("change", (e) => { S.clientFilter = e.target.value; renderSessions(); });
$("toggle-sessions").addEventListener("click", () => $("sidebar").classList.toggle("open"));

function openSession(sid) {
  stopPlayback();
  S.sid = sid;
  history.replaceState(null, "", `#${sid}`);
  $("welcome").hidden = true;
  $("session").hidden = false;
  subscribe(sid);
  renderSessions();
  $("feed").innerHTML = `<p class="muted">Loading…</p>`;
}

// ------------------------------------------------------------------ session view
function renderSession(first) {
  const s = S.snap;
  if (!s) return;
  const meta = s.meta || {};
  $("s-client").textContent = meta.client || "client";
  $("s-id").textContent = s.id;
  $("live-badge").hidden = !s.live;
  $("delete-btn").hidden = s.live;
  $("s-meta").innerHTML = [
    meta.remote && `from ${esc(meta.remote)}`,
    meta.upstream && `→ ${esc(meta.upstream)}`,
    `started ${clock(s.t0)}`,
    `input ${esc(inputLabel(s))}`,
    `output ${esc(outputLabel(s))}`,
    meta.close_reason && `ended: ${esc(meta.close_reason)}`,
  ].filter(Boolean).map((x) => `<span>${x}</span>`).join("");
  renderModels(s);
  renderStats();
  renderFeed();
  renderInspector();
  if (S.tab === "config") renderConfig();
  if (S.tab === "events" && s.live) loadEvents(true);
  drawTimeline(first);
}

// Pipeline backends from probe C: one short label per handler (model id, voice, VAD settings).
const MODEL_KEYS = ["model_name", "model", "model_id", "repo_id", "checkpoint"];
function modelLabel(row) {
  const c = row.config || {};
  if (row.stage === "vad") {
    const parts = [c.vad || "silero"];
    if (c.thresh != null) parts.push(`thresh ${c.thresh}`);
    if (c.min_speech_ms != null) parts.push(`speech ≥ ${c.min_speech_ms} ms`);
    if (c.min_silence_ms != null) parts.push(`silence ${c.min_silence_ms} ms`);
    if (c.smart_turn != null) parts.push(c.smart_turn ? `Smart Turn @${c.smart_turn_threshold ?? "?"}` : "no Smart Turn");
    return parts.join(" · ");
  }
  const key = MODEL_KEYS.find((k) => typeof c[k] === "string" && c[k]);
  const parts = [key ? c[key] : row.handler];
  for (const k of ["voice", "speaker", "language"]) if (c[k] != null && c[k] !== "") parts.push(`${k} ${c[k]}`);
  return parts.join(" · ");
}
function renderModels(s) {
  const rows = (s.models || []).filter((r) => r.stage !== "other");
  const el = $("s-models");
  el.hidden = !rows.length;
  const multi = new Set(rows.map((r) => r.pipeline)).size > 1;
  el.innerHTML = rows.map((r) => `<span class="model" title="${esc(`${r.handler}\n${JSON.stringify(r.config, null, 2)}`)}"><b>${esc(r.stage.toUpperCase())}${multi ? ` p${esc(r.pipeline)}` : ""}</b>${esc(modelLabel(r))}</span>`).join("");
}

const RATE_SOURCE = { declared: "declared", default: "not declared, server default", measured: "measured from the stream", fixed: "repaired" };
function inputLabel(s) {
  const f = s.input_format || {}, m = s.mic || {};
  if (!m.rate) return f.type || "—";
  let label = `${f.type || f.codec || "audio"} ${m.rate / 1000} kHz`;
  if (m.rate_source && m.rate_source !== "declared") label += ` (${RATE_SOURCE[m.rate_source] || m.rate_source})`;
  if (m.rate_source === "measured" && m.declared_rate) label += ` · declared ${m.declared_rate / 1000} kHz`;
  return label;
}
function outputLabel(s) {
  const f = s.output_format || {};
  const rate = (s.placements.find((p) => p.rate) || {}).rate || f.rate;
  return `${f.type || f.codec || "audio"}${rate ? ` ${rate / 1000} kHz` : ""}${f.rate ? "" : rate ? " (not declared, server default)" : ""}`;
}

function renderStats() {
  const st = S.snap.stats;
  const items = [
    ["e2e p50", sec(st.e2e_p50), "end of speech → first audio, measured at the proxy"],
    ["e2e p95", sec(st.e2e_p95), ""],
    ["server e2e", sec(st.server_e2e_p50), "median e2e_s reported by speech-to-speech"],
    ["LLM TTFT", sec(st.ttft_median), "median llm_ttft_s"],
    ["tokens", `${kfmt(st.tokens_in)} <small>→ ${kfmt(st.tokens_out)}</small>`, "input → output"],
    ["turns", st.turns, ""],
    ["interruptions", st.interruptions, ""],
    ["tool calls", st.tool_calls, ""],
    ["images", st.images, ""],
    ["LLM calls", st.llm_calls, "captured by proxy B"],
    ["errors", st.errors, ""],
    ["duration", dur(st.duration), ""],
  ];
  $("stats").innerHTML = items.map(([l, v, t]) => `<div class="stat" title="${esc(t)}"><div class="l">${l}</div><div class="v">${v}</div></div>`).join("");
}

// Server latency record (speech-to-speech docs/response-latency.md, v1 and v2). A field the record
// carries as null was not measured (e.g. a backend without STT timing) and is shown as n/a. A field
// the record does not carry at all (e.g. llm_ttft_s, dropped in v2) is left out.
const LAT_FIELDS = [
  ["vad decision", "vad_decision_s", COLORS.probe],
  ["hold", "hold_s", COLORS.faint],
  ["stt", "stt_s", COLORS.user],
  ["llm ttft", "llm_ttft_s", COLORS.prefill],
  ["llm", "llm_s", COLORS.proc],
  ["tts ttfa", "tts_ttfa_s", COLORS.asst],
  ["server e2e", "e2e_s", COLORS.text],
];

function latParts(turn) {
  // stacked bar: stt, llm ttft, tools, rest of llm, tts ttfa (server values when present)
  const r = turn.responses.find((x) => x.latency) || {};
  const l = r.latency || {};
  return [["stt", l.stt_s, COLORS.user], ["ttft", l.llm_ttft_s, COLORS.prefill], ["tts", l.tts_ttfa_s, COLORS.asst]];
}

function renderFeed() {
  const turns = S.snap.turns;
  if (!turns.length) { $("feed").innerHTML = `<p class="muted">No turn yet. Talk to the client…</p>`; return; }
  const html = turns.slice().reverse().map((t) => {
    const badges = [];
    if (t.interrupted_at != null) badges.push(`<span class="badge warn">interrupted at ${tc(t.interrupted_at)}</span>`);
    const nTools = t.responses.reduce((a, r) => a + r.tools.length, 0);
    if (nTools) badges.push(`<span class="badge tool">${nTools} tool call${nTools > 1 ? "s" : ""}</span>`);
    if (t.images.length) badges.push(`<span class="badge tool">${t.images.length} image${t.images.length > 1 ? "s" : ""}</span>`);
    if (t.llm_calls.length) badges.push(`<span class="badge">${t.llm_calls.length} LLM call${t.llm_calls.length > 1 ? "s" : ""}</span>`);
    if (t.responses.some((r) => r.status && r.status !== "completed")) badges.push(`<span class="badge err">${esc(t.responses.map((r) => r.status).filter((x) => x && x !== "completed").join(", "))}</span>`);
    let user = "";
    if (t.transcript) user += `<div class="bubble u ${t.transcript_final ? "" : "partial"}">${esc(t.transcript)}</div>`;
    else if (t.source === "speech") user += `<div class="bubble u partial">…</div>`;
    for (const x of t.texts) user += `<div class="bubble u">${esc(x)}</div>`;
    if (t.images.length) user += `<div class="thumbs">${t.images.map((i) => `<img src="${esc(imgSrc(i.src))}" alt="image sent" loading="lazy">`).join("")}</div>`;
    const resps = t.responses.map((r) => {
      const tools = r.tools.map((x) => `<div class="toolcard"><code>${esc(x.name || "tool")}(${esc(trunc(x.arguments, 120))})</code>
        ${x.output != null ? `<div class="out">→ ${esc(trunc(x.output, 240))}${x.t_output != null ? ` · ${sec(x.t_output - x.t_call)}` : ""}</div>` : `<div class="out">waiting for the result…</div>`}</div>`).join("");
      const text = r.text ? `<div class="bubble a">${esc(r.text)}
        <div class="meta">${r.usage ? `<span>${kfmt(r.usage.input_tokens)} → ${kfmt(r.usage.output_tokens)} tok</span>` : ""}
        ${r.latency && r.latency.e2e_s != null ? `<span>server e2e ${sec(r.latency.e2e_s)}</span>` : ""}
        ${r.audio_s ? `<span>audio ${sec(r.audio_s, 1)}</span>` : ""}${r.truncate_ms != null ? `<span>played ${sec(r.truncate_ms / 1000, 1)}</span>` : ""}</div></div>` : "";
      return tools + text;
    }).join("");
    const parts = latParts(t);
    const total = parts.reduce((a, [, v]) => a + (v || 0), 0) || 1;
    const bar = parts.some(([, v]) => v) ? `<div class="latbar" title="${esc(parts.map(([k, v]) => `${k} ${ms(v)}`).join(" · "))}">${parts.map(([, v, c]) => `<i style="width:${((v || 0) / total) * 100}%;background:${c}"></i>`).join("")}</div>` : "";
    return `<div class="turn ${S.selTurn === t.idx ? "sel" : ""}" data-idx="${t.idx}">
      <div class="turn-head"><span>turn ${t.idx}</span><span>${tc(t.t_start)}</span>${t.proxy_e2e != null ? `<span class="badge ok">e2e ${sec(t.proxy_e2e)}</span>` : ""}${badges.join("")}</div>
      ${user}${resps}${bar}</div>`;
  }).join("");
  $("feed").innerHTML = html;
}
const trunc = (s, n) => { s = s == null ? "" : String(s); return s.length > n ? s.slice(0, n) + "…" : s; };

$("feed").addEventListener("click", (e) => {
  const el = e.target.closest(".turn");
  if (!el) return;
  S.selTurn = Number(el.dataset.idx);
  const turn = S.snap.turns.find((x) => x.idx === S.selTurn);
  if (turn) { seek(Math.max(0, (turn.speech_start ?? turn.t_start) - 0.3)); centerOn(S.pos); }
  renderFeed(); renderInspector();
});

function renderInspector() {
  const turn = S.snap && S.snap.turns.find((x) => x.idx === S.selTurn);
  if (!turn) { $("inspector").innerHTML = `<p class="muted">Click a turn for details: latencies, LLM request, TTS segments.</p>`; return; }
  const out = [];
  out.push(`<h3>Turn ${turn.idx} · ${tc(turn.t_start)}</h3>
    <div style="display:flex;gap:6px;flex-wrap:wrap"><button class="btn small" data-act="play-turn">▶ replay this turn</button>
    ${turn.responses.filter((r) => r.audio_s).map((r) => `<button class="btn small" data-act="play-resp" data-rid="${esc(r.id)}">▶ generated TTS ${turn.responses.length > 1 ? esc(r.id.slice(-6)) : ""} (${sec(r.audio_s, 1)})</button>`).join("")}</div>`);
  // latency bars
  const bars = [];
  if (turn.proxy_e2e != null) bars.push(["proxy e2e", turn.proxy_e2e, COLORS.proc]);
  for (const r of turn.responses) {
    const l = r.latency; if (!l) continue;
    const tag = turn.responses.length > 1 ? ` ·${r.id.slice(-4)}` : "";
    for (const [k, key, c] of LAT_FIELDS) if (key in l) bars.push([k + tag, l[key], c]);
  }
  if (bars.length) {
    const max = Math.max(...bars.map((b) => b[1] ?? 0), 0.001);
    const row = ([k, v, c]) => v == null
      ? `<div class="hbar na" title="The server reported no value: this stage was not measured for this backend, or did not run."><span>${esc(k)}</span><div class="track"></div><span class="val">n/a</span></div>`
      : `<div class="hbar"><span>${esc(k)}</span><div class="track"><div class="fill" style="width:${(v / max) * 100}%;background:${c}"></div></div><span class="val">${ms(v)}</span></div>`;
    const ver = turn.responses.map((r) => r.latency && r.latency.version).find((x) => x != null);
    out.push(`<h3>Latency${ver != null ? ` <small class="muted">server record v${esc(ver)}</small>` : ""}</h3><div class="hbars">${bars.map(row).join("")}</div>`);
  }
  const kv = [];
  if (turn.speech_start != null) kv.push(["speech", `${tc(turn.speech_start)} → ${turn.speech_end != null ? tc(turn.speech_end) : "…"}${turn.speech_end != null ? ` (${sec(turn.speech_end - turn.speech_start, 1)})` : ""}`]);
  if (turn.smart_turn) kv.push(["smart turn", `${num(turn.smart_turn.probability, 2)} ${turn.smart_turn.complete ? "→ end of turn" : "→ waits for more"} · ${ms(turn.smart_turn.duration_s)}`]);
  if (turn.t_transcript != null && turn.speech_end != null) kv.push(["transcribed", `+${ms(turn.t_transcript - turn.speech_end)} after end of speech`]);
  if (turn.first_audio != null) kv.push(["first audio", tc(turn.first_audio)]);
  kv.push(["tokens", `${kfmt(turn.tokens_in)} → ${kfmt(turn.tokens_out)}`]);
  out.push(`<h3>Markers</h3><dl class="kv">${kv.map(([k, v]) => `<dt>${esc(k)}</dt><dd>${esc(v)}</dd>`).join("")}</dl>`);
  // LLM calls
  if (turn.llm_calls.length) {
    out.push(`<h3>LLM calls (proxy B)</h3>`);
    for (const c of turn.llm_calls) {
      const ttft = c.t_first_token != null ? c.t_first_token - c.t_start : null;
      const total = c.t_end != null ? c.t_end - c.t_start : null;
      const u = c.usage || {};
      const tin = u.input_tokens ?? u.prompt_tokens, tout = u.output_tokens ?? u.completion_tokens;
      const tim = c.timings || {};
      const tps = tim.predicted_per_second ?? (tout && ttft != null && total ? tout / Math.max(0.001, total - ttft) : null);
      const req = c.request || {};
      const items = Array.isArray(req.input) ? req.input : Array.isArray(req.messages) ? req.messages : req.input ? [{ role: "user", content: req.input }] : [];
      const requested = c.requested_model && c.served_model && c.requested_model !== c.served_model
        ? ` <span class="muted" title="name sent by the client, ignored by the server">(requested ${esc(c.requested_model)})</span>` : "";
      out.push(`<dl class="kv"><dt>model</dt><dd>${esc(c.model || "—")}${requested} · ${esc(c.endpoint)}${c.error ? ` · <span style="color:var(--error)">${esc(c.error)}</span>` : ""}</dd>
        <dt>ttft</dt><dd>${ms(ttft)}${tim.prompt_ms != null ? ` (prefill ${Math.round(tim.prompt_ms)} ms)` : ""}</dd>
        <dt>duration</dt><dd>${ms(total)}${tps ? ` · ${num(tps, 1)} tok/s` : ""}</dd>
        <dt>tokens</dt><dd>${kfmt(tin)} → ${kfmt(tout)}${tim.prompt_n != null ? ` · prompt evaluated ${tim.prompt_n}` : ""}</dd>
        <dt>input</dt><dd>${items.length} items · ${(c.tool_names || []).length} tools</dd>
        ${c.tool_calls && c.tool_calls.length ? `<dt>calls</dt><dd>${esc(c.tool_calls.map((x) => x.name).join(", "))}</dd>` : ""}</dl>
        ${req.instructions ? `<details class="req"><summary>instructions (${String(req.instructions).length} chars)</summary><pre>${esc(req.instructions)}</pre></details>` : ""}
        <details class="req"><summary>history sent (${items.length})</summary>${items.map(renderMsg).join("")}</details>
        ${c.output_text ? `<details class="req"><summary>raw output</summary><pre>${esc(c.output_text)}</pre></details>` : ""}
        <details class="req"><summary>request JSON</summary><pre class="json">${esc(JSON.stringify(req, null, 2))}</pre></details>`);
    }
  }
  if (turn.tts_segments.length) {
    out.push(`<h3>TTS segments (probe C)</h3><ul class="seglist">${turn.tts_segments.map((x) => `<li><span class="t">${tc(x.t)}</span>${esc(x.text)}</li>`).join("")}</ul>`);
  }
  for (const r of turn.responses) {
    out.push(`<details class="req"><summary>response ${esc(r.id)} · ${esc(r.status || "in progress")}</summary><pre class="json">${esc(JSON.stringify(r, null, 2))}</pre></details>`);
  }
  $("inspector").innerHTML = out.join("");
}
function renderMsg(m) {
  const role = m.role || m.type || "item";
  let text = "";
  if (typeof m.content === "string") text = m.content;
  else if (Array.isArray(m.content)) text = m.content.map((c) => c.text ?? c.input_text ?? (c.type === "input_image" || c.type === "image_url" ? "[image]" : c.type)).join(" ");
  else if (m.type === "function_call") text = `${m.name}(${m.arguments || ""})`;
  else if (m.type === "function_call_output") text = `→ ${m.output}`;
  return `<div class="msg ${esc(role)}"><div class="role">${esc(role)}</div>${esc(trunc(text, 600))}</div>`;
}
$("inspector").addEventListener("click", (e) => {
  const b = e.target.closest("button[data-act]");
  if (!b) return;
  const turn = S.snap.turns.find((x) => x.idx === S.selTurn);
  if (b.dataset.act === "play-turn" && turn) { seek(Math.max(0, (turn.speech_start ?? turn.t_start) - 0.3)); startPlayback(); }
  if (b.dataset.act === "play-resp") playResponse(b.dataset.rid);
});

// ------------------------------------------------------------------ tabs, events, config
document.querySelectorAll(".tab").forEach((b) => b.addEventListener("click", () => {
  S.tab = b.dataset.tab;
  document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === b));
  document.querySelectorAll(".tab-panel").forEach((p) => (p.hidden = p.dataset.panel !== S.tab));
  if (S.tab === "events") loadEvents();
  if (S.tab === "config") renderConfig();
  if (S.tab === "conv") drawTimeline();
}));

let evTimer = 0;
async function loadEvents(throttled = false) {
  if (!S.sid) return;
  if (throttled) { if (Date.now() - evTimer < 2000) return; }
  evTimer = Date.now();
  const params = new URLSearchParams({ offset: S.evOffset, limit: 200 });
  if ($("ev-type").value) params.set("type", $("ev-type").value);
  if ($("ev-source").value) params.set("source", $("ev-source").value);
  const res = await fetch(`/api/sessions/${S.sid}/events?${params}`).then((r) => r.json());
  $("ev-page").textContent = `${res.total ? S.evOffset + 1 : 0}–${Math.min(S.evOffset + 200, res.total)} / ${res.total}`;
  const open = new Set([...document.querySelectorAll(".ev[open]")].map((d) => d.dataset.id));
  $("ev-list").innerHTML = res.rows.map((r) => `<details class="ev" data-id="${r.id}" ${open.has(String(r.id)) ? "open" : ""}><summary><span>${tc(r.t)}</span><span class="src ${esc(r.source)}">${esc(r.source)}</span><span>${esc(r.type || "")}</span></summary><pre class="json">${esc(JSON.stringify(r.data, null, 2))}</pre></details>`).join("") || `<p class="muted">No events.</p>`;
}
$("ev-refresh").addEventListener("click", () => loadEvents());
$("ev-type").addEventListener("change", () => { S.evOffset = 0; loadEvents(); });
$("ev-source").addEventListener("change", () => { S.evOffset = 0; loadEvents(); });
$("ev-prev").addEventListener("click", () => { S.evOffset = Math.max(0, S.evOffset - 200); loadEvents(); });
$("ev-next").addEventListener("click", () => { S.evOffset += 200; loadEvents(); });

function renderConfig() {
  const s = S.snap; if (!s) return;
  const cfg = s.config || {};
  const st = S.status || {};
  $("config").innerHTML = `
    <div class="card"><h3>Instructions</h3><pre class="json">${esc(cfg.instructions || "—")}</pre></div>
    <div class="card"><h3>Declared tools</h3>${(cfg.tools || []).length ? (cfg.tools || []).map((t) => `<details class="req"><summary>${esc(t.name || (t.function || {}).name || t.type)}</summary><pre class="json">${esc(JSON.stringify(t, null, 2))}</pre></details>`).join("") : "<p class='muted'>—</p>"}</div>
    <div class="card"><h3>Connection</h3><pre class="json">${esc(JSON.stringify({ ...s.meta, input_format: s.input_format, output_format: s.output_format, voice: cfg.voice, model: cfg.model, mic: s.mic }, null, 2))}</pre></div>
    <div class="card"><h3>Pipeline (probe C)</h3>${(s.models || []).length ? (s.models || []).map((r) => `<details class="req"><summary>${esc(r.stage.toUpperCase())} · ${esc(r.handler)}${r.pipeline != null ? ` · pipeline ${esc(r.pipeline)}` : ""} · ${esc(modelLabel(r))}</summary><pre class="json">${esc(JSON.stringify(r.config, null, 2))}</pre></details>`).join("") : "<p class='muted'>Not captured: start speech-to-speech through <code>s2snoop s2s</code>.</p>"}</div>
    <div class="card"><h3>Probe</h3><pre class="json">${esc(JSON.stringify(st.probe || {}, null, 2))}</pre></div>
    <div class="card"><h3>Errors (${s.errors.length})</h3>${s.errors.length ? s.errors.map((e) => `<div class="msg system"><div class="role">${tc(e.t)} ${esc(e.code || "")}</div>${esc(e.message)}</div>`).join("") : "<p class='muted'>None.</p>"}</div>`;
}
$("clear-btn").addEventListener("click", async () => {
  const ended = S.sessions.filter((x) => !x.live).length;
  if (!ended) return;
  if (!window.confirm(`Delete ${ended} ended session${ended > 1 ? "s" : ""} with their audio and images? Live sessions are kept.`)) return;
  await fetch("/api/sessions", { method: "DELETE" });
  const current = S.sessions.find((x) => x.id === S.sid);
  if (current && !current.live) { stopPlayback(); S.sid = null; history.replaceState(null, "", location.pathname); $("session").hidden = true; $("welcome").hidden = false; }
  S.sessions = S.sessions.filter((x) => x.live);
  renderSessions();
});
$("delete-btn").addEventListener("click", async () => {
  if (!S.sid || !window.confirm("Delete this session and its audio?")) return;
  await fetch(`/api/sessions/${S.sid}`, { method: "DELETE" });
  S.sid = null; $("session").hidden = true; $("welcome").hidden = false;
});

// ------------------------------------------------------------------ timeline
const LABEL_W = 122;
const HEADER_H = 34;  // time axis (y≈13) + latency labels (y≈28)
const LANE_PAD = 5;
const canvas = $("tl-canvas"), ctx = canvas.getContext("2d");
const scroller = $("tl-scroll"), spacer = $("tl-spacer");

function lanes() {
  const turns = S.snap ? S.snap.turns : [];
  const L = [{ id: "user", name: "User", sub: "mic + VAD", h: 72 }];
  if (S.vad.length) L.push({ id: "vad", name: "VAD", sub: "Silero prob.", h: 50 });
  if (turns.some((t) => t.smart_turn)) L.push({ id: "turn", name: "Smart Turn", sub: "end of turn?", h: 36 });
  if (turns.some((t) => t.llm_calls.length) || turns.some((t) => t.responses.some((r) => r.tools.length)))
    L.push({ id: "llm", name: "LLM + tools", sub: "proxy B", h: 40 });
  if (turns.some((t) => t.tts_segments.length) || S.spans.some((s) => /TTS/i.test(s.handler)))
    L.push({ id: "tts", name: "TTS", sub: "segments", h: 36 });
  L.push({ id: "asst", name: "Assistant", sub: "audio as heard", h: 72 });
  if (S.queues.length) L.push({ id: "queues", name: "Queues", sub: "depth", h: 40 });
  let y = HEADER_H;
  for (const l of L) { l.y = y + LANE_PAD; y += l.h + 2 * LANE_PAD; l.top = l.y - LANE_PAD; l.bottom = y; }
  return { L, height: y + 2 };
}

function duration() {
  if (!S.snap) return 1;
  let d = S.snap.stats.duration || 0;
  if (S.snap.live) d = Math.max(d, Date.now() / 1000 - S.snap.t0);
  for (const p of S.snap.placements) d = Math.max(d, p.play_start + p.audio_s);
  return Math.max(d, 1);
}

let lastLayoutH = 0;
function drawTimeline(center) {
  if (!S.snap || S.tab !== "conv") return;
  const { L, height } = lanes();
  const wrapW = $("tl-wrap").clientWidth;
  const total = LABEL_W + duration() * S.pxs + 40;
  spacer.style.width = `${total}px`;
  const sbar = Math.max(0, scroller.offsetHeight - scroller.clientHeight);
  if (height + sbar !== lastLayoutH) { scroller.style.height = `${height + sbar}px`; lastLayoutH = height + sbar; }
  const dpr = window.devicePixelRatio || 1;
  if (canvas.width !== Math.round(wrapW * dpr) || canvas.height !== Math.round(height * dpr)) {
    canvas.width = Math.round(wrapW * dpr); canvas.height = Math.round(height * dpr);
    canvas.style.width = `${wrapW}px`; canvas.style.height = `${height}px`;
  }
  if (S.follow && S.snap.live && !S.playing) scroller.scrollLeft = Math.max(0, total - wrapW);
  else if (center === true && !S.snap.live) scroller.scrollLeft = 0;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, wrapW, height);
  const x0 = scroller.scrollLeft;
  const t0 = x0 / S.pxs, t1 = (x0 + wrapW - LABEL_W) / S.pxs;
  const X = (t) => LABEL_W + t * S.pxs - x0;
  const vis = (a, b) => b >= t0 - 0.5 && a <= t1 + 0.5;
  const turns = S.snap.turns;

  ctx.save();
  ctx.beginPath(); ctx.rect(LABEL_W, 0, wrapW - LABEL_W, height); ctx.clip();
  // lane bands (alternating) and header
  L.forEach((l, i) => { ctx.fillStyle = i % 2 ? "rgba(255,255,255,.018)" : "rgba(255,255,255,0)"; ctx.fillRect(LABEL_W, l.top, wrapW, l.bottom - l.top); });
  ctx.fillStyle = "#0e1016"; ctx.fillRect(LABEL_W, 0, wrapW, HEADER_H);
  // grid: major ticks with labels, minor ticks between
  const step = S.pxs > 160 ? 0.5 : S.pxs > 60 ? 1 : S.pxs > 25 ? 2 : S.pxs > 12 ? 5 : 10;
  const minor = step / (step >= 5 ? 5 : 2);
  for (let t = Math.floor(t0 / minor) * minor; t <= t1 + minor; t += minor) {
    const x = Math.round(X(t)) + 0.5;
    const major = Math.abs(t / step - Math.round(t / step)) < 1e-6;
    ctx.fillStyle = major ? "rgba(255,255,255,.07)" : "rgba(255,255,255,.03)";
    ctx.fillRect(x, major ? 18 : 24, 1, height - (major ? 18 : 24));
  }
  ctx.font = "10.5px 'Geist Mono', monospace"; ctx.textAlign = "center"; ctx.fillStyle = COLORS.faint;
  for (let t = Math.floor(t0 / step) * step; t <= t1 + step; t += step) ctx.fillText(tc(t).replace(/\.0$/, ""), X(t), 13);
  // latency gaps (end of speech → first audio) and barge-ins
  for (const t of turns) {
    if (t.speech_end != null && t.first_audio != null && vis(t.speech_end, t.first_audio)) {
      const xa = X(t.speech_end), xb = X(t.first_audio);
      ctx.fillStyle = "rgba(245,158,11,.07)"; ctx.fillRect(xa, HEADER_H, xb - xa, height - HEADER_H);
      ctx.fillStyle = "rgba(245,158,11,.55)"; ctx.fillRect(xa, HEADER_H - 3, xb - xa, 2);
      const label = `${num(t.first_audio - t.speech_end, 2)} s`;
      ctx.fillStyle = COLORS.proc; ctx.font = "600 10.5px 'Geist Mono', monospace";
      if (xb - xa > ctx.measureText(label).width + 6) ctx.fillText(label, (xa + xb) / 2, 28);
      else ctx.fillText(num(t.first_audio - t.speech_end, 1), (xa + xb) / 2, 28);
    }
    if (t.interrupted_at != null && vis(t.interrupted_at, t.interrupted_at)) {
      const x = X(t.interrupted_at);
      ctx.fillStyle = COLORS.err; ctx.fillRect(x - 1, HEADER_H - 4, 2, height - HEADER_H + 4);
      ctx.beginPath(); ctx.moveTo(x - 5, HEADER_H - 10); ctx.lineTo(x + 5, HEADER_H - 10); ctx.lineTo(x, HEADER_H - 4); ctx.fill();
    }
  }
  ctx.textAlign = "left";
  for (const l of L) {
    ctx.fillStyle = COLORS.sep; ctx.fillRect(LABEL_W, l.bottom - 0.5, wrapW, 1);
    const mid = l.y + l.h / 2;
    if (l.id === "user") drawUser(l, X, t0, t1, vis);
    else if (l.id === "vad") drawVad(l, X, t0, t1);
    else if (l.id === "turn") for (const t of turns) {
      if (!t.smart_turn || !vis(t.smart_turn.t, t.smart_turn.t)) continue;
      const x = X(t.smart_turn.t);
      ctx.fillStyle = t.smart_turn.complete ? COLORS.probe : COLORS.proc;
      ctx.beginPath(); ctx.arc(x, mid, 4, 0, 7); ctx.fill();
      ctx.font = "10.5px 'Geist Mono', monospace"; ctx.fillText(num(t.smart_turn.probability, 2), x + 7, mid + 4);
    }
    else if (l.id === "llm") drawLlm(l, X, vis);
    else if (l.id === "tts") drawTts(l, X, vis);
    else if (l.id === "asst") drawAsst(l, X, t0, t1);
    else if (l.id === "queues") drawQueues(l, X, t0, t1);
  }
  // playhead
  const px = X(S.pos);
  ctx.fillStyle = COLORS.text; ctx.fillRect(px - 0.75, 18, 1.5, height - 18);
  ctx.beginPath(); ctx.moveTo(px - 6, 16); ctx.lineTo(px + 6, 16); ctx.lineTo(px, 24); ctx.fill();
  ctx.restore();
  // labels column
  ctx.fillStyle = "#0e1016"; ctx.fillRect(0, 0, LABEL_W, height);
  ctx.fillStyle = COLORS.sep; ctx.fillRect(LABEL_W - 1, 0, 1, height); ctx.fillRect(0, HEADER_H - 0.5, wrapW, 1);
  ctx.textAlign = "left";
  for (const l of L) {
    const mid = (l.top + l.bottom) / 2;
    ctx.fillStyle = COLORS.sep; ctx.fillRect(0, l.bottom - 0.5, LABEL_W, 1);
    ctx.fillStyle = COLORS.text; ctx.font = "600 12px Inter, sans-serif"; ctx.fillText(l.name, 12, mid - 2);
    ctx.fillStyle = COLORS.faint; ctx.font = "10.5px Inter, sans-serif"; ctx.fillText(l.sub, 12, mid + 11);
  }
  $("time-label").textContent = tc(S.pos);
  renderLegend(L);
}

function drawBars(peaks, startT, perS, X, t0, t1, mid, half, color, alphaFrom) {
  if (!peaks || !peaks.length) return;
  const i0 = Math.max(0, Math.floor((t0 - startT) * perS) - 1), i1 = Math.min(peaks.length, Math.ceil((t1 - startT) * perS) + 1);
  const barW = Math.max(1, S.pxs / perS - (S.pxs / perS > 3 ? 1 : 0));
  // aggregate when several peaks fall in one pixel
  const per = Math.max(1, Math.floor(perS / S.pxs));
  for (let i = i0; i < i1; i += per) {
    let v = 0;
    for (let k = i; k < Math.min(i + per, i1); k++) v = Math.max(v, peaks[k] || 0);
    if (!v) continue;
    const t = startT + i / perS;
    const h = Math.max(1, (v / 255) * half);
    ctx.globalAlpha = alphaFrom != null && t >= alphaFrom ? 0.25 : 1;
    ctx.fillStyle = color; ctx.fillRect(X(t), mid - h, barW, h * 2);
  }
  ctx.globalAlpha = 1;
}

const TEXT_H = 15;  // text band at the top of the voice lanes
function drawUser(l, X, t0, t1, vis) {
  const mid = l.y + TEXT_H + (l.h - TEXT_H) / 2;
  for (const t of S.snap.turns) {
    if (t.speech_start == null) continue;
    const end = t.speech_end ?? duration();
    if (!vis(t.speech_start, end)) continue;
    ctx.fillStyle = "rgba(34,211,238,.08)"; ctx.fillRect(X(t.speech_start), l.y, X(end) - X(t.speech_start), l.h);
  }
  const startS = S.mic.startS || 0;
  drawBars(S.mic.peaks, startS, S.perS, X, t0, t1, mid, (l.h - TEXT_H) / 2 - 1, COLORS.user);
  ctx.font = "italic 11.5px Inter, sans-serif";
  const turns = S.snap.turns;
  for (let i = 0; i < turns.length; i++) {
    const t = turns[i]; const text = t.transcript || t.texts.join(" ");
    if (!text) continue;
    const a = t.speech_start ?? t.t_start;
    const next = turns[i + 1] ? (turns[i + 1].speech_start ?? turns[i + 1].t_start) : a + 60;
    if (!vis(a, next)) continue;
    const maxW = X(next) - X(a) - 8;
    if (maxW < 20) continue;
    ctx.fillStyle = COLORS.text;
    ctx.fillText(fitText(`“${text}”`, maxW), X(a) + 2, l.y + 11);
  }
  for (const t of turns) for (const img of t.images) {
    if (!vis(img.t, img.t)) continue;
    ctx.fillStyle = COLORS.tool; ctx.fillRect(X(img.t) - 5, l.y + l.h - 12, 10, 8);
  }
}
function fitText(s, w) {
  if (ctx.measureText(s).width <= w) return s;
  let lo = 0, hi = s.length;
  while (lo < hi) { const m = (lo + hi + 1) >> 1; if (ctx.measureText(s.slice(0, m) + "…").width <= w) lo = m; else hi = m - 1; }
  return s.slice(0, lo) + "…";
}

function drawVad(l, X, t0, t1) {
  const top = l.y, h = l.h;
  // frames: [t, p, thr, triggered, end]
  const fr = S.vad;
  let lo = 0, hi = fr.length;
  while (lo < hi) { const m = (lo + hi) >> 1; if (fr[m][0] < t0 - 0.2) lo = m + 1; else hi = m; }
  ctx.strokeStyle = COLORS.probe; ctx.lineWidth = 1.3; ctx.beginPath();
  let started = false, thr = null, trigStart = null;
  for (let i = lo; i < fr.length && fr[i][0] <= t1 + 0.2; i++) {
    const [t, p, th, trig] = fr[i];
    const x = X(t), y = top + h - p * h;
    if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    thr = th;
    if (trig && trigStart == null) trigStart = t;
    if (!trig && trigStart != null) { ctx.save(); ctx.fillStyle = "rgba(52,211,153,.08)"; ctx.fillRect(X(trigStart), top, X(t) - X(trigStart), h); ctx.restore(); trigStart = null; }
  }
  ctx.stroke();
  if (thr != null) {
    ctx.setLineDash([4, 4]); ctx.strokeStyle = COLORS.proc; ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(LABEL_W, top + h - thr * h); ctx.lineTo(X(t1 + 1), top + h - thr * h); ctx.stroke(); ctx.setLineDash([]);
  }
}

function drawLlm(l, X, vis) {
  const y = l.y + 4, h = l.h - 8;
  ctx.font = "10.5px 'Geist Mono', monospace";
  for (const t of S.snap.turns) {
    for (const c of t.llm_calls) {
      const a = c.t_start, f = c.t_first_token ?? c.t_first_byte ?? a, b = c.t_end ?? f;
      if (!vis(a, b)) continue;
      ctx.fillStyle = COLORS.prefill; ctx.fillRect(X(a), y, Math.max(1, X(f) - X(a)), h);
      ctx.fillStyle = c.error ? COLORS.err : COLORS.proc; ctx.fillRect(X(f), y, Math.max(1, X(b) - X(f)), h);
      const u = c.usage || {}; const out = u.output_tokens ?? u.completion_tokens;
      const label = `${ms(f - a)}${out ? ` · ${out} tok` : ""}`;
      if (X(b) - X(a) > ctx.measureText(label).width + 6) { ctx.fillStyle = COLORS.bg; ctx.fillText(label, X(a) + 3, y + h - 5); }
    }
    for (const r of t.responses) for (const tool of r.tools) {
      const a = tool.t_call, b = tool.t_output ?? a;
      if (!vis(a, b)) continue;
      ctx.fillStyle = "rgba(255,210,30,.35)"; ctx.fillRect(X(a), y + h / 2 - 2, Math.max(2, X(b) - X(a)), 4);
      ctx.fillStyle = COLORS.tool; ctx.save(); ctx.translate(X(a), y + h / 2); ctx.rotate(Math.PI / 4); ctx.fillRect(-4, -4, 8, 8); ctx.restore();
      ctx.fillText(tool.name || "tool", X(a) + 7, y + 9);
    }
  }
}

function drawTts(l, X, vis) {
  const y = l.y + 3, h = l.h - 6;
  for (const s of S.spans) {
    if (!/TTS/i.test(s.handler) || s.t_start == null || s.t_end == null || !vis(s.t_start, s.t_end)) continue;
    ctx.fillStyle = "rgba(139,125,255,.3)"; ctx.fillRect(X(s.t_start), y, Math.max(1, X(s.t_end) - X(s.t_start)), h);
  }
  ctx.font = "10.5px Inter, sans-serif";
  const segs = S.snap.turns.flatMap((t) => t.tts_segments);
  for (let i = 0; i < segs.length; i++) {
    const s = segs[i]; const next = segs[i + 1] ? segs[i + 1].t : s.t + 10;
    if (!vis(s.t, next)) continue;
    ctx.fillStyle = COLORS.asst; ctx.fillRect(X(s.t), y, 2, h);
    const w = X(next) - X(s.t) - 6;
    if (w > 16) { ctx.fillStyle = COLORS.text; ctx.fillText(fitText(s.text, w), X(s.t) + 5, y + h - 7); }
  }
}

function drawAsst(l, X, t0, t1) {
  const mid = l.y + TEXT_H + (l.h - TEXT_H) / 2;
  const texts = new Map(S.snap.turns.flatMap((t) => t.responses).map((r) => [r.id, r.text]));
  const ps = S.snap.placements;
  for (let i = 0; i < ps.length; i++) {
    const p = ps[i];
    if (!(p.play_start + p.audio_s >= t0 && p.play_start <= t1)) continue;
    const text = texts.get(p.response_id);
    const next = ps[i + 1] ? ps[i + 1].play_start : p.play_start + p.audio_s + 30;
    const maxW = X(next) - X(p.play_start) - 8;
    if (text && maxW > 20) {
      ctx.font = "italic 11.5px Inter, sans-serif"; ctx.fillStyle = "rgba(196,189,255,.95)";
      ctx.fillText(fitText(`“${text}”`, maxW), X(p.play_start) + 2, l.y + 11);
    }
    const pk = S.resp[p.response_id];
    if (pk) drawBars(pk, p.play_start, S.perS, X, t0, t1, mid, (l.h - TEXT_H) / 2 - 1, COLORS.asst, p.play_start + p.played_s);
    else { ctx.fillStyle = "rgba(139,125,255,.25)"; ctx.fillRect(X(p.play_start), mid - 3, X(p.play_start + p.audio_s) - X(p.play_start), 6); }
    if (p.cut) {
      const x = X(p.play_start + p.played_s);
      ctx.strokeStyle = COLORS.err; ctx.lineWidth = 1.5; ctx.setLineDash(p.estimated_cut ? [3, 3] : []);
      ctx.beginPath(); ctx.moveTo(x, l.y); ctx.lineTo(x, l.y + l.h); ctx.stroke(); ctx.setLineDash([]);
    }
  }
}

function drawQueues(l, X, t0, t1) {
  const q = S.queues; if (!q.length) return;
  const vals = q.map((x) => Math.max(0, ...Object.values(x.depths)));
  const max = Math.max(2, ...vals);
  ctx.fillStyle = "rgba(52,211,153,.25)"; ctx.strokeStyle = COLORS.probe; ctx.lineWidth = 1;
  ctx.beginPath(); ctx.moveTo(X(q[0].t), l.y + l.h);
  for (let i = 0; i < q.length; i++) {
    const y = l.y + l.h - (vals[i] / max) * l.h;
    const tn = i + 1 < q.length ? q[i + 1].t : Math.max(q[i].t, duration());
    ctx.lineTo(X(q[i].t), y); ctx.lineTo(X(tn), y);
  }
  ctx.lineTo(X(Math.max(q[q.length - 1].t, duration())), l.y + l.h); ctx.closePath(); ctx.fill(); ctx.stroke();
}

function renderLegend(L) {
  const has = (id) => L.some((l) => l.id === id);
  const items = [["#22d3ee", "user voice"], ["#8b7dff", "assistant voice (faded: generated, not played)"], ["rgba(245,158,11,.5)", "end of speech → first audio"], ["#ff6a75", "barge-in / cut"]];
  if (has("vad")) items.push(["#34d399", "VAD probability (dashed: threshold)"]);
  if (has("llm")) items.push(["rgba(245,158,11,.45)", "LLM prefill"], ["#f59e0b", "generation"], ["#ffd21e", "tool / image"]);
  const html = items.map(([c, t]) => `<span><i style="background:${c}"></i>${t}</span>`).join("");
  if ($("tl-legend").innerHTML !== html) $("tl-legend").innerHTML = html;
}

scroller.addEventListener("scroll", () => {
  const atEnd = scroller.scrollLeft + scroller.clientWidth >= scroller.scrollWidth - 4;
  if (!atEnd && S.snap && S.snap.live && !S.playing) { S.follow = false; $("follow").checked = false; }
  drawTimeline();
}, { passive: true });
$("follow").addEventListener("change", (e) => { S.follow = e.target.checked; drawTimeline(); });
$("zoom").addEventListener("input", (e) => zoomTo(Number(e.target.value), scroller.clientWidth / 2));
scroller.addEventListener("wheel", (e) => {
  if (!(e.ctrlKey || e.metaKey)) return;
  e.preventDefault();
  zoomTo(Math.min(400, Math.max(8, S.pxs * (e.deltaY < 0 ? 1.15 : 1 / 1.15))), e.offsetX);
}, { passive: false });
function zoomTo(pxs, anchorX) {
  const t = (scroller.scrollLeft + anchorX - LABEL_W) / S.pxs;
  S.pxs = pxs; $("zoom").value = String(Math.round(pxs));
  spacer.style.width = `${LABEL_W + duration() * S.pxs + 40}px`;
  scroller.scrollLeft = Math.max(0, t * S.pxs + LABEL_W - anchorX);
  drawTimeline();
}
function timeAt(clientX) {
  const r = scroller.getBoundingClientRect();
  return (scroller.scrollLeft + clientX - r.left - LABEL_W) / S.pxs;
}
scroller.addEventListener("click", (e) => {
  const r = scroller.getBoundingClientRect();
  if (e.clientX - r.left < LABEL_W) return;
  seek(Math.max(0, timeAt(e.clientX)));
  const turn = S.snap && [...S.snap.turns].reverse().find((t) => t.t_start <= S.pos + 0.05);
  if (turn && turn.idx !== S.selTurn) { S.selTurn = turn.idx; renderFeed(); renderInspector(); }
});
scroller.addEventListener("mousemove", (e) => {
  if (!S.snap) return;
  const r = scroller.getBoundingClientRect();
  const x = e.clientX - r.left, y = e.clientY - r.top;
  const tip = $("tl-tip");
  if (x < LABEL_W) { tip.hidden = true; return; }
  const t = timeAt(e.clientX);
  const { L } = lanes();
  const lane = L.find((l) => y >= l.top && y < l.bottom);
  let text = tc(t);
  if (lane) {
    if (lane.id === "vad" && S.vad.length) {
      const f = nearest(S.vad, t, (x) => x[0]);
      if (f) text += `\nprob. ${num(f[1], 2)} · threshold ${num(f[2], 2)}${f[3] ? " · speech" : ""}`;
    } else if (lane.id === "queues" && S.queues.length) {
      const q = [...S.queues].reverse().find((x) => x.t <= t);
      if (q) text += "\n" + Object.entries(q.depths).map(([k, v]) => `${k}: ${v}`).join("\n");
    } else if (lane.id === "llm") {
      const c = S.snap.turns.flatMap((x) => x.llm_calls).find((c) => c.t_start <= t && (c.t_end ?? c.t_start) >= t);
      if (c) text += `\n${c.model || c.endpoint} · ttft ${ms((c.t_first_token ?? c.t_start) - c.t_start)} · total ${ms((c.t_end ?? c.t_start) - c.t_start)}`;
    } else if (lane.id === "asst") {
      const p = S.snap.placements.find((p) => p.play_start <= t && p.play_start + p.audio_s >= t);
      const r = p && S.snap.turns.flatMap((x) => x.responses).find((r) => r.id === p.response_id);
      if (r) text += `\n${trunc(r.text, 140)}${p.cut ? `\ncut at ${sec(p.played_s, 1)}${p.estimated_cut ? " (estimated)" : ""}` : ""}`;
    } else {
      const turn = [...S.snap.turns].reverse().find((x) => x.t_start <= t);
      if (turn) text += `\nturn ${turn.idx}${turn.transcript ? " · " + trunc(turn.transcript, 120) : ""}`;
    }
  }
  tip.textContent = text; tip.hidden = false;
  tip.style.left = `${Math.min(x + 12, r.width - 220)}px`; tip.style.top = `${Math.min(y + 12, r.height - 40)}px`;
});
scroller.addEventListener("mouseleave", () => ($("tl-tip").hidden = true));
function nearest(arr, t, key) {
  let lo = 0, hi = arr.length - 1;
  if (hi < 0) return null;
  while (lo < hi) { const m = (lo + hi) >> 1; if (key(arr[m]) < t) lo = m + 1; else hi = m; }
  return arr[lo];
}
function centerOn(t) { scroller.scrollLeft = Math.max(0, LABEL_W + t * S.pxs - scroller.clientWidth / 2); S.follow = false; $("follow").checked = false; drawTimeline(); }
window.addEventListener("resize", () => drawTimeline());

// ------------------------------------------------------------------ playback
// Web Audio: both tracks are scheduled on the same AudioContext clock, so the
// user and assistant voices stay sample-aligned (no element drift).
const A = { ctx: null, mic: null, asst: null, srcs: [], gains: {}, startCtx: 0, startPos: 0, rate: 1, resp: null };
function audioCtx() {
  if (!A.ctx) {
    A.ctx = new (window.AudioContext || window.webkitAudioContext)();
    for (const k of ["mic", "asst", "resp"]) { A.gains[k] = A.ctx.createGain(); A.gains[k].connect(A.ctx.destination); }
    A.gains.mic.gain.value = $("mute-user").checked ? 0 : 1;
    A.gains.asst.gain.value = $("mute-asst").checked ? 0 : 1;
  }
  return A.ctx;
}
async function decode(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return audioCtx().decodeAudioData(await r.arrayBuffer());
}
async function loadAudio() {
  const version = `${S.sid}:${S.snap.stats.duration}:${S.snap.placements.length}:${S.snap.live}`;
  if (version === S.audioVersion && A.mic) return true;
  const note = $("audio-note");
  note.hidden = false; note.textContent = "Loading audio…";
  try {
    [A.mic, A.asst] = await Promise.all(["mic", "assistant"].map((k) => decode(`/api/sessions/${S.sid}/audio/${k}.wav`)));
    S.audioVersion = version; note.hidden = true;
    return true;
  } catch (e) {
    note.textContent = S.status && S.status.record_audio === false ? "Audio not recorded (--no-audio)." : `Audio unavailable (${e.message}).`;
    return false;
  }
}
const playPos = () => (S.playing ? A.startPos + (A.ctx.currentTime - A.startCtx) * A.rate : S.pos);
function stopSources() {
  for (const src of A.srcs) { try { src.onended = null; src.stop(); } catch (e) { /* already stopped */ } }
  A.srcs = [];
}
function schedule(pos) {
  stopSources();
  const ctx = audioCtx();
  A.rate = Number($("speed").value);
  A.startCtx = ctx.currentTime + 0.03;
  A.startPos = pos;
  for (const [buf, gain] of [[A.mic, A.gains.mic], [A.asst, A.gains.asst]]) {
    if (!buf || pos >= buf.duration) continue;
    const src = ctx.createBufferSource();
    src.buffer = buf; src.playbackRate.value = A.rate; src.connect(gain);
    src.start(A.startCtx, pos);
    A.srcs.push(src);
  }
}
function seek(t) {
  S.pos = Math.max(0, t);
  if (S.playing) schedule(S.pos);
  drawTimeline();
}
let raf = 0;
async function startPlayback() {
  const ctx = audioCtx();
  if (ctx.state === "suspended") ctx.resume().catch(() => {});
  stopResponse();
  if (!(await loadAudio())) return;
  const end = Math.max(A.mic ? A.mic.duration : 0, A.asst ? A.asst.duration : 0);
  if (S.pos >= end - 0.05) S.pos = 0;
  S.playing = true; $("play-btn").textContent = "❚❚";
  schedule(S.pos);
  cancelAnimationFrame(raf);
  const nextFrame = (fn) => (document.hidden ? setTimeout(fn, 100) : requestAnimationFrame(fn));
  const tick = () => {
    if (!S.playing) return;
    S.pos = playPos();
    if (S.pos >= end) { S.pos = end; stopPlayback(); return; }
    const x = LABEL_W + S.pos * S.pxs - scroller.scrollLeft;
    if (x > scroller.clientWidth - 60 || x < LABEL_W) scroller.scrollLeft = Math.max(0, LABEL_W + S.pos * S.pxs - scroller.clientWidth * 0.3);
    drawTimeline();
    raf = nextFrame(tick);
  };
  raf = nextFrame(tick);
}
function stopPlayback() {
  if (S.playing) S.pos = playPos();
  S.playing = false; $("play-btn").textContent = "▶";
  stopSources(); cancelAnimationFrame(raf); clearTimeout(raf);
  drawTimeline();
}
$("play-btn").addEventListener("click", () => (S.playing ? stopPlayback() : startPlayback()));
$("speed").addEventListener("change", () => { if (S.playing) { S.pos = playPos(); schedule(S.pos); } });
$("mute-user").addEventListener("change", (e) => { if (A.ctx) A.gains.mic.gain.value = e.target.checked ? 0 : 1; });
$("mute-asst").addEventListener("change", (e) => { if (A.ctx) A.gains.asst.gain.value = e.target.checked ? 0 : 1; });
document.addEventListener("keydown", (e) => {
  if (e.target.matches("input, select, textarea") || !S.sid) return;
  if (e.code === "Space") { e.preventDefault(); S.playing ? stopPlayback() : startPlayback(); }
  if (e.code === "ArrowLeft") seek(playPos() - 2);
  if (e.code === "ArrowRight") seek(playPos() + 2);
});
function stopResponse() { if (A.resp) { try { A.resp.stop(); } catch (e) { /* ended */ } A.resp = null; } }
async function playResponse(rid) {
  stopPlayback(); stopResponse();
  const ctx = audioCtx();
  if (ctx.state === "suspended") ctx.resume().catch(() => {});
  try {
    const buf = await decode(`/api/sessions/${S.sid}/audio/response/${encodeURIComponent(rid)}.wav`);
    const src = ctx.createBufferSource();
    src.buffer = buf; src.playbackRate.value = Number($("speed").value); src.connect(A.gains.resp); src.start();
    A.resp = src;
  } catch (e) {
    $("audio-note").hidden = false; $("audio-note").textContent = `TTS unavailable (${e.message}).`;
  }
}
window.__snoop = { S, A, playPos };

// ------------------------------------------------------------------ boot
connect();
setInterval(() => { if (S.snap && S.snap.live && S.tab === "conv") drawTimeline(); }, 500);
const initial = location.hash.slice(1);
if (initial) openSession(initial);
window.addEventListener("hashchange", () => { const sid = location.hash.slice(1); if (sid && sid !== S.sid) openSession(sid); });
