import json

from s2snoop.session import LATENCY_KEY, Session


def run(events):
    s = Session("s1")
    for t, source, ev in events:
        s.apply(t, source, ev)
    return s


def speech_turn(item, start_ms, end_ms, t_evt, text):
    return [
        (t_evt[0], "s2c", {"type": "input_audio_buffer.speech_started", "audio_start_ms": start_ms, "item_id": item}),
        (t_evt[1], "s2c", {"type": "input_audio_buffer.speech_stopped", "audio_end_ms": end_ms, "item_id": item}),
        (t_evt[1] + 0.2, "s2c", {"type": "conversation.item.input_audio_transcription.completed",
                                 "item_id": item, "transcript": text}),
    ]


def response(rid, item, t0, text, samples=24000, status="completed", latency=None, usage=None):
    body = {"id": rid, "status": status, "usage": usage or {"input_tokens": 100, "output_tokens": 10}}
    if latency:
        body["metadata"] = {LATENCY_KEY: json.dumps(latency)}
    return [
        (t0, "s2c", {"type": "response.created", "response": {"id": rid}}),
        (t0 + 0.01, "s2c", {"type": "response.output_item.added", "response_id": rid,
                            "item": {"id": item, "type": "message"}}),
        (t0 + 0.3, "s2c", {"type": "response.output_audio.delta", "response_id": rid, "item_id": item,
                           "_samples": samples, "_rate": 24000}),
        (t0 + 0.31, "s2c", {"type": "response.output_audio_transcript.delta", "response_id": rid, "delta": text}),
        (t0 + 0.5, "s2c", {"type": "response.done", "response": body}),
    ]


def test_simple_turn_latencies_and_tokens():
    mic = [(0.1, "snoop", {"type": "snoop.mic_start", "t_start": 0.0, "rate": 24000})]
    lat = {"stt_s": 0.18, "llm_ttft_s": 0.11, "e2e_s": 0.9, "status": "completed"}
    s = run(mic + speech_turn("u1", 500, 2600, (0.8, 3.0), "Hello")
            + response("r1", "a1", 3.1, "Hi there!", latency=lat))
    snap = s.snapshot()
    turn = snap["turns"][0]
    assert turn["speech_start"] == 0.5 and turn["speech_end"] == 2.6
    assert turn["transcript"] == "Hello"
    assert abs(turn["proxy_e2e"] - (3.4 - 2.6)) < 1e-9
    assert turn["responses"][0]["latency"]["llm_ttft_s"] == 0.11
    assert turn["tokens_in"] == 100 and turn["tokens_out"] == 10
    assert snap["stats"]["ttft_median"] == 0.11
    assert snap["placements"][0]["play_start"] == 3.4 and snap["placements"][0]["played_s"] == 1.0


def test_tool_call_image_and_follow_up_in_same_turn():
    events = speech_turn("u1", 0, 1000, (0.1, 1.0), "Qu'est-ce que tu vois ?")
    events += [
        (1.3, "s2c", {"type": "response.created", "response": {"id": "r1"}}),
        (1.5, "s2c", {"type": "response.output_item.done", "response_id": "r1",
                      "item": {"type": "function_call", "name": "camera", "call_id": "c1", "arguments": "{}"}}),
        (1.6, "s2c", {"type": "response.done", "response": {"id": "r1", "status": "completed"}}),
        (1.9, "c2s", {"type": "conversation.item.create",
                      "item": {"type": "function_call_output", "call_id": "c1", "output": "ok"}}),
        (1.95, "c2s", {"type": "conversation.item.create",
                       "item": {"type": "message", "role": "user",
                                "content": [{"type": "input_image", "image_url": "file:images/0001.jpg"}]}}),
    ]
    events += response("r2", "a2", 2.0, "Une tasse bleue.")
    s = run(events)
    snap = s.snapshot()
    assert len(snap["turns"]) == 1
    turn = snap["turns"][0]
    assert [r["id"] for r in turn["responses"]] == ["r1", "r2"]
    tool = turn["responses"][0]["tools"][0]
    assert tool["name"] == "camera" and tool["output"] == "ok" and tool["t_output"] == 1.9
    assert turn["images"] == [{"t": 1.95, "src": "file:images/0001.jpg"}]
    assert snap["stats"]["tool_calls"] == 1 and snap["stats"]["images"] == 1


def test_barge_in_with_truncate_cuts_playback():
    mic = [(0.0, "snoop", {"type": "snoop.mic_start", "t_start": 0.0, "rate": 24000})]
    events = mic + speech_turn("u1", 0, 1000, (0.1, 1.0), "Tell me a story")
    events += response("r1", "a1", 1.1, "Once upon a time", samples=24000 * 5)  # 5 s, plays from 1.4
    events += [(3.0, "s2c", {"type": "input_audio_buffer.speech_started", "audio_start_ms": 2900, "item_id": "u2"}),
               (3.05, "c2s", {"type": "conversation.item.truncate", "item_id": "a1", "content_index": 0,
                              "audio_end_ms": 1500})]
    s = run(events)
    snap = s.snapshot()
    assert snap["turns"][0]["interrupted_at"] == 2.9
    p = snap["placements"][0]
    assert p["played_s"] == 1.5 and p["cut"] and not p["estimated_cut"]
    assert snap["stats"]["interruptions"] == 1


def test_cancelled_response_without_truncate_gets_estimated_cut():
    mic = [(0.0, "snoop", {"type": "snoop.mic_start", "t_start": 0.0, "rate": 24000})]
    events = mic + speech_turn("u1", 0, 1000, (0.1, 1.0), "Tell me a story")
    events += response("r1", "a1", 1.1, "…", samples=24000 * 5)[:4]
    events += [(3.0, "s2c", {"type": "input_audio_buffer.speech_started", "audio_start_ms": 2900, "item_id": "u2"}),
               (3.1, "s2c", {"type": "response.done", "response": {"id": "r1", "status": "cancelled"}})]
    snap = run(events).snapshot()
    p = snap["placements"][0]
    assert p["estimated_cut"] and abs(p["played_s"] - 1.5) < 1e-9


def test_llm_and_probe_attach_to_turns():
    events = speech_turn("u1", 0, 1000, (0.1, 1.0), "Hi")
    events += [(1.2, "llm", {"t_start": 1.2, "t_first_token": 1.3, "t_end": 1.8, "endpoint": "responses"}),
               (0.95, "probe", {"kind": "smart_turn", "t": 0.95, "probability": 0.4, "complete": False}),
               (1.05, "probe", {"kind": "smart_turn", "t": 1.05, "probability": 0.91, "complete": True}),
               (1.9, "probe", {"kind": "tts_input", "t": 1.9, "text": "Hi there!"})]
    snap = run(events).snapshot()
    turn = snap["turns"][0]
    assert turn["llm_calls"][0]["t_first_token"] == 1.3
    assert turn["smart_turn"]["probability"] == 0.91
    assert [x["complete"] for x in turn["smart_turns"]] == [False, True]
    assert turn["tts_segments"][0]["text"] == "Hi there!"


def test_replay_is_deterministic():
    events = speech_turn("u1", 0, 1000, (0.1, 1.0), "A") + response("r1", "a1", 1.1, "B")
    assert run(events).snapshot() == run(events).snapshot()


def test_smart_turn_before_speech_stopped_goes_to_current_turn():
    events = speech_turn("u1", 0, 1000, (0.1, 1.0), "A")
    events += [(5.0, "s2c", {"type": "input_audio_buffer.speech_started", "audio_start_ms": 5000, "item_id": "u2"}),
               (8.2, "probe", {"kind": "smart_turn", "t": 8.2, "probability": 0.9, "complete": True}),
               (8.4, "s2c", {"type": "input_audio_buffer.speech_stopped", "audio_end_ms": 8150, "item_id": "u2"})]
    snap = run(events).snapshot()
    assert snap["turns"][0]["smart_turn"] is None
    assert snap["turns"][1]["smart_turn"]["probability"] == 0.9


def test_cancel_before_speech_started_still_cuts():
    """speech-to-speech cancels the response first, then emits speech_started."""
    mic = [(0.0, "snoop", {"type": "snoop.mic_start", "t_start": 0.0, "rate": 24000})]
    events = mic + speech_turn("u1", 0, 1000, (0.1, 1.0), "Tell me a story")
    events += response("r1", "a1", 1.1, "…", samples=24000 * 5)[:4]
    events += [(2.95, "s2c", {"type": "response.done", "response": {"id": "r1", "status": "cancelled"}}),
               (3.0, "s2c", {"type": "input_audio_buffer.speech_started", "audio_start_ms": 2900, "item_id": "u2"})]
    snap = run(events).snapshot()
    assert snap["turns"][0]["interrupted_at"] == 2.9
    p = snap["placements"][0]
    assert p["cut"] and p["estimated_cut"] and abs(p["played_s"] - 1.5) < 1e-9
