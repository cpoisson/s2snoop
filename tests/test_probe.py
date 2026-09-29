from s2snoop import probe


class _Handler:
    def __init__(self, config, pipeline=0):
        self._snoop_config = config
        self.pipeline_index = pipeline


def test_describe_keeps_model_identity_and_drops_secrets():
    config = probe._describe({
        "model_name": "Qwen/Qwen3-ASR-0.6B-hf",
        "language": "auto",
        "thresh": 0.6,
        "smart_turn": True,
        "api_key": "sk-secret",
        "hf_token": "hf_secret",
        "base_url": "https://user:pass@example.org:8443/v1?api_key=sk-secret",
        "gen_kwargs": {"max_new_tokens": 256},
        "text_output_queue": object(),
    })
    assert config == {
        "model_name": "Qwen/Qwen3-ASR-0.6B-hf",
        "language": "auto",
        "thresh": 0.6,
        "smart_turn": True,
        "base_url": "https://example.org:8443/v1",
    }


def test_describe_keeps_non_finite_floats_json_safe():
    # s2s's VAD defaults max_speech_ms to float("inf"); strict JSON encoders reject it.
    config = probe._describe({"max_speech_ms": float("inf"), "thresh": float("nan")})
    assert config == {"max_speech_ms": "inf", "thresh": "nan"}


def test_store_loads_maps_legacy_infinity_to_none():
    from s2snoop.store import loads

    assert loads('{"max_speech_ms": Infinity, "a": -Infinity, "b": NaN, "c": 1.5}') == {
        "max_speech_ms": None, "a": None, "b": None, "c": 1.5}


def test_stage_from_handler_class():
    assert probe._stage("VADHandler") == "vad"
    assert probe._stage("Qwen3ASRSTTHandler") == "stt"
    assert probe._stage("LanguageModelHandler") == "llm"
    assert probe._stage("ResponsesApiModelHandler") == "llm"
    assert probe._stage("Qwen3TTSHandler") == "tts"
    assert probe._stage("TranscriptionNotifier") == "other"


def test_models_snapshot_orders_stages(monkeypatch):
    class Named(_Handler):
        pass

    handlers = []
    for name, config in [("Qwen3TTSHandler", {"voice": "Aiden"}), ("VADHandler", {"thresh": 0.6}),
                         ("Qwen3ASRSTTHandler", {"model_name": "Qwen/Qwen3-ASR-0.6B-hf"})]:
        cls = type(name, (Named,), {})
        handlers.append(cls(config))
    monkeypatch.setattr(probe, "HANDLERS", handlers)
    rows = probe.models_snapshot()
    assert [r["stage"] for r in rows] == ["vad", "stt", "tts"]
    assert rows[1] == {"handler": "Qwen3ASRSTTHandler", "stage": "stt", "pipeline": 0,
                       "config": {"model_name": "Qwen/Qwen3-ASR-0.6B-hf"}}
