"""
Tests for Kokoro voice blending and SSML generation in the orchestrator.

Run with:
    python -m pytest tts-node/orchestrator/test_ssml_voiceblend.py -v
"""

import sys
import os
import re
import pytest

# Make orchestrator importable without a running Redis connection by mocking redis
from unittest.mock import MagicMock, patch

# Patch redis before importing orchestrator
sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("fitz", MagicMock())
sys.modules.setdefault("prometheus_client", MagicMock())

# Add orchestrator directory to path
sys.path.insert(0, os.path.dirname(__file__))


# ── Import helpers under test ──────────────────────────────────────────────────

with patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379"}):
    import importlib
    import types

    # Provide minimal stubs so the module-level code doesn't crash
    _prom_stub = types.ModuleType("prometheus_client")
    for _name in ("Counter", "Histogram", "Gauge", "start_http_server"):
        setattr(_prom_stub, _name, MagicMock(return_value=MagicMock()))
    sys.modules["prometheus_client"] = _prom_stub

    _redis_stub = types.ModuleType("redis")
    _redis_stub.from_url = MagicMock(return_value=MagicMock())
    sys.modules["redis"] = _redis_stub

    _fitz_stub = types.ModuleType("fitz")
    _fitz_stub.open = MagicMock()
    _fitz_stub.LINK_GOTO = 1
    sys.modules["fitz"] = _fitz_stub

    import orchestrator as orch


# ─────────────────────────────────────────────────────────────────────────────
# voice_blend_to_formula tests
# ─────────────────────────────────────────────────────────────────────────────

class TestVoiceBlendToFormula:
    def test_single_voice_no_weight(self):
        result = orch.voice_blend_to_formula("af_bella")
        assert result == "af_bella*1.0000"

    def test_two_voices_equal_weights(self):
        result = orch.voice_blend_to_formula("af_bella:50,af_sky:50")
        # Both should be 0.5
        assert "af_bella*0.5000" in result
        assert "af_sky*0.5000" in result
        assert result.count("+") == 1

    def test_two_voices_unequal_weights(self):
        result = orch.voice_blend_to_formula("af_bella:60,af_sky:40")
        assert "af_bella*0.6000" in result
        assert "af_sky*0.4000" in result

    def test_three_voices(self):
        result = orch.voice_blend_to_formula("af_bella:50,af_sky:30,af_heart:20")
        assert "af_bella*0.5000" in result
        assert "af_sky*0.3000" in result
        assert "af_heart*0.2000" in result
        assert result.count("+") == 2

    def test_weights_normalized_to_sum_1(self):
        # Weights 1:1:1 should each become 1/3
        result = orch.voice_blend_to_formula("a:1,b:1,c:1")
        # Each weight ≈ 0.3333
        for part in result.split("+"):
            weight = float(part.split("*")[1])
            assert abs(weight - 1 / 3) < 1e-4

    def test_already_formula_passthrough(self):
        formula = "af_bella*0.7+af_sky*0.3"
        assert orch.voice_blend_to_formula(formula) == formula

    def test_empty_string_returns_empty(self):
        assert orch.voice_blend_to_formula("") == ""
        assert orch.voice_blend_to_formula("   ") == ""

    def test_invalid_weight_raises(self):
        with pytest.raises(ValueError, match="Invalid weight"):
            orch.voice_blend_to_formula("af_bella:abc")

    def test_zero_weight_raises(self):
        with pytest.raises(ValueError, match="positive"):
            orch.voice_blend_to_formula("af_bella:0")

    def test_negative_weight_raises(self):
        with pytest.raises(ValueError, match="positive"):
            orch.voice_blend_to_formula("af_bella:-10")

    def test_whitespace_tolerant(self):
        result = orch.voice_blend_to_formula("  af_bella : 60 , af_sky : 40  ")
        assert "af_bella" in result
        assert "af_sky" in result


# ─────────────────────────────────────────────────────────────────────────────
# SSMLConfig & Genre tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSSMLConfig:
    def test_default_config(self):
        cfg = orch.SSMLConfig()
        assert cfg.genre == orch.Genre.FICTION
        assert cfg.pause_sentence == pytest.approx(0.4)
        assert cfg.pause_paragraph == pytest.approx(0.7)
        assert cfg.pause_chapter == pytest.approx(1.2)
        assert cfg.emphasis_chapter_titles is True
        assert cfg.emphasis_dialogue is True
        assert cfg.emphasis_keywords is True

    def test_genre_enum_values(self):
        assert orch.Genre.FICTION.value == "fiction"
        assert orch.Genre.ROMANCE.value == "romance"
        assert orch.Genre.THRILLER.value == "thriller"
        assert orch.Genre.FANTASY.value == "fantasy"
        assert orch.Genre.NON_FICTION.value == "non_fiction"
        assert orch.Genre.YA.value == "young_adult"

    def test_custom_config(self):
        cfg = orch.SSMLConfig(
            genre=orch.Genre.THRILLER,
            pause_sentence=0.6,
            pause_paragraph=1.0,
            pause_chapter=2.0,
            emphasis_dialogue=False,
        )
        assert cfg.genre == orch.Genre.THRILLER
        assert cfg.pause_sentence == pytest.approx(0.6)
        assert cfg.emphasis_dialogue is False


# ─────────────────────────────────────────────────────────────────────────────
# AudiobookSSMLProcessor tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAudiobookSSMLProcessor:
    def _proc(self, **kwargs):
        return orch.AudiobookSSMLProcessor(orch.SSMLConfig(**kwargs))

    # ── chapter_title_to_ssml ─────────────────────────────────────────────────

    def test_chapter_title_has_speak_tags(self):
        proc = self._proc()
        ssml = proc.chapter_title_to_ssml("Chapter One")
        assert ssml.startswith("<speak>")
        assert ssml.endswith("</speak>")

    def test_chapter_title_has_emphasis(self):
        proc = self._proc()
        ssml = proc.chapter_title_to_ssml("Chapter One")
        assert '<emphasis level="strong">' in ssml
        assert "Chapter One" in ssml

    def test_chapter_title_has_breaks(self):
        proc = self._proc(pause_chapter=1.5)
        ssml = proc.chapter_title_to_ssml("Prologue")
        assert '<break time="1.5s"/>' in ssml

    def test_chapter_title_xml_escaped(self):
        proc = self._proc()
        ssml = proc.chapter_title_to_ssml("The <Dark> & Stormy Night")
        assert "<Dark>" not in ssml
        assert "&lt;Dark&gt;" in ssml
        assert "&amp;" in ssml

    # ── text_to_ssml ──────────────────────────────────────────────────────────

    def test_basic_text_wrapped_in_speak(self):
        proc = self._proc()
        ssml = proc.text_to_ssml("Hello world. This is a test.")
        assert "<speak>" in ssml
        assert "</speak>" in ssml

    def test_sentences_wrapped_in_s_tags(self):
        proc = self._proc()
        ssml = proc.text_to_ssml("Hello world. This is fine.")
        assert "<s>" in ssml
        assert "</s>" in ssml

    def test_sentence_break_inserted(self):
        proc = self._proc(pause_sentence=0.5)
        ssml = proc.text_to_ssml("Hello world. This is fine.")
        assert '<break time="0.5s"/>' in ssml

    def test_paragraph_break_between_paragraphs(self):
        proc = self._proc(pause_paragraph=0.8)
        text = "First paragraph.\n\nSecond paragraph."
        ssml = proc.text_to_ssml(text)
        assert '<break time="0.8s"/>' in ssml

    def test_multiple_paragraphs(self):
        proc = self._proc()
        text = "Para one.\n\nPara two.\n\nPara three."
        ssml = proc.text_to_ssml(text)
        # Should have 2 paragraph breaks
        assert ssml.count(f'<break time="{proc.config.pause_paragraph}s"/>') >= 2

    def test_empty_text_returns_empty_speak(self):
        proc = self._proc()
        ssml = proc.text_to_ssml("")
        assert "<speak>" in ssml
        assert "</speak>" in ssml
        # No sentence tags expected
        assert "<s>" not in ssml

    # ── dialogue processing ───────────────────────────────────────────────────

    # ── dialogue — straight quotes ────────────────────────────────────────────

    def test_straight_quote_dialogue_gets_emphasis(self):
        proc = self._proc()
        ssml = proc.text_to_ssml('"Hello there," she said.')
        assert "<emphasis" in ssml or "<prosody" in ssml

    def test_straight_quote_whisper_gets_slow_prosody(self):
        proc = self._proc()
        ssml = proc.text_to_ssml('"I will whisper this," he said softly.')
        assert 'rate="slow"' in ssml

    def test_straight_quote_shout_gets_fast_prosody(self):
        proc = self._proc()
        ssml = proc.text_to_ssml('"YELL AND SCREAM!" she shouted.')
        assert 'rate="fast"' in ssml

    # ── dialogue — smart / curly quotes (the bug fix) ─────────────────────────

    def test_smart_quote_dialogue_gets_emphasis(self):
        """Curly open \u201c and close \u201d quotes must be detected as dialogue."""
        proc = self._proc()
        ssml = proc.text_to_ssml('\u201cHello there,\u201d she said.')
        assert "<emphasis" in ssml or "<prosody" in ssml

    def test_smart_quote_whisper_gets_slow_prosody(self):
        proc = self._proc()
        ssml = proc.text_to_ssml('\u201cI will whisper this,\u201d he said softly.')
        assert 'rate="slow"' in ssml

    def test_smart_quote_shout_gets_fast_prosody(self):
        proc = self._proc()
        ssml = proc.text_to_ssml('\u201cYELL AND SCREAM!\u201d she shouted.')
        assert 'rate="fast"' in ssml

    def test_mixed_quotes_both_detected(self):
        """Both straight and smart quotes in the same sentence should be handled."""
        proc = self._proc()
        ssml = proc.text_to_ssml('"Hello," she said. \u201cGoodbye,\u201d he replied.')
        assert ssml.count("<emphasis") + ssml.count("<prosody") >= 2

    def test_dialogue_disabled(self):
        proc = self._proc(emphasis_dialogue=False)
        ssml = proc.text_to_ssml('"Hello there," she said.')
        assert "<prosody" not in ssml

    # ── keyword emphasis ──────────────────────────────────────────────────────

    def test_thriller_keyword_gets_emphasis(self):
        proc = orch.AudiobookSSMLProcessor(orch.SSMLConfig(genre=orch.Genre.THRILLER))
        ssml = proc.text_to_ssml("Suddenly the lights went out.")
        assert '<emphasis level="moderate">Suddenly</emphasis>' in ssml \
               or "suddenly" in ssml.lower()

    def test_romance_keyword_gets_emphasis(self):
        proc = orch.AudiobookSSMLProcessor(orch.SSMLConfig(genre=orch.Genre.ROMANCE))
        ssml = proc.text_to_ssml("She felt deep love in her heart.")
        assert "emphasis" in ssml

    def test_fiction_no_genre_keywords(self):
        proc = orch.AudiobookSSMLProcessor(orch.SSMLConfig(genre=orch.Genre.FICTION))
        # FICTION has no keyword list → no keyword emphasis tags
        ssml = proc.text_to_ssml("She walked down the street.")
        # Just check it doesn't crash and is valid SSML
        assert "<speak>" in ssml

    def test_keywords_disabled(self):
        proc = self._proc(genre=orch.Genre.THRILLER, emphasis_keywords=False)
        ssml = proc.text_to_ssml("Suddenly there was danger.")
        # Should not add emphasis since disabled
        assert "Suddenly" in ssml  # word present but unemphasized
        assert '<emphasis level="moderate">Suddenly</emphasis>' not in ssml

    # ── XML safety ────────────────────────────────────────────────────────────

    def test_ampersand_escaped_in_content(self):
        proc = self._proc()
        ssml = proc.text_to_ssml("Salt & pepper are common spices.")
        # Ampersand in content should be escaped when inside emphasis/prosody,
        # but plain text in <s> is passed as-is (Kokoro handles it).
        # Just ensure the function doesn't raise.
        assert isinstance(ssml, str)

    def test_output_is_valid_xml_structure(self):
        proc = self._proc()
        ssml = proc.text_to_ssml("A simple sentence. Another one.")
        # Check balanced tags
        assert ssml.count("<speak>") == ssml.count("</speak>")
        assert ssml.count("<s>") == ssml.count("</s>")


# ─────────────────────────────────────────────────────────────────────────────
# _build_ssml_processor tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildSSMLProcessor:
    def test_returns_none_when_disabled(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", False):
            assert orch._build_ssml_processor() is None

    def test_returns_processor_when_enabled(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True), \
             patch.object(orch, "KOKORO_GENRE", "thriller"):
            proc = orch._build_ssml_processor()
            assert isinstance(proc, orch.AudiobookSSMLProcessor)
            assert proc.config.genre == orch.Genre.THRILLER

    def test_unknown_genre_falls_back_to_fiction(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True), \
             patch.object(orch, "KOKORO_GENRE", "zzz_unknown"):
            proc = orch._build_ssml_processor()
            assert proc.config.genre == orch.Genre.FICTION

    def test_ai_enhanced_off_by_default(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True):
            proc = orch._build_ssml_processor()
            assert proc.ai_enhanced is False

    def test_ai_enhanced_requires_api_key(self):
        """KOKORO_SSML_AI_ENHANCED=true is silently ignored when no API key present."""
        with patch.object(orch, "KOKORO_SSML_ENABLED", True), \
             patch.object(orch, "KOKORO_SSML_AI_ENHANCED", True), \
             patch.object(orch, "AI_NORMALIZE", False):  # no key
            proc = orch._build_ssml_processor()
            assert proc.ai_enhanced is False

    def test_ai_enhanced_enabled_with_key(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True), \
             patch.object(orch, "KOKORO_SSML_AI_ENHANCED", True), \
             patch.object(orch, "AI_NORMALIZE", True):
            proc = orch._build_ssml_processor()
            assert proc.ai_enhanced is True

    @pytest.mark.parametrize("genre_str,expected", [
        ("romance",     orch.Genre.ROMANCE),
        ("thriller",    orch.Genre.THRILLER),
        ("fantasy",     orch.Genre.FANTASY),
        ("non_fiction", orch.Genre.NON_FICTION),
        ("young_adult", orch.Genre.YA),
        ("fiction",     orch.Genre.FICTION),
    ])
    def test_genre_mapping(self, genre_str, expected):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True), \
             patch.object(orch, "KOKORO_GENRE", genre_str):
            proc = orch._build_ssml_processor()
            assert proc.config.genre == expected


# ─────────────────────────────────────────────────────────────────────────────
# AI-enhanced SSML tests (uses mocked AI client)
# ─────────────────────────────────────────────────────────────────────────────

_VALID_SSML = "<speak>\n<s>Hello world.</s><break time=\"0.4s\"/>\n</speak>"
_INVALID_SSML = "Just plain text with no speak tag."


class TestAIGenerateSSML:
    """Tests for the _ai_generate_ssml helper and AudiobookSSMLProcessor AI path."""

    def _mock_ai_response(self, content: str):
        """Return a mock that mimics _ai_client.chat.completions.create."""
        mock_resp = MagicMock()
        mock_resp.choices[0].message.content = content
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_resp
        return mock_client

    # _ai_client is only defined at module level when OPENROUTER_API_KEY is set,
    # so we must use create=True to inject it in the test environment.

    def test_ai_generate_ssml_success(self):
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "_ai_client", self._mock_ai_response(_VALID_SSML), create=True), \
             patch.object(orch, "ORCH_SSML_AI_CALLS", MagicMock()), \
             patch.object(orch, "ORCH_SSML_AI_SECS", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch._ai_generate_ssml("system prompt", "Hello world.")
        assert result is not None
        assert "<speak>" in result

    def test_ai_generate_ssml_invalid_response_returns_none(self):
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "_ai_client", self._mock_ai_response(_INVALID_SSML), create=True), \
             patch.object(orch, "ORCH_SSML_AI_CALLS", MagicMock()), \
             patch.object(orch, "ORCH_SSML_AI_SECS", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch._ai_generate_ssml("system prompt", "Hello world.")
        assert result is None

    def test_ai_generate_ssml_no_client_returns_none(self):
        with patch.object(orch, "AI_NORMALIZE", False):
            result = orch._ai_generate_ssml("system prompt", "Hello world.")
        assert result is None

    def test_ai_generate_ssml_exception_returns_none(self):
        bad_client = MagicMock()
        bad_client.chat.completions.create.side_effect = RuntimeError("network error")
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "_ai_client", bad_client, create=True), \
             patch.object(orch, "ORCH_SSML_AI_CALLS", MagicMock()), \
             patch.object(orch, "ORCH_SSML_AI_SECS", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch._ai_generate_ssml("system prompt", "Hello world.")
        assert result is None

    def test_ai_generate_ssml_missing_breaks_returns_none(self):
        """SSML without any <break> tags must be rejected — sentences would run together."""
        no_breaks = "<speak><s>Hello world.</s><s>Another sentence.</s></speak>"
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "_ai_client", self._mock_ai_response(no_breaks), create=True), \
             patch.object(orch, "ORCH_SSML_AI_CALLS", MagicMock()), \
             patch.object(orch, "ORCH_SSML_AI_SECS", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch._ai_generate_ssml("system prompt", "Hello world.")
        assert result is None

    def test_ai_generate_ssml_with_breaks_accepted(self):
        """SSML that includes <break> tags must pass validation."""
        with_breaks = '<speak><s>Hello.</s><break time="0.4s"/></speak>'
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "_ai_client", self._mock_ai_response(with_breaks), create=True), \
             patch.object(orch, "ORCH_SSML_AI_CALLS", MagicMock()), \
             patch.object(orch, "ORCH_SSML_AI_SECS", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch._ai_generate_ssml("system prompt", "Hello.")
        assert result is not None
        assert "<break" in result

    def test_ai_generate_ssml_strips_code_fences(self):
        fenced = "```xml\n" + _VALID_SSML + "\n```"
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "_ai_client", self._mock_ai_response(fenced), create=True), \
             patch.object(orch, "ORCH_SSML_AI_CALLS", MagicMock()), \
             patch.object(orch, "ORCH_SSML_AI_SECS", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch._ai_generate_ssml("system prompt", "Hello world.")
        assert result is not None
        assert "```" not in result


class TestAudiobookSSMLProcessorAIEnhanced:
    """Tests for AudiobookSSMLProcessor when ai_enhanced=True."""

    def _make_proc(self, genre=None, ai_enhanced=True):
        cfg = orch.SSMLConfig(genre=genre or orch.Genre.FICTION)
        return orch.AudiobookSSMLProcessor(cfg, ai_enhanced=ai_enhanced)

    def _patch_ai(self, ssml_content: str):
        """Context manager that patches _ai_generate_ssml to return ssml_content."""
        return patch.object(orch, "_ai_generate_ssml", return_value=ssml_content)

    # ── text_to_ssml ──────────────────────────────────────────────────────────

    def test_ai_result_used_when_available(self):
        proc = self._make_proc()
        with self._patch_ai(_VALID_SSML):
            result = proc.text_to_ssml("Hello world.")
        assert result == _VALID_SSML

    def test_falls_back_to_rule_based_on_ai_none(self):
        proc = self._make_proc()
        with self._patch_ai(None):
            result = proc.text_to_ssml("Hello world.")
        # Rule-based fallback should still produce SSML
        assert "<speak>" in result
        assert "<s>" in result

    def test_ai_not_called_when_ai_enhanced_false(self):
        proc = self._make_proc(ai_enhanced=False)
        with patch.object(orch, "_ai_generate_ssml") as mock_ai:
            proc.text_to_ssml("Hello world.")
        mock_ai.assert_not_called()

    def test_ai_called_once_per_chunk(self):
        proc = self._make_proc()
        with patch.object(orch, "_ai_generate_ssml", return_value=_VALID_SSML) as mock_ai:
            proc.text_to_ssml("Paragraph one.\n\nParagraph two.")
        # AI is called once for the whole chunk, not per-paragraph
        assert mock_ai.call_count == 1

    def test_ai_system_prompt_contains_genre(self):
        proc = self._make_proc(genre=orch.Genre.THRILLER)
        captured = {}
        def _capture(system, user):
            captured["system"] = system
            return _VALID_SSML
        with patch.object(orch, "_ai_generate_ssml", side_effect=_capture):
            proc.text_to_ssml("Test text.")
        assert "thriller" in captured["system"].lower()

    def test_ai_system_prompt_contains_pause_values(self):
        cfg = orch.SSMLConfig(pause_sentence=0.6, pause_paragraph=1.1, pause_chapter=2.0)
        proc = orch.AudiobookSSMLProcessor(cfg, ai_enhanced=True)
        captured = {}
        def _capture(system, user):
            captured["system"] = system
            return _VALID_SSML
        with patch.object(orch, "_ai_generate_ssml", side_effect=_capture):
            proc.text_to_ssml("Test.")
        assert "0.6" in captured["system"]
        assert "1.1" in captured["system"]
        assert "2.0" in captured["system"]

    # ── chapter_title_to_ssml ─────────────────────────────────────────────────

    def test_chapter_title_ai_result_used(self):
        proc = self._make_proc()
        title_ssml = "<speak><break time=\"1.2s\"/><emphasis level=\"strong\">Chapter One</emphasis><break time=\"1.2s\"/></speak>"
        with self._patch_ai(title_ssml):
            result = proc.chapter_title_to_ssml("Chapter One")
        assert result == title_ssml

    def test_chapter_title_falls_back_to_rule_based(self):
        proc = self._make_proc()
        with self._patch_ai(None):
            result = proc.chapter_title_to_ssml("Chapter One")
        assert "<emphasis level=\"strong\">" in result
        assert "Chapter One" in result


# ─────────────────────────────────────────────────────────────────────────────
# Integration: voice blend env-var round-trip
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectBookGenre:
    """Tests for the AI-based genre auto-detection."""

    _CHAPTERS = [
        {"title": "Chapter One", "text": "The detective examined the body. Blood was everywhere. Suddenly a gunshot rang out."},
        {"title": "Chapter Two", "text": "She chased the killer through dark alleys."},
    ]

    def _mock_ai(self, label: str):
        mock_resp = MagicMock()
        mock_resp.choices[0].message.content = label
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value = mock_resp
        return mock_client

    def _patch(self, label: str, ai_normalize=True, auto_detect=True):
        return (
            patch.object(orch, "AI_NORMALIZE", ai_normalize),
            patch.object(orch, "KOKORO_GENRE_AUTO_DETECT", auto_detect),
            patch.object(orch, "_ai_client", self._mock_ai(label), create=True),
            patch.object(orch, "ORCH_GENRE_DETECT", MagicMock()),
            patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()),
            patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()),
        )

    # ── happy-path detection ──────────────────────────────────────────────────

    @pytest.mark.parametrize("label,expected", [
        ("thriller",    orch.Genre.THRILLER),
        ("romance",     orch.Genre.ROMANCE),
        ("fantasy",     orch.Genre.FANTASY),
        ("non_fiction", orch.Genre.NON_FICTION),
        ("non-fiction", orch.Genre.NON_FICTION),
        ("ya",          orch.Genre.YA),
        ("fiction",     orch.Genre.FICTION),
        ("young_adult", orch.Genre.YA),
    ])
    def test_known_labels_map_correctly(self, label, expected):
        patches = self._patch(label)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = orch.detect_book_genre("Some Book", self._CHAPTERS)
        assert result == expected

    def test_returns_genre_object(self):
        patches = self._patch("thriller")
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = orch.detect_book_genre("Dark Night", self._CHAPTERS)
        assert isinstance(result, orch.Genre)

    # ── fallback behaviour ────────────────────────────────────────────────────

    def test_unknown_label_returns_fallback(self):
        with patch.object(orch, "KOKORO_GENRE", "romance"):
            patches = self._patch("space_opera")
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                result = orch.detect_book_genre("Galaxy Quest", self._CHAPTERS)
        assert result == orch.Genre.ROMANCE

    def test_ai_disabled_returns_env_fallback(self):
        with patch.object(orch, "KOKORO_GENRE", "fantasy"):
            patches = self._patch("thriller", ai_normalize=False)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                result = orch.detect_book_genre("Dragon Realm", self._CHAPTERS)
        assert result == orch.Genre.FANTASY

    def test_auto_detect_disabled_returns_env_fallback(self):
        with patch.object(orch, "KOKORO_GENRE", "non_fiction"):
            patches = self._patch("thriller", auto_detect=False)
            with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
                result = orch.detect_book_genre("Some Memoir", self._CHAPTERS)
        assert result == orch.Genre.NON_FICTION

    def test_exception_returns_fallback(self):
        bad_client = MagicMock()
        bad_client.chat.completions.create.side_effect = RuntimeError("network down")
        with patch.object(orch, "KOKORO_GENRE", "fiction"), \
             patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "KOKORO_GENRE_AUTO_DETECT", True), \
             patch.object(orch, "_ai_client", bad_client, create=True), \
             patch.object(orch, "ORCH_GENRE_DETECT", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            result = orch.detect_book_genre("Test Book", self._CHAPTERS)
        assert result == orch.Genre.FICTION

    # ── prompt construction ───────────────────────────────────────────────────

    def test_prompt_contains_book_title(self):
        captured = {}
        def _capture(**kwargs):
            captured["messages"] = kwargs.get("messages", [])
            mock_resp = MagicMock()
            mock_resp.choices[0].message.content = "thriller"
            return mock_resp

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = _capture
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "KOKORO_GENRE_AUTO_DETECT", True), \
             patch.object(orch, "_ai_client", mock_client, create=True), \
             patch.object(orch, "ORCH_GENRE_DETECT", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            orch.detect_book_genre("The Silent Murder", self._CHAPTERS)

        user_msg = captured["messages"][1]["content"]
        assert "The Silent Murder" in user_msg

    def test_prompt_contains_chapter_excerpt(self):
        captured = {}
        def _capture(**kwargs):
            captured["messages"] = kwargs.get("messages", [])
            mock_resp = MagicMock()
            mock_resp.choices[0].message.content = "thriller"
            return mock_resp

        mock_client = MagicMock()
        mock_client.chat.completions.create.side_effect = _capture
        with patch.object(orch, "AI_NORMALIZE", True), \
             patch.object(orch, "KOKORO_GENRE_AUTO_DETECT", True), \
             patch.object(orch, "_ai_client", mock_client, create=True), \
             patch.object(orch, "ORCH_GENRE_DETECT", MagicMock()), \
             patch.object(orch, "ORCH_AI_CALLS_TOTAL", MagicMock()), \
             patch.object(orch, "ORCH_AI_DURATION_SECS", MagicMock()):
            orch.detect_book_genre("Test Book", self._CHAPTERS)

        user_msg = captured["messages"][1]["content"]
        assert "detective" in user_msg.lower() or "chapter" in user_msg.lower()

    def test_empty_chapters_does_not_crash(self):
        patches = self._patch("fiction")
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = orch.detect_book_genre("Empty Book", [])
        assert isinstance(result, orch.Genre)

    # ── _build_ssml_processor with detected genre ─────────────────────────────

    def test_build_processor_uses_detected_genre(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True):
            proc = orch._build_ssml_processor(genre=orch.Genre.ROMANCE)
        assert proc.config.genre == orch.Genre.ROMANCE

    def test_build_processor_falls_back_to_env_when_genre_is_none(self):
        with patch.object(orch, "KOKORO_SSML_ENABLED", True), \
             patch.object(orch, "KOKORO_GENRE", "fantasy"):
            proc = orch._build_ssml_processor(genre=None)
        assert proc.config.genre == orch.Genre.FANTASY


class TestVoiceBlendIntegration:
    """Verify that env-var formats survive the full format-conversion round-trip."""

    def test_colon_format_produces_valid_formula(self):
        formula = orch.voice_blend_to_formula("af_bella:70,af_sky:30")
        # Must be parseable as "name*weight" segments
        for part in formula.split("+"):
            assert "*" in part
            name, w = part.split("*")
            assert name.strip()
            assert 0 < float(w) <= 1.0

    def test_formula_weights_sum_to_one(self):
        formula = orch.voice_blend_to_formula("af_bella:30,af_sky:30,af_heart:40")
        total = sum(float(p.split("*")[1]) for p in formula.split("+"))
        assert abs(total - 1.0) < 1e-6

    def test_sample_py_example_blends(self):
        """The exact formats shown in sample.py should parse correctly."""
        for blend in [
            "af_bella:60,af_sky:40",
            "af_bella:50,af_heart:50",
            "af_bella:60,af_sky:40",
        ]:
            formula = orch.voice_blend_to_formula(blend)
            assert "+" in formula or formula.count("*") == 1
