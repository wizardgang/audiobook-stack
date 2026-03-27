"""
Tests for Kokoro voice blending in the orchestrator.

Run with:
    python -m pytest tts-node/orchestrator/test_voice_blend.py -v
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
