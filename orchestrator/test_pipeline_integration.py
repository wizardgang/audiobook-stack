"""
Integration tests for orchestrator.process_job()

These tests exercise the full process_job() pipeline against a real Abogen
server:
  watcher job JSON → /api/extract → chunking → Redis TTS queue

Requirements:
  - Set ABOGEN_HOST to a running Abogen instance before running.
  - Optionally set ABOGEN_TEST_FILE to an EPUB/PDF path to test real extraction.
    If not set, a minimal synthetic EPUB is generated automatically.

Example:
    ABOGEN_HOST=http://localhost:8808 pytest orchestrator/test_pipeline_integration.py -v
    ABOGEN_HOST=http://localhost:8808 ABOGEN_TEST_FILE=/path/to/book.epub pytest ...

All tests are skipped when ABOGEN_HOST is not set.

In-memory Redis:
    The module-level `orch.r` is replaced with an InMemoryRedis mock so no
    Redis server is required.
"""

import json
import os
import sys
import types
import uuid
import zipfile
import collections
import pytest

from pathlib import Path
from unittest.mock import patch, MagicMock

# ── Bootstrap: prevent redis/fitz/prometheus from connecting at import time ──

sys.modules.setdefault("redis", MagicMock())
sys.modules.setdefault("fitz", MagicMock())

_prom_stub = types.ModuleType("prometheus_client")
for _attr in ("Counter", "Histogram", "Gauge", "start_http_server"):
    setattr(_prom_stub, _attr, MagicMock(return_value=MagicMock()))
sys.modules["prometheus_client"] = _prom_stub

_redis_stub = types.ModuleType("redis")
_redis_stub.from_url = MagicMock(return_value=MagicMock())
sys.modules["redis"] = _redis_stub

_fitz_stub = types.ModuleType("fitz")
_fitz_stub.open = MagicMock()
_fitz_stub.LINK_GOTO = 1
sys.modules["fitz"] = _fitz_stub

sys.path.insert(0, os.path.dirname(__file__))

with patch.dict(os.environ, {"REDIS_URL": "redis://localhost:6379"}):
    import orchestrator as orch

# ── Skip guard ───────────────────────────────────────────────────────────────

ABOGEN_HOST = os.environ.get("ABOGEN_HOST", "").rstrip("/")
pytestmark = pytest.mark.skipif(
    not ABOGEN_HOST,
    reason="Set ABOGEN_HOST=http://<host>:<port> to run integration tests",
)


# ── In-memory Redis mock ─────────────────────────────────────────────────────

class InMemoryRedis:
    """
    Covers exactly the Redis operations used by process_job():
    hset, hgetall, hget, llen, lpush, rpop.
    """

    def __init__(self):
        self._hashes: dict = collections.defaultdict(dict)
        self._lists: dict = collections.defaultdict(list)

    def hset(self, key, mapping=None, **kwargs):
        data = {**(mapping or {}), **kwargs}
        self._hashes[key].update(data)
        return len(data)

    def hgetall(self, key):
        return dict(self._hashes.get(key, {}))

    def hget(self, key, field):
        return self._hashes.get(key, {}).get(field)

    def lpush(self, key, *values):
        for v in values:
            self._lists[key].insert(0, v)
        return len(self._lists[key])

    def rpop(self, key):
        lst = self._lists.get(key, [])
        return lst.pop() if lst else None

    def llen(self, key):
        return len(self._lists.get(key, []))

    def lrange(self, key, start, end):
        lst = self._lists.get(key, [])
        stop = end if end >= 0 else len(lst) + end + 1
        return lst[start:stop + 1]

    def drain_list(self, key):
        """Helper: pop all items from a list and return them."""
        items = []
        while True:
            item = self.rpop(key)
            if item is None:
                break
            items.append(item)
        return items


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def fake_r():
    return InMemoryRedis()


@pytest.fixture()
def book_file(tmp_path):
    """
    Returns the path from ABOGEN_TEST_FILE env var if set and exists,
    otherwise builds a minimal valid EPUB on the fly.
    """
    env_path = os.environ.get("ABOGEN_TEST_FILE", "")
    if env_path and Path(env_path).exists():
        return Path(env_path)
    return _make_minimal_epub(tmp_path)


@pytest.fixture()
def book_job(book_file):
    return {"id": str(uuid.uuid4()), "path": str(book_file)}


# ── Core runner ──────────────────────────────────────────────────────────────

def run_job(job_dict, fake_redis, tmp_path, env_overrides=None):
    """
    Patch module-level orchestrator globals and call process_job() once.

    env_overrides keys mirror the KOKORO_* env var names (string values).
    """
    defaults = {
        "KOKORO_VOICE_BLEND": "",
        "KOKORO_SPEED":       "1.0",
    }
    if env_overrides:
        defaults.update(env_overrides)

    voice_blend = defaults["KOKORO_VOICE_BLEND"]
    speed       = float(defaults["KOKORO_SPEED"])

    chunks_dir = tmp_path / "chunks"
    chunks_dir.mkdir(parents=True, exist_ok=True)

    with (
        patch.object(orch, "r",                  fake_redis),
        patch.object(orch, "CHUNKS_DIR",         chunks_dir),
        patch.object(orch, "AI_NORMALIZE",       False),
        patch.object(orch, "KOKORO_VOICE_BLEND", voice_blend),
        patch.object(orch, "KOKORO_SPEED",       speed),
        patch.dict(os.environ, {
            "ABOGEN_HOST":        ABOGEN_HOST,
            "OPENROUTER_API_KEY": "",
        }),
    ):
        orch.process_job(json.dumps(job_dict))

    return fake_redis


def _drain_tts_jobs(fake_redis):
    return [json.loads(r) for r in fake_redis.drain_list("pipeline:tts")]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Basic pipeline
# ─────────────────────────────────────────────────────────────────────────────

class TestBasicPipeline:

    def test_at_least_one_chunk_enqueued(self, fake_r, book_job, tmp_path):
        """process_job() enqueues at least one TTS job for a non-empty book."""
        run_job(book_job, fake_r, tmp_path)
        assert fake_r.llen("pipeline:tts") >= 1

    def test_book_state_queued_or_error(self, fake_r, book_job, tmp_path):
        """Book state is set to either 'queued' (success) or 'error' (bad file)."""
        run_job(book_job, fake_r, tmp_path)
        state = fake_r.hgetall(f"book:{book_job['id']}")
        assert state.get("status") in ("queued", "error"), (
            f"Unexpected status: {state}"
        )

    def test_book_state_queued_on_success(self, fake_r, book_job, tmp_path):
        """On successful extraction the book state is 'queued'."""
        run_job(book_job, fake_r, tmp_path)
        state = fake_r.hgetall(f"book:{book_job['id']}")
        if state.get("status") == "error":
            pytest.skip(f"Extraction failed: {state.get('error')} — check ABOGEN_TEST_FILE")
        assert state["status"] == "queued"
        assert int(state.get("total_chunks", 0)) >= 1

    def test_required_fields_in_every_job(self, fake_r, book_job, tmp_path):
        """Every enqueued TTS job contains the required payload fields."""
        run_job(book_job, fake_r, tmp_path)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued — check ABOGEN_TEST_FILE or server")

        required = ("book_id", "chunk_idx", "chapter_idx", "chapter_title", "title",
                    "text", "chunk_file", "is_chapter_start", "voice", "speed", "total")
        for job in jobs:
            for field in required:
                assert field in job, f"Missing field '{field}' in job {job.get('chunk_idx')}"

    def test_chunk_files_exist_on_disk(self, fake_r, book_job, tmp_path):
        """chunk_file paths in the queued jobs point to files that exist."""
        run_job(book_job, fake_r, tmp_path)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued")

        for job in jobs:
            assert Path(job["chunk_file"]).exists(), (
                f"Missing chunk file: {job['chunk_file']}"
            )

    def test_meta_json_written(self, fake_r, book_job, tmp_path):
        """meta.json is written inside the book's chunk directory."""
        run_job(book_job, fake_r, tmp_path)
        state = fake_r.hgetall(f"book:{book_job['id']}")
        if state.get("status") == "error":
            pytest.skip("Extraction failed")

        meta_file = tmp_path / "chunks" / book_job["id"] / "meta.json"
        assert meta_file.exists()
        meta = json.loads(meta_file.read_text())
        assert isinstance(meta, list)
        assert len(meta) >= 1

    def test_chunk_state_set_in_redis(self, fake_r, book_job, tmp_path):
        """Each enqueued chunk has a state hash in Redis."""
        run_job(book_job, fake_r, tmp_path)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued")

        for job in jobs:
            state = fake_r.hgetall(f"chunk:{job['book_id']}:{job['chunk_idx']}")
            assert state.get("status") == "queued", (
                f"chunk {job['chunk_idx']} missing queued state"
            )

    def test_chunk_idx_sequential(self, fake_r, book_job, tmp_path):
        """chunk_idx values are sequential starting from 0."""
        run_job(book_job, fake_r, tmp_path)
        jobs = sorted(_drain_tts_jobs(fake_r), key=lambda j: j["chunk_idx"])
        if not jobs:
            pytest.skip("No jobs enqueued")

        assert [j["chunk_idx"] for j in jobs] == list(range(len(jobs)))

    def test_total_matches_enqueued_count(self, fake_r, book_job, tmp_path):
        """The 'total' field in each job equals the number of jobs enqueued."""
        run_job(book_job, fake_r, tmp_path)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued")

        expected = len(jobs)
        for job in jobs:
            assert job["total"] == expected

    def test_book_id_consistent_across_jobs(self, fake_r, book_job, tmp_path):
        """All jobs share the same book_id from the original watcher job."""
        run_job(book_job, fake_r, tmp_path)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued")

        assert all(j["book_id"] == book_job["id"] for j in jobs)


# ─────────────────────────────────────────────────────────────────────────────
# 2. Voice blend propagation
# ─────────────────────────────────────────────────────────────────────────────

class TestVoiceBlend:

    def _jobs(self, fake_r, book_job, tmp_path, overrides):
        run_job(book_job, fake_r, tmp_path, env_overrides=overrides)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued — check ABOGEN_TEST_FILE or server")
        return jobs

    def test_voice_formula_in_all_jobs(self, fake_r, book_job, tmp_path):
        """Voice blend is converted to Kokoro formula and set on every job."""
        jobs = self._jobs(fake_r, book_job, tmp_path,
                          {"KOKORO_VOICE_BLEND": "af_bella:60,af_sky:40"})
        for job in jobs:
            assert "*" in job["voice"], f"Expected formula, got: {job['voice']}"
            assert "af_bella" in job["voice"]
            assert "af_sky" in job["voice"]

    def test_speed_propagated_to_all_jobs(self, fake_r, book_job, tmp_path):
        """Custom KOKORO_SPEED is set on every enqueued job."""
        jobs = self._jobs(fake_r, book_job, tmp_path, {"KOKORO_SPEED": "1.25"})
        for job in jobs:
            assert float(job["speed"]) == pytest.approx(1.25)

    def test_no_blend_gives_empty_voice(self, fake_r, book_job, tmp_path):
        """When KOKORO_VOICE_BLEND is empty, voice field is empty string."""
        jobs = self._jobs(fake_r, book_job, tmp_path, {"KOKORO_VOICE_BLEND": ""})
        for job in jobs:
            assert job["voice"] == ""


# ─────────────────────────────────────────────────────────────────────────────
# 3. Chunk splitting
# ─────────────────────────────────────────────────────────────────────────────

class TestChunkSplitting:

    def test_only_first_part_is_chapter_start(self, fake_r, book_job, tmp_path):
        """For multi-part chapters, is_chapter_start is True only for part 0."""
        run_job(book_job, fake_r, tmp_path)
        jobs = _drain_tts_jobs(fake_r)
        if not jobs:
            pytest.skip("No jobs enqueued")

        # Group by chapter_idx
        by_chapter: dict = collections.defaultdict(list)
        for j in jobs:
            by_chapter[j["chapter_idx"]].append(j)

        for ch_idx, parts in by_chapter.items():
            parts.sort(key=lambda j: j["chunk_idx"])
            starts = [p for p in parts if p["is_chapter_start"]]
            assert len(starts) == 1, (
                f"Chapter {ch_idx} has {len(starts)} chapter-start chunks"
            )
            assert starts[0]["chunk_idx"] == parts[0]["chunk_idx"]


# ─────────────────────────────────────────────────────────────────────────────
# Minimal EPUB builder (used when ABOGEN_TEST_FILE is not set)
# ─────────────────────────────────────────────────────────────────────────────

def _make_minimal_epub(tmp_path: Path) -> Path:
    epub_path = tmp_path / "minimal_test.epub"
    with zipfile.ZipFile(epub_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", """\
<?xml version="1.0"?>
<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">
  <rootfiles>
    <rootfile full-path="OEBPS/content.opf"
              media-type="application/oebps-package+xml"/>
  </rootfiles>
</container>""")
        zf.writestr("OEBPS/content.opf", """\
<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0"
         unique-identifier="uid">
  <metadata xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>Integration Test Book</dc:title>
    <dc:language>en</dc:language>
    <dc:identifier id="uid">integration-test-001</dc:identifier>
  </metadata>
  <manifest>
    <item id="ch1" href="chapter1.xhtml"
          media-type="application/xhtml+xml"/>
    <item id="ch2" href="chapter2.xhtml"
          media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx"
          media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="ch1"/>
    <itemref idref="ch2"/>
  </spine>
</package>""")
        zf.writestr("OEBPS/chapter1.xhtml", """\
<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter One</title></head>
  <body>
    <h1>Chapter One</h1>
    <p>It was a dark and stormy night. The rain fell in torrents across
       the cobblestones. Scrooge sat alone in his counting-house, the
       fire barely glowing in the grate.</p>
    <p>His clerk, Bob Cratchit, huddled over a single coal. The cold
       seeped through the walls. Outside, carolers sang in the street
       but their voices seemed far away and faint.</p>
    <p>A knock came at the door. Scrooge looked up with a scowl and
       said nothing. The knock came again, louder this time.</p>
  </body>
</html>""")
        zf.writestr("OEBPS/chapter2.xhtml", """\
<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Chapter Two</title></head>
  <body>
    <h1>Chapter Two</h1>
    <p>Morning broke cold and grey over the city. The market stalls
       opened one by one as the tradespeople arrived. Children ran
       between the carts chasing a loose chicken.</p>
    <p>Grace watched from the upstairs window, her cup of tea growing
       cold in her hands. She had not slept. The events of the previous
       night still played through her mind in sharp detail.</p>
  </body>
</html>""")
        zf.writestr("OEBPS/toc.ncx", """\
<?xml version="1.0" encoding="utf-8"?>
<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/" version="2005-1">
  <head>
    <meta name="dtb:uid" content="integration-test-001"/>
  </head>
  <docTitle><text>Integration Test Book</text></docTitle>
  <navMap>
    <navPoint id="np1" playOrder="1">
      <navLabel><text>Chapter One</text></navLabel>
      <content src="chapter1.xhtml"/>
    </navPoint>
    <navPoint id="np2" playOrder="2">
      <navLabel><text>Chapter Two</text></navLabel>
      <content src="chapter2.xhtml"/>
    </navPoint>
  </navMap>
</ncx>""")
    return epub_path
