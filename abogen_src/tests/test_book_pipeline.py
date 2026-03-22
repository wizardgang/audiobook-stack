"""
Integration test: validate a real book through the full orchestrator pipeline.

Simulates exactly what orchestrator.py does — extract → skip filter → chunk —
and asserts the output matches the expected format for Redis TTS jobs.

Usage:
    # Run against a specific book:
    pytest tests/test_book_pipeline.py --book "C:/path/to/book.epub" -v

    # Run against multiple books defined in BOOKS below:
    pytest tests/test_book_pipeline.py -v

    # Show full chapter/chunk breakdown:
    pytest tests/test_book_pipeline.py --book "C:/path/to/book.epub" -v -s
"""

import re
import sys
import pytest
from pathlib import Path

# ── allow running from abogen_src/ root ──────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── orchestrator constants (mirrored) ────────────────────────────────────────
CHUNK_SIZE_CHARS = 3000
MIN_CHAPTER_CHARS = 150

_DEFAULT_SKIP = (
    "copyright,legal notice,all rights reserved,terms of use,"
    "no part of this,unauthorized reproduction,intellectual property,"
    "isbn,ebook edition,digital edition,printed in,printing history,"
    "publishing group,published by,an imprint of,"
    "berkley,penguin,random house,harpercollins,simon & schuster,"
    "macmillan,hachette,scholastic,tor books,orbit,del rey,"
    "doubleday,viking,knopf,anchor books,vintage books,"
    "st. martin,bloomsbury,little brown,grand central,"
    "title page,half title,series page,"
    "dedication,epigraph,inscription,"
    "table of contents,contents,brief contents,full contents,"
    "list of figures,list of tables,list of illustrations,list of maps,"
    "map,maps,family tree,genealogy,cast of characters,dramatis personae,"
    "timeline,chronology,a note on,note to the reader,"
    "translator's note,editor's note,note on the translation,"
    "note on the text,note on sources,a word from,"
    "acknowledgment,acknowledgement,with thanks,special thanks,"
    "bibliography,selected bibliography,works cited,works consulted,"
    "references,further reading,suggested reading,recommended reading,"
    "notes,endnotes,footnotes,source notes,"
    "index,general index,subject index,name index,"
    "glossary,glossary of terms,key terms,"
    "appendix,appendices,"
    "permissions,permissions acknowledgment,credits,"
    "about the author,about the authors,about the illustrator,"
    "about the publisher,from the publisher,about this book,"
    "about the type,colophon,"
    "also by,other books by,by the same author,other titles by,"
    "titles by,books by,also available,also from,"
    "more by,more books by,other works by,"
    "praise for,advance praise,endorsement,blurb,"
    "what readers are saying,what critics say,"
    "excerpt from,preview of,continue reading,sneak peek,"
    "reading group guide,book club guide,discussion questions,"
    "questions for discussion,topics for discussion,"
    "a conversation with,interview with,q&a with,q & a with,"
    "if you enjoyed,readers also enjoyed,readers who liked,"
    "visit us at,follow us on,connect with,sign up for,join our,"
    "newsletter,mailing list,stay connected,"
    "oceanofpdf,ebookbike,ebook bike,ebookelo,ebook3000,"
    "z-library,zlibrary,libgen,library genesis,freebookspot"
)
SKIP_PATTERNS = [p.strip().lower() for p in _DEFAULT_SKIP.split(",") if p.strip()]


# ── books to test (edit paths or pass --book on CLI) ─────────────────────────
BOOKS = [
    pytest.param(
        "C:/Users/ADMIN/Downloads/_OceanofPDF.com_48_laws_of_powers_-_Robert_Greene.epub",
        id="48_laws_epub",
    ),
    pytest.param(
        "C:/Users/ADMIN/Downloads/Sophies_World_About_the_History_of_Philos_.epub",
        id="sophies_world_epub",
    ),
]


# ── pytest hook: --book CLI argument ─────────────────────────────────────────
def pytest_addoption(parser):
    parser.addoption("--book", action="store", default=None,
                     help="Path to a single book to test")


def pytest_generate_tests(metafunc):
    if "book_path" in metafunc.fixturenames:
        cli_book = metafunc.config.getoption("--book", default=None)
        if cli_book:
            metafunc.parametrize("book_path", [cli_book])
        else:
            metafunc.parametrize("book_path", BOOKS)


# ── helpers ───────────────────────────────────────────────────────────────────

def should_skip(title: str, text: str):
    t = title.strip().lower()
    for pat in SKIP_PATTERNS:
        if pat in t:
            return f"title matches '{pat}'"
    if len(text.strip()) < MIN_CHAPTER_CHARS:
        return f"too short ({len(text.strip())} chars)"
    return None


def split_chunks(text: str, max_chars: int = CHUNK_SIZE_CHARS):
    """Simple greedy sentence splitter — mirrors orchestrator fallback."""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    chunks, current = [], ""
    for sentence in sentences:
        if not sentence.strip():
            continue
        if len(current) + len(sentence) + 1 > max_chars:
            if current:
                chunks.append(current.strip())
            current = sentence if len(sentence) <= max_chars else ""
            if len(sentence) > max_chars:
                for i in range(0, len(sentence), max_chars):
                    chunks.append(sentence[i:i + max_chars].strip())
        else:
            current = (current + " " + sentence).lstrip()
    if current.strip():
        chunks.append(current.strip())
    return chunks


def build_chunk_jobs(chapters, chunk_size=CHUNK_SIZE_CHARS):
    """Produce the same chunk job dicts that orchestrator.py pushes to Redis."""
    jobs = []
    global_chunk_idx = 0
    for ch_idx, ch in enumerate(chapters):
        ch_title = ch.title
        ch_text = ch.text.strip()
        skip_reason = should_skip(ch_title, ch_text)
        if skip_reason:
            continue
        ch_chunks = split_chunks(ch_text, chunk_size)
        for part_idx, chunk_text in enumerate(ch_chunks):
            title = ch_title if len(ch_chunks) == 1 else f"{ch_title} - Part {part_idx + 1}"
            jobs.append({
                "chunk_idx":     global_chunk_idx,
                "chapter_idx":   ch_idx,
                "chapter_title": ch_title,
                "title":         title,
                "text":          chunk_text,
            })
            global_chunk_idx += 1
    return jobs


# ── test class ────────────────────────────────────────────────────────────────

class TestBookPipeline:
    """Validate a real book through the full orchestrator pipeline."""

    @pytest.fixture(autouse=True)
    def extract(self, book_path):
        path = Path(book_path)
        if not path.exists():
            pytest.skip(f"Book not found: {book_path}")

        from abogen.text_extractor import extract_from_path
        result = extract_from_path(path)

        self.path      = path
        self.chapters  = result.chapters
        self.jobs      = build_chunk_jobs(self.chapters)
        self.skipped   = [
            ch for ch in self.chapters
            if should_skip(ch.title, ch.text.strip())
        ]
        self.kept      = [
            ch for ch in self.chapters
            if not should_skip(ch.title, ch.text.strip())
        ]

    # ── extraction ────────────────────────────────────────────────────────────

    def test_has_chapters(self):
        """Extraction must return at least one chapter."""
        assert len(self.chapters) >= 1, "No chapters extracted"

    def test_chapters_have_title_and_text(self):
        """Every chapter must have a non-empty title and text."""
        for ch in self.chapters:
            assert ch.title and ch.title.strip(), f"Empty title in chapter: {ch!r}"
            assert isinstance(ch.text, str), f"text is not a string: {ch!r}"

    def test_total_chars_nonzero(self):
        """Total extracted character count must be > 0."""
        total = sum(len(ch.text) for ch in self.chapters)
        assert total > 0, "Extraction returned zero characters"

    # ── skip filter ───────────────────────────────────────────────────────────

    def test_kept_chapters_not_empty(self):
        """At least one chapter must survive the skip filter."""
        assert len(self.kept) >= 1, (
            f"All {len(self.chapters)} chapters were skipped.\n"
            + "\n".join(f"  {ch.title}: {should_skip(ch.title, ch.text)}"
                        for ch in self.chapters[:10])
        )

    def test_kept_chapters_meet_min_length(self):
        """Every kept chapter must have >= MIN_CHAPTER_CHARS characters."""
        for ch in self.kept:
            assert len(ch.text.strip()) >= MIN_CHAPTER_CHARS, (
                f"Kept chapter '{ch.title}' is too short: {len(ch.text.strip())} chars"
            )

    def test_no_skip_pattern_in_kept_titles(self):
        """No kept chapter title should match a skip pattern."""
        for ch in self.kept:
            t = ch.title.strip().lower()
            for pat in SKIP_PATTERNS:
                assert pat not in t, (
                    f"Kept chapter '{ch.title}' matches skip pattern '{pat}'"
                )

    # ── chunking ──────────────────────────────────────────────────────────────

    def test_has_chunk_jobs(self):
        """Pipeline must produce at least one TTS chunk job."""
        assert len(self.jobs) >= 1, "No chunk jobs produced"

    def test_chunk_job_fields(self):
        """Every chunk job must have all required orchestrator fields."""
        required = {"chunk_idx", "chapter_idx", "chapter_title", "title", "text"}
        for job in self.jobs:
            missing = required - job.keys()
            assert not missing, f"Chunk job missing fields {missing}: {job}"

    def test_chunk_idx_sequential(self):
        """chunk_idx must be strictly sequential starting from 0."""
        indices = [j["chunk_idx"] for j in self.jobs]
        assert indices == list(range(len(self.jobs))), (
            f"chunk_idx not sequential: {indices[:10]}…"
        )

    def test_chunk_size_within_limit(self):
        """Every chunk text must be <= CHUNK_SIZE_CHARS."""
        oversized = [
            (j["chunk_idx"], len(j["text"]), j["title"])
            for j in self.jobs
            if len(j["text"]) > CHUNK_SIZE_CHARS
        ]
        assert not oversized, (
            f"{len(oversized)} chunks exceed {CHUNK_SIZE_CHARS} chars:\n"
            + "\n".join(f"  chunk {i}: {n} chars — {t}" for i, n, t in oversized[:5])
        )

    def test_chunk_text_nonempty(self):
        """Every chunk must have non-empty text."""
        empty = [j["chunk_idx"] for j in self.jobs if not j["text"].strip()]
        assert not empty, f"Empty text in chunks: {empty}"

    def test_chapter_idx_monotonic(self):
        """chapter_idx must never decrease (chunks are in chapter order)."""
        prev = -1
        for job in self.jobs:
            assert job["chapter_idx"] >= prev, (
                f"chapter_idx went backwards at chunk {job['chunk_idx']}: "
                f"{prev} → {job['chapter_idx']}"
            )
            prev = job["chapter_idx"]

    def test_multipart_chapters_labelled(self):
        """Chapters split into multiple chunks must have '- Part N' in title."""
        from collections import Counter
        counts = Counter(j["chapter_idx"] for j in self.jobs)
        for job in self.jobs:
            if counts[job["chapter_idx"]] > 1:
                assert "- Part" in job["title"], (
                    f"Multi-chunk chapter missing Part label: '{job['title']}'"
                )

    # ── summary (printed with -s) ─────────────────────────────────────────────

    def test_print_summary(self, capsys):
        """Print a human-readable pipeline summary (visible with pytest -s)."""
        total_chars = sum(len(ch.text) for ch in self.chapters)
        est_mins = total_chars / 900
        lines = [
            f"\n{'='*60}",
            f"  Book     : {self.path.name}",
            f"  Chapters : {len(self.chapters)} extracted, "
            f"{len(self.skipped)} skipped, {len(self.kept)} kept",
            f"  TTS jobs : {len(self.jobs)} chunks",
            f"  Total    : {total_chars:,} chars  (~{est_mins:.0f} min audio)",
            f"{'─'*60}",
        ]
        for ch in self.kept[:5]:
            ch_jobs = [j for j in self.jobs if j["chapter_idx"] == self.chapters.index(ch)]
            lines.append(
                f"  {'[OK]':<6} {len(ch.text):>7,} chars  "
                f"{len(ch_jobs):>2} chunk(s)  {ch.title[:45]}"
            )
        if len(self.kept) > 5:
            lines.append(f"  ... and {len(self.kept) - 5} more chapters")
        if self.skipped:
            lines.append(f"{'─'*60}")
            for ch in self.skipped[:3]:
                lines.append(
                    f"  {'[SKIP]':<6} {should_skip(ch.title, ch.text):<35}  {ch.title[:35]}"
                )
            if len(self.skipped) > 3:
                lines.append(f"  ... and {len(self.skipped) - 3} more skipped")
        lines.append(f"{'='*60}\n")
        output = "\n".join(lines).encode("utf-8", errors="replace").decode("utf-8")
        with capsys.disabled():
            sys.stdout.buffer.write((output + "\n").encode("utf-8", errors="replace"))
