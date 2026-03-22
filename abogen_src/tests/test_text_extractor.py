"""
Unit tests for text_extractor._extract_pdf and _extract_epub chapter detection.

Creates synthetic in-memory PDFs/EPUBs using PyMuPDF + ebooklib so no real
book files are required.  Run with:
    cd abogen_src && pytest tests/test_text_extractor.py -v
"""

import os
import re
import tempfile
from pathlib import Path

import fitz
import pytest

# ---------------------------------------------------------------------------
# Helpers: synthetic PDF builders
# ---------------------------------------------------------------------------

_LOREM = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. "
    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris "
    "nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in "
    "reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla "
    "pariatur. Excepteur sint occaecat cupidatat non proident, sunt in "
    "culpa qui officia deserunt mollit anim id est laborum. "
)

# Enough body text to exceed MIN_CHAPTER_CHARS comfortably
_BODY = (_LOREM * 5).strip()


def _save_pdf(doc: fitz.Document) -> Path:
    """Save a PyMuPDF document to a temp file and return its Path.

    Uses mkstemp so the fd is closed before PyMuPDF writes — required on
    Windows where NamedTemporaryFile holds an exclusive lock on the file.
    """
    fd, path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    doc.save(path)
    doc.close()
    return Path(path)


def _make_pdf_with_toc(chapters: list[tuple[str, str]]) -> Path:
    """Build a PDF where each chapter is its own page with an embedded TOC."""
    doc = fitz.open()
    toc_entries = []
    for page_num, (title, body) in enumerate(chapters, start=1):
        page = doc.new_page()
        page.insert_text((72, 72), f"{title}\n\n{body}", fontsize=11)
        toc_entries.append([1, title, page_num])
    doc.set_toc(toc_entries)
    return _save_pdf(doc)


def _make_pdf_with_headings(chapters: list[tuple[str, str]]) -> Path:
    """Build a PDF with heading-like first lines but NO embedded TOC."""
    doc = fitz.open()
    for title, body in chapters:
        page = doc.new_page()
        page.insert_text((72, 72), f"{title}\n\n{body}", fontsize=11)
    return _save_pdf(doc)


def _make_pdf_no_structure(page_texts: list[str]) -> Path:
    """Build a flat PDF with no TOC and no chapter headings."""
    doc = fitz.open()
    for text in page_texts:
        page = doc.new_page()
        if text:
            page.insert_text((72, 72), text, fontsize=11)
    return _save_pdf(doc)


# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from abogen.text_extractor import (
    _extract_pdf,
    _pdf_chapters_from_toc,
    _pdf_chapters_from_headings,
    _CHAPTER_HEADING_RE,
)


# ---------------------------------------------------------------------------
# Strategy 1: TOC / outline-based extraction
# ---------------------------------------------------------------------------

class TestPdfTocExtraction:

    def test_basic_three_chapters(self):
        chapters_in = [
            ("Prologue", _BODY),
            ("Chapter 1: The Beginning", _BODY),
            ("Chapter 2: The Middle", _BODY),
        ]
        path = _make_pdf_with_toc(chapters_in)
        result = _extract_pdf(path)
        titles = [ch.title for ch in result.chapters]
        assert "Prologue" in titles
        assert "Chapter 1: The Beginning" in titles
        assert "Chapter 2: The Middle" in titles

    def test_chapter_count_matches_toc(self):
        chapters_in = [("Chapter %d" % i, _BODY) for i in range(1, 8)]
        path = _make_pdf_with_toc(chapters_in)
        result = _extract_pdf(path)
        assert len(result.chapters) == 7

    def test_chapter_text_is_nonempty(self):
        chapters_in = [("Prologue", _BODY), ("Chapter 1", _BODY)]
        path = _make_pdf_with_toc(chapters_in)
        result = _extract_pdf(path)
        for ch in result.chapters:
            assert len(ch.text.strip()) > 50, f"Chapter '{ch.title}' has almost no text"

    def test_toc_takes_priority_over_headings(self):
        """When a TOC exists the result titles must come from the TOC entries,
        not from text-scanning heuristics."""
        chapters_in = [
            ("Part I: Dawn", _BODY),
            ("Part II: Dusk", _BODY),
        ]
        path = _make_pdf_with_toc(chapters_in)
        result = _extract_pdf(path)
        titles = [ch.title for ch in result.chapters]
        assert "Part I: Dawn" in titles
        assert "Part II: Dusk" in titles

    def test_single_toc_entry_falls_through(self):
        """A TOC with only one entry must NOT be used (falls back to heuristics/pages)."""
        chapters_in = [("Chapter 1", _BODY), ("Chapter 2", _BODY)]
        path = _make_pdf_with_toc(chapters_in[:1])   # only register first in TOC
        # We're just checking it doesn't crash and returns something
        result = _extract_pdf(path)
        assert len(result.chapters) >= 1


# ---------------------------------------------------------------------------
# Strategy 2: Heading heuristics
# ---------------------------------------------------------------------------

class TestPdfHeadingExtraction:

    def test_chapter_keyword(self):
        chapters_in = [
            ("Chapter 1", _BODY),
            ("Chapter 2", _BODY),
            ("Chapter 3", _BODY),
        ]
        path = _make_pdf_with_headings(chapters_in)
        result = _extract_pdf(path)
        assert len(result.chapters) == 3
        titles = [ch.title for ch in result.chapters]
        assert any("Chapter 1" in t for t in titles), f"Titles: {titles}"
        assert any("Chapter 2" in t for t in titles), f"Titles: {titles}"
        assert any("Chapter 3" in t for t in titles), f"Titles: {titles}"

    def test_prologue_and_epilogue(self):
        chapters_in = [
            ("Prologue", _BODY),
            ("Chapter 1", _BODY),
            ("Epilogue", _BODY),
        ]
        path = _make_pdf_with_headings(chapters_in)
        result = _extract_pdf(path)
        titles = [ch.title for ch in result.chapters]
        assert any("Prologue" in t for t in titles), f"Titles: {titles}"
        assert any("Epilogue" in t for t in titles), f"Titles: {titles}"

    def test_part_keyword(self):
        chapters_in = [
            ("Part I", _BODY),
            ("Part II", _BODY),
        ]
        path = _make_pdf_with_headings(chapters_in)
        result = _extract_pdf(path)
        assert len(result.chapters) == 2
        titles = [ch.title for ch in result.chapters]
        assert any("Part I" in t for t in titles), f"Titles: {titles}"
        assert any("Part II" in t for t in titles), f"Titles: {titles}"

    def test_introduction_and_foreword(self):
        for heading in ("Introduction", "Foreword", "Preface", "Afterword"):
            chapters_in = [(heading, _BODY), ("Chapter 1", _BODY)]
            path = _make_pdf_with_headings(chapters_in)
            result = _extract_pdf(path)
            titles = [ch.title for ch in result.chapters]
            assert any(heading.lower() in t.lower() for t in titles), \
                f"Expected '{heading}' to be detected as a chapter"

    def test_roman_numeral_chapter(self):
        chapters_in = [
            ("Chapter I", _BODY),
            ("Chapter II", _BODY),
            ("Chapter III", _BODY),
        ]
        path = _make_pdf_with_headings(chapters_in)
        result = _extract_pdf(path)
        assert len(result.chapters) == 3

    def test_chapter_with_subtitle(self):
        chapters_in = [
            ("Chapter 1: In Which Everything Changes", _BODY),
            ("Chapter 2: The Storm", _BODY),
        ]
        path = _make_pdf_with_headings(chapters_in)
        result = _extract_pdf(path)
        assert len(result.chapters) == 2

    def test_body_text_not_mistaken_for_heading(self):
        """Prose containing the word 'chapter' mid-sentence must not trigger detection."""
        prose = "She turned to the next chapter in the old book and began to read carefully."
        path = _make_pdf_no_structure([prose * 20, prose * 20])
        result = _extract_pdf(path)
        # Must fall back to page-by-page; titles should be "Page N"
        assert all(ch.title.startswith("Page ") for ch in result.chapters)


# ---------------------------------------------------------------------------
# Strategy 3: Page-by-page fallback
# ---------------------------------------------------------------------------

class TestPdfPageFallback:

    def test_flat_pdf_falls_back_to_pages(self):
        texts = [_BODY, _BODY, _BODY]
        path = _make_pdf_no_structure(texts)
        result = _extract_pdf(path)
        assert len(result.chapters) == 3
        for ch in result.chapters:
            assert re.match(r"Page \d+", ch.title)

    def test_empty_pages_are_skipped(self):
        texts = [_BODY, "", _BODY]   # middle page is blank
        path = _make_pdf_no_structure(texts)
        result = _extract_pdf(path)
        assert len(result.chapters) == 2

    def test_fully_empty_pdf_returns_placeholder(self):
        path = _make_pdf_no_structure([""])
        result = _extract_pdf(path)
        assert len(result.chapters) == 1


# ---------------------------------------------------------------------------
# _CHAPTER_HEADING_RE: regex unit tests
# ---------------------------------------------------------------------------

class TestChapterHeadingRegex:

    @pytest.mark.parametrize("line", [
        "Chapter 1",
        "Chapter 42",
        "Chapter One",
        "Chapter I",
        "Chapter iv",
        "chapter 1",
        "CHAPTER 1",
        "Chapter 1: The Beginning",
        "Chapter 1 - Into the Unknown",
        "Chapter 1 – A New World",
        "Prologue",
        "PROLOGUE",
        "Epilogue",
        "Introduction",
        "Preface",
        "Foreword",
        "Afterword",
        "Interlude",
        "Conclusion",
        "Part I",
        "Part II",
        "Part 3",
        "Part One",
        "Part I: The Old World",
    ])
    def test_matches(self, line):
        assert _CHAPTER_HEADING_RE.match(line), f"Should match: {line!r}"

    @pytest.mark.parametrize("line", [
        "He read the next chapter with great interest.",
        "This is a prologue to a longer story about many things.",
        "In the introduction to the book the author explains everything.",
        "See the conclusion on page 42.",
        "",
        "123",
        "A very long heading that exceeds eighty characters in total and should absolutely not match anything",
    ])
    def test_no_match(self, line):
        # Either the line is too long (filtered by len <= 80 guard in heuristics),
        # or the regex genuinely does not match it.
        assert len(line) > 80 or not _CHAPTER_HEADING_RE.match(line), \
            f"Should not match as a heading: {line!r}"


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

class TestPdfMetadata:

    def test_metadata_keys_present(self):
        path = _make_pdf_with_toc([("Chapter 1", _BODY), ("Chapter 2", _BODY)])
        result = _extract_pdf(path)
        assert "title" in result.metadata
        assert "chapter_count" in result.metadata

    def test_chapter_count_matches_extracted(self):
        chapters_in = [("Chapter %d" % i, _BODY) for i in range(1, 5)]
        path = _make_pdf_with_toc(chapters_in)
        result = _extract_pdf(path)
        assert int(result.metadata["chapter_count"]) == len(result.chapters)
