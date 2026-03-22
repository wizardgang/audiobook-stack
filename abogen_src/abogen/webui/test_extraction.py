"""
Interactive extraction diagnostic for real PDF/EPUB books.

Usage:
    python test_extraction.py path/to/book.pdf
    python test_extraction.py path/to/book.epub
    python test_extraction.py path/to/book.pdf --full   # print full chapter text

What it shows:
  - Which extraction strategy was used (TOC / headings / page-by-page)
  - All detected chapters with title, character count, and a text preview
  - Metadata dict returned by the extractor
  - Whether the skip-filter in the orchestrator would drop any chapter
"""

import sys
import re
import json
from pathlib import Path

# Force UTF-8 output so non-ASCII characters in book text don't crash the
# Windows console (which defaults to cp1252).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# ── orchestrator skip patterns (mirrored from orchestrator.py defaults) ────
_DEFAULT_SKIP = (
    # ── Copyright / legal ────────────────────────────────────────────────────
    "copyright,legal notice,all rights reserved,terms of use,"
    "no part of this,unauthorized reproduction,intellectual property,"
    "isbn,ebook edition,digital edition,printed in,printing history,"

    # ── Publisher imprints (common Big-5 + major imprints) ───────────────────
    "publishing group,published by,an imprint of,"
    "berkley,penguin,random house,harpercollins,simon & schuster,"
    "macmillan,hachette,scholastic,tor books,orbit,del rey,"
    "doubleday,viking,knopf,anchor books,vintage books,"
    "st. martin,bloomsbury,little brown,grand central,"

    # ── Front matter ─────────────────────────────────────────────────────────
    "title page,half title,series page,"
    "dedication,epigraph,inscription,"
    "table of contents,contents,brief contents,full contents,"
    "list of figures,list of tables,list of illustrations,list of maps,"
    "map,maps,family tree,genealogy,cast of characters,dramatis personae,"
    "timeline,chronology,a note on,note to the reader,"
    "translator's note,editor's note,note on the translation,"
    "note on the text,note on sources,a word from,"

    # ── Back matter ───────────────────────────────────────────────────────────
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

    # ── Marketing / promotional ───────────────────────────────────────────────
    "praise for,advance praise,endorsement,blurb,"
    "what readers are saying,what critics say,"
    "excerpt from,preview of,continue reading,sneak peek,"
    "reading group guide,book club guide,discussion questions,"
    "questions for discussion,topics for discussion,"
    "a conversation with,interview with,q&a with,q & a with,"
    "if you enjoyed,readers also enjoyed,readers who liked,"
    "visit us at,follow us on,connect with,sign up for,join our,"
    "newsletter,mailing list,stay connected,"

    # ── Piracy watermarks ─────────────────────────────────────────────────────
    "oceanofpdf,ebookbike,ebook bike,ebookelo,ebook3000,"
    "z-library,zlibrary,libgen,library genesis,freebookspot"
)
SKIP_PATTERNS = [p.strip().lower() for p in _DEFAULT_SKIP.split(",") if p.strip()]
MIN_CHARS = 150


def _would_skip(title: str, text: str) -> str | None:
    t = title.strip().lower()
    for pat in SKIP_PATTERNS:
        if pat in t:
            return f"title matches skip pattern '{pat}'"
    if len(text.strip()) < MIN_CHARS:
        return f"too short ({len(text.strip())} chars < {MIN_CHARS})"
    return None


def _detect_strategy(chapters: list[dict], path: Path) -> str:
    """Guess which strategy was used based on the chapter titles."""
    titles = [ch["title"] for ch in chapters]
    if any(re.match(r"Page \d+", t) for t in titles):
        return "page-by-page fallback"
    chapter_like = re.compile(
        r"^(?:chapter|prologue|epilogue|part|introduction|foreword|preface|afterword)",
        re.IGNORECASE,
    )
    if all(chapter_like.match(t) for t in titles):
        return "heading heuristics (no TOC found)"
    return "TOC / outline (or EPUB navigation)"


def run(file_path: Path, full_text: bool = False):
    print(f"\n{'='*60}")
    print(f"  Book: {file_path.name}")
    print(f"{'='*60}\n")

    # ── Use the Flask test client (same as test_chunk_api.py) ──────────────
    from abogen.webui.app import create_app

    print("Initialising Abogen…")
    app = create_app()
    app.testing = True
    client = app.test_client()

    with open(file_path, "rb") as f:
        resp = client.post(
            "/api/extract",
            data={"file": f},
            content_type="multipart/form-data",
        )

    if resp.status_code != 200:
        print(f"ERROR: /api/extract returned {resp.status_code}")
        print(resp.data.decode())
        sys.exit(1)

    data = json.loads(resp.data)
    if not data.get("success"):
        print(f"ERROR: {data.get('error')}")
        sys.exit(1)

    chapters = data.get("chapters", [])
    metadata = data.get("metadata", {})

    strategy = _detect_strategy(chapters, file_path)

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"Strategy detected : {strategy}")
    print(f"Chapters found    : {len(chapters)}")
    print(f"Metadata          : {json.dumps(metadata, indent=2)}\n")

    # ── Per-chapter breakdown ────────────────────────────────────────────────
    skipped_count = 0
    total_chars = 0

    for i, ch in enumerate(chapters):
        title = ch.get("title", f"Chapter {i+1}")
        text = ch.get("text", "")
        chars = len(text.strip())
        total_chars += chars
        skip_reason = _would_skip(title, text)

        status = f"[SKIP: {skip_reason}]" if skip_reason else "[OK]"
        if skip_reason:
            skipped_count += 1

        preview = text.strip().replace("\n", " ")[:120]
        if len(text.strip()) > 120:
            preview += "…"

        print(f"  {i+1:3}. {status} '{title}'")
        print(f"       {chars:,} chars | preview: {preview}")

        if full_text:
            print(f"\n{'-'*50}\n{text.strip()}\n{'-'*50}\n")

    # ── Footer ───────────────────────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  Total characters : {total_chars:,}")
    print(f"  Chapters OK      : {len(chapters) - skipped_count}")
    print(f"  Chapters skipped : {skipped_count}")
    estimated_mins = total_chars / 900   # ~900 chars/min average TTS
    print(f"  Est. audio length: ~{estimated_mins:.0f} min  (~{estimated_mins/60:.1f} h)")
    print(f"{'─'*60}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_extraction.py <path_to_book> [--full]")
        print("       --full  print complete chapter text (verbose)")
        sys.exit(1)

    book = Path(sys.argv[1])
    if not book.exists():
        print(f"File not found: {book}")
        sys.exit(1)

    verbose = "--full" in sys.argv
    run(book, full_text=verbose)
