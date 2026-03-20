"""
Orchestrator service
- Dequeues PDF paths from pipeline:orchestrate
- Extracts text via PyMuPDF, splits into ~CHUNK_SIZE_CHARS chunks
- Writes chunk .txt files to CHUNKS_DIR
- Pushes individual chunk jobs onto pipeline:tts queue
- Tracks job state in Redis hashes
"""

import os
import re
import json
import time
import math
import logging
import redis
import fitz          # PyMuPDF
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [orchestrator] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL        = os.environ.get("REDIS_URL",        "redis://localhost:6379")
CHUNKS_DIR       = Path(os.environ.get("CHUNKS_DIR",  "/chunks"))
CHUNK_SIZE_CHARS = int(os.environ.get("CHUNK_SIZE_CHARS", "3000"))

QUEUE_ORCHESTRATE = "pipeline:orchestrate"
QUEUE_TTS         = "pipeline:tts"

r = redis.from_url(REDIS_URL, decode_responses=True)


# ── Text extraction ──────────────────────────────────────────────────────────

def filter_text(text: str) -> str:
    """Apply content filters to remove unwanted text like URLs."""
    # Remove URLs (http/https/www)
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # Add other word filters here if needed:
    # text = re.sub(r"\b(word_to_remove|another_word)\b", "", text, flags=re.IGNORECASE)
    return text

def is_toc_page(page, page_text: str, page_num: int, total_pages: int) -> bool:
    """
    Advanced detection for Table of Contents or Index pages mapping.
    Uses native PDF internal links for perfect structural detection.
    """
    # 1. Structural Metadata check (The most reliable method)
    # A true PDF Table of Contents almost always contains internal hyperlinks (LINK_GOTO).
    links = page.get_links()
    internal_links = [link for link in links if link.get("kind") == fitz.LINK_GOTO]
    
    # If a page contains 7+ internal navigation links, it is definitively a TOC, Index, or Glossary.
    if len(internal_links) >= 7:
        log.info("Bypassing page %d: Detected %d internal metadata links (likely TOC/Index)", page_num, len(internal_links))
        return True

    # 2. Heuristic Text fallback (For basic, non-hyperlinked PDFs)
    if page_num > 30 and page_num < (total_pages * 0.9):
        # Only check text heuristics on the first 30 pages (TOC) and last 10% (Index)
        return False
        
    lines = [line.strip() for line in page_text.split('\n') if line.strip()]
    if not lines:
        return False
        
    first_lines = " ".join(lines[:3]).lower()
    if "table of contents" in first_lines or first_lines.startswith("contents") or "index" in first_lines:
        return True
        
    toc_line_matches = 0
    for line in lines:
        if re.search(r'(?:\.{3,}|\b\s+)\d{1,4}$', line):
            toc_line_matches += 1
            
    if len(lines) > 5 and (toc_line_matches / len(lines)) > 0.3:
        log.info("Bypassing page %d: Text closely resembles a Table of Contents format", page_num)
        return True
        
    return False

def extract_text(pdf_path: Path) -> str:
    doc = fitz.open(str(pdf_path))
    pages = []
    total_pages = len(doc)
    
    for page_num, page in enumerate(doc, start=1):
        page_text = page.get_text("text")
        
        # Skip Table of Contents and Index pages
        if is_toc_page(page, page_text, page_num, total_pages):
            continue
            
        pages.append(page_text)
        
    doc.close()
    text = "\n".join(pages)
    
    # Filter out links and other unwanted words
    text = filter_text(text)
    
    # Normalise whitespace but preserve paragraph breaks
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


# ── Chunking ─────────────────────────────────────────────────────────────────

def split_into_chunks(text: str, max_chars: int) -> list[str]:
    """
    Uses Abogen's native NLP chunking to accurately split text into sentences,
    then merges them into chunks up to max_chars for worker distribution.
    Falls back to hard split if a single sentence exceeds max_chars.
    """
    import requests
    
    ABOGEN_HOST = os.environ.get("ABOGEN_HOST", "http://localhost:8808")
    
    try:
        resp = requests.post(
            f"{ABOGEN_HOST}/api/chunk",
            json={"text": text, "level": "sentence"},
            timeout=180
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(data.get("error", "Unknown chunking error"))
            
        sentences = [c.get("original_text", "") for c in data.get("chunks", [])]
    except Exception as e:
        log.warning("Failed to use /api/chunk (%s), falling back to fast regex.", e)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        
    chunks, current = [], ""

    for sentence in sentences:
        if not sentence.strip():
            continue
        if len(current) + len(sentence) + 1 > max_chars:
            if current:
                chunks.append(current.strip())
            # If a single sentence is too long, hard split it
            if len(sentence) > max_chars:
                for i in range(0, len(sentence), max_chars):
                    chunks.append(sentence[i:i + max_chars].strip())
                current = ""
            else:
                current = sentence
        else:
            current = (current + " " + sentence).lstrip()

    if current.strip():
        chunks.append(current.strip())

    return chunks


# ── Job tracking ──────────────────────────────────────────────────────────────

def set_book_state(book_id: str, **kwargs):
    r.hset(f"book:{book_id}", mapping={k: str(v) for k, v in kwargs.items()})


def set_chunk_state(book_id: str, chunk_idx: int, **kwargs):
    r.hset(f"chunk:{book_id}:{chunk_idx}", mapping={k: str(v) for k, v in kwargs.items()})


# ── Main processing loop ──────────────────────────────────────────────────────

def process_job(raw: str):
    job = json.loads(raw)
    book_id  = job["id"]
    pdf_path = Path(job["path"])

    log.info("Processing book %s — %s", book_id[:8], pdf_path.name)

    set_book_state(book_id,
        filename=pdf_path.name,
        status="extracting",
        started_at=time.time(),
    )

    # 1. External Extraction via API (Supports Native Chapters!)
    ABOGEN_HOST = os.environ.get("ABOGEN_HOST", "http://localhost:8808")
    try:
        import requests
        with open(pdf_path, "rb") as f:
            resp = requests.post(f"{ABOGEN_HOST}/api/extract", files={"file": f}, timeout=300)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(data.get("error", "Unknown extraction error"))
        chapters = data.get("chapters", [])
    except Exception as exc:
        log.error("API Text extraction failed: %s", exc)
        set_book_state(book_id, status="error", error=str(exc))
        return

    # 2. Split chapters into chunks
    book_chunks_dir = CHUNKS_DIR / book_id
    book_chunks_dir.mkdir(parents=True, exist_ok=True)
    
    chapter_metadata = []
    global_chunk_idx = 0
    
    for ch_idx, chap in enumerate(chapters):
        ch_title = chap.get("title", f"Chapter {ch_idx+1}")
        ch_text = chap.get("text", "")
        # Filter urls etc
        ch_text = filter_text(ch_text)
        if not ch_text.strip(): continue
        
        ch_chunks = split_into_chunks(ch_text, CHUNK_SIZE_CHARS)
        
        for c_text in ch_chunks:
            chunk_file = book_chunks_dir / f"chunk_{global_chunk_idx:04d}.txt"
            chunk_file.write_text(c_text, encoding="utf-8")
            
            chunk_job = {
                "book_id": book_id,
                "chunk_idx": global_chunk_idx,
                "chapter_idx": ch_idx,
                "chapter_title": ch_title,
                "title": f"{ch_title} - Part {global_chunk_idx}",
                "text": c_text,
                "chunk_file": str(chunk_file)
            }
            chapter_metadata.append(chunk_job)
            global_chunk_idx += 1
            
    total = global_chunk_idx
    
    # Save a metadata file so the merger knows the chapter mapping
    meta_file = book_chunks_dir / "meta.json"
    meta_file.write_text(json.dumps(chapter_metadata, indent=2), encoding="utf-8")

    set_book_state(book_id,
        status="queued",
        total_chunks=total,
        done_chunks=0,
        title=pdf_path.stem,
    )

    # 3. Enqueue TTS jobs
    for c_job in chapter_metadata:
        c_job["total"] = total
        r.lpush(QUEUE_TTS, json.dumps(c_job))
        set_chunk_state(book_id, c_job["chunk_idx"], status="queued", file=c_job["chunk_file"])

    log.info("  %d chapters -> %d total chunks", len(chapters), total)
    log.info("  Enqueued %d TTS jobs for %s", total, book_id[:8])


def main():
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Orchestrator ready, consuming %s", QUEUE_ORCHESTRATE)

    while True:
        result = r.brpop(QUEUE_ORCHESTRATE, timeout=5)
        if result is None:
            continue
        _, raw = result
        try:
            process_job(raw)
        except Exception as exc:
            log.exception("Unexpected error processing job: %s", exc)


if __name__ == "__main__":
    main()

