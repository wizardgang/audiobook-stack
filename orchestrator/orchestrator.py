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
from concurrent.futures import ThreadPoolExecutor, as_completed
import redis
import fitz          # PyMuPDF
from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram, Gauge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [orchestrator] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL        = os.environ.get("REDIS_URL",        "redis://localhost:6379")
CHUNKS_DIR       = Path(os.environ.get("CHUNKS_DIR",  "/chunks"))
CHUNK_SIZE_CHARS = int(os.environ.get("CHUNK_SIZE_CHARS", "3000"))

# ── Prometheus Metrics ────────────────────────────────────────────────────────
ORCH_BOOKS_TOTAL      = Counter('pipeline_orchestrator_books_total',
                                'Books ingested by orchestrator', ['status'])
ORCH_CHAPTERS_TOTAL   = Counter('pipeline_orchestrator_chapters_total',
                                'Chapters encountered during extraction', ['status'])
ORCH_CHUNKS_ENQUEUED  = Counter('pipeline_orchestrator_chunks_enqueued_total',
                                'TTS chunks pushed to the queue')
ORCH_EXTRACTION_SECS  = Histogram('pipeline_orchestrator_extraction_seconds',
                                  'PDF text extraction duration',
                                  buckets=[1, 5, 15, 30, 60, 120, 300])
ORCH_CHUNKING_SECS    = Histogram('pipeline_orchestrator_chunking_seconds',
                                  'Chapter chunking duration per book',
                                  buckets=[1, 5, 15, 30, 60, 120, 300])
ORCH_NORMALIZE_SECS   = Histogram('pipeline_orchestrator_ai_normalize_seconds',
                                  'AI text normalization latency per chunk',
                                  buckets=[0.1, 0.3, 0.5, 1, 2, 5, 10])
ORCH_AI_CALLS_TOTAL   = Counter('pipeline_orchestrator_ai_calls_total',
                                'AI API calls made', ['type', 'status'])
ORCH_AI_DURATION_SECS = Histogram('pipeline_orchestrator_ai_duration_seconds',
                                  'AI API call latency', ['type'],
                                  buckets=[0.1, 0.5, 1, 2, 5, 10, 30])
ORCH_QUEUE_DEPTH      = Gauge('pipeline_queue_tts_depth',
                               'Current TTS queue depth (live)')
ORCH_WATCHDOG_TOTAL   = Counter('pipeline_orchestrator_watchdog_resurrections_total',
                                'Ghost chunks re-queued by the watchdog')

# Chapters whose titles match any of these patterns (case-insensitive) are skipped.
# Override via env: SKIP_CHAPTERS="copyright,acknowledgment,about the author,bibliography"
_default_skip = (
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
SKIP_CHAPTER_PATTERNS = [
    p.strip().lower()
    for p in os.environ.get("SKIP_CHAPTERS", _default_skip).split(",")
    if p.strip()
]

# Chapters with fewer characters than this after cleaning are skipped (e.g. blank/title pages).
MIN_CHAPTER_CHARS = int(os.environ.get("MIN_CHAPTER_CHARS", "150"))

QUEUE_ORCHESTRATE = "pipeline:orchestrate"
QUEUE_TTS         = "pipeline:tts"

# ── AI text normalization (optional) ─────────────────────────────────────────
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL   = os.environ.get("OPENROUTER_MODEL", "google/gemini-2.0-flash-001")
AI_NORMALIZE       = bool(OPENROUTER_API_KEY)

if AI_NORMALIZE:
    from openai import OpenAI as _OpenAI
    _ai_client = _OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=OPENROUTER_API_KEY,
    )
    log_init = logging.getLogger(__name__)
    log_init.info("AI normalization enabled — model: %s", OPENROUTER_MODEL)

_NORMALIZE_SYSTEM = (
    "You are a text preprocessor for audiobook text-to-speech conversion. "
    "Clean the following text for TTS narration:\n"
    "- Fix hyphenated line breaks (e.g. 're-\\nplace' → 'replace')\n"
    "- Expand abbreviations that sound wrong when spoken: "
    "Dr.→Doctor, Mr.→Mister, Mrs.→Missus, Ms.→Miss, Prof.→Professor, "
    "vs.→versus, etc.→and so on, e.g.→for example, i.e.→that is, "
    "approx.→approximately, dept.→department, St.→Saint\n"
    "- Remove footnote/endnote markers such as [1], (ibid.), (op. cit.), *\n"
    "- Fix OCR artifacts: broken words, stray characters, doubled spaces\n"
    "- Preserve paragraph breaks and natural punctuation rhythm\n"
    "- Do NOT summarize, paraphrase, or change any meaning\n"
    "- Return ONLY the cleaned text, nothing else"
)


def normalize_text_for_tts(text: str) -> str:
    """Send chunk text to OpenRouter/Gemini for TTS preprocessing. Falls back to original on error."""
    if not AI_NORMALIZE:
        return text
    t0 = time.time()
    try:
        resp = _ai_client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {"role": "system", "content": _NORMALIZE_SYSTEM},
                {"role": "user",   "content": text},
            ],
            max_tokens=2048,
            temperature=0.1,
        )
        normalized = resp.choices[0].message.content
        elapsed = time.time() - t0
        ORCH_AI_CALLS_TOTAL.labels(type="normalize", status="success").inc()
        ORCH_NORMALIZE_SECS.observe(elapsed)
        ORCH_AI_DURATION_SECS.labels(type="normalize").observe(elapsed)
        return normalized.strip() if normalized else text
    except Exception as exc:
        ORCH_AI_CALLS_TOTAL.labels(type="normalize", status="fallback").inc()
        log.warning("AI normalization failed, using original text: %s", exc)
        return text

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

def split_into_chunks_ai(text: str, max_chars: int) -> list[str] | None:
    """
    Use Gemini to group paragraphs into semantically coherent TTS chunks.
    Respects scene breaks, dialogue exchanges, and narrative rhythm.

    Strategy: send only paragraph indices + char counts + 90-char previews
    to the model — no full text returned — so output tokens are minimal (~200).
    Returns a list of chunk strings, or None on failure so the caller falls back.
    """
    paragraphs = [p.strip() for p in re.split(r'\n{2,}', text) if p.strip()]
    if len(paragraphs) < 2:
        return None

    target_min = int(max_chars * 0.6)

    # Build compact paragraph index: index, size, 90-char preview
    lines = []
    for i, p in enumerate(paragraphs):
        preview = p[:90].replace('\n', ' ')
        suffix  = "..." if len(p) > 90 else ""
        lines.append(f"[{i}] {len(p)}ch: {preview}{suffix}")

    prompt = (
        f"Group these book paragraphs into audiobook narration chunks.\n"
        f"Target size per chunk: {target_min}–{max_chars} characters.\n"
        f"Rules:\n"
        f"- Group only consecutive paragraphs\n"
        f"- Prefer to split at scene transitions, after complete dialogue exchanges, "
        f"or at clear topic/mood shifts — not mid-action or mid-conversation\n"
        f"- Every paragraph index must appear in exactly one group\n"
        f"- Output ONLY valid JSON: an array of arrays of indices. "
        f"Example: [[0,1,2],[3,4],[5,6,7,8]]\n\n"
        f"Paragraphs:\n" + "\n".join(lines)
    )

    t0 = time.time()
    try:
        resp = _ai_client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=512,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content or ""

        # Extract the JSON array from the response
        match = re.search(r'\[\s*\[[\s\S]*?\]\s*\]', raw)
        if not match:
            log.warning("AI chunking: no JSON array found in response")
            ORCH_AI_CALLS_TOTAL.labels(type="chunk", status="fallback").inc()
            return None

        groups = json.loads(match.group())

        # Validate every paragraph is covered exactly once
        all_indices = sorted(i for g in groups for i in g)
        if all_indices != list(range(len(paragraphs))):
            log.warning("AI chunking: incomplete or duplicate paragraph coverage, falling back")
            ORCH_AI_CALLS_TOTAL.labels(type="chunk", status="fallback").inc()
            return None

        chunks = []
        for group in groups:
            chunk_text = "\n\n".join(paragraphs[i] for i in sorted(group))
            if chunk_text.strip():
                chunks.append(chunk_text.strip())

        elapsed = time.time() - t0
        ORCH_AI_CALLS_TOTAL.labels(type="chunk", status="success").inc()
        ORCH_AI_DURATION_SECS.labels(type="chunk").observe(elapsed)
        log.info("    AI chunking: %d paragraphs → %d chunks", len(paragraphs), len(chunks))
        return chunks if chunks else None

    except Exception as exc:
        ORCH_AI_CALLS_TOTAL.labels(type="chunk", status="error").inc()
        log.warning("AI chunking failed (%s), falling back to sentence splitting", exc)
        return None


def split_into_chunks(text: str, max_chars: int) -> list[str]:
    """
    Split chapter text into TTS-ready chunks.
    1. Try AI-intelligent paragraph grouping (respects narrative structure).
    2. Fall back to Abogen NLP sentence splitter + greedy merge.
    3. Last resort: regex sentence split + greedy merge.
    """
    # ── 1. AI intelligent chunking ───────────────────────────────────────────
    if AI_NORMALIZE:
        ai_chunks = split_into_chunks_ai(text, max_chars)
        if ai_chunks:
            return ai_chunks

    # ── 2. Abogen NLP sentence splitter ──────────────────────────────────────
    import requests
    ABOGEN_HOST = os.environ.get("ABOGEN_HOST", "http://localhost:8808")

    try:
        resp = requests.post(
            f"{ABOGEN_HOST}/api/chunk",
            json={"text": text, "level": "sentence"},
            timeout=180,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(data.get("error", "Unknown chunking error"))
        sentences = [c.get("original_text", "") for c in data.get("chunks", [])]
    except Exception as e:
        log.warning("Failed to use /api/chunk (%s), falling back to fast regex.", e)
        sentences = re.split(r'(?<=[.!?])\s+', text)

    # ── 3. Greedy sentence merge ──────────────────────────────────────────────
    chunks, current = [], ""
    for sentence in sentences:
        if not sentence.strip():
            continue
        if len(current) + len(sentence) + 1 > max_chars:
            if current:
                chunks.append(current.strip())
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

def should_skip_chapter(title: str, text: str) -> str | None:
    """
    Returns a reason string if the chapter should be skipped, else None.
    Checks title patterns and minimum content length.
    """
    t = title.strip().lower()
    for pattern in SKIP_CHAPTER_PATTERNS:
        if pattern in t:
            return f"title matches skip pattern '{pattern}'"
    if len(text.strip()) < MIN_CHAPTER_CHARS:
        return f"too short ({len(text.strip())} chars < {MIN_CHAPTER_CHARS})"
    return None


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
    extraction_start = time.time()
    try:
        import requests
        with open(pdf_path, "rb") as f:
            resp = requests.post(f"{ABOGEN_HOST}/api/extract", files={"file": f}, timeout=300)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(data.get("error", "Unknown extraction error"))
        chapters = data.get("chapters", [])
        ORCH_EXTRACTION_SECS.observe(time.time() - extraction_start)
    except Exception as exc:
        log.error("API Text extraction failed: %s", exc)
        set_book_state(book_id, status="error", error=str(exc))
        ORCH_BOOKS_TOTAL.labels(status="failed").inc()
        return

    # 2. Split chapters into chunks
    chunking_start = time.time()
    book_chunks_dir = CHUNKS_DIR / book_id
    book_chunks_dir.mkdir(parents=True, exist_ok=True)

    chapter_metadata = []
    global_chunk_idx = 0

    skipped = 0
    for ch_idx, chap in enumerate(chapters):
        ch_title = chap.get("title", f"Chapter {ch_idx+1}")
        ch_text = filter_text(chap.get("text", ""))

        reason = should_skip_chapter(ch_title, ch_text)
        if reason:
            log.info("  Skipping chapter '%s': %s", ch_title, reason)
            ORCH_CHAPTERS_TOTAL.labels(status="skipped").inc()
            skipped += 1
            continue

        ORCH_CHAPTERS_TOTAL.labels(status="processed").inc()
        ch_chunks = split_into_chunks(ch_text, CHUNK_SIZE_CHARS)

        for part_idx, c_text in enumerate(ch_chunks):
            chunk_file = book_chunks_dir / f"chunk_{global_chunk_idx:04d}.txt"
            chunk_file.write_text(c_text, encoding="utf-8")

            title = ch_title if len(ch_chunks) == 1 else f"{ch_title} - Part {part_idx + 1}"
            chunk_job = {
                "book_id": book_id,
                "chunk_idx": global_chunk_idx,
                "chapter_idx": ch_idx,
                "chapter_title": ch_title,
                "title": title,
                "text": c_text,
                "chunk_file": str(chunk_file)
            }
            chapter_metadata.append(chunk_job)
            global_chunk_idx += 1

    total = global_chunk_idx
    ORCH_CHUNKING_SECS.observe(time.time() - chunking_start)

    # AI text normalization — clean all chunks in parallel before TTS
    if AI_NORMALIZE and chapter_metadata:
        log.info("  Normalizing %d chunks via %s...", total, OPENROUTER_MODEL)

        def _normalize(item):
            idx, meta = item
            return idx, normalize_text_for_tts(meta["text"])

        completed = 0
        with ThreadPoolExecutor(max_workers=12) as pool:
            futures = {pool.submit(_normalize, (i, m)): i for i, m in enumerate(chapter_metadata)}
            for future in as_completed(futures):
                idx, cleaned = future.result()
                chapter_metadata[idx]["text"] = cleaned
                Path(chapter_metadata[idx]["chunk_file"]).write_text(cleaned, encoding="utf-8")
                completed += 1
                if completed % 50 == 0 or completed == total:
                    log.info("  Normalized %d/%d chunks", completed, total)

        log.info("  AI normalization complete.")

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
        # Redis OOM prevention limit: Block enqueueing if queue is extremely saturated
        while r.llen(QUEUE_TTS) > 1000:
            log.warning("Redis queue heavily saturated (>1000 tasks). Suspending chunk injection to protect RAM...")
            time.sleep(15)

        c_job["total"] = total
        r.lpush(QUEUE_TTS, json.dumps(c_job))
        set_chunk_state(book_id, c_job["chunk_idx"], status="queued", file=c_job["chunk_file"])

    ORCH_CHUNKS_ENQUEUED.inc(total)
    ORCH_BOOKS_TOTAL.labels(status="extracted").inc()
    ORCH_QUEUE_DEPTH.set(r.llen(QUEUE_TTS))

    log.info("  %d chapters -> %d used, %d skipped -> %d total chunks",
             len(chapters), len(chapters) - skipped, skipped, total)
    log.info("  Enqueued %d TTS jobs for %s", total, book_id[:8])


def main():
    CHUNKS_DIR.mkdir(parents=True, exist_ok=True)

    prom_port = int(os.environ.get("PROMETHEUS_PORT", "8001"))
    start_http_server(prom_port)
    log.info("Prometheus metrics on port %d", prom_port)
    log.info("Orchestrator ready, consuming %s", QUEUE_ORCHESTRATE)

    last_watchdog_at = 0.0

    while True:
        # Update live queue depth gauge
        try:
            ORCH_QUEUE_DEPTH.set(r.llen(QUEUE_TTS))
        except Exception:
            pass

        # Ghost Worker Watchdog: Sweep for stuck chunks every 60s
        now = time.time()
        if now - last_watchdog_at >= 60:
            last_watchdog_at = now
            try:
                for key in r.scan_iter("chunk:*:*"):
                    chunk_data = r.hgetall(key)
                    if chunk_data.get("status") == "processing":
                        started_at = float(chunk_data.get("started_at", 0))
                        # If chunk is stuck in 'processing' for over 30 mins (1800s)
                        if now - started_at > 1800:
                            book_id = key.split(":")[1]
                            chunk_idx = int(key.split(":")[2])
                            log.warning("Ghost chunk detected (Processing > 30mins): %s part %d. Resurrecting into queue... ", book_id, chunk_idx)

                            # Rebuild payload via its local meta.json
                            book_chunks_dir = CHUNKS_DIR / book_id
                            meta_file = book_chunks_dir / "meta.json"
                            if meta_file.exists():
                                metadata = json.loads(meta_file.read_text(encoding="utf-8"))
                                for c_job in metadata:
                                    if c_job["chunk_idx"] == chunk_idx:
                                        c_job["total"] = len(metadata)
                                        r.lpush(QUEUE_TTS, json.dumps(c_job))
                                        set_chunk_state(book_id, chunk_idx, status="queued")
                                        ORCH_WATCHDOG_TOTAL.inc()
                                        break
            except Exception as e:
                log.error("Watchdog sweep failed: %s", e)

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

