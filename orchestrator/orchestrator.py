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
from dataclasses import dataclass
from enum import Enum
from typing import Optional
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

# ── Kokoro Voice Blending ─────────────────────────────────────────────────────
# Voice blend in "name:weight,name:weight" format, e.g. "af_bella:60,af_sky:40"
# Also accepts the native formula format: "af_bella*0.6+af_sky*0.4"
KOKORO_VOICE_BLEND = os.environ.get("KOKORO_VOICE_BLEND", "")
KOKORO_SPEED       = float(os.environ.get("KOKORO_SPEED", "1.0"))

# ── Kokoro SSML ───────────────────────────────────────────────────────────────
KOKORO_SSML_ENABLED     = os.environ.get("KOKORO_SSML_ENABLED", "false").lower() == "true"
# KOKORO_GENRE is the *fallback* when auto-detection is disabled or fails.
# Valid values: fiction | romance | thriller | fantasy | non_fiction | ya
KOKORO_GENRE            = os.environ.get("KOKORO_GENRE", "fiction")
# When True (default), genre is inferred from the book title + opening text via AI.
# Set to "false" to pin the genre using KOKORO_GENRE instead.
KOKORO_GENRE_AUTO_DETECT = os.environ.get("KOKORO_GENRE_AUTO_DETECT", "true").lower() == "true"
# When True, the AI model generates SSML markup instead of the rule-based fallback.
# Requires OPENROUTER_API_KEY (same key used for text normalisation).
KOKORO_SSML_AI_ENHANCED = os.environ.get("KOKORO_SSML_AI_ENHANCED", "false").lower() == "true"


# ── Voice Blending ────────────────────────────────────────────────────────────

def voice_blend_to_formula(blend: str) -> str:
    """Convert colon-weight blend format to Kokoro formula.

    Accepts two formats:
      • "af_bella:60,af_sky:40"  → "af_bella*0.60+af_sky*0.40"
      • "af_bella*0.6+af_sky*0.4" → returned as-is (already a formula)
    Weights are normalized to sum to 1 automatically.
    """
    blend = blend.strip()
    if not blend:
        return ""
    if "*" in blend:
        # Already a Kokoro formula — pass through
        return blend
    parts = []
    total_weight = 0.0
    segments = []
    for segment in blend.split(","):
        segment = segment.strip()
        if not segment:
            continue
        if ":" in segment:
            voice, raw_w = segment.rsplit(":", 1)
            voice = voice.strip()
            try:
                w = float(raw_w.strip())
            except ValueError:
                raise ValueError(f"Invalid weight for voice '{voice}': '{raw_w}'")
        else:
            voice, w = segment, 100.0
        if w <= 0:
            raise ValueError(f"Weight for '{voice}' must be positive")
        segments.append((voice, w))
        total_weight += w

    if total_weight <= 0:
        raise ValueError("Voice blend weights must sum to a positive value")

    for voice, w in segments:
        parts.append(f"{voice}*{w / total_weight:.4f}")
    return "+".join(parts)


# ── SSML Generation ───────────────────────────────────────────────────────────

class Genre(Enum):
    FICTION     = "fiction"
    ROMANCE     = "romance"
    THRILLER    = "thriller"
    FANTASY     = "fantasy"
    NON_FICTION = "non_fiction"
    YA          = "young_adult"


@dataclass
class SSMLConfig:
    """Configuration for SSML generation in the Kokoro pipeline."""
    genre: Genre = Genre.FICTION

    # Pause durations in seconds
    pause_sentence: float  = 0.4
    pause_paragraph: float = 0.7
    pause_chapter: float   = 1.2

    # Emphasis toggles
    emphasis_chapter_titles: bool = True
    emphasis_dialogue: bool       = True
    emphasis_keywords: bool       = True


_GENRE_KEYWORDS: dict[Genre, list[str]] = {
    Genre.THRILLER:    ["suddenly", "dead", "kill", "murder", "blood",
                        "scream", "danger", "secret", "dark", "shadow"],
    Genre.ROMANCE:     ["love", "heart", "kiss", "forever", "beautiful",
                        "passion", "desire", "whisper", "tender"],
    Genre.FANTASY:     ["magic", "dragon", "power", "ancient", "spell",
                        "wizard", "kingdom", "quest", "destiny"],
    Genre.NON_FICTION: ["important", "remember", "key", "essential",
                        "critical", "fundamental", "significant"],
    Genre.YA:          ["dream", "hope", "friend", "brave", "discover"],
}

# Matches dialogue quoted with straight (") or smart (\u201c / \u201d) quotes.
# Smart quotes use DIFFERENT open/close code points so a backreference (?P=q)
# would never match — we use an explicit alternation instead.
_DIALOGUE_RE = re.compile(
    r'(?:'
    r'["\u201c](?P<content>[^"\u201c\u201d]+)["\u201d]'  # double: straight or curly
    r'|'
    r"['\u2018](?P<content2>[^'\u2018\u2019]+)['\u2019]"  # single: straight or curly
    r')'
)


class AudiobookSSMLProcessor:
    """Wraps pre-normalized text in SSML suitable for Kokoro TTS.

    Normalization (numbers, abbreviations, etc.) is expected to have already
    happened upstream.  This class adds structural SSML markup:
    pauses, sentence tags, dialogue prosody, and genre keyword emphasis.

    When *ai_enhanced* is True (and an AI client is configured) each chunk is
    sent to the AI model for richer, context-aware SSML generation.  The
    rule-based path acts as an automatic fallback on any AI failure.
    """

    def __init__(self, config: Optional[SSMLConfig] = None, ai_enhanced: bool = False):
        self.config = config or SSMLConfig()
        self.ai_enhanced = ai_enhanced
        self._keywords = _GENRE_KEYWORDS.get(self.config.genre, [])

    # ── public API ────────────────────────────────────────────────────────────

    def chapter_title_to_ssml(self, title: str) -> str:
        """Wrap a chapter title with emphasis and surrounding pauses."""
        if self.ai_enhanced:
            system = _SSML_AI_TITLE_SYSTEM.format(
                genre=self.config.genre.value,
                pause_c=self.config.pause_chapter,
            )
            result = _ai_generate_ssml(system, title)
            if result:
                return result
            log.debug("AI chapter-title SSML failed; using rule-based fallback")

        cfg = self.config
        return (
            f'<speak>'
            f'<break time="{cfg.pause_chapter}s"/>'
            f'<emphasis level="strong">{self._esc(title)}</emphasis>'
            f'<break time="{cfg.pause_chapter}s"/>'
            f'</speak>'
        )

    def text_to_ssml(self, text: str) -> str:
        """Convert a pre-normalized text chunk to SSML.

        When ai_enhanced=True, calls the AI model first and falls back to the
        rule-based implementation if the AI call fails or returns invalid output.
        """
        if self.ai_enhanced:
            system = _SSML_AI_SYSTEM.format(
                genre=self.config.genre.value,
                pause_s=self.config.pause_sentence,
                pause_p=self.config.pause_paragraph,
                pause_c=self.config.pause_chapter,
            )
            result = _ai_generate_ssml(system, text)
            if result:
                return result
            log.debug("AI SSML failed; using rule-based fallback")

        return self._rule_based_ssml(text)

    # ── rule-based implementation ─────────────────────────────────────────────

    def _rule_based_ssml(self, text: str) -> str:
        """Deterministic, regex-based SSML generation (no AI call)."""
        cfg = self.config
        paragraphs = [p.strip() for p in re.split(r'\n{2,}', text) if p.strip()]
        parts = ["<speak>"]
        for i, para in enumerate(paragraphs):
            parts.append(self._para_to_ssml(para))
            if i < len(paragraphs) - 1:
                parts.append(f'<break time="{cfg.pause_paragraph}s"/>')
        parts.append("</speak>")
        return "\n".join(parts)

    # ── internal helpers ──────────────────────────────────────────────────────

    def _para_to_ssml(self, para: str) -> str:
        cfg = self.config
        sentences = re.split(r'(?<=[.!?])\s+', para)
        out = []
        for sent in sentences:
            sent = sent.strip()
            if not sent:
                continue
            sent = self._process_dialogue(sent)
            if cfg.emphasis_keywords and self._keywords:
                sent = self._add_keyword_emphasis(sent)
            out.append(f'<s>{sent}</s><break time="{cfg.pause_sentence}s"/>')
        return "\n".join(out)

    def _process_dialogue(self, sent: str) -> str:
        if not self.config.emphasis_dialogue:
            return sent

        def _replace(m: re.Match) -> str:
            # group "content" = double-quoted, "content2" = single-quoted
            content = m.group("content") or m.group("content2") or ""
            low = content.lower()
            if any(w in low for w in ["!", "scream", "shout", "yell"]):
                return f'<prosody rate="fast" volume="loud">&#x201c;{self._esc(content)}&#x201d;</prosody>'
            elif any(w in low for w in ["whisper", "quiet", "soft"]):
                return f'<prosody rate="slow" volume="soft">&#x201c;{self._esc(content)}&#x201d;</prosody>'
            else:
                return f'<emphasis level="moderate">&#x201c;{self._esc(content)}&#x201d;</emphasis>'

        return _DIALOGUE_RE.sub(_replace, sent)

    def _add_keyword_emphasis(self, sent: str) -> str:
        for kw in self._keywords:
            sent = re.sub(
                rf'\b({re.escape(kw)})\b',
                r'<emphasis level="moderate">\1</emphasis>',
                sent,
                flags=re.IGNORECASE,
            )
        return sent

    @staticmethod
    def _esc(text: str) -> str:
        """Minimal XML escaping for SSML content."""
        return (text
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;"))


# ── AI-enhanced SSML generation ───────────────────────────────────────────────

_SSML_AI_SYSTEM = """\
You are an audiobook narration specialist that produces SSML markup for Kokoro TTS.

Task: Convert the supplied pre-normalized text into SSML that results in natural,
engaging audiobook narration. Do NOT alter any words — only add SSML tags.

Valid tags (Kokoro-compatible subset):
  <speak>            — root wrapper (required, exactly one)
  <s>…</s>           — sentence boundary
  <break time="Xs"/> — silence pause (X = float seconds, REQUIRED after every </s>)
  <emphasis level="moderate|strong|reduced"> — word/phrase stress
  <prosody rate="slow|fast|x-slow|x-fast" volume="soft|loud|medium"> — voice quality

Rules:
1. Wrap the entire output in a single <speak>…</speak> block.
2. Wrap each sentence in <s>…</s> and add <break time="{pause_s}s"/> immediately after every </s>.
3. Add <break time="{pause_p}s"/> between paragraphs.
4. Detect dialogue (text inside " " or \u201c \u201d):
   • whisper / soft / quiet → <prosody rate="slow" volume="soft">
   • shout / yell / scream / exclamation → <prosody rate="fast" volume="loud">
   • otherwise → <emphasis level="moderate">
5. Emphasize emotionally significant words appropriate to the genre.
6. Preserve all original words exactly — no paraphrasing.
7. Escape & → &amp;  < → &lt;  > → &gt; inside text nodes.
8. Return ONLY the SSML — no explanation, no code fences.

Genre: {genre}
Sentence pause: {pause_s}s   Paragraph pause: {pause_p}s   Chapter pause: {pause_c}s

--- EXAMPLE ---
Input:
  \u201cI need to leave,\u201d she whispered. He stared at her. \u201cNow!\u201d he shouted.

  The door slammed shut.

Output:
  <speak>
  <s><prosody rate="slow" volume="soft">\u201cI need to leave,\u201d</prosody> she whispered.</s><break time="{pause_s}s"/>
  <s>He stared at her.</s><break time="{pause_s}s"/>
  <s><prosody rate="fast" volume="loud">\u201cNow!\u201d</prosody> he shouted.</s><break time="{pause_s}s"/>
  <break time="{pause_p}s"/>
  <s>The door slammed shut.</s><break time="{pause_s}s"/>
  </speak>
--- END EXAMPLE ---
"""

_SSML_AI_TITLE_SYSTEM = """\
You are an audiobook narration specialist producing SSML for Kokoro TTS.

Task: Wrap the given chapter title in an SSML block that delivers it with emphasis
and appropriate surrounding pauses. Return ONLY the SSML — no explanation.

Output format (fill in pauses and title text):
<speak>
<break time="{pause_c}s"/>
<emphasis level="strong">TITLE HERE</emphasis>
<break time="{pause_c}s"/>
</speak>

Genre: {genre}   Chapter pause: {pause_c}s
Escape & → &amp;  < → &lt;  > → &gt; inside text nodes.
"""

_SSML_VALID_RE = re.compile(r"<speak[\s>]", re.IGNORECASE)
_SSML_BREAK_RE = re.compile(r"<break\b", re.IGNORECASE)


def _ai_generate_ssml(prompt_system: str, user_text: str) -> Optional[str]:
    """
    Call the AI model to generate SSML for *user_text*.

    Returns the SSML string on success, or None on any failure so the
    caller can fall back to rule-based generation.
    Metrics are recorded for every attempt.
    """
    if not AI_NORMALIZE:
        return None  # AI client not configured
    t0 = time.time()
    try:
        resp = _ai_client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {"role": "system", "content": prompt_system},
                {"role": "user",   "content": user_text},
            ],
            max_tokens=4096,
            temperature=0.2,
        )
        raw = (resp.choices[0].message.content or "").strip()
        elapsed = time.time() - t0

        # Strip accidental code fences the model may add despite instructions
        raw = re.sub(r"^```(?:xml|ssml)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw).strip()

        # Must contain a <speak> root
        if not _SSML_VALID_RE.search(raw):
            log.warning("AI SSML missing <speak> tag — falling back to rule-based")
            ORCH_SSML_AI_CALLS.labels(status="invalid").inc()
            ORCH_SSML_AI_SECS.observe(elapsed)
            return None

        # Must contain at least one <break> — if absent the model ignored the
        # pause instructions and sentences will run together without silence.
        if not _SSML_BREAK_RE.search(raw):
            log.warning("AI SSML missing <break> tags — falling back to rule-based")
            ORCH_SSML_AI_CALLS.labels(status="no_breaks").inc()
            ORCH_SSML_AI_SECS.observe(elapsed)
            return None

        ORCH_SSML_AI_CALLS.labels(status="success").inc()
        ORCH_AI_CALLS_TOTAL.labels(type="ssml", status="success").inc()
        ORCH_SSML_AI_SECS.observe(elapsed)
        ORCH_AI_DURATION_SECS.labels(type="ssml").observe(elapsed)
        return raw

    except Exception as exc:
        elapsed = time.time() - t0
        ORCH_SSML_AI_CALLS.labels(status="error").inc()
        ORCH_AI_CALLS_TOTAL.labels(type="ssml", status="error").inc()
        ORCH_SSML_AI_SECS.observe(elapsed)
        log.warning("AI SSML generation failed (%s) — falling back to rule-based", exc)
        return None


_GENRE_DETECT_SYSTEM = """\
You are a book genre classifier for an audiobook TTS pipeline.

Given a book title and a short excerpt from the opening, output EXACTLY one of
these genre labels — nothing else, no explanation, no punctuation:

  fiction | romance | thriller | fantasy | non_fiction | ya

Definitions:
  fiction     – general/literary fiction, historical fiction, mystery, horror,
                science fiction, contemporary fiction
  romance     – stories where the central plot is a romantic relationship
  thriller    – suspense, action, crime, spy, legal, political thrillers
  fantasy     – epic/high fantasy, urban fantasy, fairy tales, mythology
  non_fiction – biography, memoir, self-help, history, science, business
  ya          – Young Adult (teen protagonists, coming-of-age)

If genuinely ambiguous, pick the single best-matching label.
"""

_GENRE_LABEL_MAP: dict[str, Genre] = {
    "fiction":     Genre.FICTION,
    "romance":     Genre.ROMANCE,
    "thriller":    Genre.THRILLER,
    "fantasy":     Genre.FANTASY,
    "non_fiction": Genre.NON_FICTION,
    "non-fiction": Genre.NON_FICTION,
    "ya":          Genre.YA,
    "young_adult": Genre.YA,
}


def detect_book_genre(book_title: str, chapters: list) -> Genre:
    """Infer the book's genre from its title and opening text using the AI model.

    Samples up to 800 chars from the first non-trivial chapter body so the
    prompt stays small (≤ ~250 input tokens).

    Falls back to the KOKORO_GENRE env-var default on any failure.
    """
    fallback_genre = _GENRE_LABEL_MAP.get(KOKORO_GENRE.lower(), Genre.FICTION)

    if not AI_NORMALIZE or not KOKORO_GENRE_AUTO_DETECT:
        return fallback_genre

    # Build a compact sample: title + first chapter title + opening excerpt
    sample_lines = [f"Title: {book_title}"]
    for ch in chapters[:3]:
        ch_title = ch.get("title", "").strip()
        ch_text  = (ch.get("text") or "").strip()[:800]
        if ch_title:
            sample_lines.append(f"Chapter: {ch_title}")
        if ch_text:
            sample_lines.append(ch_text)
            break  # one chapter excerpt is enough

    sample = "\n".join(sample_lines)

    t0 = time.time()
    try:
        resp = _ai_client.chat.completions.create(
            model=OPENROUTER_MODEL,
            messages=[
                {"role": "system", "content": _GENRE_DETECT_SYSTEM},
                {"role": "user",   "content": sample},
            ],
            max_tokens=10,   # we only need one word back
            temperature=0.0,
        )
        raw_label = (resp.choices[0].message.content or "").strip().lower()
        genre = _GENRE_LABEL_MAP.get(raw_label)

        if genre is None:
            log.warning("Genre detection returned unknown label '%s' — using fallback '%s'",
                        raw_label, fallback_genre.value)
            ORCH_GENRE_DETECT.labels(genre="unknown", status="fallback").inc()
            ORCH_AI_CALLS_TOTAL.labels(type="genre_detect", status="fallback").inc()
            return fallback_genre

        elapsed = time.time() - t0
        log.info("  Genre detected: %s (%.2fs)", genre.value, elapsed)
        ORCH_GENRE_DETECT.labels(genre=genre.value, status="success").inc()
        ORCH_AI_CALLS_TOTAL.labels(type="genre_detect", status="success").inc()
        ORCH_AI_DURATION_SECS.labels(type="genre_detect").observe(elapsed)
        return genre

    except Exception as exc:
        log.warning("Genre detection failed (%s) — using fallback '%s'", exc, fallback_genre.value)
        ORCH_GENRE_DETECT.labels(genre=fallback_genre.value, status="error").inc()
        ORCH_AI_CALLS_TOTAL.labels(type="genre_detect", status="error").inc()
        return fallback_genre


def _build_ssml_processor(genre: Optional[Genre] = None) -> Optional[AudiobookSSMLProcessor]:
    """Return an SSMLProcessor configured from env vars (and optional detected genre).

    *genre* — pass the result of ``detect_book_genre()`` to use a per-book
    genre instead of the global KOKORO_GENRE default.  When None the env-var
    default is used.

    When KOKORO_SSML_AI_ENHANCED=true (and OPENROUTER_API_KEY is set) the
    processor will use the AI model for SSML markup; falls back to rule-based
    on any AI failure.
    """
    if not KOKORO_SSML_ENABLED:
        return None
    if genre is None:
        genre_map = {g.value: g for g in Genre}
        genre = genre_map.get(KOKORO_GENRE.lower(), Genre.FICTION)
    ai_enhanced = KOKORO_SSML_AI_ENHANCED and AI_NORMALIZE
    if ai_enhanced:
        log.info("SSML AI-enhanced mode enabled (model=%s, genre=%s)", OPENROUTER_MODEL, genre.value)
    return AudiobookSSMLProcessor(SSMLConfig(genre=genre), ai_enhanced=ai_enhanced)


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
ORCH_SSML_AI_CALLS    = Counter('pipeline_orchestrator_ssml_ai_calls_total',
                                'AI SSML generation calls', ['status'])
ORCH_SSML_AI_SECS     = Histogram('pipeline_orchestrator_ssml_ai_seconds',
                                  'AI SSML generation latency per chunk',
                                  buckets=[0.2, 0.5, 1, 2, 5, 10, 30])
ORCH_GENRE_DETECT     = Counter('pipeline_orchestrator_genre_detect_total',
                                'Genre auto-detection outcomes', ['genre', 'status'])

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
    "Dr.->Doctor, Mr.->Mister, Mrs.->Missus, Ms.->Miss, Prof.->Professor, "
    "vs.->versus, etc.->and so on, e.g.->for example, i.e.->that is, "
    "approx.->approximately, dept.->department, St.->Saint, "
    "aka->also known as, a.k.a.->also known as, "
    "asap->as soon as possible, a.s.a.p.->as soon as possible, "
    "fyi->for your information, f.y.i.->for your information, "
    "diy->do it yourself, rip->rest in peace, "
    "CEO->Chief Executive Officer, CFO->Chief Financial Officer, "
    "COO->Chief Operating Officer, CTO->Chief Technology Officer, "
    "US->United States, UK->United Kingdom, UN->United Nations, "
    "NASA->the National Aeronautics and Space Administration, "
    "FBI->the Federal Bureau of Investigation, "
    "CIA->the Central Intelligence Agency, "
    "NYC->New York City, LA->Los Angeles, DC->Washington DC, "
    "mph->miles per hour, km->kilometers, kg->kilograms, "
    "lb->pounds, ft->feet, yr->year, yrs->years, "
    "avg->average, max->maximum, min->minimum, "
    "no.->number, vol.->volume, ch.->chapter, p.->page, pp.->pages\n"
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
            temperature=0.0,
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

_ABBREV_MAP = {
    r"\baka\b":    "also known as",
    r"\ba\.k\.a\.": "also known as",
    r"\basap\b":   "as soon as possible",
    r"\bfyi\b":    "for your information",
    r"\bdiy\b":    "do it yourself",
    r"\brip\b":    "rest in peace",
    r"\bviz\b":    "namely",
    r"\bbtw\b":    "by the way",
    r"\bimo\b":    "in my opinion",
    r"\bimho\b":   "in my humble opinion",
    r"\betc\.\b":  "and so on",
    r"\be\.g\.\b": "for example",
    r"\bi\.e\.\b": "that is",
    r"\bvs\.\b":   "versus",
    r"\bDr\.\b":   "Doctor",
    r"\bMr\.\b":   "Mister",
    r"\bMrs\.\b":  "Missus",
    r"\bMs\.\b":   "Miss",
    r"\bProf\.\b": "Professor",
    r"\bSt\.\b":   "Saint",
    r"\bno\.\b":   "number",
    r"\bvol\.\b":  "volume",
    r"\bch\.\b":   "chapter",
    r"\bpp\.\b":   "pages",
    r"\bp\.\b":    "page",
}

def filter_text(text: str) -> str:
    """Remove URLs and expand common abbreviations for TTS clarity."""
    # Remove URLs
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # Expand abbreviations deterministically (runs even without AI key)
    for pattern, replacement in _ABBREV_MAP.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
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
            temperature=0.0,
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

    # Resolve voice blend formula and SSML processor for this job.
    # Per-job overrides take precedence over env-var defaults.
    voice_blend_raw = job.get("voice_blend", KOKORO_VOICE_BLEND)
    try:
        voice_formula = voice_blend_to_formula(voice_blend_raw) if voice_blend_raw else ""
    except ValueError as e:
        log.warning("Invalid KOKORO_VOICE_BLEND '%s': %s — using default voice", voice_blend_raw, e)
        voice_formula = ""

    tts_speed = float(job.get("speed", KOKORO_SPEED))

    ssml_enabled = job.get("ssml_enabled", KOKORO_SSML_ENABLED)
    # Genre and SSML processor are resolved after extraction so detect_book_genre
    # has access to the chapter list. Placeholders set here.
    ssml_proc = None

    if voice_formula:
        log.info("  Voice blend formula: %s", voice_formula)

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
        log.info("  Extracted %d chapters from %s", len(chapters), pdf_path.name)
        for i, ch in enumerate(chapters):
            title  = ch.get("title", f"Chapter {i+1}")
            chars  = len(ch.get("text", "").strip())
            tts_chunks = math.ceil(chars / CHUNK_SIZE_CHARS) if chars else 0
            log.info("    [%3d] %-50s  %7d chars  -> ~%d TTS chunk(s)",
                     i + 1, title[:50], chars, tts_chunks)

        # Auto-detect genre from title + opening text, then build the SSML processor.
        if ssml_enabled:
            detected_genre = detect_book_genre(pdf_path.stem, chapters)
            ssml_proc = _build_ssml_processor(genre=detected_genre)
            log.info("  SSML enabled (genre=%s, ai_enhanced=%s)",
                     detected_genre.value, ssml_proc.ai_enhanced if ssml_proc else False)

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
            is_first_part = part_idx == 0
            chunk_job = {
                "book_id": book_id,
                "chunk_idx": global_chunk_idx,
                "chapter_idx": ch_idx,
                "chapter_title": ch_title,
                "title": title,
                "text": c_text,
                "chunk_file": str(chunk_file),
                # Kokoro voice blend & SSML metadata
                "voice": voice_formula,
                "speed": tts_speed,
                "is_ssml": bool(ssml_proc),
                "is_chapter_start": is_first_part,
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

    # SSML conversion — wrap normalized text in SSML markup for Kokoro
    if ssml_proc and chapter_metadata:
        log.info("  Applying SSML markup to %d chunks...", total)
        for meta in chapter_metadata:
            is_chapter_start = meta.get("is_chapter_start", False)
            if is_chapter_start:
                # Prepend a chapter-title SSML block before the content
                title_ssml = ssml_proc.chapter_title_to_ssml(meta["chapter_title"])
                content_ssml = ssml_proc.text_to_ssml(meta["text"])
                # Strip the outer <speak> from content and merge into one document
                inner = content_ssml.replace("<speak>\n", "").replace("<speak>", "").replace("\n</speak>", "").replace("</speak>", "")
                merged = f"<speak>\n{title_ssml.replace('<speak>', '').replace('</speak>', '')}\n{inner}\n</speak>"
                meta["text"] = merged
            else:
                meta["text"] = ssml_proc.text_to_ssml(meta["text"])
            Path(meta["chunk_file"]).write_text(meta["text"], encoding="utf-8")
        log.info("  SSML markup applied.")

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

