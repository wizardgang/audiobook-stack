"""F5-TTS Worker service
- Runs on each node (local or remote)
- Pulls chunk jobs from pipeline:tts
- Synthesizes audio directly using F5-TTS (zero-shot voice cloning)
- Writes MP3 to shared OUTPUT_DIR; spools locally if OUTPUT_DIR is offline
- Background thread flushes spool -> OUTPUT_DIR when the mount recovers
- Exports Prometheus metrics on port 8000

Key features:
  - pysbd sentence-boundary segmentation for natural phrase splits
  - Rolling-context reference: last ~3 s of generated audio becomes the
    next segment's ref, maintaining consistent voice/prosody across the book
  - Chapter/heading detection: resets to base ref + adds silence padding
    and fade-in/out for a "title card" effect
  - Tensor-mismatch fallback: splits failing segments recursively (//3)
    until the model succeeds or the minimum size floor is hit
"""

import io
import os
import re
import contextlib
import json
import tempfile
import time
import shutil
import logging
import threading
from typing import Any
import redis
import numpy as np
import soundfile as sf
from pathlib import Path
from pydub import AudioSegment
from prometheus_client import start_http_server, Counter, Histogram, Gauge

WORKER_ID = os.environ.get("WORKER_ID", "f5-local-1")


class _WorkerIdFilter(logging.Filter):
    """Inject worker_id into third-party log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "worker_id"):
            record.worker_id = "-"
        return True


_handler = logging.StreamHandler()
_handler.addFilter(_WorkerIdFilter())
_handler.setFormatter(logging.Formatter(
    fmt="%(asctime)s [f5-worker] %(levelname)s [%(worker_id)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(_handler)


log = logging.LoggerAdapter(
    logging.getLogger(__name__),
    {"worker_id": WORKER_ID}
)

REDIS_URL          = os.environ.get("REDIS_URL",           "redis://localhost:6379")
OUTPUT_DIR         = Path(os.environ.get("OUTPUT_DIR",     "/data/outputs"))
SPOOL_DIR          = Path(os.environ.get("SPOOL_DIR",      "/spool"))
SMB_RETRY_INTERVAL = int(os.environ.get("SMB_RETRY_INTERVAL", "30"))

# F5-TTS configuration
F5_MODEL       = os.environ.get("F5_MODEL",       "F5TTS_v1_Base")
F5_REF_AUDIO   = os.environ.get("F5_REF_AUDIO",   "/ref/reference.wav")
F5_REF_TEXT    = os.environ.get("F5_REF_TEXT",    "")
F5_SPEED       = float(os.environ.get("F5_SPEED", "0.9"))
F5_MIN_SPEED   = float(os.environ.get("F5_MIN_SPEED", "0.6"))
F5_MAX_SPEED   = float(os.environ.get("F5_MAX_SPEED", "1.3"))
F5_DEVICE      = os.environ.get("F5_DEVICE",      "cuda" if os.environ.get("USE_GPU", "false").lower() == "true" else "cpu")

# Max chars per synthesis call — keep ref_mel + gen_mel within the model's
# positional-encoding budget (≤200 chars leaves plenty of headroom with an 8 s ref).
F5_MAX_CHARS         = int(os.environ.get("F5_MAX_CHARS", "200"))
F5_PROGRESS_LOG_STEP = max(1, int(os.environ.get("F5_PROGRESS_LOG_STEP", "10")))

# Tensor-mismatch recovery: recursive segment splitting.
# MIN_CHARS = floor for sub-segment size; MAX_DEPTH = max recursion levels.
F5_RETRY_SPLIT_MIN_CHARS = int(os.environ.get("F5_RETRY_SPLIT_MIN_CHARS", "30"))
F5_RETRY_SPLIT_MAX_DEPTH = int(os.environ.get("F5_RETRY_SPLIT_MAX_DEPTH", "6"))

# Rolling context: after each segment, use the last N seconds of generated
# audio as the reference for the next segment to maintain voice consistency.
F5_ROLLING_CTX_ENABLED = os.environ.get("F5_ROLLING_CTX", "true").lower() == "true"
F5_ROLLING_CTX_SECS    = float(os.environ.get("F5_ROLLING_CTX_SECS", "3.0"))

# Chapter / heading padding: silence + crossfade applied to detected headings.
F5_CHAPTER_PAUSE_PRE_MS  = int(os.environ.get("F5_CHAPTER_PAUSE_PRE_MS",  "1500"))
F5_CHAPTER_PAUSE_POST_MS = int(os.environ.get("F5_CHAPTER_PAUSE_POST_MS", "2500"))
F5_CHAPTER_FADE_MS       = int(os.environ.get("F5_CHAPTER_FADE_MS",       "400"))

QUEUE_TTS    = "pipeline:tts"
QUEUE_DONE   = "pipeline:done"

#  Prometheus Metrics
JOBS_PROCESSED  = Counter('f5_worker_jobs_total', 'Total chunks processed', ['worker_id', 'status'])
TTS_LATENCY     = Histogram('f5_worker_tts_latency_seconds', 'F5-TTS synthesis latency per segment', ['worker_id'])
REDIS_RECONNECTS = Counter('f5_worker_redis_reconnects_total', 'Redis reconnection count', ['worker_id'])
WORKER_HEARTBEAT = Gauge('f5_worker_heartbeat_timestamp', 'Last activity timestamp', ['worker_id'])
WORKER_STATUS   = Gauge('f5_worker_status', 'Worker state (0=Idle, 1=Processing, 2=Error)', ['worker_id'])
WORKER_LOGS     = Counter('f5_worker_logs_total', 'Warning/error log count', ['worker_id', 'level'])
JOB_START_TIME  = Gauge('f5_worker_job_start_timestamp_seconds', 'Chunk start timestamp', ['worker_id'])
JOB_COMPLETION_TIME = Gauge('f5_worker_job_completion_timestamp_seconds', 'Chunk completion timestamp', ['worker_id'])
JOB_DURATION    = Histogram('f5_worker_job_processing_duration_seconds', 'End-to-end chunk processing time', ['worker_id'])
OUTPUT_WRITE_SECS = Histogram('f5_worker_output_write_seconds', 'Time to write MP3 to destination', ['worker_id', 'dest'],
                               buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30])
SPOOL_FILES     = Gauge('f5_worker_spool_files', 'MP3 files waiting in local spool', ['worker_id'])
SPOOL_FLUSHED   = Counter('f5_worker_spool_flushed_total', 'MP3 files flushed from spool to OUTPUT_DIR', ['worker_id'])

r = redis.from_url(REDIS_URL, decode_responses=True)

#  F5-TTS engine (lazy-loaded on first use)
_tts_engine = None
_tts_lock   = threading.Lock()


def get_tts_engine():
    """Return a shared F5TTS instance, initializing it on first call."""
    global _tts_engine
    if _tts_engine is not None:
        return _tts_engine
    with _tts_lock:
        if _tts_engine is not None:
            return _tts_engine
        log.info("Loading F5-TTS model '%s' on device '%s' ...", F5_MODEL, F5_DEVICE)
        from f5_tts.api import F5TTS
        _tts_engine = F5TTS(model=F5_MODEL, device=F5_DEVICE)
        log.info("F5-TTS model loaded.")
    return _tts_engine


#  pysbd sentence segmenter (lazy-loaded)
_pysbd_seg  = None
_pysbd_lock = threading.Lock()


def _get_pysbd():
    global _pysbd_seg
    if _pysbd_seg is not None:
        return _pysbd_seg
    with _pysbd_lock:
        if _pysbd_seg is None:
            import pysbd
            _pysbd_seg = pysbd.Segmenter(language="en", clean=True)
            log.info("pysbd segmenter initialised.")
    return _pysbd_seg


#  SMB / Output-dir availability probe

def _output_available() -> bool:
    try:
        probe = OUTPUT_DIR / ".probe"
        probe.touch()
        probe.unlink()
        return True
    except OSError:
        return False


def save_mp3(book_id: str, chunk_idx: int, mp3_bytes: bytes) -> str:
    """
    Write mp3_bytes to OUTPUT_DIR/{book_id}/chunk_NNNN.mp3.
    Falls back to SPOOL_DIR if OUTPUT_DIR is unavailable.
    Returns the destination path string.
    """
    rel = Path(book_id) / f"chunk_{chunk_idx:04d}.mp3"

    if _output_available():
        try:
            dest = OUTPUT_DIR / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            t0 = time.monotonic()
            with dest.open("wb") as f:
                f.write(mp3_bytes)
            OUTPUT_WRITE_SECS.labels(worker_id=WORKER_ID, dest="output").observe(time.monotonic() - t0)
            log.info("  Wrote MP3 -> output (%d bytes)", len(mp3_bytes))
            return str(dest)
        except OSError as exc:
            log.warning("OUTPUT_DIR write failed (%s) - falling back to spool", exc)

    spool_path = SPOOL_DIR / rel
    spool_path.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.monotonic()
    spool_path.write_bytes(mp3_bytes)
    OUTPUT_WRITE_SECS.labels(worker_id=WORKER_ID, dest="spool").observe(time.monotonic() - t0)
    depth = sum(1 for _ in SPOOL_DIR.rglob("*.mp3"))
    SPOOL_FILES.labels(worker_id=WORKER_ID).set(depth)
    log.warning("Spooled to local disk: %s (%d files pending flush)", spool_path.name, depth)
    return str(spool_path)


def spool_flush_loop():
    """Daemon thread: periodically flush spooled MP3s to OUTPUT_DIR."""
    log.info("Spool flush thread started (retry interval %ds)", SMB_RETRY_INTERVAL)
    while True:
        time.sleep(SMB_RETRY_INTERVAL)
        spooled = sorted(SPOOL_DIR.rglob("*.mp3"))
        if not spooled:
            SPOOL_FILES.labels(worker_id=WORKER_ID).set(0)
            continue
        if not _output_available():
            log.debug("Spool flush: OUTPUT_DIR still offline, %d files waiting", len(spooled))
            SPOOL_FILES.labels(worker_id=WORKER_ID).set(len(spooled))
            continue

        log.info("Spool flush: OUTPUT_DIR back online - flushing %d file(s)", len(spooled))
        flushed = 0
        for src in spooled:
            rel  = src.relative_to(SPOOL_DIR)
            dest = OUTPUT_DIR / rel
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                with src.open("rb") as fsrc, dest.open("wb") as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                src.unlink()
                flushed += 1
                SPOOL_FLUSHED.labels(worker_id=WORKER_ID).inc()
            except OSError as exc:
                log.warning("Spool flush failed for %s: %s - will retry", src.name, exc)
                break

        remaining = len(spooled) - flushed
        SPOOL_FILES.labels(worker_id=WORKER_ID).set(remaining)
        if flushed:
            log.info("Spool flush: moved %d file(s) to output (%d remaining)", flushed, remaining)


#  Text segmentation (pysbd)

def _split_text(text: str, max_chars: int) -> list[str]:
    """
    Split *text* into segments of at most *max_chars* characters using pysbd
    sentence-boundary detection.  Falls back to word-boundary hard-cut only
    when a single sentence itself exceeds *max_chars*.
    """
    if len(text) <= max_chars:
        return [text]

    sentences = _get_pysbd().segment(text)
    chunks: list[str] = []
    current = ""

    for sentence in sentences:
        candidate = (current + " " + sentence).strip() if current else sentence
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(sentence) > max_chars:
                # Single sentence too long — word-boundary hard split
                words = sentence.split()
                current = ""
                for word in words:
                    trial = (current + " " + word).strip() if current else word
                    if len(trial) <= max_chars:
                        current = trial
                    else:
                        if current:
                            chunks.append(current)
                        current = word
            else:
                current = sentence

    if current:
        chunks.append(current)

    return [c for c in chunks if c]


#  Rolling context reference

# Module-level state: updated after each synthesised segment.
# chapter/heading chunks clear this so the next body chunk starts fresh.
_rolling_ctx: dict[str, Any] = {"path": None, "text": ""}


def _update_rolling_ctx(wav: np.ndarray, sr: int, text: str) -> None:
    """
    Extract the last F5_ROLLING_CTX_SECS of *wav* and write it to a temp WAV.
    Estimate the corresponding spoken text by proportional char slice.
    """
    if not F5_ROLLING_CTX_ENABLED:
        return
    ctx_samples = int(F5_ROLLING_CTX_SECS * sr)
    ctx_wav = wav[-ctx_samples:] if len(wav) > ctx_samples else wav

    # Estimate the text portion spoken in the captured window
    ctx_ratio = len(ctx_wav) / max(1, len(wav))
    ctx_chars = max(20, int(len(text) * ctx_ratio))
    ctx_text = text[-ctx_chars:].strip()
    # Start at a word boundary so the ref text begins cleanly
    sp = ctx_text.find(" ")
    if 0 < sp < len(ctx_text) - 5:
        ctx_text = ctx_text[sp:].strip()
    if not ctx_text:
        ctx_text = text[-30:].strip()

    old_path = _rolling_ctx.get("path")
    try:
        fd, tmp_path = tempfile.mkstemp(suffix="_ctx.wav", prefix="f5_")
        os.close(fd)
        sf.write(tmp_path, ctx_wav, sr, subtype="PCM_16")
        _rolling_ctx["path"] = tmp_path
        _rolling_ctx["text"] = ctx_text
        if old_path and old_path != tmp_path and os.path.exists(old_path):
            os.unlink(old_path)
    except OSError as exc:
        log.warning("Rolling ctx write failed: %s", exc)


def _clear_rolling_ctx() -> None:
    """Delete the rolling-context temp file and reset state."""
    path = _rolling_ctx.get("path")
    if path and os.path.exists(path):
        try:
            os.unlink(path)
        except OSError:
            pass
    _rolling_ctx["path"] = None
    _rolling_ctx["text"] = ""


def _get_ref() -> tuple[str, str]:
    """Return (ref_audio_path, ref_text) from rolling ctx if valid, else base."""
    path = _rolling_ctx.get("path")
    text = _rolling_ctx.get("text", "")
    if F5_ROLLING_CTX_ENABLED and path and os.path.exists(path) and text:
        return path, text
    return F5_REF_AUDIO, F5_REF_TEXT


#  Chapter / heading detection and padding

_HEADING_RE = re.compile(
    r"^\s*(chapter|prologue|epilogue|part\b|book\b|section|interlude|"
    r"introduction|foreword|preface|afterword|appendix|notes?\b|"
    r"acknowledgements?|dedication)\b",
    re.IGNORECASE,
)


def _is_chapter_heading(text: str) -> bool:
    """
    Return True when the chunk looks like a chapter/section heading:
    short (≤200 chars) and starts with a known heading keyword.
    """
    stripped = text.strip()
    return len(stripped) <= 200 and bool(_HEADING_RE.match(stripped))


def _add_chapter_padding(mp3_bytes: bytes) -> bytes:
    """
    Wrap a heading MP3 with:
      - F5_CHAPTER_PAUSE_PRE_MS  ms of leading silence
      - fade-in of F5_CHAPTER_FADE_MS ms
      - fade-out of F5_CHAPTER_FADE_MS ms
      - F5_CHAPTER_PAUSE_POST_MS ms of trailing silence

    The fade-in / fade-out creates a smooth crossfade point at both
    boundaries when the audiobook player concatenates adjacent chunks.
    """
    seg = AudioSegment.from_mp3(io.BytesIO(mp3_bytes))
    seg = seg.fade_in(F5_CHAPTER_FADE_MS).fade_out(F5_CHAPTER_FADE_MS)
    silence_pre  = AudioSegment.silent(duration=F5_CHAPTER_PAUSE_PRE_MS,  frame_rate=seg.frame_rate)
    silence_post = AudioSegment.silent(duration=F5_CHAPTER_PAUSE_POST_MS, frame_rate=seg.frame_rate)
    padded = silence_pre + seg + silence_post
    buf = io.BytesIO()
    padded.export(buf, format="mp3", bitrate="128k")
    return buf.getvalue()


#  F5-TTS progress stub
class _NoopProgress:
    """Minimal stand-in for gr.Progress() - satisfies F5-TTS's progress.tqdm() calls."""
    def __call__(self, *args, **kwargs): pass
    def tqdm(self, iterable, *args, **kwargs): return iterable

class _LogProgress:
    """Log incremental inference progress without noisy tqdm bars."""

    def __init__(self, label: str):
        self.label = label
        self._last_pct = -1

    def __call__(self, *args, **kwargs):
        if not args:
            return
        value = args[0]
        if not isinstance(value, (int, float)):
            return
        pct = int(value * 100) if value <= 1 else int(value)
        pct = max(0, min(100, pct))
        if pct == 100 or (pct - self._last_pct) >= F5_PROGRESS_LOG_STEP:
            self._last_pct = pct
            log.info("    %s progress: %d%%", self.label, pct)

    def tqdm(self, iterable, *args, **kwargs):
        total = kwargs.get("total")
        if total is None:
            try:
                total = len(iterable)
            except Exception:
                total = None

        if total and total > 0:
            last_bucket = -1
            for idx, item in enumerate(iterable, start=1):
                pct = int((idx * 100) / total)
                bucket = pct // F5_PROGRESS_LOG_STEP
                if bucket > last_bucket or idx == total:
                    last_bucket = bucket
                    log.info("    %s progress: %d%%", self.label, pct)
                yield item
            return

        for idx, item in enumerate(iterable, start=1):
            if idx == 1 or idx % F5_PROGRESS_LOG_STEP == 0:
                log.info("    %s step: %d", self.label, idx)
            yield item


def _infer_segment_with_fallback(
    engine,
    ref_audio: str,
    ref_text: str,
    seg_text: str,
    speed: float,
    label: str,
    depth: int = 0,
) -> tuple[np.ndarray, int]:
    """
    Run one F5 infer call, and if it hits the known tensor-size mismatch error,
    recursively split the text into smaller parts (//3 each level) and retry.
    """
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            wav, sr, _ = engine.infer(
                ref_file=ref_audio,
                ref_text=ref_text,
                gen_text=seg_text,
                speed=speed,
                show_info=lambda *args, **kwargs: None,
                progress=_LogProgress(label),
            )
        return wav, sr
    except Exception as exc:
        msg = str(exc)
        is_tensor_mismatch = "Sizes of tensors must match" in msg
        can_retry_split = (
            is_tensor_mismatch
            and depth < F5_RETRY_SPLIT_MAX_DEPTH
            and len(seg_text) > F5_RETRY_SPLIT_MIN_CHARS
        )
        if not can_retry_split:
            raise

        next_max = max(F5_RETRY_SPLIT_MIN_CHARS, len(seg_text) // 3)
        sub_segments = _split_text(seg_text, next_max)
        if len(sub_segments) <= 1:
            raise

        log.warning(
            "    %s tensor mismatch at depth %d; retrying as %d sub-segments (max_chars=%d)",
            label,
            depth,
            len(sub_segments),
            next_max,
        )

        parts: list[np.ndarray] = []
        out_sr = None
        for j, sub in enumerate(sub_segments, start=1):
            child_label = f"{label}.{j}/{len(sub_segments)}"
            child_wav, child_sr = _infer_segment_with_fallback(
                engine=engine,
                ref_audio=ref_audio,
                ref_text=ref_text,
                seg_text=sub,
                speed=speed,
                label=child_label,
                depth=depth + 1,
            )
            if out_sr is None:
                out_sr = child_sr
            elif child_sr != out_sr:
                raise RuntimeError(
                    f"Sample rate mismatch while recovering segment: {out_sr} vs {child_sr}"
                )
            parts.append(child_wav)

        merged = np.concatenate(parts) if len(parts) > 1 else parts[0]
        return merged, out_sr



#  F5-TTS synthesis

def synthesize_text(
    text: str,
    speed_multiplier: float = 1.0,
    is_heading: bool = False,
) -> tuple[bytes, float]:
    """
    Synthesize *text* using F5-TTS and return (mp3_bytes, audio_duration_seconds).

    Segmentation:
      - Long texts are split via pysbd (sentence boundaries) and concatenated.

    Reference audio:
      - is_heading=True  → always uses the fixed base reference (F5_REF_AUDIO).
      - is_heading=False → uses the rolling-context ref if one exists, otherwise
        falls back to the base reference.

    Rolling context:
      After each successfully synthesised segment the last F5_ROLLING_CTX_SECS
      of generated audio is extracted and written to a temp WAV which becomes
      the reference for the *next* segment (and the next chunk).  This keeps
      the voice/prosody consistent throughout the book without drifting from
      a stale base reference.
    """
    if not Path(F5_REF_AUDIO).exists():
        raise FileNotFoundError(
            f"Base reference audio not found: {F5_REF_AUDIO}. "
            "Set F5_REF_AUDIO env var to a valid .wav file path."
        )

    engine   = get_tts_engine()
    segments = _split_text(text, F5_MAX_CHARS)
    word_count = len(text.split())
    effective_speed = max(F5_MIN_SPEED, min(F5_MAX_SPEED, F5_SPEED * speed_multiplier))
    log.info("  Effective speed: %.2f  |  heading: %s", effective_speed, is_heading)
    log.info("  Text: %d chars, %d words -> %d segment(s)", len(text), word_count, len(segments))

    # Headings always use the fixed base reference so that rolling context
    # (which may reflect the cadence of preceding body text) is not carried
    # into the distinct heading voice.
    if is_heading:
        ref_audio, ref_text = F5_REF_AUDIO, F5_REF_TEXT
        log.info("  ref: base (heading)")
    else:
        ref_audio, ref_text = _get_ref()
        src = "rolling-ctx" if ref_audio != F5_REF_AUDIO else "base-ref"
        log.info("  ref: %s (%d chars)", src, len(ref_text))

    # Sanity-check: rolling ctx file might have been deleted by OS cleanup
    if ref_audio != F5_REF_AUDIO and not Path(ref_audio).exists():
        log.warning("  Rolling ctx file gone, falling back to base ref")
        ref_audio, ref_text = F5_REF_AUDIO, F5_REF_TEXT

    audio_parts: list[np.ndarray] = []
    sample_rate = 24000  # F5-TTS default

    for i, seg in enumerate(segments):
        seg_words = len(seg.split())
        seg_pct = int(((i + 1) * 100) / len(segments))
        log.info("  Segment %d/%d (%d%%) starting ...", i + 1, len(segments), seg_pct)
        t_seg = time.monotonic()
        with TTS_LATENCY.labels(worker_id=WORKER_ID).time():
            wav, sr = _infer_segment_with_fallback(
                engine=engine,
                ref_audio=ref_audio,
                ref_text=ref_text,
                seg_text=seg,
                speed=effective_speed,
                label=f"seg {i + 1}/{len(segments)}",
            )
        seg_dur     = len(wav) / sr
        seg_elapsed = time.monotonic() - t_seg
        rtf = seg_elapsed / seg_dur if seg_dur > 0 else 0
        log.info("  Seg %d/%d: %d words -> %.1fs audio  (%.1fs wall, RTF %.2f)",
                 i + 1, len(segments), seg_words, seg_dur, seg_elapsed, rtf)
        sample_rate = sr
        audio_parts.append(wav)

        # Update rolling context for the next segment (not for headings)
        if not is_heading:
            _update_rolling_ctx(wav, sr, seg)
            ref_audio, ref_text = _get_ref()

    combined  = np.concatenate(audio_parts) if len(audio_parts) > 1 else audio_parts[0]
    total_dur = len(combined) / sample_rate

    # Convert numpy float32 WAV -> MP3 bytes via pydub
    buf = io.BytesIO()
    sf.write(buf, combined, sample_rate, format="WAV", subtype="PCM_16")
    buf.seek(0)
    audio_seg = AudioSegment.from_wav(buf)
    mp3_buf = io.BytesIO()
    audio_seg.export(mp3_buf, format="mp3", bitrate="128k")
    return mp3_buf.getvalue(), total_dur


#  Redis helpers

def set_chunk_state(book_id: str, chunk_idx: int, **kwargs):
    r.hset(f"chunk:{book_id}:{chunk_idx}", mapping={k: str(v) for k, v in kwargs.items()})


def increment_done(book_id: str) -> tuple[int, int]:
    """Atomically bump done_chunks; return (done, total)."""
    done  = r.hincrby(f"book:{book_id}", "done_chunks", 1)
    total = int(r.hget(f"book:{book_id}", "total_chunks") or 0)
    return done, total


#  Job processor

def process_job(raw: str):
    job       = json.loads(raw)
    book_id   = job["book_id"]
    chunk_idx = int(job["chunk_idx"])
    total     = int(job["total"])
    title     = job["title"]
    chunk_file = Path(job["chunk_file"])

    start_time = time.time()
    JOB_START_TIME.labels(worker_id=WORKER_ID).set(start_time)

    text = job.get("text")
    speed = float(job.get("speed", 1.0))
    chunk_missing = False
    log.info("|> [%d/%d] %s  (book %s)", chunk_idx + 1, total, title[:60], book_id[:8])

    set_chunk_state(book_id, chunk_idx,
        status="processing",
        worker=WORKER_ID,
        started_at=time.time(),
    )

    # Read chunk text (prefer embedded payload, fall back to file)
    if not text:
        try:
            text = chunk_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            JOBS_PROCESSED.labels(worker_id=WORKER_ID, status="failed").inc()
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
            log.error("Chunk file missing: %s", chunk_file)
            set_chunk_state(book_id, chunk_idx, status="error", error="chunk file missing")
            chunk_missing = True

    if chunk_missing:
        done, total_chunks = increment_done(book_id)
        pct = (done / total_chunks * 100.0) if total_chunks else 0.0
        log.info("  Progress: %d/%d chunks complete (%.1f%%)", done, total_chunks, pct)
        if done == total_chunks:
            book_title = r.hget(f"book:{book_id}", "title") or title
            r.lpush(QUEUE_DONE, json.dumps({
                "book_id": book_id,
                "title":   book_title,
                "total":   total_chunks,
                "out_dir": str(OUTPUT_DIR / book_id),
            }))
            r.hset(f"book:{book_id}", "status", "merging")
            log.info("  All chunks done - merge triggered for '%s'", book_title)
        return

    # Detect chapter / section headings
    heading = _is_chapter_heading(text)
    if heading:
        log.info("  Detected chapter heading — using base ref + pause padding")

    try:
        mp3_bytes, audio_dur = synthesize_text(text, speed_multiplier=speed, is_heading=heading)

        if heading:
            # Add silence + crossfade padding around the heading audio
            mp3_bytes = _add_chapter_padding(mp3_bytes)
            audio_dur += (F5_CHAPTER_PAUSE_PRE_MS + F5_CHAPTER_PAUSE_POST_MS) / 1000.0
            log.info(
                "  Chapter padding applied (pre=%dms, post=%dms, fade=%dms)",
                F5_CHAPTER_PAUSE_PRE_MS, F5_CHAPTER_PAUSE_POST_MS, F5_CHAPTER_FADE_MS,
            )
            # Reset rolling context so the next body chunk starts fresh from base ref
            _clear_rolling_ctx()
            log.info("  Rolling context cleared after heading")

        dest_path = save_mp3(book_id, chunk_idx, mp3_bytes)

        elapsed = time.time() - start_time
        log.info("<| chunk %d done  %.1fs audio  %d KB  %.1fs wall",
                 chunk_idx, audio_dur, len(mp3_bytes) // 1024, elapsed)

        set_chunk_state(book_id, chunk_idx,
            status="done",
            output=dest_path,
            finished_at=time.time(),
        )
        JOBS_PROCESSED.labels(worker_id=WORKER_ID, status="success").inc()

        completion_time = time.time()
        JOB_COMPLETION_TIME.labels(worker_id=WORKER_ID).set(completion_time)
        JOB_DURATION.labels(worker_id=WORKER_ID).observe(completion_time - start_time)

    except Exception as exc:
        JOBS_PROCESSED.labels(worker_id=WORKER_ID, status="failed").inc()
        WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
        log.error("<| chunk %d FAILED: %s", chunk_idx, exc)
        set_chunk_state(book_id, chunk_idx, status="error", error=str(exc))
        # Continue - always increment so Merger can finalize remaining chunks.

    done, total_chunks = increment_done(book_id)
    pct = (done / total_chunks * 100.0) if total_chunks else 0.0
    log.info("  Progress: %d/%d chunks complete (%.1f%%)", done, total_chunks, pct)

    if done == total_chunks:
        book_title = r.hget(f"book:{book_id}", "title") or title
        r.lpush(QUEUE_DONE, json.dumps({
            "book_id": book_id,
            "title":   book_title,
            "total":   total_chunks,
            "out_dir": str(OUTPUT_DIR / book_id),
        }))
        r.hset(f"book:{book_id}", "status", "merging")
        log.info("  All chunks done - merge triggered for '%s'", book_title)


#  Main loop

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    # Eagerly warm up the model so the first chunk doesn't pay load time
    try:
        get_tts_engine()
    except Exception as exc:
        log.warning("F5-TTS model pre-load failed (will retry on first job): %s", exc)

    # Warm up pysbd so the first chunk doesn't pay import time
    try:
        _get_pysbd()
    except Exception as exc:
        log.warning("pysbd init failed: %s", exc)

    flush_thread = threading.Thread(target=spool_flush_loop, daemon=True, name="spool-flush")
    flush_thread.start()

    prom_port = int(os.environ.get("PROMETHEUS_PORT", "8000"))
    log.info("Starting Prometheus metrics server on port %d", prom_port)
    start_http_server(prom_port)

    log.info("F5-TTS Worker %s ready - consuming %s", WORKER_ID, QUEUE_TTS)
    log.info(
        "Model: %s | Device: %s | Ref audio: %s | Base speed: %.2f (min %.2f / max %.2f) | Progress step: %d%%",
        F5_MODEL,
        F5_DEVICE,
        F5_REF_AUDIO,
        F5_SPEED,
        F5_MIN_SPEED,
        F5_MAX_SPEED,
        F5_PROGRESS_LOG_STEP,
    )
    log.info(
        "Rolling ctx: %s (%.1fs window) | Chapter pause: pre=%dms post=%dms fade=%dms",
        "enabled" if F5_ROLLING_CTX_ENABLED else "disabled",
        F5_ROLLING_CTX_SECS,
        F5_CHAPTER_PAUSE_PRE_MS,
        F5_CHAPTER_PAUSE_POST_MS,
        F5_CHAPTER_FADE_MS,
    )
    log.info("Output dir: %s | Spool dir: %s | Flush interval: %ds",
             OUTPUT_DIR, SPOOL_DIR, SMB_RETRY_INTERVAL)

    while True:
        try:
            if r.get("pipeline:state") == "paused":
                WORKER_STATUS.labels(worker_id=WORKER_ID).set(0)
                time.sleep(5)
                continue

            WORKER_STATUS.labels(worker_id=WORKER_ID).set(0)
            result = r.brpop(QUEUE_TTS, timeout=5)
            WORKER_HEARTBEAT.labels(worker_id=WORKER_ID).set_to_current_time()

            try:
                r.hset("worker:heartbeats", WORKER_ID, int(time.time()))
            except Exception:
                pass

            if result is None:
                continue

            _, raw = result
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(1)
            process_job(raw)

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(2)
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="warning").inc()
            log.warning("Lost connection to Redis - retrying in 5s (%s)", exc.__class__.__name__)
            REDIS_RECONNECTS.labels(worker_id=WORKER_ID).inc()
            time.sleep(5)
        except Exception as exc:
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(2)
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
            log.exception("Unexpected error in main loop: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    main()
