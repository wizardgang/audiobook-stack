"""F5-TTS Worker service
- Runs on each node (local or remote)
- Pulls chunk jobs from pipeline:tts
- Synthesizes audio directly using F5-TTS (zero-shot voice cloning)
- Writes MP3 to shared OUTPUT_DIR; spools locally if OUTPUT_DIR is offline
- Background thread flushes spool -> OUTPUT_DIR when the mount recovers
- Exports Prometheus metrics on port 8000
"""

import io
import os
import contextlib
import json
import time
import shutil
import logging
import threading
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

# Max chars per synthesis call - F5-TTS degrades on very long inputs.
# Keep this low enough that ref_mel + gen_mel stays within the model's
# positional-encoding budget (rule of thumb: ≤400 chars per segment).
F5_MAX_CHARS   = int(os.environ.get("F5_MAX_CHARS", "400"))
F5_PROGRESS_LOG_STEP = max(1, int(os.environ.get("F5_PROGRESS_LOG_STEP", "10")))
# Recovery for tensor-size mismatch failures (typically: reference audio too long).
# MIN_CHARS is the floor for sub-segment size; MAX_DEPTH is how many times to split.
F5_RETRY_SPLIT_MIN_CHARS = int(os.environ.get("F5_RETRY_SPLIT_MIN_CHARS", "30"))
F5_RETRY_SPLIT_MAX_DEPTH = int(os.environ.get("F5_RETRY_SPLIT_MAX_DEPTH", "6"))

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


#  Text segmentation 

def _split_text(text: str, max_chars: int) -> list[str]:
    """
    Split text into segments of at most max_chars characters, breaking on
    sentence boundaries (period, exclamation, question mark) where possible.
    """
    if len(text) <= max_chars:
        return [text]

    segments = []
    while text:
        if len(text) <= max_chars:
            segments.append(text)
            break
        # Find a good break point within the limit
        boundary = -1
        for punct in ('.', '!', '?', '\n'):
            pos = text.rfind(punct, 0, max_chars)
            if pos > boundary:
                boundary = pos
        if boundary <= 0:
            # No sentence boundary - hard-cut at max_chars
            boundary = max_chars - 1
        segments.append(text[:boundary + 1].strip())
        text = text[boundary + 1:].strip()

    return [s for s in segments if s]


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
    recursively split the text into smaller parts and retry.
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

def synthesize_text(text: str, speed_multiplier: float = 1.0) -> tuple[bytes, float]:
    """
    Synthesize text using F5-TTS and return (mp3_bytes, audio_duration_seconds).
    Long texts are split into segments and concatenated.
    """
    ref_audio = F5_REF_AUDIO
    ref_text  = F5_REF_TEXT

    if not Path(ref_audio).exists():
        raise FileNotFoundError(
            f"Reference audio not found: {ref_audio}. "
            "Set F5_REF_AUDIO env var to a valid .wav file path."
        )

    engine   = get_tts_engine()
    segments = _split_text(text, F5_MAX_CHARS)
    word_count = len(text.split())
    effective_speed = max(F5_MIN_SPEED, min(F5_MAX_SPEED, F5_SPEED * speed_multiplier))
    log.info("  Effective speed: %.2f", effective_speed)
    log.info("  Text: %d chars, %d words -> %d segment(s)", len(text), word_count, len(segments))

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
        seg_dur   = len(wav) / sr
        seg_elapsed = time.monotonic() - t_seg
        rtf = seg_elapsed / seg_dur if seg_dur > 0 else 0
        log.info("  Seg %d/%d: %d words -> %.1fs audio  (%.1fs wall, RTF %.2f)",
                 i + 1, len(segments), seg_words, seg_dur, seg_elapsed, rtf)
        sample_rate = sr
        audio_parts.append(wav)

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

    try:
        mp3_bytes, audio_dur = synthesize_text(text, speed_multiplier=speed)
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

