"""ChatTTS Worker service
- Pulls chunk jobs from pipeline:tts
- Synthesizes audio directly using ChatTTS (local inference, no HTTP call)
- Writes MP3 to shared OUTPUT_DIR; spools locally if OUTPUT_DIR is offline
- Background thread flushes spool → OUTPUT_DIR when the mount recovers
- Exports Prometheus metrics on port 8004 (default)
"""

import io
import os
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [chattts] %(levelname)s [%(worker_id)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

WORKER_ID = os.environ.get("WORKER_ID", "chattts-local-1")

log = logging.LoggerAdapter(
    logging.getLogger(__name__),
    {"worker_id": WORKER_ID},
)

REDIS_URL          = os.environ.get("REDIS_URL",          "redis://localhost:6379")
OUTPUT_DIR         = Path(os.environ.get("OUTPUT_DIR",    "/data/outputs"))
SPOOL_DIR          = Path(os.environ.get("SPOOL_DIR",     "/spool"))
SMB_RETRY_INTERVAL = int(os.environ.get("SMB_RETRY_INTERVAL", "30"))

# ChatTTS configuration
# Speed: integer 1–9 where 5 is normal pace. Maps to the job's float speed multiplier.
CHATTTS_SPEED       = int(os.environ.get("CHATTTS_SPEED",       "5"))
CHATTTS_TEMPERATURE = float(os.environ.get("CHATTTS_TEMPERATURE", "0.3"))
CHATTTS_TOP_P       = float(os.environ.get("CHATTTS_TOP_P",      "0.7"))
CHATTTS_TOP_K       = int(os.environ.get("CHATTTS_TOP_K",        "20"))
# Integer seed for reproducible speaker embedding (same voice across all chunks).
CHATTTS_SPEAKER_SEED = int(os.environ.get("CHATTTS_SPEAKER_SEED", "42"))
# Max chars per synthesis segment — quality degrades on very long inputs.
CHATTTS_MAX_CHARS   = int(os.environ.get("CHATTTS_MAX_CHARS",   "300"))
CHATTTS_DEVICE      = os.environ.get(
    "CHATTTS_DEVICE",
    "cuda" if os.environ.get("USE_GPU", "false").lower() == "true" else "cpu",
)

# ChatTTS sample rate is always 24 kHz
CHATTTS_SAMPLE_RATE = 24000

QUEUE_TTS  = "pipeline:tts"
QUEUE_DONE = "pipeline:done"

# ── Prometheus Metrics ────────────────────────────────────────────────────────
JOBS_PROCESSED   = Counter("chattts_worker_jobs_total", "Total chunks processed", ["worker_id", "status"])
TTS_LATENCY      = Histogram("chattts_worker_tts_latency_seconds", "ChatTTS synthesis latency per segment", ["worker_id"])
REDIS_RECONNECTS = Counter("chattts_worker_redis_reconnects_total", "Redis reconnection count", ["worker_id"])
WORKER_HEARTBEAT = Gauge("chattts_worker_heartbeat_timestamp", "Last activity timestamp", ["worker_id"])
WORKER_STATUS    = Gauge("chattts_worker_status", "Worker state (0=Idle, 1=Processing, 2=Error)", ["worker_id"])
WORKER_LOGS      = Counter("chattts_worker_logs_total", "Warning/error log count", ["worker_id", "level"])
JOB_START_TIME   = Gauge("chattts_worker_job_start_timestamp_seconds", "Chunk start timestamp", ["worker_id"])
JOB_COMPLETION_TIME = Gauge("chattts_worker_job_completion_timestamp_seconds", "Chunk completion timestamp", ["worker_id"])
JOB_DURATION     = Histogram("chattts_worker_job_processing_duration_seconds", "End-to-end chunk processing time", ["worker_id"])
OUTPUT_WRITE_SECS = Histogram(
    "chattts_worker_output_write_seconds", "Time to write MP3 to destination",
    ["worker_id", "dest"], buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30],
)
SPOOL_FILES  = Gauge("chattts_worker_spool_files", "MP3 files waiting in local spool", ["worker_id"])
SPOOL_FLUSHED = Counter("chattts_worker_spool_flushed_total", "MP3 files flushed from spool to OUTPUT_DIR", ["worker_id"])

r = redis.from_url(REDIS_URL, decode_responses=True)

# ── ChatTTS engine (lazy-loaded on first use) ─────────────────────────────────
_chat       = None
_spk_emb    = None
_engine_lock = threading.Lock()


def get_engine():
    """Return a shared (chat, spk_emb) pair, loading the model on first call."""
    global _chat, _spk_emb
    if _chat is not None:
        return _chat, _spk_emb
    with _engine_lock:
        if _chat is not None:
            return _chat, _spk_emb
        log.info("Loading ChatTTS model on device '%s' ...", CHATTTS_DEVICE)
        import ChatTTS
        chat = ChatTTS.Chat()
        chat.load(device=CHATTTS_DEVICE, compile=False)

        # Fix a speaker embedding so every chunk in the book sounds the same.
        # torch.manual_seed makes sample_random_speaker deterministic.
        import torch
        torch.manual_seed(CHATTTS_SPEAKER_SEED)
        spk_emb = chat.sample_random_speaker()

        _chat, _spk_emb = chat, spk_emb
        log.info("ChatTTS model loaded (speaker seed=%d).", CHATTTS_SPEAKER_SEED)
    return _chat, _spk_emb


# ── Output / spool helpers ────────────────────────────────────────────────────

def _output_available() -> bool:
    try:
        probe = OUTPUT_DIR / ".probe"
        probe.touch()
        probe.unlink()
        return True
    except OSError:
        return False


def save_mp3(book_id: str, chunk_idx: int, mp3_bytes: bytes) -> str:
    rel = Path(book_id) / f"chunk_{chunk_idx:04d}.mp3"

    if _output_available():
        try:
            dest = OUTPUT_DIR / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            t0 = time.monotonic()
            with dest.open("wb") as f:
                f.write(mp3_bytes)
            OUTPUT_WRITE_SECS.labels(worker_id=WORKER_ID, dest="output").observe(time.monotonic() - t0)
            log.info("  Wrote MP3 → output (%d bytes)", len(mp3_bytes))
            return str(dest)
        except OSError as exc:
            log.warning("OUTPUT_DIR write failed (%s) — falling back to spool", exc)

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

        log.info("Spool flush: OUTPUT_DIR back online — flushing %d file(s)", len(spooled))
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
                log.warning("Spool flush failed for %s: %s — will retry", src.name, exc)
                break

        remaining = len(spooled) - flushed
        SPOOL_FILES.labels(worker_id=WORKER_ID).set(remaining)
        if flushed:
            log.info("Spool flush: moved %d file(s) to output (%d remaining)", flushed, remaining)


# ── Text segmentation ─────────────────────────────────────────────────────────

def _split_text(text: str, max_chars: int) -> list[str]:
    """
    Split text into segments of at most max_chars, breaking on sentence
    boundaries (period, exclamation, question mark, newline) where possible.
    """
    if len(text) <= max_chars:
        return [text]

    segments = []
    while text:
        if len(text) <= max_chars:
            segments.append(text)
            break
        boundary = -1
        for punct in (".", "!", "?", "\n"):
            pos = text.rfind(punct, 0, max_chars)
            if pos > boundary:
                boundary = pos
        if boundary <= 0:
            boundary = max_chars - 1
        segments.append(text[: boundary + 1].strip())
        text = text[boundary + 1 :].strip()

    return [s for s in segments if s]


def _speed_to_chattts(speed_multiplier: float) -> int:
    """
    Map a job speed float (0.5–2.0, 1.0=normal) to ChatTTS speed int (1–9, 5=normal).
    The base speed is CHATTTS_SPEED (env var); the job multiplier scales it.
    """
    scaled = CHATTTS_SPEED * speed_multiplier
    return max(1, min(9, round(scaled)))


# ── Synthesis ─────────────────────────────────────────────────────────────────

def synthesize_text(text: str, speed_multiplier: float = 1.0) -> tuple[bytes, float]:
    """
    Synthesize text using ChatTTS and return (mp3_bytes, audio_duration_seconds).
    Long texts are split into segments and concatenated.
    """
    import ChatTTS

    chat, spk_emb = get_engine()
    segments = _split_text(text, CHATTTS_MAX_CHARS)
    chattts_speed = _speed_to_chattts(speed_multiplier)

    log.info(
        "  Text: %d chars → %d segment(s), speed=%d",
        len(text), len(segments), chattts_speed,
    )

    params_infer = ChatTTS.Chat.InferCodeParams(
        spk_emb=spk_emb,
        temperature=CHATTTS_TEMPERATURE,
        top_P=CHATTTS_TOP_P,
        top_K=CHATTTS_TOP_K,
        speed=chattts_speed,
    )
    params_refine = ChatTTS.Chat.RefineTextParams(
        prompt="[oral_2][laugh_0][break_4]",
    )

    audio_parts: list[np.ndarray] = []

    for i, seg in enumerate(segments):
        t_seg = time.monotonic()
        with TTS_LATENCY.labels(worker_id=WORKER_ID).time():
            wavs = chat.infer(
                [seg],
                params_refine_text=params_refine,
                params_infer_code=params_infer,
            )
        wav = wavs[0]
        if wav.ndim > 1:
            wav = wav[0]
        seg_dur     = len(wav) / CHATTTS_SAMPLE_RATE
        seg_elapsed = time.monotonic() - t_seg
        rtf = seg_elapsed / seg_dur if seg_dur > 0 else 0.0
        log.info(
            "  Seg %d/%d: %.1fs audio  (%.1fs wall, RTF %.2f)",
            i + 1, len(segments), seg_dur, seg_elapsed, rtf,
        )
        audio_parts.append(wav)

    combined  = np.concatenate(audio_parts) if len(audio_parts) > 1 else audio_parts[0]
    total_dur = len(combined) / CHATTTS_SAMPLE_RATE

    # float32 numpy → WAV → MP3
    buf = io.BytesIO()
    sf.write(buf, combined, CHATTTS_SAMPLE_RATE, format="WAV", subtype="PCM_16")
    buf.seek(0)
    audio_seg = AudioSegment.from_wav(buf)
    mp3_buf = io.BytesIO()
    audio_seg.export(mp3_buf, format="mp3", bitrate="128k")
    return mp3_buf.getvalue(), total_dur


# ── Redis helpers ─────────────────────────────────────────────────────────────

def set_chunk_state(book_id: str, chunk_idx: int, **kwargs):
    r.hset(f"chunk:{book_id}:{chunk_idx}", mapping={k: str(v) for k, v in kwargs.items()})


def increment_done(book_id: str) -> tuple[int, int]:
    done  = r.hincrby(f"book:{book_id}", "done_chunks", 1)
    total = int(r.hget(f"book:{book_id}", "total_chunks") or 0)
    return done, total


# ── Job processor ─────────────────────────────────────────────────────────────

def process_job(raw: str):
    job        = json.loads(raw)
    book_id    = job["book_id"]
    chunk_idx  = int(job["chunk_idx"])
    total      = int(job["total"])
    title      = job["title"]
    chunk_file = Path(job["chunk_file"])
    speed      = float(job.get("speed", 1.0))

    start_time = time.time()
    JOB_START_TIME.labels(worker_id=WORKER_ID).set(start_time)

    text = job.get("text")
    log.info("┌ [%d/%d] %s  (book %s)", chunk_idx + 1, total, title[:60], book_id[:8])

    set_chunk_state(book_id, chunk_idx,
        status="processing",
        worker=WORKER_ID,
        started_at=time.time(),
    )

    if not text:
        try:
            text = chunk_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            JOBS_PROCESSED.labels(worker_id=WORKER_ID, status="failed").inc()
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
            log.error("Chunk file missing: %s", chunk_file)
            set_chunk_state(book_id, chunk_idx, status="error", error="chunk file missing")
            increment_done(book_id)
            return

    try:
        mp3_bytes, audio_dur = synthesize_text(text, speed_multiplier=speed)
        dest_path = save_mp3(book_id, chunk_idx, mp3_bytes)

        elapsed = time.time() - start_time
        log.info("└ chunk %d done  %.1fs audio  %d KB  %.1fs wall",
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
        log.error("└ chunk %d FAILED: %s", chunk_idx, exc)
        set_chunk_state(book_id, chunk_idx, status="error", error=str(exc))
        # Always continue — increment so Merger can finalize remaining chunks.

    done, total_chunks = increment_done(book_id)
    log.info("  Progress: %d/%d chunks complete", done, total_chunks)

    if done == total_chunks:
        book_title = r.hget(f"book:{book_id}", "title") or title
        r.lpush(QUEUE_DONE, json.dumps({
            "book_id": book_id,
            "title":   book_title,
            "total":   total_chunks,
            "out_dir": str(OUTPUT_DIR / book_id),
        }))
        r.hset(f"book:{book_id}", "status", "merging")
        log.info("  All chunks done — merge triggered for '%s'", book_title)


# ── Main loop ─────────────────────────────────────────────────────────────────

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    # Warm up model before first job to avoid cold-start latency.
    try:
        get_engine()
    except Exception as exc:
        log.warning("ChatTTS model pre-load failed (will retry on first job): %s", exc)

    flush_thread = threading.Thread(target=spool_flush_loop, daemon=True, name="spool-flush")
    flush_thread.start()

    prom_port = int(os.environ.get("PROMETHEUS_PORT", "8004"))
    log.info("Starting Prometheus metrics server on port %d", prom_port)
    start_http_server(prom_port)

    log.info("ChatTTS Worker %s ready — consuming %s", WORKER_ID, QUEUE_TTS)
    log.info("Device: %s | Speed: %d | Temp: %.2f | Speaker seed: %d | Max chars: %d",
             CHATTTS_DEVICE, CHATTTS_SPEED, CHATTTS_TEMPERATURE,
             CHATTTS_SPEAKER_SEED, CHATTTS_MAX_CHARS)
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
            log.warning("Lost connection to Redis — retrying in 5s (%s)", exc.__class__.__name__)
            REDIS_RECONNECTS.labels(worker_id=WORKER_ID).inc()
            time.sleep(5)
        except Exception as exc:
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(2)
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
            log.exception("Unexpected error in main loop: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    main()
