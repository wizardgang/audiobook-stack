"""
Worker service
- Runs on each node (local or remote)
- Pulls chunk jobs from pipeline:tts
- Calls the local abogen HTTP API with resilient backoff
- Writes MP3 to shared OUTPUT_DIR; spools locally if OUTPUT_DIR is offline
- Background thread flushes spool → OUTPUT_DIR when the mount recovers
- Exports Prometheus metrics on port 8000
"""

import os
import json
import time
import shutil
import logging
import threading
import requests
import redis
from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram, Gauge

WORKER_ID = os.environ.get("WORKER_ID", "local-1")

log = logging.LoggerAdapter(
    logging.getLogger(__name__),
    {"worker_id": WORKER_ID}
)

REDIS_URL          = os.environ.get("REDIS_URL",          "redis://localhost:6379")
ABOGEN_HOST        = os.environ.get("ABOGEN_HOST",        "http://localhost:8808")
OUTPUT_DIR         = Path(os.environ.get("OUTPUT_DIR",    "/data/outputs"))
SPOOL_DIR          = Path(os.environ.get("SPOOL_DIR",     "/spool"))
SMB_RETRY_INTERVAL = int(os.environ.get("SMB_RETRY_INTERVAL", "30"))

# How long to wait for abogen to finish a chunk (large chapters may take time)
TTS_TIMEOUT_S = int(os.environ.get("TTS_TIMEOUT_S", "600"))

QUEUE_TTS    = "pipeline:tts"
QUEUE_DONE   = "pipeline:done"

# ── Prometheus Metrics ───────────────────────────────────────────────────────
JOBS_PROCESSED = Counter('abogen_worker_jobs_total', 'Total chunks processed by this worker', ['worker_id', 'status'])
API_LATENCY = Histogram('abogen_worker_api_latency_seconds', 'Latency of the Abogen TTS API calls', ['worker_id'])
REDIS_RECONNECTS = Counter('abogen_worker_redis_reconnects_total', 'Number of times Redis connection was lost and restored', ['worker_id'])
WORKER_HEARTBEAT = Gauge('abogen_worker_heartbeat_timestamp', 'Last successful activity timestamp', ['worker_id'])
WORKER_STATUS = Gauge('abogen_worker_status', 'Worker state (0=Idle, 1=Processing, 2=Error)', ['worker_id'])
WORKER_LOGS = Counter('abogen_worker_logs_total', 'Number of warning and error logs generated', ['worker_id', 'level'])
JOB_START_TIME = Gauge('abogen_worker_job_start_timestamp_seconds', 'Timestamp when the current or last chunk started processing', ['worker_id'])
JOB_COMPLETION_TIME = Gauge('abogen_worker_job_completion_timestamp_seconds', 'Timestamp when the last chunk completed processing', ['worker_id'])
JOB_DURATION = Histogram('abogen_worker_job_processing_duration_seconds', 'Total time taken to process a chunk from start to finish', ['worker_id'])
OUTPUT_WRITE_SECS = Histogram('abogen_worker_output_write_seconds', 'Time to write MP3 to its final destination', ['worker_id', 'dest'], buckets=[0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30])
SPOOL_FILES       = Gauge('abogen_worker_spool_files', 'Number of MP3 files waiting in the local spool', ['worker_id'])
SPOOL_FLUSHED     = Counter('abogen_worker_spool_flushed_total', 'MP3 files successfully flushed from spool to OUTPUT_DIR', ['worker_id'])

r = redis.from_url(REDIS_URL, decode_responses=True)


# ── SMB / Output-dir availability probe ──────────────────────────────────────

def _output_available() -> bool:
    """Return True if OUTPUT_DIR is mounted and writable right now."""
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

    Falls back to SPOOL_DIR if OUTPUT_DIR is unavailable for any reason
    (mount offline, permission denied, network error, etc.).
    The background flush thread will deliver spooled files later.

    Returns the destination path string (output or spool).
    """
    rel = Path(book_id) / f"chunk_{chunk_idx:04d}.mp3"

    # Try primary destination — catch ANY OSError (permission, network, stale mount)
    if _output_available():
        try:
            dest = OUTPUT_DIR / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            t0 = time.monotonic()
            # Open explicitly to avoid any metadata-setting wrappers on SMB mounts
            with dest.open("wb") as f:
                f.write(mp3_bytes)
            OUTPUT_WRITE_SECS.labels(worker_id=WORKER_ID, dest="output").observe(time.monotonic() - t0)
            log.info("  Wrote MP3 → output (%d bytes)", len(mp3_bytes))
            return str(dest)
        except OSError as exc:
            log.warning("OUTPUT_DIR write failed (%s) — falling back to spool", exc)

    # OUTPUT_DIR unavailable — spool to local disk
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
    """
    Daemon thread: every SMB_RETRY_INTERVAL seconds, attempt to flush any
    MP3 files sitting in SPOOL_DIR to OUTPUT_DIR.
    The thread never exits — it is started once at worker startup.
    """
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
            # Reconstruct relative path: SPOOL_DIR / book_id / chunk_NNNN.mp3
            rel = src.relative_to(SPOOL_DIR)
            dest = OUTPUT_DIR / rel
            try:
                dest.parent.mkdir(parents=True, exist_ok=True)
                # Use copyfileobj instead of shutil.move/copy2 — copy2 calls
                # os.utime/os.chmod on the destination which SMB rejects (EPERM)
                # even when plain writes succeed.
                with src.open("rb") as fsrc, dest.open("wb") as fdst:
                    shutil.copyfileobj(fsrc, fdst)
                src.unlink()
                flushed += 1
                SPOOL_FLUSHED.labels(worker_id=WORKER_ID).inc()
            except OSError as exc:
                log.warning("Spool flush failed for %s: %s — will retry", src.name, exc)
                break  # mount went away again; stop and wait for next interval

        remaining = len(spooled) - flushed
        SPOOL_FILES.labels(worker_id=WORKER_ID).set(remaining)
        if flushed:
            log.info("Spool flush: moved %d file(s) to output (%d remaining)", flushed, remaining)


def set_chunk_state(book_id: str, chunk_idx: int, **kwargs):
    r.hset(f"chunk:{book_id}:{chunk_idx}", mapping={k: str(v) for k, v in kwargs.items()})


def increment_done(book_id: str) -> tuple[int, int]:
    """Atomically bump done_chunks; return (done, total)."""
    done  = r.hincrby(f"book:{book_id}", "done_chunks", 1)
    total = int(r.hget(f"book:{book_id}", "total_chunks") or 0)
    return done, total


def call_abogen(text: str, title: str, chunk_idx: int) -> bytes:
    """
    POST text to the abogen /api/generate endpoint with infinite resilient backoff.
    Returns raw MP3 bytes.
    """
    payload = {
        "text":   text,
        "title":  f"{title} - part {chunk_idx:04d}",
        "format": "mp3",
        "use_gpu": os.environ.get("USE_GPU", "false").lower() == "true",
    }
    
    retry_delay = 5
    max_retries = 5
    attempts = 0
    while True:
        attempts += 1
        with API_LATENCY.labels(worker_id=WORKER_ID).time():
            try:
                resp = requests.post(
                    f"{ABOGEN_HOST}/api/generate",
                    json=payload,
                    timeout=TTS_TIMEOUT_S,
                    stream=True,
                )
                resp.raise_for_status()
                return resp.content
            except requests.exceptions.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else "?"
                error_body = exc.response.text if exc.response is not None else str(exc)
                
                if exc.response is not None and 400 <= exc.response.status_code < 500:
                    # Don't retry client formatting errors
                    raise Exception(f"Fatal Client Error {status_code}: {error_body}")
                
                if attempts >= max_retries:
                    WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
                    raise Exception(f"Max retries exceeded! Abogen API Error {status_code}: {error_body}")
                    
                WORKER_LOGS.labels(worker_id=WORKER_ID, level="warning").inc()
                log.warning("Abogen HTTP Error (%s) - %s. Retrying in %ds...", status_code, error_body, retry_delay)
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)
            except requests.exceptions.RequestException as exc:
                if attempts >= max_retries:
                    WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
                    raise Exception(f"Max retries exceeded! Abogen Connection Drop: {exc}")
                    
                WORKER_LOGS.labels(worker_id=WORKER_ID, level="warning").inc()
                log.warning("Abogen Connection Drop (%s: %s). Retrying in %ds...", exc.__class__.__name__, str(exc), retry_delay)
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60)


def process_job(raw: str):
    job       = json.loads(raw)
    book_id   = job["book_id"]
    chunk_idx = int(job["chunk_idx"])
    total     = int(job["total"])
    title     = job["title"]
    chunk_file = Path(job["chunk_file"])
    
    start_time = time.time()
    JOB_START_TIME.labels(worker_id=WORKER_ID).set(start_time)
    
    # Extract the text directly from the Redis payload (bypassing file syncing)
    # If using older orchestrated jobs, fallback securely to disk
    text = job.get("text")

    log.info("Book %s chunk %d/%d", book_id[:8], chunk_idx + 1, total)

    set_chunk_state(book_id, chunk_idx,
        status="processing",
        worker=WORKER_ID,
        started_at=time.time(),
    )

    # Read chunk text
    if not text:
        try:
            text = chunk_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            JOBS_PROCESSED.labels(worker_id=WORKER_ID, status="failed").inc()
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
            log.error("Chunk file missing: %s", chunk_file)
            set_chunk_state(book_id, chunk_idx, status="error", error="chunk file missing")
            return

    # Call abogen
    try:
        mp3_bytes = call_abogen(text, title, chunk_idx)

        # Write output MP3 (falls back to local spool if OUTPUT_DIR is offline)
        dest_path = save_mp3(book_id, chunk_idx, mp3_bytes)

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
        log.error("abogen failed permanently for chunk %d: %s", chunk_idx, exc)
        set_chunk_state(book_id, chunk_idx, status="error", error=str(exc))
        # Do NOT return here! The flow must continue so the book's total chunks increments.

    # Atomically increment regardless of failure so the Merger can finalize the remaining successful chunks!
    done, total_chunks = increment_done(book_id)
    log.info("  Chunk progress: %d/%d", done, total_chunks)

    # Notify merger when all chunks are complete
    if done == total_chunks:
        book_title = r.hget(f"book:{book_id}", "title") or title
        r.lpush(QUEUE_DONE, json.dumps({
            "book_id":  book_id,
            "title":    book_title,
            "total":    total_chunks,
            "out_dir":  str(OUTPUT_DIR / book_id),
        }))
        r.hset(f"book:{book_id}", "status", "merging")
        log.info("All chunks done for book %s — merge triggered", book_id[:8])


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SPOOL_DIR.mkdir(parents=True, exist_ok=True)

    # Start background spool-flush thread
    flush_thread = threading.Thread(target=spool_flush_loop, daemon=True, name="spool-flush")
    flush_thread.start()

    # Start Prometheus server
    prom_port = int(os.environ.get("PROMETHEUS_PORT", "8000"))
    log.info("Starting Prometheus metrics server on port %d", prom_port)
    start_http_server(prom_port)

    log.info("Worker %s ready, consuming %s from %s", WORKER_ID, QUEUE_TTS, ABOGEN_HOST)
    log.info("Output dir: %s | Spool dir: %s | Flush interval: %ds",
             OUTPUT_DIR, SPOOL_DIR, SMB_RETRY_INTERVAL)

    while True:
        try:
            # Check global pipeline state to enable pausing
            if r.get("pipeline:state") == "paused":
                WORKER_STATUS.labels(worker_id=WORKER_ID).set(0) # Idle
                time.sleep(5)
                continue
                
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(0) # Idle
            result = r.brpop(QUEUE_TTS, timeout=5)
            WORKER_HEARTBEAT.labels(worker_id=WORKER_ID).set_to_current_time()
            
            # Broadcast heartbeat presence for Redis observers
            try:
                r.hset("worker:heartbeats", WORKER_ID, int(time.time()))
            except Exception:
                pass
                
            if result is None:
                continue
                
            _, raw = result
            
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(1) # Processing
            process_job(raw)
            
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(2) # Error
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="warning").inc()
            log.warning("Lost connection to Redis! Retrying in 5 seconds... (%s)", exc.__class__.__name__)
            REDIS_RECONNECTS.labels(worker_id=WORKER_ID).inc()
            time.sleep(5)
        except Exception as exc:
            WORKER_STATUS.labels(worker_id=WORKER_ID).set(2) # Error
            WORKER_LOGS.labels(worker_id=WORKER_ID, level="error").inc()
            log.exception("Unexpected error in worker main loop: %s", exc)
            time.sleep(1)


if __name__ == "__main__":
    main()

