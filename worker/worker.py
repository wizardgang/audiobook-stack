"""
Worker service
- Runs on each node (local or remote)
- Pulls chunk jobs from pipeline:tts
- Calls the local abogen HTTP API with resilient backoff
- Writes MP3 to shared OUTPUT_DIR
- Exports Prometheus metrics on port 8000
"""

import os
import json
import time
import logging
import requests
import redis
from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram, Gauge

WORKER_ID = os.environ.get("WORKER_ID", "local-1")

log = logging.LoggerAdapter(
    logging.getLogger(__name__),
    {"worker_id": WORKER_ID}
)

REDIS_URL   = os.environ.get("REDIS_URL",   "redis://localhost:6379")
ABOGEN_HOST = os.environ.get("ABOGEN_HOST", "http://localhost:8808")
OUTPUT_DIR  = Path(os.environ.get("OUTPUT_DIR", "/data/outputs"))

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

r = redis.from_url(REDIS_URL, decode_responses=True)


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
        
        # Write output MP3
        out_dir = OUTPUT_DIR / book_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"chunk_{chunk_idx:04d}.mp3"
        out_path.write_bytes(mp3_bytes)

        set_chunk_state(book_id, chunk_idx,
            status="done",
            output=str(out_path),
            finished_at=time.time(),
        )
        JOBS_PROCESSED.labels(worker_id=WORKER_ID, status="success").inc()
        
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
    if done >= total_chunks:
        r.lpush(QUEUE_DONE, json.dumps({
            "book_id":  book_id,
            "title":    title,
            "total":    total_chunks,
            "out_dir":  str(OUTPUT_DIR / book_id),
        }))
        r.hset(f"book:{book_id}", "status", "merging")
        log.info("All chunks done for book %s — merge triggered", book_id[:8])


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Start Prometheus server
    prom_port = int(os.environ.get("PROMETHEUS_PORT", "8000"))
    log.info("Starting Prometheus metrics server on port %d", prom_port)
    start_http_server(prom_port)
    
    log.info("Worker %s ready, consuming %s from %s", WORKER_ID, QUEUE_TTS, ABOGEN_HOST)

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

