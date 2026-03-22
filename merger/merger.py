import os
import shutil
import logging
import subprocess
import redis
import json
import time
from pathlib import Path
from prometheus_client import start_http_server, Counter, Histogram, Gauge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [merger] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL     = os.environ.get("REDIS_URL",      "redis://localhost:6379")
OUTPUT_DIR    = Path(os.environ.get("OUTPUT_DIR",    "/outputs"))
AUDIOBOOK_DIR = Path(os.environ.get("AUDIOBOOK_DIR", "/audiobooks"))
CHUNKS_DIR    = Path(os.environ.get("CHUNKS_DIR",    "/chunks"))
KEEP_CHUNKS   = os.environ.get("KEEP_CHUNKS", "false").lower() == "true"

# Output format: "mp3" or "m4b"
OUTPUT_FORMAT = os.environ.get("OUTPUT_FORMAT", "m4b")

QUEUE_DONE = "pipeline:done"

# ── Prometheus Metrics ────────────────────────────────────────────────────────
MERGER_TOTAL          = Counter('pipeline_merger_merges_total',
                                'Merge jobs completed', ['status'])
MERGER_DURATION       = Histogram('pipeline_merger_duration_seconds',
                                  'Total merge job duration end-to-end',
                                  buckets=[30, 60, 120, 300, 600, 1200, 1800, 3600])
MERGER_FFMPEG_SECS    = Histogram('pipeline_merger_ffmpeg_seconds',
                                  'ffmpeg encode/concat wall time',
                                  buckets=[5, 15, 30, 60, 120, 300, 600, 1200])
MERGER_CHUNK_WAIT     = Histogram('pipeline_merger_chunk_wait_seconds',
                                  'Time spent waiting for chunk files to arrive (NFS lag)',
                                  buckets=[0, 5, 15, 30, 60, 120, 300, 600])
MERGER_CHUNKS_TOTAL   = Counter('pipeline_merger_chunks_merged_total',
                                'Total individual MP3 chunks merged into audiobooks')
MERGER_QUEUE_DEPTH    = Gauge('pipeline_merger_queue_depth',
                               'Current merge queue depth')

r = redis.from_url(REDIS_URL, decode_responses=True)


def set_book_state(book_id: str, **kwargs):
    r.hset(f"book:{book_id}", mapping={k: str(v) for k, v in kwargs.items()})


def _load_chunk_lookup(meta_dir: Path) -> dict[int, dict]:
    """Load meta.json from the chunks directory into a dict keyed by chunk_idx."""
    meta_json_file = meta_dir / "meta.json"
    if not meta_json_file.exists():
        log.warning("meta.json not found in %s — chapter grouping will be disabled", meta_dir)
        return {}
    return {e["chunk_idx"]: e for e in json.loads(meta_json_file.read_text(encoding="utf-8"))}


def build_concat_list(chunk_dir: Path, total: int, meta_dir: Path) -> Path:
    """Write an ffmpeg concat file listing chunks sorted by (chapter_idx, chunk_idx)."""
    concat_file = chunk_dir / "concat.txt"
    chunk_lookup = _load_chunk_lookup(meta_dir)

    ordered = sorted(
        (idx for idx in range(total)
         if (chunk_dir / f"chunk_{idx:04d}.mp3").exists()),
        key=lambda idx: (chunk_lookup.get(idx, {}).get("chapter_idx", idx), idx),
    )

    lines = []
    for idx in ordered:
        chunk = chunk_dir / f"chunk_{idx:04d}.mp3"
        escaped = str(chunk).replace("'", "'\\''")
        lines.append(f"file '{escaped}'")
        if idx not in {e["chunk_idx"] for e in chunk_lookup.values()}:
            log.warning("Missing chunk %d, skipping in concatenation...", idx)

    concat_file.write_text("\n".join(lines), encoding="utf-8")
    return concat_file


def get_duration_ms(filepath: Path) -> int:
    cmd = [
        "ffprobe", "-v", "error", "-show_entries", "format=duration", 
        "-of", "default=noprint_wrappers=1:nokey=1", str(filepath)
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return int(float(res.stdout.strip()) * 1000)
    except Exception as e:
        log.warning("Could not get duration for %s: %s", filepath, e)
        return 0


def build_ffmetadata(chunk_dir: Path, total: int, title: str, meta_dir: Path) -> Path:
    """Build an FFMETADATA file with one chapter marker per book chapter.

    Multiple TTS chunks that belong to the same chapter (same chapter_idx)
    are collapsed into a single chapter entry so the audiobook player shows
    the original chapter structure, not individual TTS split points.
    """
    chunk_lookup = _load_chunk_lookup(meta_dir)

    ffmeta = [";FFMETADATA1", f"title={title}"]

    current_time_ms = 0
    # Track the current open chapter: (chapter_idx, chapter_title, start_ms)
    open_chapter: tuple[int, str, int] | None = None

    def _flush(end_ms: int):
        if open_chapter is None:
            return
        _, ch_title, ch_start = open_chapter
        ffmeta.append("[CHAPTER]")
        ffmeta.append("TIMEBASE=1/1000")
        ffmeta.append(f"START={ch_start}")
        ffmeta.append(f"END={end_ms}")
        ffmeta.append(f"title={ch_title}")

    # Sort by (chapter_idx, chunk_idx) so watchdog-requeued or out-of-order
    # chunks are always grouped correctly regardless of arrival order.
    ordered = sorted(
        ((idx, chunk_lookup.get(idx, {})) for idx in range(total)
         if (chunk_dir / f"chunk_{idx:04d}.mp3").exists()),
        key=lambda x: (x[1].get("chapter_idx", x[0]), x[0]),
    )

    for idx, meta in ordered:
        chunk_file = chunk_dir / f"chunk_{idx:04d}.mp3"
        duration_ms = get_duration_ms(chunk_file)
        ch_idx   = meta.get("chapter_idx", idx)
        ch_title = meta.get("chapter_title", f"Chapter {ch_idx + 1}")

        # New chapter starts when chapter_idx changes
        if open_chapter is None or open_chapter[0] != ch_idx:
            _flush(current_time_ms)
            open_chapter = (ch_idx, ch_title, current_time_ms)

        current_time_ms += duration_ms

    _flush(current_time_ms)

    ffmeta_path = chunk_dir / "ffmetadata.txt"
    ffmeta_path.write_text("\n".join(ffmeta), encoding="utf-8")
    log.info("  FFMETADATA: %d TTS chunks → %d chapter markers",
             total, ffmeta.count("[CHAPTER]"))
    return ffmeta_path


def merge_to_mp3(concat_file: Path, ffmeta_path: Path, out_path: Path, title: str):
    tmp_path = out_path.with_suffix(".tmp.mp3")
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-i", str(ffmeta_path),
        "-map_metadata", "1",
        "-c", "copy",
        str(tmp_path),
    ]
    log.info("  ffmpeg merge → %s", out_path.name)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr}")

    validate_output(tmp_path)
    tmp_path.rename(out_path)
    log.info("  Validated and finalised → %s", out_path.name)


def validate_output(out_path: Path):
    """Run ffprobe on the finished file — raises RuntimeError if moov atom is missing or file is corrupt."""
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "a:0",
        "-show_entries", "stream=codec_name,duration",
        "-of", "default=noprint_wrappers=1",
        str(out_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0 or "moov atom not found" in result.stderr:
        raise RuntimeError(f"Output file is corrupt (moov atom missing or invalid):\n{result.stderr}")


def merge_to_m4b(concat_file: Path, ffmeta_path: Path, out_path: Path, title: str):
    """
    Concatenate MP3s and re-encode to AAC inside M4B or M4A container.
    Injects precise chapter markers. Writes to a temp file first and only
    renames to the final path after ffprobe validation — prevents corrupt
    partial files from landing in the audiobook library.
    """
    tmp_path = out_path.with_suffix(".tmp.m4b")
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-i", str(ffmeta_path),
        "-map_metadata", "1",
        "-c:a", "aac",
        "-b:a", "64k",
        "-vn",
        "-movflags", "+faststart",
        str(tmp_path),
    ]
    log.info("  ffmpeg encode m4b/m4a → %s", out_path.name)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr}")

    validate_output(tmp_path)
    tmp_path.rename(out_path)
    log.info("  Validated and finalised → %s", out_path.name)


def process_merge(raw: str):
    job       = json.loads(raw)
    book_id   = job["book_id"]
    title     = job["title"]
    total     = int(job["total"])
    chunk_dir = Path(job["out_dir"])          # MP3 files live here
    meta_dir  = CHUNKS_DIR / book_id          # meta.json lives here (written by orchestrator)

    log.info("Merging book %s — %s (%d chunks)", book_id[:8], title, total)
    job_start = time.time()
    set_book_state(book_id, status="merging", merge_started_at=job_start)

    # Wait up to 10 minutes for all chunk MP3s to land on the shared volume
    wait_start = time.time()
    for attempt in range(120):
        missing = [
            chunk_dir / f"chunk_{i:04d}.mp3"
            for i in range(total)
            if not (chunk_dir / f"chunk_{i:04d}.mp3").exists()
        ]
        if not missing:
            break
        if attempt % 6 == 0:  # log every 30s
            log.info("  Waiting for %d/%d chunks to arrive... (%ds elapsed)",
                     len(missing), total, attempt * 5)
        time.sleep(5)
    else:
        log.error("  Timed out waiting for chunks — %d still missing after 10 min: %s",
                  len(missing), [p.name for p in missing[:5]])
        MERGER_CHUNK_WAIT.observe(time.time() - wait_start)
        MERGER_TOTAL.labels(status="failed").inc()
        set_book_state(book_id, status="error", error="chunk files missing at merge time")
        return

    MERGER_CHUNK_WAIT.observe(time.time() - wait_start)

    # Build ffmpeg concat list and chapter metadata map
    try:
        concat_file = build_concat_list(chunk_dir, total, meta_dir)
        ffmeta_path = build_ffmetadata(chunk_dir, total, title, meta_dir)
    except FileNotFoundError as exc:
        log.error("  %s", exc)
        MERGER_TOTAL.labels(status="failed").inc()
        set_book_state(book_id, status="error", error=str(exc))
        return

    # Prepare output directory
    safe_title = "".join(c if c.isalnum() or c in " -_" else "_" for c in title)
    book_out_dir = AUDIOBOOK_DIR / safe_title
    book_out_dir.mkdir(parents=True, exist_ok=True)

    ext      = OUTPUT_FORMAT
    out_path = book_out_dir / f"{safe_title}.{ext}"

    ffmpeg_start = time.time()
    try:
        if ext in ("m4b", "m4a"):
            merge_to_m4b(concat_file, ffmeta_path, out_path, title)
        else:
            merge_to_mp3(concat_file, ffmeta_path, out_path, title)
        MERGER_FFMPEG_SECS.observe(time.time() - ffmpeg_start)
    except RuntimeError as exc:
        log.error("  Merge failed: %s", exc)
        MERGER_TOTAL.labels(status="failed").inc()
        MERGER_DURATION.observe(time.time() - job_start)
        set_book_state(book_id, status="error", error=str(exc))
        return

    # Cleanup
    if not KEEP_CHUNKS:
        try:
            shutil.rmtree(chunk_dir)
            txt_dir = CHUNKS_DIR / book_id
            if txt_dir.exists():
                shutil.rmtree(txt_dir)
        except Exception as exc:
            log.warning("  Cleanup warning: %s", exc)

    MERGER_TOTAL.labels(status="success").inc()
    MERGER_CHUNKS_TOTAL.inc(total)
    MERGER_DURATION.observe(time.time() - job_start)

    set_book_state(book_id,
        status="complete",
        output=str(out_path),
        finished_at=time.time(),
    )
    log.info("Book complete: %s", out_path)


def main():
    AUDIOBOOK_DIR.mkdir(parents=True, exist_ok=True)

    prom_port = int(os.environ.get("PROMETHEUS_PORT", "8002"))
    start_http_server(prom_port)
    log.info("Prometheus metrics on port %d", prom_port)
    log.info("Merger ready, consuming %s", QUEUE_DONE)

    while True:
        try:
            MERGER_QUEUE_DEPTH.set(r.llen(QUEUE_DONE))
        except Exception:
            pass

        result = r.brpop(QUEUE_DONE, timeout=5)
        if result is None:
            continue
        _, raw = result
        try:
            job = json.loads(raw)
            current_status = r.hget(f"book:{job['book_id']}", "status")
            if current_status == "complete":
                log.info("Skipping duplicate merge for book %s — already complete", job['book_id'][:8])
                continue
            process_merge(raw)
        except Exception as exc:
            log.exception("Unexpected error in merger: %s", exc)


if __name__ == "__main__":
    main()

