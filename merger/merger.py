import os
import shutil
import logging
import subprocess
import redis
import json
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [merger] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL     = os.environ.get("REDIS_URL",      "redis://localhost:6379")
OUTPUT_DIR    = Path(os.environ.get("OUTPUT_DIR",    "/outputs"))
AUDIOBOOK_DIR = Path(os.environ.get("AUDIOBOOK_DIR", "/audiobooks"))
KEEP_CHUNKS   = os.environ.get("KEEP_CHUNKS", "false").lower() == "true"

# Output format: "mp3" or "m4b"
OUTPUT_FORMAT = os.environ.get("OUTPUT_FORMAT", "m4b")

QUEUE_DONE = "pipeline:done"

r = redis.from_url(REDIS_URL, decode_responses=True)


def set_book_state(book_id: str, **kwargs):
    r.hset(f"book:{book_id}", mapping={k: str(v) for k, v in kwargs.items()})


def build_concat_list(chunk_dir: Path, total: int) -> Path:
    """Write an ffmpeg concat file listing chunks in order."""
    concat_file = chunk_dir / "concat.txt"
    lines = []
    for idx in range(total):
        chunk = chunk_dir / f"chunk_{idx:04d}.mp3"
        if not chunk.exists():
            raise FileNotFoundError(f"Missing chunk: {chunk}")
        # ffmpeg concat format requires escaped paths
        escaped = str(chunk).replace("'", "'\\''")
        lines.append(f"file '{escaped}'")
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


def build_ffmetadata(chunk_dir: Path, total: int, title: str) -> Path:
    import json
    meta_json_file = chunk_dir / "meta.json"
    chunk_meta = []
    if meta_json_file.exists():
        chunk_meta = json.loads(meta_json_file.read_text(encoding="utf-8"))
        
    ffmeta = [";FFMETADATA1", f"title={title}"]
    
    current_time = 0
    current_chapter = None
    chapter_start = 0
    
    for idx in range(total):
        chunk_file = chunk_dir / f"chunk_{idx:04d}.mp3"
        duration = get_duration_ms(chunk_file)
        
        c_title = "Chapter"
        if idx < len(chunk_meta):
            c_title = chunk_meta[idx].get("chapter_title", f"Chapter {chunk_meta[idx].get('chapter_idx', 0) + 1}")
            
        if current_chapter != c_title:
            if current_chapter is not None:
                ffmeta.append("[CHAPTER]")
                ffmeta.append("TIMEBASE=1/1000")
                ffmeta.append(f"START={chapter_start}")
                ffmeta.append(f"END={current_time}")
                ffmeta.append(f"title={current_chapter}")
            current_chapter = c_title
            chapter_start = current_time
            
        current_time += duration
        
    if current_chapter is not None:
        ffmeta.append("[CHAPTER]")
        ffmeta.append("TIMEBASE=1/1000")
        ffmeta.append(f"START={chapter_start}")
        ffmeta.append(f"END={current_time}")
        ffmeta.append(f"title={current_chapter}")
        
    ffmeta_path = chunk_dir / "ffmetadata.txt"
    ffmeta_path.write_text("\n".join(ffmeta), encoding="utf-8")
    return ffmeta_path


def merge_to_mp3(concat_file: Path, ffmeta_path: Path, out_path: Path, title: str):
    cmd = [
        "ffmpeg", "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", str(concat_file),
        "-i", str(ffmeta_path),
        "-map_metadata", "1",
        "-c", "copy",
        str(out_path),
    ]
    log.info("  ffmpeg merge → %s", out_path.name)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr}")


def merge_to_m4b(concat_file: Path, ffmeta_path: Path, out_path: Path, title: str):
    """
    Concatenate MP3s and re-encode to AAC inside M4B or M4A container.
    Injects precise chapter markers.
    """
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
        str(out_path),
    ]
    log.info("  ffmpeg encode m4b/m4a → %s", out_path.name)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{result.stderr}")


def process_merge(raw: str):
    job      = json.loads(raw)
    book_id  = job["book_id"]
    title    = job["title"]
    total    = int(job["total"])
    chunk_dir = Path(job["out_dir"])

    log.info("Merging book %s — %s (%d chunks)", book_id[:8], title, total)
    set_book_state(book_id, status="merging", merge_started_at=time.time())

    # Wait up to 60 s for any straggling chunk files to land
    for attempt in range(12):
        missing = [
            chunk_dir / f"chunk_{i:04d}.mp3"
            for i in range(total)
            if not (chunk_dir / f"chunk_{i:04d}.mp3").exists()
        ]
        if not missing:
            break
        log.info("  Waiting for %d chunks to arrive...", len(missing))
        time.sleep(5)
    else:
        log.error("  Timed out waiting for chunks — aborting merge")
        set_book_state(book_id, status="error", error="chunk files missing at merge time")
        return

    # Build ffmpeg concat list and chapter metadata map
    try:
        concat_file = build_concat_list(chunk_dir, total)
        ffmeta_path = build_ffmetadata(chunk_dir, total, title)
    except FileNotFoundError as exc:
        log.error("  %s", exc)
        set_book_state(book_id, status="error", error=str(exc))
        return

    # Prepare output directory
    safe_title = "".join(c if c.isalnum() or c in " -_" else "_" for c in title)
    book_out_dir = AUDIOBOOK_DIR / safe_title
    book_out_dir.mkdir(parents=True, exist_ok=True)

    ext     = OUTPUT_FORMAT
    out_path = book_out_dir / f"{safe_title}.{ext}"

    try:
        if ext in ("m4b", "m4a"):
            merge_to_m4b(concat_file, ffmeta_path, out_path, title)
        else:
            merge_to_mp3(concat_file, ffmeta_path, out_path, title)
    except RuntimeError as exc:
        log.error("  Merge failed: %s", exc)
        set_book_state(book_id, status="error", error=str(exc))
        return

    # Cleanup
    if not KEEP_CHUNKS:
        try:
            shutil.rmtree(chunk_dir)
            # Also remove text chunks dir under /chunks/<book_id>
            from pathlib import Path as P
            txt_dir = P("/chunks") / book_id
            if txt_dir.exists():
                shutil.rmtree(txt_dir)
        except Exception as exc:
            log.warning("  Cleanup warning: %s", exc)

    set_book_state(book_id,
        status="complete",
        output=str(out_path),
        finished_at=time.time(),
    )
    log.info("Book complete: %s", out_path)


def main():
    AUDIOBOOK_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Merger ready, consuming %s", QUEUE_DONE)

    while True:
        result = r.brpop(QUEUE_DONE, timeout=5)
        if result is None:
            continue
        _, raw = result
        try:
            process_merge(raw)
        except Exception as exc:
            log.exception("Unexpected error in merger: %s", exc)


if __name__ == "__main__":
    main()

