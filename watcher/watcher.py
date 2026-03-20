"""
Watcher service
Monitors /watch for new PDF files and enqueues them for orchestration.
Uses watchdog for inotify-based events with a polling fallback.
"""

import os
import time
import json
import hashlib
import logging
import redis
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [watcher] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


REDIS_URL   = os.environ.get("REDIS_URL",   "redis://localhost:6379")
WATCH_DIR   = Path(os.environ.get("WATCH_DIR",  "/watch"))
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))

# Redis keys
QUEUE_ORCHESTRATE = "pipeline:orchestrate"   # list — new jobs
SEEN_SET          = "pipeline:seen_files"    # set  — already enqueued hashes

r = redis.from_url(REDIS_URL, decode_responses=True)


def file_id(path: Path) -> str:
    """Stable identifier: sha1 of absolute path string."""
    return hashlib.sha1(str(path.resolve()).encode()).hexdigest()


def enqueue(path: Path):
    fid = file_id(path)
    if r.sismember(SEEN_SET, fid):
        return  # already processed

    # Wait until the file is fully written (size stable for 2s)
    prev_size = -1
    for _ in range(6):
        size = path.stat().st_size
        if size == prev_size and size > 0:
            break
        prev_size = size
        time.sleep(0.5)

    job = {
        "id":       fid,
        "path":     str(path.resolve()),
        "filename": path.name,
        "queued_at": time.time(),
    }
    r.lpush(QUEUE_ORCHESTRATE, json.dumps(job))
    r.sadd(SEEN_SET, fid)
    log.info("Enqueued: %s  (id=%s)", path.name, fid[:8])


class PDFHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.lower().endswith((".pdf", ".epub")):
            enqueue(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory and event.dest_path.lower().endswith((".pdf", ".epub")):
            enqueue(Path(event.dest_path))


def scan_existing():
    """Pick up any files dropped while the container was offline."""
    for ext in ("*.pdf", "*.epub"):
        for f in WATCH_DIR.glob(ext):
            enqueue(f)




def main():
    WATCH_DIR.mkdir(parents=True, exist_ok=True)
    log.info("Watching %s", WATCH_DIR)

    scan_existing()

    handler  = PDFHandler()
    observer = Observer()
    observer.schedule(handler, str(WATCH_DIR), recursive=False)
    observer.start()

    try:
        while True:
            # Secondary polling pass for filesystems where inotify is unreliable
            scan_existing()
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()

