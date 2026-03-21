"""
invalidate_chunks.py — re-queue specific (or all failed) chunks for a book.

Usage (inside the orchestrator container):
    python invalidate_chunks.py <book_id> [chunk_indices]

    chunk_indices:
        (omitted)       — re-queue only chunks with status "error"
        all             — re-queue every chunk for the book
        0,5,23          — re-queue the listed comma-separated indices

Environment vars honoured: REDIS_URL, CHUNKS_DIR, OUTPUT_DIR
"""

import json
import os
import sys
import time
from pathlib import Path

import redis

REDIS_URL  = os.environ.get("REDIS_URL",   "redis://localhost:6379")
CHUNKS_DIR = Path(os.environ.get("CHUNKS_DIR", "/chunks"))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/outputs"))
QUEUE_TTS  = "pipeline:tts"


def main():
    if len(sys.argv) < 2:
        print("Usage: python invalidate_chunks.py <book_id> [all | errors | 0,5,23]")
        sys.exit(1)

    book_id   = sys.argv[1]
    filter_arg = sys.argv[2].lower() if len(sys.argv) > 2 else "errors"

    r = redis.from_url(REDIS_URL, decode_responses=True)

    # ── Load meta.json ────────────────────────────────────────────────────────
    meta_file = CHUNKS_DIR / book_id / "meta.json"
    if not meta_file.exists():
        print(f"ERROR: meta.json not found at {meta_file}")
        print("  The book may not have been processed by this orchestrator node,")
        print("  or CHUNKS_DIR is not mounted correctly.")
        sys.exit(1)

    all_chunks: list[dict] = json.loads(meta_file.read_text(encoding="utf-8"))
    total = len(all_chunks)

    book_hash = r.hgetall(f"book:{book_id}")
    if not book_hash:
        print(f"ERROR: book:{book_id} not found in Redis")
        sys.exit(1)

    book_title = book_hash.get("title", book_id)
    print(f"\nBook : {book_title} ({book_id[:12]}…)")
    print(f"Total chunks in meta.json: {total}")

    # ── Resolve which indices to invalidate ───────────────────────────────────
    if filter_arg == "all":
        target_indices = list(range(total))
    elif filter_arg == "errors":
        target_indices = []
        for meta in all_chunks:
            idx   = meta["chunk_idx"]
            state = r.hgetall(f"chunk:{book_id}:{idx}")
            if state.get("status") in ("error", "failed", ""):
                target_indices.append(idx)
        if not target_indices:
            print("No error chunks found — nothing to do.")
            sys.exit(0)
    else:
        try:
            target_indices = [int(x.strip()) for x in filter_arg.split(",")]
        except ValueError:
            print(f"ERROR: unrecognised chunk spec '{filter_arg}'")
            print("  Expected: all | errors | comma-separated integers like 0,5,23")
            sys.exit(1)

    # ── Validate indices ───────────────────────────────────────────────────────
    invalid = [i for i in target_indices if i < 0 or i >= total]
    if invalid:
        print(f"ERROR: indices out of range (0–{total-1}): {invalid}")
        sys.exit(1)

    print(f"Chunks to invalidate: {len(target_indices)}  ({target_indices[:10]}{'…' if len(target_indices) > 10 else ''})\n")

    # ── Invalidate and re-queue ────────────────────────────────────────────────
    requeued = 0
    deleted_mp3s = 0

    for idx in sorted(target_indices):
        chunk_key = f"chunk:{book_id}:{idx}"
        state     = r.hgetall(chunk_key)
        prev_status = state.get("status", "unknown")

        # Delete the existing MP3 output if present
        out_path_str = state.get("output", "")
        if out_path_str:
            out_path = Path(out_path_str)
            # Also try the canonical OUTPUT_DIR path in case 'output' points to a spool
            canonical = OUTPUT_DIR / book_id / f"chunk_{idx:04d}.mp3"
            for p in {out_path, canonical}:
                if p.exists():
                    try:
                        p.unlink()
                        deleted_mp3s += 1
                        print(f"  [{idx:04d}] deleted {p}")
                    except OSError as exc:
                        print(f"  [{idx:04d}] WARNING: could not delete {p}: {exc}")

        # Only decrement done_chunks if this chunk was previously counted as done
        if prev_status == "done":
            r.hincrby(f"book:{book_id}", "done_chunks", -1)

        # Reset chunk state
        r.hset(chunk_key, mapping={
            "status":     "queued",
            "worker":     "",
            "started_at": "",
            "finished_at": "",
            "output":     "",
            "error":      "",
        })

        # Re-push job to TTS queue
        job = dict(all_chunks[idx])  # copy so we don't mutate meta
        job["total"] = total
        r.lpush(QUEUE_TTS, json.dumps(job))
        requeued += 1
        print(f"  [{idx:04d}] was '{prev_status}' → re-queued")

    # ── Reset book status so the pipeline watches for completion again ─────────
    current_book_status = r.hget(f"book:{book_id}", "status")
    if current_book_status in ("done", "merging", "error", "failed"):
        r.hset(f"book:{book_id}", "status", "queued")
        print(f"\nBook status reset: '{current_book_status}' → 'queued'")

    print(f"\nDone: {requeued} chunk(s) re-queued, {deleted_mp3s} MP3(s) deleted.")
    print(f"TTS queue depth now: {r.llen(QUEUE_TTS)}")


if __name__ == "__main__":
    main()
