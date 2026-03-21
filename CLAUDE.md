# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

A distributed TTS pipeline that converts EPUB/PDF books into chapterized `.m4b` audiobooks. Books dropped into `inbox/` flow through five Python microservices connected by Redis queues, ending as playable audiobooks in Audiobookshelf.

## Commands

### Run the full stack locally
```bash
make up          # builds images and starts all services; creates required directories
make down        # stop all services
make logs        # tail logs from all services
make reset       # flush Redis DB and clear chunks/outputs (audiobooks preserved)
```

### Inject a test book
```bash
make inject FILE=path/to/book.pdf
```

### Queue inspection
```bash
make queue       # show TTS and done queue depths
make status      # show Redis state for the last processed book
```

### Queue control (affects the whole worker fleet)
```bash
make pause       # set pipeline:state=paused in Redis; workers drain current chunk then stop
make resume      # delete pipeline:state; workers resume
```

### Remote worker deployment (SSH + rsync)
```bash
make worker-remote HOST=192.168.1.10 WORKER_ID=gpu-node-1
```

### Run integration tests (requires running Abogen Flask server on localhost:8808)
```bash
# From abogen_src/abogen/webui/
python test_chunk_api.py path/to/document.pdf
python test_jobs_api.py [optional_file_path]
```

### Run pytest (unit tests inside abogen_src)
```bash
cd abogen_src && pip install -e ".[dev]" && pytest
```

## Architecture

```
inbox/ (PDF/EPUB)
  └─ Watcher ──────── pipeline:orchestrate ──► Orchestrator
                                                    │ PyMuPDF text extraction
                                                    │ ~3000-char chunks
                                                    ▼
                                              pipeline:tts  (chunk JSON includes full text)
                                                    │
                              ┌─────────┬──────────┘
                           Worker    Worker    (local or remote; self-balancing via brpop)
                              │        │  calls Abogen HTTP API (/api/generate)
                              └────────┘
                                   │ MP3 files → shared volume
                                   ▼
                              pipeline:done
                                   │
                                Merger  (ffprobe durations → FFMETADATA chapters → .m4b)
                                   │
                              audiobooks/ ──► Audiobookshelf (port 13378)
```

**Key architectural decisions:**
- **Chunk text lives in Redis** — workers never read source files; only shared volume access needed is for MP3 output.
- **Workers are stateless and horizontally scalable** — deploy extras with `make worker-remote`; they pull from the same Redis queue.
- **`pipeline:state` key** controls the entire fleet: if set to `paused`, all workers stop polling after their current chunk.
- **Abogen Flask API** (`abogen-worker`, port 8808) is the TTS engine. The `worker` service calls it via HTTP with exponential backoff (5 retries).

## Services at a Glance

| Container | Source | Key env vars |
|-----------|--------|--------------|
| `pipeline-watcher` | `watcher/watcher.py` | `REDIS_URL`, `INBOX_DIR` |
| `pipeline-orchestrator` | `orchestrator/orchestrator.py` | `REDIS_URL`, `CHUNKS_DIR` |
| `pipeline-worker` | `worker/worker.py` | `REDIS_URL`, `OUTPUT_DIR`, `WORKER_ID`, `ABOGEN_URL` |
| `abogen-worker` | `abogen_src/` + root `Dockerfile` | port 8808 |
| `pipeline-merger` | `merger/merger.py` | `REDIS_URL`, `OUTPUT_DIR` |
| `pipeline-redis` | official Redis image | port 6379 |
| `pipeline-prometheus` | config at `config/prometheus.yml` | port 9090 |
| `pipeline-grafana` | config at `config/grafana/` | port 3000 |
| `audiobookshelf` | official image | port 13378 |

## Observability

Worker exports Prometheus metrics on port 8000:
- `abogen_worker_jobs_total` — job counter by status
- `abogen_worker_api_latency_seconds` — Abogen API call latency
- `abogen_worker_heartbeat_timestamp` — liveness signal
- `abogen_worker_status` — current state (Idle / Processing / Error)
- `abogen_worker_job_processing_duration_seconds` — end-to-end chunk duration

Prometheus scrape targets are defined in `config/prometheus.yml`. Add remote worker IPs there to aggregate multi-node metrics in Grafana.

## Abogen WebUI (vendored in `abogen_src/`)

The vendored Abogen library (`abogen_src/abogen/`) is a self-contained Flask app exposing a TTS HTTP API. Key files:
- `webui/app.py` — Flask entry point
- `webui/service.py` — Kokoro/Supertonic TTS synthesis logic
- `webui/routes/api.py` — `/api/generate`, `/api/extract`, `/api/chunk`, `/api/jobs`, voice/settings endpoints
- `webui/conversion_runner.py` — batch job management

The root `Dockerfile` builds Abogen with CPU-only PyTorch (removes PyQt6 GUI deps). For GPU support, install optional extras defined in `abogen_src/pyproject.toml` (CUDA 12.6/12.8/13.0 or ROCm variants).

## Environment Configuration

Key `.env` variables:
```
REDIS_URL=redis://localhost:6379
OUTPUT_DIR=./audiobooks       # shared volume for MP3/M4B output
CHUNKS_DIR=./chunks           # temporary chunk storage
WORKER_ID=local-1             # unique ID per worker node for Prometheus labels
```

For multi-machine setups, `OUTPUT_DIR` should point to an NFS share accessible by all worker nodes.
