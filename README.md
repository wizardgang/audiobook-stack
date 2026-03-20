# Audiobook TTS-Node Pipeline

A distributed, highly scalable, and self-balancing Text-to-Speech (TTS) orchestration pipeline for automatically converting full-length novels (EPUB/PDF) into chapterized M4B/M4A audiobooks using the Abogen AI Generator.

## Architecture

This pipeline is strictly decoupled to enable massive remote hardware scaling:

* **Watcher**: Monitors the `inbox/` directory for incoming PDFs and EPUBs.
* **Orchestrator**: Parses books, extracts native chapters, slices them into ~3-minute text chunks, and injects job payloads directly into a centralized Redis queue (`pipeline:tts`).
* **Workers**: Run locally or on remote bare-metal network servers. They atomicaly poll Redis for text payloads via `brpop` (self-balancing based on their processing speed), generate high-fidelity MP3s using Abogen, and drop outputs to a network drive. 
* **Merger**: Monitors the `pipeline:done` Redis queue. Extracts precise chunk durations using `ffprobe`, generates native FFMETADATA chapter markers, and losslessly merges the MP3 chunks into a single, cohesive `.m4b` audiobook playable natively in applications like Audiobookshelf.

## Features

* **Complete Fault Tolerance**: Workers feature 5-loop Exponential Backoffs on Abogen API drops/CUDA crashes. A single corrupt chunk gracefully fails out without breaking the loop or hanging the book.
* **Decentralized Distribution**: Text chunks live explicitly inside the Redis JSON data; workers do not require shared filesystem access to read source documents! 
* **Worker Fleet Deployment**: Built-in Makefile SSH bindings to instantly spin up remote Docker workers on any external server (`make worker-remote`).
* **Telemetry & Observability**: Integrated Prometheus & Grafana stack capturing real-time pipeline status. Custom metrics like `abogen_worker_heartbeat_timestamp`, `abogen_worker_status` (Idle/Processing/Error), API latency, and log severities are actively tracked and aggregated natively by `worker_id`.

## Quick Start

### 1. Master Server Setup
1. Edit the `.env` file to point `OUTPUT_DIR` and `CHUNKS_DIR` to your shared NFS volumes, and supply your local `REDIS_URL`.
2. Boot the infrastructure:
```bash
docker compose -f docker-compose.yaml up -d --build
```
3. Drop a book into `inbox/`. The orchestrator will parse the book into chapters and queue them up instantly!

### 2. Remote Worker Deployment
If you have additional GPUs or Abogen nodes over the network, you can securely inject the worker daemon to them over SSH:
```bash
make worker-remote HOST=192.168.1.10 WORKER_ID=gpu-node-1 USER=root DEST=/opt/tts-node
```
The remote worker natively fetches chunks from the central Redis queue, renders them via its local Abogen process, and pushes output `.mp3` files to the shared network volume natively mapped in `.env`.

### 3. Queue Control
You can freeze and thaw the entire fleet instantly natively via Redis global state overrides:
* `make pause`: Instructs all workers to suspend polling the queue once they finish their current chunk. 
* `make resume`: Instantly wakes the fleet up.

## Services Included
* `pipeline-watcher`
* `pipeline-orchestrator`
* `pipeline-worker`
* `abogen-worker`
* `pipeline-merger`
* `pipeline-redis`
* `pipeline-prometheus`
* `pipeline-grafana`
* `audiobookshelf` 
