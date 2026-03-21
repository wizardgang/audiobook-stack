FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CHUNKS_DIR=/chunks

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Layer 1: dependencies (cached until requirements.txt changes) ─────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Layer 2: source (only invalidates layers below this line) ─────────────────
COPY orchestrator.py invalidate_chunks.py ./

RUN mkdir -p /chunks /watch

CMD ["python", "orchestrator.py"]
