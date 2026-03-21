FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OUTPUT_DIR=/data/outputs

# ── Layer 1: system deps (ffmpeg — cached until this line changes) ────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# ── Layer 2: python deps (cached until requirements.txt changes) ──────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Layer 3: source (only invalidates layers below this line) ─────────────────
COPY merger.py .

RUN mkdir -p /data/outputs

CMD ["python", "merger.py"]
