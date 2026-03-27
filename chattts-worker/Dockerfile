FROM python:3.12-slim

# ffmpeg required by pydub for WAV → MP3 encoding
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OUTPUT_DIR=/data/outputs \
    # ChatTTS downloads model weights here on first run
    HF_HOME=/models

# ── Layer 1: dependencies (cached until requirements.txt changes) ──────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Layer 2: source ────────────────────────────────────────────────────────────
COPY worker.py .

RUN mkdir -p /data/outputs /outputs /spool /models

EXPOSE 8004

CMD ["python", "worker.py"]
