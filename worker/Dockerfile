FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OUTPUT_DIR=/data/outputs

# ── Layer 1: dependencies (cached until requirements.txt changes) ─────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Layer 2: source (only invalidates layers below this line) ─────────────────
COPY worker.py .

RUN mkdir -p /data/outputs /outputs /outputs-root /chunks /chunks-root /spool

EXPOSE 8000

CMD ["python", "worker.py"]
