FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CHUNKS_DIR=/chunks

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# pymupdf is the package name for the 'fitz' module
RUN pip install --no-cache-dir \
    redis \
    pymupdf \
    requests

COPY orchestrator.py .

# Create the chunks directory and ensure it exists
RUN mkdir -p /chunks
RUN mkdir -p /watch

# Run the orchestrator
CMD ["python", "orchestrator.py"]
