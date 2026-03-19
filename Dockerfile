# syntax=docker/dockerfile:1

# ── Stage 1: Build ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
        git \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Clone the source
RUN git clone --depth=1 https://github.com/denizsafak/abogen.git /build

# Install CPU-only PyTorch first so pip won't pull in the CUDA wheels (~700 MB).
RUN pip install --no-cache-dir \
        torch torchvision torchaudio \
        --index-url https://download.pytorch.org/whl/cpu

# Install abogen; keep torch on the CPU index so the CUDA wheel isn't pulled in
RUN pip install --no-cache-dir \
        --extra-index-url https://download.pytorch.org/whl/cpu \
        . && \
    pip uninstall -y PyQt6 PyQt6-Qt6 PyQt6-sip PyQt6-WebEngine PyQt6-WebEngine-Qt6 2>/dev/null; true

# ── Stage 2: Runtime ────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

LABEL org.opencontainers.image.title="abogen-cpu" \
      org.opencontainers.image.description="Abogen TTS audiobook generator – CPU-only, web UI" \
      org.opencontainers.image.source="https://github.com/denizsafak/abogen"

# Runtime system deps only (no Qt/X11 needed – web UI is Flask-only)
RUN apt-get update && apt-get install -y --no-install-recommends \
        espeak-ng \
        ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copy the installed Python environment from the builder
COPY --from=builder /usr/local/lib/python3.12 /usr/local/lib/python3.12
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy the source tree (needed for the editable install .pth reference)
COPY --from=builder /build /app/abogen

WORKDIR /app

# Volume mount points
RUN mkdir -p /data/uploads /data/outputs

ENV PYTHONUNBUFFERED=1 \
    # Disable CUDA entirely – torch won't touch GPU paths
    CUDA_VISIBLE_DEVICES="" \
    # CPU thread tuning
    OMP_NUM_THREADS=4 \
    MKL_NUM_THREADS=4 \
    # abogen web server config (read by abogen.webui.app:main)
    ABOGEN_HOST=0.0.0.0 \
    ABOGEN_PORT=8808 \
    ABOGEN_UPLOAD_ROOT=/data/uploads \
    ABOGEN_OUTPUT_ROOT=/data/outputs \
    TZ=UTC

EXPOSE 8808

# Use the web-only entry point – no PyQt6 import anywhere in this call path
CMD ["abogen-web"]
