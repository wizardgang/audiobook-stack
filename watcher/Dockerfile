# Use Python 3.11 slim for a small footprint
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Set environment variables
# WATCH_DIR is where the PDFs will be dropped
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WATCH_DIR=/watch \
    REDIS_URL=redis://localhost:6379

# Install dependencies
# Watchdog and Redis are the only external requirements
RUN pip install --no-cache-dir \
    watchdog \
    redis

# Copy the watcher script
# Assuming your script is named 'watcher.py'
COPY watcher.py .

# Create the watch directory
RUN mkdir -p /watch

# Run the watcher
CMD ["python", "watcher.py"]
