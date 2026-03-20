# Use a slim version of Python to keep the image small
FROM python:3.12-slim

# Set the working directory in the container
WORKDIR /app

# Set environment variables
# PYTHONUNBUFFERED=1 ensures logs are sent straight to terminal without buffering
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    OUTPUT_DIR=/data/outputs

# Install system dependencies if needed (none strictly required for these libs, 
# but good to have if you ever add sound processing tools like ffmpeg)
# RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# We install them before copying the code to leverage Docker's layer caching
RUN pip install --no-cache-dir requests redis prometheus-client

# Copy the worker script into the container
# Assuming your script is named 'worker.py'
COPY worker.py .

# Create the output directory and ensure it has the right permissions
RUN mkdir -p /data/outputs

# Expose Port 8000 for the Prometheus metrics scraper
EXPOSE 8000

# Run the worker
CMD ["python", "worker.py"]
