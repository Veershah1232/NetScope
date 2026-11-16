# probe/Dockerfile
# NetScope Probe container image
#
# Build context is the project root (not probe/), so we can access
# schemas/ and targets/ alongside probe/.

FROM python:3.11-slim

# Install system tools needed by some TLS libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY probe/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY schemas/     ./schemas/
COPY probe/       ./probe/
COPY targets/     ./targets/
COPY ingestion/   ./ingestion/

# Default environment (overridden by docker-compose or cloud deployment)
ENV PROBE_ID=local-01
ENV PROBE_INTERVAL=30
ENV PROBE_OUTPUT=/output/measurements.jsonl
ENV MINIO_ENDPOINT=http://localhost:9000
ENV MINIO_ACCESS_KEY=minioadmin
ENV MINIO_SECRET_KEY=minioadmin
ENV MINIO_BUCKET=netscope-raw

# Create output directory
RUN mkdir -p /output

# Entrypoint: run the probe in continuous mode
CMD ["python", "-m", "probe.netscope_probe.probe", \
     "--targets-file", "targets/targets.yaml"]
