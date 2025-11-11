FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install PostgreSQL client tools (for pg_isready) and dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2-binary numpy sentence-transformers

# Note: The load script will be mounted as a volume in docker-compose
# No need to COPY it here since we'll use the live version from the host

# Default command (can be overridden in docker-compose)
CMD ["python", "scripts/load_filtered_csvs.py"]
