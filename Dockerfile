FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy packaging and dependency files
COPY pyproject.toml uv.lock* ./

# Install python dependencies in editable mode with dev & airflow extras
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -e ".[dev,airflow]"

# Copy source code and scripts
COPY . .

# Create non-root system user and configure ownership
RUN useradd -m -u 10001 quantaflow && chown -R quantaflow:quantaflow /app
USER quantaflow

ENTRYPOINT ["python", "scripts/run_pipeline.py"]
