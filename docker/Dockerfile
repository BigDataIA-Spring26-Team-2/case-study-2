FROM python:3.13

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files FIRST (for layer caching)
COPY pyproject.toml poetry.lock ./

# Install poetry
RUN pip install --no-cache-dir poetry==1.8.5

# Configure poetry for container
RUN poetry config virtualenvs.create false \
    && poetry config installer.max-workers 1

# Install all dependencies with max verbosity (so we see what's happening)
RUN poetry install -vvv --no-interaction --no-ansi --no-root --without dev

# CRITICAL FIX: Force reinstall CPU-only PyTorch
# --force-reinstall: Replace existing torch
# --no-deps: Don't reinstall sentence-transformers or other deps
RUN pip install --force-reinstall --no-deps \
    torch==2.5.1+cpu \
    torchvision==0.20.1+cpu \
    torchaudio==2.5.1+cpu \
    --index-url https://download.pytorch.org/whl/cpu

# Copy application code
COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app
COPY dashboard.py ./

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000 8501