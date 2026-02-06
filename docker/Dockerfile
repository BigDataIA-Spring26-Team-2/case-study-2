FROM python:3.13

WORKDIR /app

# Install CPU-only torch first (avoids nvidia hang)
RUN pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Install dependencies
RUN pip install --no-cache-dir poetry \
 && poetry config virtualenvs.create false \
 && poetry install --no-interaction --no-ansi --no-root

# Copy application code
COPY app ./app
COPY dashboard.py ./

# Default command (overridden by docker-compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]