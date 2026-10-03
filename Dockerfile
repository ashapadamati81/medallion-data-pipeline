# Portable demo image: runs the full pipeline in DuckDB local mode.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
# Spark is intentionally left out of the demo image (no JVM in slim);
# local mode needs only pandas/duckdb/pyarrow.
RUN pip install --no-cache-dir pandas duckdb pyarrow

COPY . .
# Bake a small dataset so the image is runnable as-is.
RUN python generate_sample_data.py --customers 200 --orders-per-day 1000

CMD ["python", "run_pipeline.py", "--mode", "local", "--steps", "bronze,silver,gold,quality"]
