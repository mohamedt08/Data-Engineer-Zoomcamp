FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# system deps needed for some wheels / psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy the script (mount your parquet file at runtime)
COPY ingest_data.py /app/

RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir pandas psycopg2-binary pyarrow sqlalchemy tqdm click

ENTRYPOINT ["python", "ingest_data.py"]