FROM python:3.13.1-slim

# uv binary (multi-stage)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

# deps first for caching
COPY pyproject.toml .python-version uv.lock ./
RUN uv sync --locked

# copy app code (all scripts)
COPY ingest_green_trips.py ingest_green_trips.py
COPY ingest_yellow_trips.py ingest_yellow_trips.py
COPY ingest_zones.py ingest_zones.py
COPY run_ingests.sh run_ingests.sh
RUN chmod +x run_ingests.sh

# run all ingests in sequence by default
ENTRYPOINT ["/code/run_ingests.sh"]
