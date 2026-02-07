# NYC Taxi Data Pipeline

A containerized data ingestion pipeline that loads NYC taxi trip data and zone information into PostgreSQL with proper data types.

## Overview

This pipeline orchestrates three sequential ingestion scripts:

1. **Green Taxi Trips** — Downloads 2025-11 green taxi trip data (parquet format)
2. **Yellow Taxi Trips** — Downloads 2025-11 yellow taxi trip data (parquet format)
3. **Taxi Zone Lookup** — Downloads taxi zone reference data (CSV format)

All data is loaded into PostgreSQL with proper column types (TIMESTAMP for datetimes, BIGINT for integers, NUMERIC for floats, TEXT for strings).

## Architecture

```
docker-compose.yaml
├── pgdatabase (PostgreSQL 18)
├── pgadmin (pgAdmin web UI on port 8080)
└── ingest-all (Python runner)
    ├── ingest_green_trips.py
    ├── ingest_yellow_trips.py
    ├── ingest_zones.py
    └── run_ingests.sh (orchestrator)
```

## Prerequisites

- Docker & Docker Compose
- ~100 MB free disk space (for database volume)
- Internet access (to download data sources)

## Quick Start

### Run the complete pipeline:

```bash
cd pipeline
docker compose up
```

The pipeline will:
1. Initialize PostgreSQL database
2. Download green taxi data (~1.2 MB, ~47K rows)
3. Download yellow taxi data (~71 MB, ~4.2M rows)
4. Download taxi zone data (~12 KB, ~265 rows)
5. Load all data into the `ny_taxi` database with typed columns
6. Exit successfully

Expected runtime: ~2–5 minutes (depending on network and system speed)

## Database Schema

### Tables Created

**green_taxi_trips** (46,912 rows)
- Contains green cab trip records with proper column typing (TIMESTAMP, BIGINT, NUMERIC, TEXT)

**yellow_taxi_trips** (4,181,444 rows)
- Contains yellow cab trip records with proper column typing

**taxi_zones** (265 rows)
- Reference table with zone IDs, borough, zone name, and service area

## Data Sources

| Source | URL |
|--------|-----|
| Green Taxi | https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet |
| Yellow Taxi | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet |
| Taxi Zones | https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv |

## Configuration

All settings are controlled via environment variables in `docker-compose.yaml`:

To customize, edit these values in `docker-compose.yaml` before running `docker compose up`.

## Running Individual Scripts

To run a single ingest script (instead of the full pipeline):

```bash
# Green taxi only
docker compose run ingest-all bash -c "uv run ingest_green_trips.py"

# Yellow taxi only
docker compose run ingest-all bash -c "uv run ingest_yellow_trips.py"

# Zones only
docker compose run ingest-all bash -c "uv run ingest_zones.py"
```

## Stopping the Pipeline

```bash
# Stop all containers
docker compose down

# Stop and remove all data (reset database)
docker compose down -v
```

## Troubleshooting

### "database 'root' does not exist"
This warning appears during startup and is normal—it's from pgAdmin probing for connections. The database is created automatically on first run.

### Pipeline hangs or times out
Check internet connection; the data downloads may take time depending on network speed. Yellow taxi data is ~71 MB.

### Permission denied on run_ingests.sh
The Dockerfile automatically makes the script executable. If running locally, run:
```bash
chmod +x run_ingests.sh
```

## Development

To test locally (requires Python 3.13+):

```bash
# Install dependencies
uv sync

# Activate environment
source .venv/bin/activate

# Run a script directly (requires running database)
uv run ingest_green_trips.py
```
