#!/bin/bash
set -euo pipefail

echo "Starting sequential ingestion: green -> yellow -> zones"

# Run green trips
echo "Running ingest_green_trips.py"
uv run ingest_green_trips.py

# Run yellow trips
echo "Running ingest_yellow_trips.py"
uv run ingest_yellow_trips.py

# Run zones
echo "Running ingest_zones.py"
uv run ingest_zones.py

echo "All ingests finished."
