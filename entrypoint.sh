#!/bin/bash
# Stop script if any command fails
set -e

echo "Running data pipelines..."
python run_pipeline.py

echo "Starting FastAPI server..."
exec uvicorn main:app --host 0.0.0.0 --port 8000 --reload