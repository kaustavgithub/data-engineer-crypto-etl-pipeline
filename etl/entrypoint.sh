#!/bin/bash
set -e

echo "🚀 Running ETL immediately on container startup..."

# Run the ETL once at startup
python3 /app/etl_daily.py /app/logs/etl_pipeline.log

echo "✅ Initial ETL run completed successfully."

echo "🕒 Starting cron in the foreground..."
cron -f
