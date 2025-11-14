FROM python:3.11-slim

# Install system packages including cron
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create logs directory
RUN mkdir -p /app/logs

# Copy ETL files
COPY requirements.txt /app/requirements.txt
COPY etl_daily.py /app/etl_daily.py
COPY cronjob /etc/cron.d/etl-cron
COPY entrypoint.sh /app/entrypoint.sh

# Install requirements
RUN pip install --no-cache-dir -r /app/requirements.txt

# Fix permissions
RUN chmod 0644 /etc/cron.d/etl-cron && \
    crontab /etc/cron.d/etl-cron && \
    chmod +x /app/entrypoint.sh

# Default command → entrypoint script
ENTRYPOINT ["/app/entrypoint.sh"]
