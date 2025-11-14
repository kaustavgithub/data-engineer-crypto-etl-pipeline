# -------------------------------
# Stage 1: Base Image
# -------------------------------
FROM python:3.11-slim

# Install system utilities + cron
RUN apt-get update && apt-get install -y --no-install-recommends \
    cron \
    gcc \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# -------------------------------
# Stage 2: ETL Code
# -------------------------------
WORKDIR /app
RUN mkdir -p /app/logs

# Copy Python requirements and ETL script
COPY requirements.txt /app/requirements.txt
COPY etl_daily.py /app/etl_daily.py

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# -------------------------------
# Stage 3: Cron Setup
# -------------------------------
# Copy cron job file
COPY cronjob /etc/cron.d/etl-cron

# Set permission
RUN chmod 0644 /etc/cron.d/etl-cron

# Register cron job
RUN crontab /etc/cron.d/etl-cron

# -------------------------------
# Stage 4: Container Start Command
# -------------------------------
CMD ["cron", "-f"]
