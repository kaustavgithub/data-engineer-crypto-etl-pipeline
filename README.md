# Crypto ETL Pipeline (Docker + PostgreSQL + Cron)

A production-style ETL pipeline that extracts cryptocurrency price data from a public API, transforms it using Python (Pandas), and loads it into a PostgreSQL database. The project is fully containerized using Docker and scheduled to run daily at midnight via cron.

## 🧩 Project Features

- Daily ETL pipeline scheduled with cron inside Docker  
- Python-based ETL using `requests`, `pandas`, and `sqlalchemy`  
- PostgreSQL for persistent storage  
- pgAdmin for database management  
- Raw psycopg2 batch inserts for performance  
- Idempotent loading using composite primary keys  
- Retry logic for unreliable external APIs  
- Persistent host-mounted logs for auditing and debugging  
- Docker Compose orchestration for multi-service setup  

## 📁 Project Structure
```
project-root/
│
├── etl/
│   ├── etl_daily.py          # Main ETL pipeline
│   ├── Dockerfile            # ETL container image
│   ├── requirements.txt      # Python dependencies
│   ├── cronjob               # Cron schedule (daily at midnight)
│
├── db/
│   └── init.sql              # PostgreSQL schema initializer
│
├── compose/
│   └── docker-compose.yml    # Full stack orchestration (db, pgadmin, etl)
│
├── logs/                     # Persistent logs (gitignored)
│
└── README.md
```

## 🚀 How to Run

### 1. Navigate into the `compose/` directory:

cd compose

### 2. Build and start all services:

docker compose up -d --build

This launches:

- PostgreSQL (database)  
- pgAdmin (UI on http://localhost:8080)  
- ETL container (scheduled daily via cron)  

## 🗄️ Database Access

### PostgreSQL

| Setting        | Value     |
|----------------|-----------|
| Host           | db        |
| Port           | 5432      |
| Database       | etldb  |
| Username       | admin   |
| Password       | adminpass    |

### pgAdmin (Web UI)  
URL: http://localhost:8080

| Field       | Value             |
|-------------|-------------------|
| Email       | admin@admin.com   |
| Password    | admin             |

After opening pgAdmin, register a new server:

- Hostname: `db`
- Port: `5432`
- Username: `admin`
- Password: `adminpass`

## 🧪 ETL Overview

### Extract  
Fetches live crypto pricing data from the public API with automatic retry logic.

### Transform  
Cleans incoming fields, enforces schema, and adds a `load_timestamp`.

### Load  
Batch-inserts into PostgreSQL using psycopg2 executemany with:

ON CONFLICT (coin_id, load_timestamp) DO NOTHING

This ensures no duplicates if the ETL re-runs for the same timestamp.

## 🕒 Scheduling (Cron)

The ETL runs automatically every day at **00:00 (midnight)**.

Cron configuration is located in:

etl/cronjob

Logs are stored permanently on the host machine:

- logs/etl_pipeline.log
- logs/cron.log

## 📦 Stop the Stack

docker compose down

## 📑 Notes

- The database schema is created using db/init.sql on first startup.  
- The ETL script includes a fallback CREATE TABLE IF NOT EXISTS.  
- All logs persist on the host via bind mount for safe storage.  

## 📘 License

This project is provided for educational purposes.
