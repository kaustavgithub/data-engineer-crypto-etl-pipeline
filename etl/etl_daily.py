from datetime import datetime
import logging
import os
import sys
import pandas as pd
import requests
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Text, Numeric, TIMESTAMP
)
from sqlalchemy.sql import text
from time import sleep

# --- logging setup ---
def setup_logging(log_path="etl_pipeline.log"):
    """Setup logging with configurable log file path."""
    logger = logging.getLogger("etl_extract")
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers.clear()
    
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    
    # File handler
    fh = logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    
    return logger

logger = None  # Will be initialized in main


#Load DB creds
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST", "db")
PG_DB   = os.getenv("PG_DB", "practice")

# --- fetch function ---
def fetch_coingecko_markets(vs_currency="usd", per_page=250, page=1, max_retries=3, backoff=1.0):
    """Fetch coin market data from CoinGecko. Returns list of dicts on success."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h"
    }
    session = requests.Session()
    attempt = 0
    while attempt < max_retries:
        try:
            logger.info(f"Fetching CoinGecko markets (page={page}, per_page={per_page}) attempt {attempt+1}")
            resp = session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError("unexpected response format")
            logger.info(f"Fetched {len(data)} records from CoinGecko")
            return data
        except Exception as e:
            attempt += 1
            logger.warning(f"Fetch attempt {attempt} failed: {e}")
            if attempt < max_retries:
                sleep(backoff * attempt)
            else:
                logger.error("Max retries reached — raising exception")
                raise


# --- transform function ---
def markets_to_dataframe(records):
    """Convert CoinGecko market records to a cleaned pandas DataFrame."""
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records)
    # Keep / rename useful columns
    keep = {
        "id": "coin_id",
        "symbol": "symbol",
        "name": "name",
        "current_price": "current_price",
        "market_cap": "market_cap",
        "total_volume": "total_volume",
        "high_24h": "high_24h",
        "low_24h": "low_24h",
        "price_change_percentage_24h": "pct_change_24h",
        "last_updated": "last_updated"
    }
    present = [c for c in keep.keys() if c in df.columns]
    df = df[present].rename(columns={k: keep[k] for k in present})
    # Normalize types
    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")
    df["total_volume"] = pd.to_numeric(df["total_volume"], errors="coerce")
    df["pct_change_24h"] = pd.to_numeric(df["pct_change_24h"], errors="coerce")
    # Parse last_updated to UTC timestamp if present
    if "last_updated" in df.columns:
        df["last_updated"] = pd.to_datetime(df["last_updated"], utc=True, errors="coerce")
    # Add ETL load timestamp
    df["load_timestamp"] = pd.Timestamp.utcnow()
    return df


# --- run extract step ---
def create_crypto_prices_table(engine):
    metadata = MetaData()

    crypto_prices = Table(
        "crypto_prices",
        metadata,
        Column("coin_id", Text, nullable=False),
        Column("symbol", Text),
        Column("name", Text),
        Column("current_price", Numeric(20, 6)),
        Column("market_cap", Numeric(30, 2)),
        Column("total_volume", Numeric(30, 2)),
        Column("high_24h", Numeric(20, 6)),
        Column("low_24h", Numeric(20, 6)),
        Column("pct_change_24h", Numeric(10, 6)),
        Column("last_updated", TIMESTAMP(timezone=True)),
        Column("load_timestamp", TIMESTAMP(timezone=True), nullable=False),
        # Composite primary key:
        schema=None
    )

    # Set composite PK via DDL (SQLAlchemy Table accepts primary_key on columns,
    # but to keep the definition explicit we set primary_key on the Column objects)
    crypto_prices.c.coin_id.primary_key = True
    crypto_prices.c.load_timestamp.primary_key = True

    # Create the table
    with engine.begin() as conn:
        metadata.create_all(conn)
        # sanity check: list tables
        res = conn.execute(text(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema();"
        ))
        tables = [r[0] for r in res.fetchall()]
        if "crypto_prices" in tables:
            print("✅ Table `crypto_prices` exists (or was created).")
        else:
            print("⚠️ Table `crypto_prices` not found after create_all().")


def prepare_rows(df):
    rows = []
    for row in df.itertuples(index=False):
        cleaned = tuple(
            None if pd.isna(v) else v
            for v in row
        )
        rows.append(cleaned)
    return rows


def load_to_postgres(engine, df):
    """
    Batch insert DataFrame rows into Postgres using DBAPI executemany.
    Returns a dict with attempt_count and elapsed seconds.
    """
    # SQL template (positional %s placeholders)
    INSERT_SQL = """
    INSERT INTO crypto_prices (
        coin_id, symbol, name, current_price,
        market_cap, total_volume, high_24h,
        low_24h, pct_change_24h, last_updated,
        load_timestamp
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (coin_id, load_timestamp) DO NOTHING;
    """

    # Prepare rows: use your prepare_rows function (itertuples -> None for NaN)
    rows = prepare_rows(df)  # uses the helper you already created

    if not rows:
        logger.info("No rows to load.")
        return {"attempted": 0, "elapsed_s": 0.0}

    start = datetime.utcnow()
    logger.info(f"Starting load of {len(rows)} rows to Postgres")

    # Use raw_connection() to ensure we get a psycopg2 connection and executemany behavior
    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.executemany(INSERT_SQL, rows)
            # rowcount for executemany is not always meaningful for INSERT ON CONFLICT;
            # we'll report attempted rows and let DB logs show exact inserts.
        raw_conn.commit()
        elapsed = (datetime.utcnow() - start).total_seconds()
        logger.info(f"Load finished: attempted {len(rows)} rows; elapsed {elapsed:.2f}s")
        return {"attempted": len(rows), "elapsed_s": elapsed}
    except Exception as e:
        logger.exception("Load failed")
        raise
    finally:
        raw_conn.close()


def main():
    """Main ETL execution with configurable log path."""
    # Get log path from command-line arguments
    log_path = sys.argv[1] if len(sys.argv) > 1 else "etl_pipeline.log"
    
    # Setup logging with the provided log path
    global logger
    logger = setup_logging(log_path)
    
    try:
        start = datetime.utcnow()
        logger.info("ETL extract started")
        records = fetch_coingecko_markets(vs_currency="usd", per_page=250, page=1)
        df = markets_to_dataframe(records)
        logger.info(f"Extract complete: {len(df)} rows; time taken {(datetime.utcnow()-start).total_seconds():.1f}s")
        conn_str = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:5432/{PG_DB}"
        engine = create_engine(conn_str)
        create_crypto_prices_table(engine)
        result = load_to_postgres(engine, df)
        logger.info(result)
    except Exception as e:
        logger.exception("ETL extract failed")
        raise


if __name__ == "__main__":
    main()