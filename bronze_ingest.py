"""
BRONZE layer — raw ingestion.

For every raw CSV, bronze stores a faithful copy plus ingestion metadata:
  * ``ingest_ts``   – when this row was ingested (UTC)
  * ``source_file`` – which raw file it came from (lineage)
  * ``load_date``   – business batch date, used for partitioning

No cleaning happens here on purpose: bronze is the immutable,
replayable record of "what the source gave us".  If silver logic ever
needs to change, we rebuild from bronze instead of re-pulling sources.

Layout:  data/bronze/<table>/load_date=YYYY-MM-DD/part-*.parquet
"""
from __future__ import annotations

from datetime import datetime, timezone

from config import BRONZE_DIR, LOAD_DATES, RAW_DIR
from engine import Engine, get_engine

BRONZE_TABLES = ("customers", "products", "orders")


def _add_metadata(engine: Engine, df, source_file: str, load_date: str):
    """Add ingest_ts / source_file / load_date columns, engine-aware."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if engine.name == "spark":
        from pyspark.sql import functions as F
        return (df.withColumn("ingest_ts", F.lit(now))
                  .withColumn("source_file", F.lit(source_file))
                  .withColumn("load_date", F.lit(load_date)))
    df = df.copy()
    df["ingest_ts"] = now
    df["source_file"] = source_file
    df["load_date"] = load_date
    return df


def run_bronze(engine: Engine, load_dates: list[str] | None = None) -> dict:
    """Ingest raw CSVs for ``load_dates`` into bronze. Returns row counts."""
    load_dates = load_dates or LOAD_DATES
    counts: dict[str, int] = {}
    for table in BRONZE_TABLES:
        total = 0
        for ld in load_dates:
            src = RAW_DIR / f"{table}_{ld}.csv"
            if not src.exists():
                print(f"[bronze] WARNING: missing {src.name}, skipping")
                continue
            df = engine.read_csv(src)
            df = _add_metadata(engine, df, src.name, ld)
            out = BRONZE_DIR / table
            engine.write_parquet(df, out, mode="overwrite",
                                 partition_by=["load_date"])
            n = engine.count(df)
            total += n
            print(f"[bronze] {table} @ {ld}: {n:,} rows -> {out}")
        counts[table] = total
    return counts


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "spark", "local"])
    args = ap.parse_args()
    eng = get_engine(args.mode)
    try:
        run_bronze(eng)
    finally:
        eng.stop()
