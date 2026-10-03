"""
Project-wide configuration for the medallion data pipeline.

All paths resolve relative to the project root so the pipeline runs
unchanged from a laptop, a container, or an Airflow worker.
"""
from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

RAW_DIR = PROJECT_ROOT / "data" / "raw"        # generator output (CSVs)
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"  # raw + ingestion metadata
SILVER_DIR = PROJECT_ROOT / "data" / "silver"  # cleaned, conformed
GOLD_DIR = PROJECT_ROOT / "data" / "gold"      # star-schema marts
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"  # DQ reports, query outputs

for _d in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, REPORTS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Load dates (one batch of source files per load date)
# ---------------------------------------------------------------------------
LOAD_DATES = ["2026-09-30", "2026-10-01"]

# ---------------------------------------------------------------------------
# Data-quality thresholds
# ---------------------------------------------------------------------------
DQ = {
    # max allowed fraction of NULLs per checked column
    "null_thresholds": {
        "orders": {"order_id": 0.0, "customer_id": 0.0, "product_id": 0.0,
                   "order_date": 0.0, "quantity": 0.0},
        "customers": {"customer_id": 0.0, "email": 0.05},
        "products": {"product_id": 0.0, "unit_price": 0.0},
    },
    # natural keys that must be unique per layer/table
    "unique_keys": {
        "silver_orders": ["order_id"],
        "silver_products": ["product_id"],
        "dim_customer": ["customer_sk"],
        "fact_orders": ["order_id"],
    },
    # freshness: max(order_date) must be within N days of the load date
    "freshness_days": 2,
    # minimum expected rows (guards against empty/partial loads)
    "min_rows": {"bronze_orders": 100, "silver_orders": 100, "fact_orders": 100},
}

# ---------------------------------------------------------------------------
# Expected schemas (used by schema-conformance checks)
# ---------------------------------------------------------------------------
EXPECTED_COLUMNS = {
    "bronze_orders": ["order_id", "customer_id", "product_id", "order_date",
                      "quantity", "unit_price", "discount_pct",
                      "ingest_ts", "source_file", "load_date"],
    "silver_orders": ["order_id", "customer_id", "product_id", "order_date",
                      "quantity", "unit_price", "discount_pct", "line_total",
                      "load_date"],
    "dim_customer": ["customer_sk", "customer_id", "first_name", "last_name",
                     "email", "phone", "address", "city", "state", "zip",
                     "signup_date", "customer_tier",
                     "effective_from", "effective_to", "is_current"],
    "dim_product": ["product_id", "product_name", "category",
                    "subcategory", "unit_price"],
    "fact_orders": ["order_id", "customer_sk", "product_id", "order_date",
                    "quantity", "unit_price", "discount_pct", "line_total"],
}

# Columns tracked for SCD Type 2 change detection on customers
SCD2_TRACKED_COLUMNS = ["email", "phone", "address", "city", "state",
                        "zip", "customer_tier"]


def table_path(layer_dir: Path, table: str) -> Path:
    """Parquet dataset location for a table inside a layer directory."""
    return layer_dir / table


def env_flag(name: str, default: str = "") -> str:
    return os.environ.get(name, default)
