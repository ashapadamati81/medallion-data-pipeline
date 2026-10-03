"""
SILVER layer — cleaned, conformed, historically tracked.

* ``silver_orders``   – deduplicated, type-cast, invalid rows removed,
  ``line_total`` derived.  Partitioned by ``load_date``.
* ``silver_products`` – deduplicated, trimmed, categories standardized.
* ``dim_customer``    – full Slowly Changing Dimension Type 2: every
  customer has one row per version of their attributes, with
  ``effective_from`` / ``effective_to`` / ``is_current`` tracking.
  New/changed customers in each batch open a new version; changed
  attributes are taken from ``SCD2_TRACKED_COLUMNS`` (see config).

All transformations are plain SQL so they run identically on Spark
and on the DuckDB local engine.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from config import (BRONZE_DIR, LOAD_DATES, SCD2_TRACKED_COLUMNS, SILVER_DIR)
from engine import Engine, get_engine

CURRENT_END = "9999-12-31"  # open-ended effective_to for current versions


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _register_bronze(engine: Engine) -> None:
    for table in ("customers", "products", "orders"):
        df = engine.read_parquet(BRONZE_DIR / table)
        engine.register(df, f"bronze_{table}")


def _changed_predicate(alias_new: str = "i", alias_old: str = "d") -> str:
    """Null-safe inequality across all SCD2-tracked columns."""
    return "\n     OR ".join(
        f"COALESCE({alias_new}.{c}, '') <> COALESCE({alias_old}.{c}, '')"
        for c in SCD2_TRACKED_COLUMNS
    )


# ---------------------------------------------------------------------------
# silver_orders
# ---------------------------------------------------------------------------
ORDERS_SQL = """
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY load_date DESC) AS rn
  FROM bronze_orders
),
deduped AS (SELECT * FROM ranked WHERE rn = 1),
typed AS (
  SELECT
    TRIM(order_id)            AS order_id,
    TRIM(customer_id)         AS customer_id,
    TRIM(product_id)          AS product_id,
    order_date,                                   -- validated below, then cast
    CAST(quantity   AS INTEGER) AS quantity,
    CAST(unit_price AS DOUBLE)  AS unit_price,
    CAST(discount_pct AS DOUBLE) AS discount_pct,
    load_date
  FROM deduped
  WHERE order_date LIKE '____-__-__'              -- drop malformed dates
),
cleaned AS (
  SELECT *, CAST(order_date AS DATE) AS order_dt FROM typed
)
SELECT
  order_id, customer_id, product_id,
  order_dt AS order_date,
  quantity, unit_price, discount_pct,
  ROUND(quantity * unit_price * (1 - discount_pct), 2) AS line_total,
  load_date
FROM cleaned
WHERE order_id    IS NOT NULL AND order_id    <> ''
  AND customer_id IS NOT NULL AND customer_id <> ''
  AND product_id  IS NOT NULL AND product_id  <> ''
  AND quantity > 0
  AND unit_price >= 0
"""


def build_silver_orders(engine: Engine) -> int:
    df = engine.sql(ORDERS_SQL)
    engine.write_parquet(df, SILVER_DIR / "orders",
                         partition_by=["load_date"])
    n = engine.count(df)
    print(f"[silver] orders: {n:,} rows")
    return n


# ---------------------------------------------------------------------------
# silver_products
# ---------------------------------------------------------------------------
PRODUCTS_SQL = """
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY load_date DESC) AS rn
  FROM bronze_products
),
deduped AS (SELECT * FROM ranked WHERE rn = 1)
SELECT
  TRIM(product_id)   AS product_id,
  TRIM(product_name) AS product_name,
  UPPER(TRIM(category))    AS category,     -- standardize casing
  TRIM(subcategory)  AS subcategory,
  CAST(unit_price AS DOUBLE) AS unit_price
FROM deduped
WHERE product_id IS NOT NULL AND product_id <> ''
  AND unit_price IS NOT NULL AND unit_price >= 0
"""


def build_silver_products(engine: Engine) -> int:
    df = engine.sql(PRODUCTS_SQL)
    engine.write_parquet(df, SILVER_DIR / "products")
    n = engine.count(df)
    print(f"[silver] products: {n:,} rows")
    return n


# ---------------------------------------------------------------------------
# dim_customer (SCD Type 2)
# ---------------------------------------------------------------------------
INCOMING_SQL = """
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY TRIM(customer_id) ORDER BY load_date DESC) AS rn
  FROM bronze_customers
  WHERE load_date = '{ld}'
),
deduped AS (SELECT * FROM ranked WHERE rn = 1)
SELECT
  TRIM(customer_id) AS customer_id,
  TRIM(first_name)  AS first_name,
  TRIM(last_name)   AS last_name,
  NULLIF(LOWER(TRIM(email)), '') AS email,
  TRIM(phone)   AS phone,
  TRIM(address) AS address,
  TRIM(city)    AS city,
  TRIM(state)   AS state,
  TRIM(zip)     AS zip,
  CAST(signup_date AS DATE) AS signup_date,
  TRIM(customer_tier) AS customer_tier
FROM deduped
WHERE TRIM(customer_id) <> ''
"""

DIM_COLUMNS = ("customer_sk, customer_id, first_name, last_name, email, "
               "phone, address, city, state, zip, signup_date, "
               "customer_tier, effective_from, effective_to, is_current")


def _scd2_first_load(engine: Engine, ld: str) -> None:
    q = f"""
    SELECT
      CONCAT(i.customer_id, '_', CAST(DATE '{ld}' AS STRING)) AS customer_sk,
      i.customer_id, i.first_name, i.last_name, i.email, i.phone, i.address,
      i.city, i.state, i.zip, i.signup_date, i.customer_tier,
      DATE '{ld}' AS effective_from, DATE '{CURRENT_END}' AS effective_to,
      true AS is_current
    FROM ({INCOMING_SQL.format(ld=ld)}) i
    """
    df = engine.sql(q)
    engine.write_parquet(df, SILVER_DIR / "dim_customer")
    print(f"[silver] dim_customer first load @ {ld}: "
          f"{engine.count(df):,} versions")


def _scd2_incremental(engine: Engine, ld: str) -> None:
    prev_day = (date.fromisoformat(ld) - timedelta(days=1)).isoformat()
    engine.register(engine.read_parquet(SILVER_DIR / "dim_customer"),
                    "dim_existing")
    engine.register(engine.sql(INCOMING_SQL.format(ld=ld)), "incoming")

    changed = f"""
    SELECT i.customer_id
    FROM incoming i
    JOIN dim_existing d
      ON d.customer_id = i.customer_id AND d.is_current = true
    WHERE {_changed_predicate()}
    """
    new_ids = """
    SELECT i.customer_id
    FROM incoming i
    LEFT JOIN dim_existing d ON d.customer_id = i.customer_id
    WHERE d.customer_id IS NULL
    """
    engine.register(engine.sql(changed), "changed_ids")
    engine.register(engine.sql(new_ids), "new_ids")

    n_changed = engine.count(engine.sql("SELECT * FROM changed_ids"))
    n_new = engine.count(engine.sql("SELECT * FROM new_ids"))
    print(f"[silver] dim_customer @ {ld}: {n_new:,} new, "
          f"{n_changed:,} changed")

    new_version_cols = (
        f"CONCAT(i.customer_id, '_', CAST(DATE '{ld}' AS STRING)) "
        f"AS customer_sk, i.customer_id, i.first_name, i.last_name, i.email, "
        f"i.phone, i.address, i.city, i.state, i.zip, i.signup_date, "
        f"i.customer_tier, DATE '{ld}' AS effective_from, "
        f"DATE '{CURRENT_END}' AS effective_to, true AS is_current"
    )
    final_q = f"""
    -- unchanged history (everything except the versions being expired)
    SELECT {DIM_COLUMNS} FROM dim_existing
    WHERE NOT (is_current = true
           AND customer_id IN (SELECT customer_id FROM changed_ids))
    UNION ALL
    -- expire the old versions of changed customers
    SELECT customer_sk, customer_id, first_name, last_name, email, phone,
           address, city, state, zip, signup_date, customer_tier,
           effective_from, DATE '{prev_day}' AS effective_to,
           false AS is_current
    FROM dim_existing
    WHERE is_current = true
      AND customer_id IN (SELECT customer_id FROM changed_ids)
    UNION ALL
    -- new versions for changed customers
    SELECT {new_version_cols}
    FROM incoming i
    WHERE i.customer_id IN (SELECT customer_id FROM changed_ids)
    UNION ALL
    -- brand-new customers
    SELECT {new_version_cols}
    FROM incoming i
    WHERE i.customer_id IN (SELECT customer_id FROM new_ids)
    """
    df = engine.sql(final_q)
    engine.write_parquet(df, SILVER_DIR / "dim_customer")
    print(f"[silver] dim_customer total versions: {engine.count(df):,}")


def build_dim_customer(engine: Engine, load_dates: list[str]) -> None:
    for ld in sorted(load_dates):
        dim_path = Path(SILVER_DIR / "dim_customer")
        if dim_path.exists():
            _scd2_incremental(engine, ld)
        else:
            _scd2_first_load(engine, ld)


# ---------------------------------------------------------------------------
# driver
# ---------------------------------------------------------------------------
def run_silver(engine: Engine, load_dates: list[str] | None = None) -> dict:
    load_dates = load_dates or LOAD_DATES
    _register_bronze(engine)
    counts = {
        "silver_orders": build_silver_orders(engine),
        "silver_products": build_silver_products(engine),
    }
    build_dim_customer(engine, load_dates)
    dim = engine.read_parquet(SILVER_DIR / "dim_customer")
    counts["dim_customer_versions"] = engine.count(dim)
    return counts


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "spark", "local"])
    args = ap.parse_args()
    eng = get_engine(args.mode)
    try:
        _register_bronze(eng)
        run_silver(eng)
    finally:
        eng.stop()
