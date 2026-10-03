"""
GOLD layer — analytics-ready star schema.

* ``dim_customer`` – one row per customer: the *current* SCD2 version
  (analysts usually want "as-is"; the full history stays in silver).
* ``dim_product``  – cleaned product attributes.
* ``fact_orders``  – one row per order line, with surrogate keys and
  measures.  Customer key is resolved *as-was*: the dimension version
  that was current on the order date (correct historical attribution).

Plus three sample analytical queries whose results are written to
``data/reports/`` – the kind of thing a BI tool would consume.
"""
from __future__ import annotations

from config import GOLD_DIR, SILVER_DIR
from engine import Engine, get_engine

DIM_CUSTOMER_SQL = """
SELECT customer_sk, customer_id, first_name, last_name, email,
       city, state, customer_tier
FROM silver_dim_customer
WHERE is_current = true
"""

DIM_PRODUCT_SQL = """
SELECT product_id, product_name, category, subcategory, unit_price
FROM silver_products
"""

# -- as-was join: each order gets the customer version active that day ----
FACT_ORDERS_SQL = """
SELECT
  o.order_id,
  dc.customer_sk,
  o.product_id,
  o.order_date,
  o.quantity,
  o.unit_price,
  o.discount_pct,
  o.line_total
FROM silver_orders o
JOIN silver_dim_customer dc
  ON dc.customer_id = o.customer_id
 AND o.order_date >= dc.effective_from
 AND o.order_date <= dc.effective_to
"""


def build_gold(engine: Engine) -> dict:
    engine.register(engine.read_parquet(SILVER_DIR / "orders"),
                    "silver_orders")
    engine.register(engine.read_parquet(SILVER_DIR / "products"),
                    "silver_products")
    engine.register(engine.read_parquet(SILVER_DIR / "dim_customer"),
                    "silver_dim_customer")

    counts = {}
    for name, query in (("dim_customer", DIM_CUSTOMER_SQL),
                        ("dim_product", DIM_PRODUCT_SQL),
                        ("fact_orders", FACT_ORDERS_SQL)):
        df = engine.sql(query)
        engine.write_parquet(df, GOLD_DIR / name)
        counts[name] = engine.count(df)
        print(f"[gold] {name}: {counts[name]:,} rows")
    return counts


# ---------------------------------------------------------------------------
# sample analytical queries (results -> data/reports/*.csv)
# ---------------------------------------------------------------------------
QUERIES = {
    "revenue_by_category_month": """
        SELECT SUBSTR(CAST(f.order_date AS STRING), 1, 7) AS order_month,
               p.category,
               COUNT(*)              AS orders,
               ROUND(SUM(f.line_total), 2) AS revenue
        FROM fact_orders f
        JOIN dim_product p ON p.product_id = f.product_id
        GROUP BY 1, 2
        ORDER BY 1, 4 DESC
    """,
    "top_customers": """
        SELECT c.customer_id, c.first_name, c.last_name, c.customer_tier,
               COUNT(*) AS orders,
               ROUND(SUM(f.line_total), 2) AS lifetime_revenue
        FROM fact_orders f
        JOIN dim_customer c ON c.customer_sk = f.customer_sk
        GROUP BY 1, 2, 3, 4
        ORDER BY lifetime_revenue DESC
        LIMIT 10
    """,
    "daily_order_trend": """
        SELECT SUBSTR(CAST(f.order_date AS STRING), 1, 10) AS order_day,
               COUNT(*) AS orders,
               COUNT(DISTINCT f.customer_sk) AS active_customers,
               ROUND(AVG(f.line_total), 2) AS avg_order_value
        FROM fact_orders f
        GROUP BY 1
        ORDER BY 1
    """,
}


def run_analytical_queries(engine: Engine, reports_dir=None) -> dict:
    from config import REPORTS_DIR
    reports_dir = reports_dir or REPORTS_DIR
    engine.register(engine.read_parquet(GOLD_DIR / "fact_orders"),
                    "fact_orders")
    engine.register(engine.read_parquet(GOLD_DIR / "dim_customer"),
                    "dim_customer")
    engine.register(engine.read_parquet(GOLD_DIR / "dim_product"),
                    "dim_product")
    out = {}
    for name, query in QUERIES.items():
        df = engine.sql(query)
        path = reports_dir / f"{name}.csv"
        if engine.name == "spark":
            df.coalesce(1).write.mode("overwrite").option("header", "true") \
              .csv(str(path))
        else:
            df.to_csv(path, index=False)
        out[name] = engine.count(df)
        print(f"[gold] query '{name}': {out[name]:,} rows -> {path.name}")
    return out


def run_gold(engine: Engine) -> dict:
    counts = build_gold(engine)
    counts["queries"] = run_analytical_queries(engine)
    return counts


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "spark", "local"])
    args = ap.parse_args()
    eng = get_engine(args.mode)
    try:
        run_gold(eng)
    finally:
        eng.stop()
