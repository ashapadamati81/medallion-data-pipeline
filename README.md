# Medallion Data Pipeline — E-commerce

A portfolio-grade batch data pipeline implementing the **medallion architecture**
(bronze → silver → gold) on synthetic e-commerce data. Built with **PySpark**,
with a **DuckDB local-run mode** so the whole thing executes on a laptop with
no JVM. Orchestrated stage-by-stage, quality-gated at every layer, and
schedulable via the included **Airflow DAG**.

> Demo project: data is synthetically generated (but realistic, with genuine
> messiness — duplicates, nulls, bad dates, drifting customer attributes),
> and every number below was produced by actually running the pipeline.

---

## Architecture

```
┌──────────────┐      ┌───────────────────────────────┐      ┌────────────────────────┐      ┌─────────────────────────┐
│  Source CSVs │      │            BRONZE             │      │         SILVER         │      │          GOLD           │
│              │      │        (raw + metadata)       │      │   (clean + conformed)  │      │   (star-schema marts)   │
│ customers_   │─────▶│ bronze/customers              │─────▶│ dim_customer (SCD2)    │─────▶│ dim_customer (current)  │
│  YYYY-MM-DD  │      │  + ingest_ts, source_file,    │      │  versions w/ effective  │      │ dim_product           │
│ products_    │─────▶│ bronze/products   load_date   │─────▶│  _from/_to, is_current │─────▶│ fact_orders (as-was    │
│  YYYY-MM-DD  │      │                               │      │                        │      │  customer key join)     │
│ orders_      │─────▶│ bronze/orders                 │─────▶│ silver/orders (deduped,│      │                         │
│  YYYY-MM-DD  │      │  partitioned by load_date     │      │  typed, line_total)    │      │  + 3 analytical queries │
└──────────────┘      └───────────────────────────────┘      │ silver/products        │      └─────────────────────────┘
                                                             └────────────────────────┘
        generate_sample_data.py      bronze_ingest.py              silver_clean.py              gold_marts.py
                                              └────────────── quality_checks.py ──────────────┘
                                                     (row counts, nulls, uniqueness,
                                                      schema, freshness — every layer)
```

**What each layer does**

| Layer | Purpose | Key behaviors |
|---|---|---|
| **Bronze** | Immutable raw landing zone | Faithful copy of source CSVs + `ingest_ts`, `source_file`, `load_date`. Partitioned by `load_date`. Never cleaned — rebuilds start here. |
| **Silver** | Cleaned, conformed, historized | Dedup (`order_id`), type casting, invalid-row filtering (`qty ≤ 0`, bad dates, null keys), string normalization, `line_total` derived. **Customers are SCD Type 2**: new/changed attributes open a new version with `effective_from/_to`, `is_current`; surrogate key `customer_id_effective_from`. |
| **Gold** | Analytics-ready star schema | `dim_customer` (current snapshot), `dim_product`, `fact_orders` joined on the **as-was** customer version (order date within the version's effective range — correct historical attribution). Three sample BI queries ship with the project. |

**Data model (gold)**

```
┌──────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│   dim_customer   │        │    fact_orders   │        │   dim_product    │
│──────────────────│        │──────────────────│        │──────────────────│
│ PK customer_sk   │──┐  ┌──│ PK order_id      │  ┌──▶│ PK product_id    │
│    customer_id   │  └──│  │ FK customer_sk   │──┘     │    product_name  │
│    first_name    │     │  │ FK product_id    │──┐     │    category      │
│    last_name     │     │  │    order_date    │  └─────│    subcategory   │
│    email         │     │  │    quantity  (m) │        │    unit_price    │
│    city, state   │     │  │    unit_price(m) │        └──────────────────┘
│    customer_tier │     │  │    discount  (m) │
└──────────────────┘     │  │    line_total(m)│
                         │  └──────────────────┘
                         │   (m) = measure
                         │
   silver.dim_customer holds the FULL SCD2 history
   (all versions); gold keeps the current snapshot.
```

---

## Quickstart

```bash
cd medallion-data-pipeline

# 1. environment
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt        # pandas, duckdb, pyarrow (+ pyspark)

# 2. run everything (DuckDB local mode — no Java/Spark needed)
python run_pipeline.py --mode local

# 3. Spark mode (needs Java 11+; same code, same SQL)
python run_pipeline.py --mode spark
#    --mode auto  → tries Spark, falls back to DuckDB

# 4. run only some steps / dates
python run_pipeline.py --steps bronze,silver,gold,quality
python run_pipeline.py --load-dates 2026-10-01

# 5. optional: Great Expectations suites on gold
python ge_suites.py --mode local
```

A full local run takes ~5 seconds and ends with `SUCCESS` only if every
data-quality check passes (exit code 1 otherwise — the Airflow task relies
on this).

**Airflow**: drop `dags/medallion_pipeline_dag.py` into your Airflow `dags/`
folder with the project on the worker's `PYTHONPATH`
(`MEDALLION_PROJECT_DIR` env var). `MEDALLION_ENGINE=spark|local` picks the
engine. DAG: `generate_sample_data >> bronze_ingest >> silver_clean >>
gold_marts >> quality_checks` (`@daily`, retries=1, failure callback stub
included).

**Docker** (demo): `docker build -t medallion . && docker run medallion`
runs the pipeline end-to-end in local mode.

---

## Sample output (from a real run)

```
[silver] orders: 5,911 rows            # 6,062 raw → 151 dupes/invalid removed
[silver] dim_customer first load @ 2026-09-30: 500 versions
[silver] dim_customer @ 2026-10-01: 25 new, 21 changed
[gold] fact_orders: 5,911 rows         # zero join drops
```

SCD2 in action — customer `C00001` upgraded tiers between batches:

| customer_id | customer_tier | city | effective_from | effective_to | is_current |
|---|---|---|---|---|---|
| C00001 | Standard | Chicago | 2026-09-30 | 2026-09-30 | false |
| C00001 | Gold | Chicago | 2026-10-01 | 9999-12-31 | true |

**Data-quality report** (`data/reports/dq_report_latest.json`, console excerpt):

```
 [✓] bronze | orders       | row_count_min     | 6,062 rows (minimum 100)
 [✓] bronze | orders       | schema            | all expected columns present
 [✓] silver | orders       | unique(order_id)  | 0 duplicate key rows in 5,908 total
 [✓] silver | orders       | null_rate<email>  | ...
 [✓] silver | dim_customer | unique(customer_sk)| 0 duplicate key rows in 546 total
 [✓]   gold | fact_orders  | freshness         | max(order_date)=2026-10-01, 0 day(s) behind (allowed 2)
 ----------------------------------------------------
 16/16 checks passed
```

**Sample analytical queries** (`data/reports/*.csv`):

* `revenue_by_category_month` — revenue & order counts per category per month
* `top_customers` — top 10 customers by lifetime revenue
* `daily_order_trend` — orders, active customers, avg order value per day

---

## Tech stack

| Piece | Choice | Why |
|---|---|---|
| Compute | PySpark 3.x (+ DuckDB local mode) | Same SQL runs on both via the `Engine` abstraction in `engine.py` |
| Storage | Parquet (Hive-partitioned) | Columnar, portable, partition pruning |
| Orchestration | Apache Airflow (`dags/`) | One task per layer + DQ gate; `run_pipeline.py` for local runs |
| Data quality | Custom check framework (`quality_checks.py`) + optional Great Expectations (`ge_suites.py`) | Custom = zero-dep, engine-agnostic; GX = industry-standard suites |
| SCD | Type 2 in pure SQL | Portable across engines (no Delta `MERGE` required) |
| Packaging | `requirements.txt`, `Dockerfile`, `.venv`-friendly | Reproducible anywhere |

## Design notes & honest limitations

* **Two engines, one codebase**: every transformation is a plain `SELECT`
  (CTEs, window functions, joins) executed through `engine.sql()`. DuckDB
  and Spark SQL both understand it — that's the whole trick.
* **Partition-aware overwrites**: bronze uses dynamic partition overwrite so
  daily batches accumulate instead of wiping the table (a classic gotcha).
* **As-was joins**: facts join the dimension version active on the order
  date, not just the current row — this is what makes SCD2 actually useful.
* **Limitations**: sample data is synthetic; Spark mode needs a JVM
  (untested here — no Java in this build env, DuckDB mode is the verified
  path); the Airflow DAG is valid code but needs a real Airflow to schedule;
  email/phone PII in the sample data is fake.

## Project layout

```
medallion-data-pipeline/
├── README.md                  # this file
├── requirements.txt
├── Dockerfile
├── config.py                  # paths, DQ thresholds, expected schemas
├── engine.py                  # SparkEngine / DuckDBEngine abstraction
├── generate_sample_data.py    # synthetic e-commerce CSVs (2 batches)
├── bronze_ingest.py           # raw → bronze (+ metadata, partitioned)
├── silver_clean.py            # cleaning, dedup, SCD Type 2 customers
├── gold_marts.py              # star schema + analytical queries
├── quality_checks.py          # per-layer DQ framework + JSON reports
├── ge_suites.py               # optional Great Expectations suites
├── run_pipeline.py            # end-to-end driver (CLI)
├── dags/
│   └── medallion_pipeline_dag.py
└── data/                      # generated at runtime (gitignored)
    ├── raw/  bronze/  silver/  gold/  reports/
```
