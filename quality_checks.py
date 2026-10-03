"""
Data-quality checks for every medallion layer.

Each check is a small function returning a result dict:
    {"layer": ..., "table": ..., "check": ..., "status": "PASS|FAIL|WARN",
     "details": "..."}

``run_quality()`` executes the plan below, prints a human-readable
summary, writes ``data/reports/dq_report_<ts>.json`` plus a
``dq_report_latest.json``, and returns ``True`` iff no FAIL occurred
(so Airflow can branch / alert on it).

Why custom instead of Great Expectations here: zero heavy dependencies,
runs identically on Spark and DuckDB, and the check logic itself is the
portfolio piece.  (A Great Expectations suite can wrap the same tables;
see README "Extensions".)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from config import (BRONZE_DIR, DQ, EXPECTED_COLUMNS, GOLD_DIR,
                    REPORTS_DIR, SILVER_DIR)
from engine import Engine, get_engine

Result = dict


def _first_row(engine: Engine, query: str) -> dict:
    """Run ``query`` and return the first row as a plain dict.

    Hides the Spark-vs-pandas result-shape difference in one place so
    every check below stays engine-agnostic.
    """
    df = engine.sql(query)
    if engine.name == "duckdb":
        return dict(df.iloc[0])
    row = df.collect()[0]
    return row.asDict()


# ---------------------------------------------------------------------------
# individual checks (engine-agnostic: everything through engine.sql)
# ---------------------------------------------------------------------------
def check_row_count_min(engine: Engine, view: str, minimum: int,
                        layer: str, table: str) -> Result:
    n = engine.count(engine.sql(f"SELECT * FROM {view}"))
    ok = n >= minimum
    return {"layer": layer, "table": table, "check": "row_count_min",
            "status": "PASS" if ok else "FAIL",
            "details": f"{n:,} rows (minimum {minimum:,})"}


def check_null_rates(engine: Engine, view: str, thresholds: dict,
                     layer: str, table: str) -> list[Result]:
    results = []
    for col, thresh in thresholds.items():
        r = _first_row(engine,
                       f"SELECT COUNT(*) AS n, "
                       f"SUM(CASE WHEN {col} IS NULL "
                       f"OR CAST({col} AS STRING) = '' THEN 1 ELSE 0 END) "
                       f"AS n_null FROM {view}")
        n, n_null = r["n"], r["n_null"]
        rate = (n_null / n) if n else 1.0
        results.append({
            "layer": layer, "table": table,
            "check": f"null_rate<{col}",
            "status": "PASS" if rate <= thresh else "FAIL",
            "details": f"{rate:.2%} null/empty "
                       f"({n_null:,}/{n:,}, threshold {thresh:.2%})"})
    return results


def check_unique(engine: Engine, view: str, keys: list[str],
                 layer: str, table: str) -> Result:
    # portable key concat (no CONCAT_WS in DuckDB)
    concat = " || '|' || ".join(f"CAST({k} AS STRING)" for k in keys)
    r = _first_row(engine,
                   f"SELECT COUNT(*) AS n, "
                   f"COUNT(DISTINCT ({concat})) AS n_distinct FROM {view}")
    n, nd = r["n"], r["n_distinct"]
    dupes = n - nd
    return {"layer": layer, "table": table,
            "check": f"unique({', '.join(keys)})",
            "status": "PASS" if dupes == 0 else "FAIL",
            "details": f"{dupes:,} duplicate key rows in {n:,} total"}


def check_schema(engine: Engine, view: str, expected: list[str],
                 layer: str, table: str) -> Result:
    df = engine.sql(f"SELECT * FROM {view} LIMIT 0")
    actual = list(df.columns) if engine.name == "duckdb" \
        else df.columns
    missing = [c for c in expected if c not in actual]
    extra = [c for c in actual if c not in expected]
    ok = not missing
    details = "all expected columns present" if ok \
        else f"missing: {missing}"
    if extra:
        details += f"; extra (tolerated): {extra}"
    return {"layer": layer, "table": table, "check": "schema",
            "status": "PASS" if ok else "FAIL", "details": details}


def check_freshness(engine: Engine, view: str, date_col: str,
                    ref_date: str, max_days: int,
                    layer: str, table: str) -> Result:
    r = _first_row(engine,
                   f"SELECT CAST(MAX({date_col}) AS STRING) AS max_d "
                   f"FROM {view}")
    max_d = r["max_d"]
    if max_d is None:
        return {"layer": layer, "table": table, "check": "freshness",
                "status": "FAIL", "details": "no dates present"}
    from datetime import date as _date
    lag = (_date.fromisoformat(ref_date)
           - _date.fromisoformat(str(max_d)[:10])).days
    return {"layer": layer, "table": table, "check": "freshness",
            "status": "PASS" if 0 <= lag <= max_days else "FAIL",
            "details": f"max({date_col})={str(max_d)[:10]}, "
                       f"{lag} day(s) behind {ref_date} "
                       f"(allowed {max_days})"}


# ---------------------------------------------------------------------------
# plan + runner
# ---------------------------------------------------------------------------
def _register_all(engine: Engine) -> None:
    engine.register(engine.read_parquet(BRONZE_DIR / "orders"),
                    "dq_bronze_orders")
    engine.register(engine.read_parquet(SILVER_DIR / "orders"),
                    "dq_silver_orders")
    engine.register(engine.read_parquet(SILVER_DIR / "dim_customer"),
                    "dq_dim_customer")
    engine.register(engine.read_parquet(GOLD_DIR / "fact_orders"),
                    "dq_fact_orders")


def run_quality(engine: Engine, ref_date: str | None = None) -> bool:
    """Run the full DQ plan. Returns True iff every check passed."""
    from config import LOAD_DATES
    ref_date = ref_date or max(LOAD_DATES)
    _register_all(engine)

    results: list[Result] = []
    # --- bronze ---
    results.append(check_row_count_min(
        engine, "dq_bronze_orders", DQ["min_rows"]["bronze_orders"],
        "bronze", "orders"))
    results.append(check_schema(
        engine, "dq_bronze_orders", EXPECTED_COLUMNS["bronze_orders"],
        "bronze", "orders"))
    # --- silver ---
    results.append(check_row_count_min(
        engine, "dq_silver_orders", DQ["min_rows"]["silver_orders"],
        "silver", "orders"))
    results.extend(check_null_rates(
        engine, "dq_silver_orders", DQ["null_thresholds"]["orders"],
        "silver", "orders"))
    results.append(check_unique(
        engine, "dq_silver_orders", DQ["unique_keys"]["silver_orders"],
        "silver", "orders"))
    results.append(check_schema(
        engine, "dq_silver_orders", EXPECTED_COLUMNS["silver_orders"],
        "silver", "orders"))
    results.append(check_unique(
        engine, "dq_dim_customer", DQ["unique_keys"]["dim_customer"],
        "silver", "dim_customer"))
    results.append(check_schema(
        engine, "dq_dim_customer", EXPECTED_COLUMNS["dim_customer"],
        "silver", "dim_customer"))
    # --- gold ---
    results.append(check_row_count_min(
        engine, "dq_fact_orders", DQ["min_rows"]["fact_orders"],
        "gold", "fact_orders"))
    results.append(check_unique(
        engine, "dq_fact_orders", DQ["unique_keys"]["fact_orders"],
        "gold", "fact_orders"))
    results.append(check_schema(
        engine, "dq_fact_orders", EXPECTED_COLUMNS["fact_orders"],
        "gold", "fact_orders"))
    results.append(check_freshness(
        engine, "dq_fact_orders", "order_date", ref_date,
        DQ["freshness_days"], "gold", "fact_orders"))

    _write_report(results)
    _print_summary(results)
    return all(r["status"] != "FAIL" for r in results)


def _write_report(results: list[Result]) -> Path:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    payload = {"generated_at": ts, "results": results,
               "summary": {"PASS": sum(r["status"] == "PASS"
                                       for r in results),
                           "FAIL": sum(r["status"] == "FAIL"
                                       for r in results),
                           "WARN": sum(r["status"] == "WARN"
                                       for r in results)}}
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamped = REPORTS_DIR / f"dq_report_{ts}.json"
    latest = REPORTS_DIR / "dq_report_latest.json"
    for p in (stamped, latest):
        p.write_text(json.dumps(payload, indent=2))
    print(f"[quality] report -> {stamped.name}")
    return stamped


def _print_summary(results: list[Result]) -> None:
    print("\n================ DATA QUALITY SUMMARY ================")
    for r in results:
        mark = {"PASS": "✓", "FAIL": "✗", "WARN": "!"}[r["status"]]
        print(f" [{mark}] {r['layer']:>6} | {r['table']:<12} | "
              f"{r['check']:<22} | {r['details']}")
    fails = sum(r["status"] == "FAIL" for r in results)
    print(f"----------------------------------------------------\n"
          f" {len(results) - fails}/{len(results)} checks passed\n"
          f"====================================================\n")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "spark", "local"])
    args = ap.parse_args()
    eng = get_engine(args.mode)
    try:
        ok = run_quality(eng)
        raise SystemExit(0 if ok else 1)
    finally:
        eng.stop()
