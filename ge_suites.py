"""
Optional Great Expectations validation of the gold layer.

This is a *demonstration* of GX suites on top of the pipeline — the
built-in ``quality_checks.py`` remains the primary gate (zero heavy
deps, runs on Spark and DuckDB alike).  Run this after ``gold``:

    python ge_suites.py [--mode local]

Requires:  pip install great-expectations
"""
from __future__ import annotations

import argparse
import sys

import great_expectations as gx

from config import GOLD_DIR
from engine import get_engine


def _validate_df(df, suite_name: str,
                 expectations: list) -> tuple[bool, dict]:
    """Validate one pandas DataFrame against an expectation suite."""
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(f"{suite_name}_ds")
    asset = ds.add_dataframe_asset(name=f"{suite_name}_asset")
    batch = asset.get_batch(
        asset.build_batch_request(options={"dataframe": df}))
    suite = gx.ExpectationSuite(name=suite_name)
    for exp in expectations:
        suite.add_expectation(exp)
    result = batch.validate(suite)
    stats = result.statistics
    summary = {"evaluated": stats["evaluated_expectations"],
               "successful": stats["successful_expectations"],
               "success": bool(result.success)}
    return bool(result.success), summary


E = gx.expectations  # shorthand


def validate_fact_orders(df) -> tuple[bool, dict]:
    return _validate_df(df, "fact_orders_suite", [
        E.ExpectTableRowCountToBeBetween(min_value=100),
        E.ExpectColumnValuesToNotBeNull(column="order_id"),
        E.ExpectColumnValuesToNotBeNull(column="customer_sk"),
        E.ExpectColumnValuesToBeUnique(column="order_id"),
        E.ExpectColumnValuesToBeBetween(column="quantity", min_value=1),
        E.ExpectColumnValuesToBeBetween(column="line_total", min_value=0),
    ])


def validate_dim_customer(df) -> tuple[bool, dict]:
    return _validate_df(df, "dim_customer_suite", [
        E.ExpectTableRowCountToBeBetween(min_value=1),
        E.ExpectColumnValuesToNotBeNull(column="customer_sk"),
        E.ExpectColumnValuesToBeUnique(column="customer_sk"),
        E.ExpectColumnValuesToBeInSet(
            column="customer_tier",
            value_set=["Standard", "Gold", "Platinum"]),
    ])


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "spark", "local"])
    args = ap.parse_args()

    engine = get_engine(args.mode)
    try:
        # DuckDB engine returns pandas already; convert Spark DFs if needed.
        fact = engine.read_parquet(GOLD_DIR / "fact_orders")
        dim = engine.read_parquet(GOLD_DIR / "dim_customer")
        if engine.name == "spark":
            fact, dim = fact.toPandas(), dim.toPandas()
    finally:
        engine.stop()

    overall = True
    for name, df, fn in (("fact_orders", fact, validate_fact_orders),
                         ("dim_customer", dim, validate_dim_customer)):
        ok, summary = fn(df)
        mark = "✓" if ok else "✗"
        print(f"[gx] [{mark}] {name}: "
              f"{summary['successful']}/{summary['evaluated']} "
              f"expectations passed")
        overall &= ok

    print("[gx] ALL SUITES PASSED" if overall else "[gx] SUITE FAILURES")
    return 0 if overall else 1


if __name__ == "__main__":
    raise SystemExit(main())
