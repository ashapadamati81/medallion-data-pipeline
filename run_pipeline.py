"""
End-to-end pipeline driver.

Runs bronze -> silver -> gold -> quality in one process, on either the
Spark or the DuckDB engine:

    python run_pipeline.py --mode local          # laptop, no Java needed
    python run_pipeline.py --mode spark          # PySpark (needs Java)
    python run_pipeline.py --mode auto           # Spark if available

    python run_pipeline.py --steps generate,bronze,silver,gold,quality
    python run_pipeline.py --load-dates 2026-09-30,2026-10-01

Exit code is 0 only if every data-quality check passed.
"""
from __future__ import annotations

import argparse
import sys

from bronze_ingest import run_bronze
from config import LOAD_DATES
from engine import get_engine
from gold_marts import run_gold
from quality_checks import run_quality
from silver_clean import run_silver


def main() -> int:
    ap = argparse.ArgumentParser(description="Medallion pipeline driver")
    ap.add_argument("--mode", default="auto",
                    choices=["auto", "spark", "local"],
                    help="execution engine")
    ap.add_argument("--steps", default="generate,bronze,silver,gold,quality",
                    help="comma-separated subset of steps to run")
    ap.add_argument("--load-dates", default=",".join(LOAD_DATES),
                    help="comma-separated batch dates to process")
    args = ap.parse_args()

    steps = [s.strip() for s in args.steps.split(",") if s.strip()]
    load_dates = [d.strip() for d in args.load_dates.split(",") if d.strip()]

    if "generate" in steps:
        import generate_sample_data
        sys.argv = ["generate_sample_data"]  # keep argparse defaults
        print("=== STEP: generate sample data ===")
        generate_sample_data.main()

    engine = get_engine(args.mode)
    print(f"=== engine: {engine.name} ===")
    try:
        if "bronze" in steps:
            print("\n=== STEP: bronze ===")
            run_bronze(engine, load_dates)
        if "silver" in steps:
            print("\n=== STEP: silver ===")
            run_silver(engine, load_dates)
        if "gold" in steps:
            print("\n=== STEP: gold ===")
            run_gold(engine)
        if "quality" in steps:
            print("\n=== STEP: quality ===")
            ok = run_quality(engine, ref_date=max(load_dates))
            if not ok:
                print("[pipeline] DATA QUALITY FAILURES — see report above")
                return 1
    finally:
        engine.stop()

    print("\n[pipeline] SUCCESS — all steps complete, quality checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
