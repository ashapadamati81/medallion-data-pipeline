"""
Airflow DAG: medallion e-commerce pipeline.

Runs the same stage functions as ``run_pipeline.py`` — one task per
medallion layer plus data generation and quality gates:

    generate_sample_data >> bronze >> silver >> gold >> quality_checks

The ``quality_checks`` task raises on any FAIL, which marks the DAG run
failed and triggers ``on_failure_callback`` (wire it to Slack/PagerDuty
in production).

Deploy: copy the project onto the Airflow worker (or bake it into the
worker image) and point ``PROJECT_DIR`` at it.  Only this file needs to
live in Airflow's ``dags/`` folder if you add the project root to
``PYTHONPATH``.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

# Make the pipeline package importable on the worker.
PROJECT_DIR = os.environ.get(
    "MEDALLION_PROJECT_DIR", "/opt/medallion-data-pipeline")
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # pragma: no cover - allows syntax checks w/o Airflow
    DAG = PythonOperator = None  # type: ignore

ENGINE_MODE = os.environ.get("MEDALLION_ENGINE", "spark")  # or "local"


def _task_generate(**_):
    import generate_sample_data
    generate_sample_data.main()


def _task_bronze(**_):
    from bronze_ingest import run_bronze
    from engine import get_engine
    eng = get_engine(ENGINE_MODE)
    try:
        run_bronze(eng)
    finally:
        eng.stop()


def _task_silver(**_):
    from engine import get_engine
    from silver_clean import run_silver
    eng = get_engine(ENGINE_MODE)
    try:
        run_silver(eng)
    finally:
        eng.stop()


def _task_gold(**_):
    from engine import get_engine
    from gold_marts import run_gold
    eng = get_engine(ENGINE_MODE)
    try:
        run_gold(eng)
    finally:
        eng.stop()


def _task_quality(**_):
    """Raises RuntimeError on any failed check -> DAG run goes red."""
    from config import LOAD_DATES
    from engine import get_engine
    from quality_checks import run_quality
    eng = get_engine(ENGINE_MODE)
    try:
        ok = run_quality(eng, ref_date=max(LOAD_DATES))
    finally:
        eng.stop()
    if not ok:
        raise RuntimeError("data-quality checks FAILED (see task log)")


def _on_failure(context):
    ti = context["task_instance"]
    print(f"ALERT: task {ti.task_id} failed in DAG run "
          f"{context['run_id']} — page the on-call here.")


if DAG is not None:  # pragma: no cover - only built inside Airflow
    with DAG(
        dag_id="medallion_ecommerce_pipeline",
        description="Bronze -> Silver -> Gold e-commerce pipeline with DQ gates",
        schedule_interval="@daily",
        start_date=datetime(2026, 10, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-engineering",
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "on_failure_callback": _on_failure,
        },
        tags=["medallion", "ecommerce", "data-quality"],
    ) as dag:
        generate = PythonOperator(task_id="generate_sample_data",
                                  python_callable=_task_generate)
        bronze = PythonOperator(task_id="bronze_ingest",
                                python_callable=_task_bronze)
        silver = PythonOperator(task_id="silver_clean",
                                python_callable=_task_silver)
        gold = PythonOperator(task_id="gold_marts",
                              python_callable=_task_gold)
        quality = PythonOperator(task_id="quality_checks",
                                 python_callable=_task_quality)

        generate >> bronze >> silver >> gold >> quality
