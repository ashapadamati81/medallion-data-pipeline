"""
Execution-engine abstraction.

The pipeline is written once against the small ``Engine`` interface below.
Two implementations ship:

* ``SparkEngine``  – real PySpark (Databricks/EMR/Dataproc, or local Spark
  if Java is available).  This is the production path.
* ``DuckDBEngine`` – zero-dependency local mode.  DuckDB executes the same
  SQL, so every transformation below runs unchanged on a laptop.

All transformations in this project are expressed as plain ``SELECT``
statements (CTEs, window functions, joins) executed through
``engine.sql()``.  Both Spark SQL and DuckDB understand that dialect, which
is what makes the two engines interchangeable here.

Usage:
    from engine import get_engine
    engine = get_engine("local")          # DuckDB, always works
    engine = get_engine("spark")          # PySpark, needs Java
    engine = get_engine("auto")           # Spark if it starts, else DuckDB
"""
from __future__ import annotations

from pathlib import Path


class Engine:
    """Minimal interface every engine must implement."""

    name = "base"

    def read_csv(self, path: str | Path):
        raise NotImplementedError

    def read_parquet(self, path: str | Path):
        raise NotImplementedError

    def register(self, df, name: str) -> None:
        """Make ``df`` queryable as table ``name`` in ``sql()``."""
        raise NotImplementedError

    def sql(self, query: str):
        raise NotImplementedError

    def write_parquet(self, df, path: str | Path, mode: str = "overwrite",
                      partition_by: list[str] | None = None) -> None:
        raise NotImplementedError

    def count(self, df) -> int:
        raise NotImplementedError

    def stop(self) -> None:  # noqa: D102
        pass


# ---------------------------------------------------------------------------
# PySpark
# ---------------------------------------------------------------------------
class SparkEngine(Engine):
    """Production engine: PySpark (local[*] unless configured otherwise)."""

    name = "spark"

    def __init__(self, app_name: str = "medallion-pipeline",
                 master: str = "local[*]"):
        from pyspark.sql import SparkSession  # deferred: needs Java at runtime
        self.spark = (
            SparkSession.builder.appName(app_name).master(master)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        # calmer logs for demos
        self.spark.sparkContext.setLogLevel("WARN")

    def read_csv(self, path):
        return (self.spark.read.option("header", "true")
                .option("inferSchema", "true").csv(str(path)))

    def read_parquet(self, path):
        return self.spark.read.parquet(str(path))

    def register(self, df, name: str) -> None:
        df.createOrReplaceTempView(name)

    def sql(self, query: str):
        return self.spark.sql(query)

    def write_parquet(self, df, path, mode="overwrite", partition_by=None):
        writer = df.write.mode(mode)
        if partition_by:
            # dynamic overwrite: replace only touched partitions,
            # so daily batches accumulate instead of wiping the table
            writer = writer.option("partitionOverwriteMode", "dynamic") \
                           .partitionBy(*partition_by)
        writer.parquet(str(path))

    def count(self, df) -> int:
        return df.count()

    def stop(self) -> None:
        self.spark.stop()


# ---------------------------------------------------------------------------
# DuckDB (local fallback)
# ---------------------------------------------------------------------------
class DuckDBEngine(Engine):
    """Local engine: DuckDB in-process, pandas DataFrames between steps."""

    name = "duckdb"

    def __init__(self):
        import duckdb  # deferred import so Spark-only envs stay lean
        self._duckdb = duckdb
        self.con = duckdb.connect()

    def read_csv(self, path):
        return self.con.execute(
            f"SELECT * FROM read_csv_auto('{path}')").df()

    def read_parquet(self, path):
        p = Path(path)
        # support dataset directories (incl. hive-partitioned subdirs)
        pattern = str(p / "**" / "*.parquet") if p.is_dir() else str(p)
        return self.con.execute(
            f"SELECT * FROM read_parquet('{pattern}')").df()

    def register(self, df, name: str) -> None:
        self.con.register(name, df)

    def sql(self, query: str):
        return self.con.execute(query).df()

    def write_parquet(self, df, path, mode="overwrite", partition_by=None):
        import shutil
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        if partition_by:
            # Hive-style partitioned dataset via pyarrow.
            # "overwrite" replaces only the partitions present in `df`,
            # never the whole table (so multi-batch loads accumulate).
            import pyarrow as pa
            import pyarrow.parquet as pq
            if mode == "overwrite":
                combos = df[partition_by].drop_duplicates()
                for _, row in combos.iterrows():
                    part_dir = p.joinpath(
                        *[f"{c}={row[c]}" for c in partition_by])
                    if part_dir.exists():
                        shutil.rmtree(part_dir)
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_to_dataset(table, root_path=str(p),
                                partition_cols=partition_by)
        else:
            if mode == "overwrite" and p.exists():
                shutil.rmtree(p)
            p.mkdir(parents=True, exist_ok=True)
            df.to_parquet(p / "part-0.parquet", index=False)

    def count(self, df) -> int:
        return len(df)

    def stop(self) -> None:
        self.con.close()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
def get_engine(mode: str = "auto") -> Engine:
    """Return an Engine.

    mode="spark"  -> SparkEngine (raises if Spark can't start)
    mode="local"  -> DuckDBEngine
    mode="auto"   -> try Spark, fall back to DuckDB with a warning
    """
    mode = (mode or "auto").lower()
    if mode == "local":
        return DuckDBEngine()
    if mode == "spark":
        return SparkEngine()  # raises RuntimeError-ish if Java is missing
    # auto
    try:
        return SparkEngine()
    except Exception as exc:  # noqa: BLE001 - any Spark startup failure
        print(f"[engine] Spark unavailable ({exc}); using DuckDB local mode.")
        return DuckDBEngine()
