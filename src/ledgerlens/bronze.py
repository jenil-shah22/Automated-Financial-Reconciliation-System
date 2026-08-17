"""Bronze layer: land the source files as Delta tables, unchanged.

WHAT BRONZE IS FOR
------------------
Bronze is not a staging area you clean things in. It is the evidence locker.
Its single job is to make the raw extract queryable and replayable without
altering it, so that when a transformation turns out to be wrong six months
from now you can rebuild from here instead of asking an upstream team for a
file nobody can reproduce.

Three rules, and every one of them is a rule because breaking it is tempting:

1. **No casting.** Every column lands as a string. See schemas.py for why -
   the short version is that casting at ingest destroys the evidence the
   contract engine needs to diagnose the problem correctly.
2. **No filtering.** Bronze row count must equal the source file row count.
   Not approximately. There is an assertion.
3. **No renaming or reordering.** The header is checked against the contract
   before the file is read, because Spark maps a supplied schema POSITIONALLY -
   a reordered upstream extract would silently load vendor_code into
   invoice_number and every downstream number would be wrong but plausible.

Only three columns are added, all lineage, all underscore-prefixed so they can
never collide with a source column: when the row was ingested, which file it
came from, and which batch wrote it.

RUNS IN TWO PLACES
------------------
The same code runs on Databricks and on a local Spark session. Databricks is
detected by its runtime environment variable; locally we build a Delta-enabled
session and write to a directory. The pipeline is therefore portable, which
matters because the project brief lists local PySpark as the fallback if
Databricks Free Edition is unavailable - that should be a swap, not a rewrite.
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Sequence

from .config import AP_CSV, DATA_DIR, GL_CSV, RAW_DIR
from .schemas import (
    AP_SOURCE_COLUMNS,
    BRONZE_AP_SCHEMA,
    BRONZE_GL_SCHEMA,
    GL_SOURCE_COLUMNS,
    Column,
    assert_header_matches,
    column_names,
    to_spark_schema,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


# =============================================================================
# Where tables live
# =============================================================================
@dataclass(frozen=True)
class LakehouseConfig:
    """Resolves logical table names to a physical location.

    `catalog` mode writes managed Unity Catalog tables (Databricks).
    `path` mode writes Delta directories (local). Same DataFrame, same schema,
    same Delta format - only the destination differs.
    """

    mode: str = "path"                    # "path" | "catalog"
    base_path: Path = DATA_DIR / "lakehouse"
    catalog: str = "ledgerlens"
    schema: str = "bronze"

    def target(self, table: str) -> str:
        if self.mode == "catalog":
            return f"{self.catalog}.{self.schema}.{table}"
        return str(self.base_path / self.schema / table)

    @classmethod
    def detect(cls) -> "LakehouseConfig":
        """Databricks sets DATABRICKS_RUNTIME_VERSION; nothing else does."""
        if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            return cls(mode="catalog")
        return cls(mode="path")


# =============================================================================
# Spark session
# =============================================================================
def get_spark(app_name: str = "ledgerlens") -> "SparkSession":
    """Return the ambient Databricks session, or build a local Delta one."""
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # The dataset is ~2k rows. The default 200 shuffle partitions would
        # produce 200 near-empty files and spend all its time on scheduling.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .master("local[*]")
    )

    try:
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:  # pragma: no cover - delta-spark not installed
        pass

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# =============================================================================
# Ingest
# =============================================================================
def _read_source_csv(
    spark: "SparkSession",
    path: Path,
    source_columns: Sequence[str],
    schema_columns: Sequence[Column],
    dataset: str,
) -> "DataFrame":
    """Read one CSV with an explicit all-string schema.

    The header check happens FIRST, against the raw file, and it is not
    optional. Spark applies a supplied schema by position and silently ignores
    what the header actually says, so without this check a reordered extract
    loads cleanly and wrongly.
    """
    with open(path, "r", encoding="utf-8") as fh:
        header = fh.readline().strip().split(",")
    assert_header_matches(header, source_columns, dataset)

    # Only the source columns are in the file; lineage is added after.
    file_schema = to_spark_schema(
        [c for c in schema_columns if not c.name.startswith("_")]
    )

    return (
        spark.read.option("header", "true")
        # Keep empty cells as empty strings rather than NULL. The contract
        # engine distinguishes "absent" from "present but unreadable", and
        # collapsing both to NULL at read time would erase that distinction
        # before any rule can act on it.
        .option("nullValue", None)
        .option("emptyValue", "")
        .option("mode", "PERMISSIVE")
        .schema(file_schema)
        .csv(str(path))
    )


def _add_lineage(df: "DataFrame", source_path: Path, batch_id: str) -> "DataFrame":
    from pyspark.sql import functions as F

    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(str(source_path.resolve())))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def write_delta(df: "DataFrame", target: str, mode: str, is_path: bool) -> None:
    """Write a Delta table, full-refresh.

    v0.1 has no CDC: every run replaces the table. That is a stated limitation
    rather than an oversight - incremental load needs a watermark the synthetic
    extract does not carry, and pretending otherwise would be claiming an
    unbuilt feature.
    """
    writer = df.write.format("delta").mode(mode)
    if is_path:
        writer.save(target)
    else:
        writer.saveAsTable(target)


@dataclass
class IngestResult:
    dataset: str
    target: str
    source_rows: int
    bronze_rows: int
    batch_id: str

    @property
    def complete(self) -> bool:
        return self.source_rows == self.bronze_rows


def _count_source_rows(path: Path) -> int:
    """Count data rows in the file itself, independent of Spark.

    Deliberately not `df.count()`. The point of this control is to compare what
    Spark loaded against what the file contains, so it must be measured without
    Spark - otherwise a read that silently dropped rows would agree with itself.
    """
    with open(path, "r", encoding="utf-8") as fh:
        return max(sum(1 for _ in fh) - 1, 0)


def ingest_dataset(
    spark: "SparkSession",
    dataset: str,
    source_path: Path,
    source_columns: Sequence[str],
    schema_columns: Sequence[Column],
    cfg: LakehouseConfig,
    batch_id: str,
) -> IngestResult:
    df = _read_source_csv(spark, source_path, source_columns, schema_columns, dataset)
    df = _add_lineage(df, source_path, batch_id)

    # Enforce declared column order so the Delta table matches the schema
    # registry exactly, rather than however the reader happened to arrange it.
    df = df.select(*column_names(schema_columns))

    target = cfg.target(f"gl" if dataset == "gl" else "ap_subledger")
    write_delta(df, target, mode="overwrite", is_path=(cfg.mode == "path"))

    source_rows = _count_source_rows(source_path)
    bronze_rows = (
        spark.read.format("delta").load(target).count()
        if cfg.mode == "path"
        else spark.table(target).count()
    )

    result = IngestResult(dataset, target, source_rows, bronze_rows, batch_id)
    if not result.complete:
        raise RuntimeError(
            f"Bronze ingest lost rows for {dataset}: file had {source_rows}, "
            f"bronze has {bronze_rows}. Bronze must be lossless."
        )
    return result


def run(
    raw_dir: Path | None = None,
    cfg: LakehouseConfig | None = None,
    spark: "SparkSession" | None = None,
) -> List[IngestResult]:
    raw_dir = Path(raw_dir) if raw_dir is not None else RAW_DIR
    cfg = cfg or LakehouseConfig.detect()
    spark = spark or get_spark()

    batch_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"

    if cfg.mode == "catalog":
        # Schemas are created, the catalog is not. Catalog creation needs
        # elevated privileges that a Free Edition workspace may not grant, and
        # failing here would abort the run after the header checks have already
        # passed. The catalog is treated as pre-existing infrastructure.
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")

    return [
        ingest_dataset(spark, "gl", raw_dir / GL_CSV.name, GL_SOURCE_COLUMNS,
                       BRONZE_GL_SCHEMA, cfg, batch_id),
        ingest_dataset(spark, "ap", raw_dir / AP_CSV.name, AP_SOURCE_COLUMNS,
                       BRONZE_AP_SCHEMA, cfg, batch_id),
    ]


# =============================================================================
# CLI
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.bronze",
        description="Ingest source CSVs into bronze Delta tables.",
    )
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--mode", choices=["path", "catalog"], default=None)
    parser.add_argument("--base-path", type=Path, default=DATA_DIR / "lakehouse")
    args = parser.parse_args(argv)

    cfg = LakehouseConfig.detect()
    if args.mode:
        cfg = LakehouseConfig(mode=args.mode, base_path=args.base_path)
    elif cfg.mode == "path":
        cfg = LakehouseConfig(mode="path", base_path=args.base_path)

    results = run(raw_dir=args.raw_dir, cfg=cfg)

    print(f"Bronze ingest  (mode={cfg.mode}, batch={results[0].batch_id})")
    for r in results:
        print(f"  {r.dataset:<4} {r.bronze_rows:>6} rows  ->  {r.target}")
    print("  all datasets lossless: file row count == bronze row count")
    return 0


if __name__ == "__main__":
    sys.exit(main())
