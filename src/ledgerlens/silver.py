"""Silver layer: enforce the contracts, quarantine the failures, type the rest.

WHAT SILVER IS FOR
------------------
Bronze preserved the evidence. Silver applies judgment: it decides which rows
are fit to reconcile, records why the others were not, and only then applies
types.

The order matters and is not negotiable. Casting happens *after* the contract,
never before, because a cast is a destructive operation on bad data - it turns
"N/A" into NULL and destroys the distinction between a value that was absent
and a value that was unreadable. By the time a row reaches the cast, the
contract has already guaranteed the cast will succeed. A cast failure in silver
is therefore a bug in the contract, not bad data, and should be investigated as
one.

NEVER SILENTLY DROP A ROW
-------------------------
Every bronze row leaves this layer through exactly one of two doors: the silver
table or the quarantine table. `silver_rows + quarantine_rows == bronze_rows`
is asserted on every run. A row that fails is not deleted, it is *filed* - with
the full list of rule ids that rejected it, because a row breaching three rules
is three tickets for three different people.

WHY QUARANTINE STAYS UNTYPED
----------------------------
The quarantine table keeps every source column as a string, exactly as bronze
had it. These rows failed the contract, so they cannot be assumed castable -
typing this table would mean the rows that most need investigating are the ones
that fail to load.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Sequence, Tuple

from .bronze import LakehouseConfig, get_spark, write_delta
from .config import RAW_DIR, load_contracts
from .quality import (
    assert_patterns_are_anchored,
    assert_patterns_compile,
    failed_rule_count_expr,
    failed_rule_ids_expr,
    flag_column,
    reject_rules,
    rule_count_exprs,
    rule_flag_exprs,
)
from .schemas import (
    BRONZE_AP_SCHEMA,
    BRONZE_GL_SCHEMA,
    SILVER_AP_SCHEMA,
    SILVER_GL_SCHEMA,
    Column,
    column_names,
)

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession


# =============================================================================
# Casting
# =============================================================================
def cast_expr(column: str, dtype: str) -> str:
    """SQL to convert a bronze string into its silver type.

    Explicit formats everywhere. `to_date(col)` without a format falls back to
    Spark's permissive parser, which accepts several layouts and silently
    returns NULL for the rest - exactly the inference behaviour this project
    exists to avoid.
    """
    if dtype == "string":
        # Conform, not clean: trailing whitespace in a code column is noise.
        # Provably a no-op on data that passed the contract, since every code
        # column is guarded by a regex or a domain rule that a padded value
        # would already have failed.
        return f"trim({column})"
    if dtype == "date":
        return f"to_date({column}, 'yyyy-MM-dd')"
    if dtype == "timestamp":
        # The generator writes ISO-8601 with a literal Z suffix.
        return f"to_timestamp({column}, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")"
    if dtype == "decimal_18_2":
        return f"cast({column} AS DECIMAL(18,2))"
    if dtype == "int":
        return f"cast({column} AS INT)"
    if dtype == "long":
        return f"cast({column} AS BIGINT)"
    if dtype == "boolean":
        return f"cast({column} AS BOOLEAN)"
    raise ValueError(f"No cast defined for dtype '{dtype}' on column '{column}'")


def _silver_select(schema: Sequence[Column]) -> List[str]:
    """Build the projection that turns a bronze row into a silver row."""
    out = []
    for col in schema:
        if col.name in {"_ingested_at", "_batch_id", "_source_file"}:
            out.append(col.name)  # already typed at ingest
            continue
        out.append(f"{cast_expr(col.name, col.dtype)} AS {col.name}")
    return out


# =============================================================================
# Contract application
# =============================================================================
def annotate(df: "DataFrame", rules: Sequence[Dict[str, Any]]) -> "DataFrame":
    """Attach `_failed_rule_ids` and `_failed_rule_count` to every row.

    Both are computed for ALL rows, passing and failing alike. A passing row
    carries an empty id list rather than a NULL, so the column is never
    ambiguous and downstream filters do not need to special-case it.
    """
    from pyspark.sql import functions as F

    return df.withColumn(
        "_failed_rule_ids", F.expr(failed_rule_ids_expr(rules))
    ).withColumn(
        "_failed_rule_count", F.expr(failed_rule_count_expr())
    )


def split(df: "DataFrame") -> Tuple["DataFrame", "DataFrame"]:
    """Partition annotated rows into (clean, quarantined)."""
    clean = df.filter("_failed_rule_ids = ''")
    quarantined = df.filter("_failed_rule_ids <> ''")
    return clean, quarantined


def to_silver(clean: "DataFrame", schema: Sequence[Column]) -> "DataFrame":
    return clean.selectExpr(*_silver_select(schema))


def to_quarantine(quarantined: "DataFrame", dataset: str,
                  bronze_schema: Sequence[Column]) -> "DataFrame":
    """Quarantine rows keep their original strings, plus why they were rejected."""
    from pyspark.sql import functions as F

    return (
        quarantined.select(*column_names(bronze_schema),
                           "_failed_rule_ids", "_failed_rule_count")
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_dataset", F.lit(dataset))
    )


def rule_violation_counts(
    df: "DataFrame", rules: Sequence[Dict[str, Any]]
) -> Dict[str, int]:
    """Per-rule violation counts, including the rules that fired zero times.

    Two passes, deliberately. The `unique` rules carry a window function, and
    Spark forbids a window inside an aggregate - `count_if(count(*) OVER (...))`
    is a parse error. Windows are legal in a projection, so the predicates are
    projected to boolean flags first and aggregated second.

    Zero-count rules are reported on purpose. A rule that suddenly starts
    rejecting rows is as much a signal as one that stops, and a scorecard
    listing only non-zero rules cannot distinguish "clean" from "that check
    silently stopped running".
    """
    flags = rule_flag_exprs(rules)
    if not flags:
        return {}

    # Step 1: project one boolean per rule (windows allowed here).
    projected = df.selectExpr(
        *[f"{sql} AS `{flag_column(rid)}`" for rid, sql in flags.items()]
    )
    # Step 2: aggregate the flags (no windows left to offend the parser).
    counts = rule_count_exprs(rules)
    row = projected.selectExpr(
        *[f"{sql} AS `{rid}`" for rid, sql in counts.items()]
    ).first()
    return {rid: int(row[rid]) for rid in flags}


# =============================================================================
# Pipeline
# =============================================================================
@dataclass
class SilverResult:
    dataset: str
    bronze_rows: int
    silver_rows: int
    quarantine_rows: int
    violations: Dict[str, int]
    silver_target: str
    quarantine_target: str

    @property
    def total_violations(self) -> int:
        return sum(self.violations.values())

    @property
    def conserved(self) -> bool:
        return self.silver_rows + self.quarantine_rows == self.bronze_rows


def read_delta(spark: "SparkSession", target: str, is_path: bool) -> "DataFrame":
    if is_path:
        return spark.read.format("delta").load(target)
    return spark.table(target)


def process_dataset(
    spark: "SparkSession",
    dataset: str,
    rules: Sequence[Dict[str, Any]],
    bronze_schema: Sequence[Column],
    silver_schema: Sequence[Column],
    bronze_cfg: LakehouseConfig,
    silver_cfg: LakehouseConfig,
    quarantine_cfg: LakehouseConfig,
) -> SilverResult:
    table = "gl" if dataset == "gl" else "ap_subledger"
    is_path = bronze_cfg.mode == "path"

    bronze = read_delta(spark, bronze_cfg.target(table), is_path)

    # Deliberately NOT cached. The annotated frame is scanned more than once
    # below, and on a classic cluster `.cache()` would avoid recomputing the
    # window functions behind the uniqueness rules. Serverless compute rejects
    # explicit persistence outright (NOT_SUPPORTED_WITH_SERVERLESS) because it
    # manages its own caching, so the choice is made for us. Leaving the hint
    # in would trade a portability bug for an optimisation the platform
    # already performs - and at this data volume it is not measurable anyway.
    annotated = annotate(bronze, rules)

    bronze_rows = bronze.count()
    clean, quarantined = split(annotated)

    silver_df = to_silver(clean, silver_schema)
    quarantine_df = to_quarantine(quarantined, dataset, bronze_schema)

    silver_target = silver_cfg.target(table)
    quarantine_target = quarantine_cfg.target(table)

    write_delta(silver_df, silver_target, "overwrite", is_path)
    write_delta(quarantine_df, quarantine_target, "overwrite", is_path)

    silver_rows = read_delta(spark, silver_target, is_path).count()
    quarantine_rows = read_delta(spark, quarantine_target, is_path).count()
    violations = rule_violation_counts(bronze, rules)

    result = SilverResult(
        dataset=dataset,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        violations=violations,
        silver_target=silver_target,
        quarantine_target=quarantine_target,
    )

    if not result.conserved:
        raise RuntimeError(
            f"Row conservation failed for {dataset}: bronze had {bronze_rows}, "
            f"silver has {silver_rows} and quarantine has {quarantine_rows} "
            f"({silver_rows + quarantine_rows}). Every row must leave bronze "
            f"through exactly one door."
        )
    return result


def run(
    cfg: LakehouseConfig | None = None,
    spark: "SparkSession" | None = None,
    contracts: Dict[str, Any] | None = None,
) -> List[SilverResult]:
    cfg = cfg or LakehouseConfig.detect()
    spark = spark or get_spark()
    contracts = contracts or load_contracts()

    # Fail before touching data, not halfway through.
    assert_patterns_compile(contracts)
    assert_patterns_are_anchored(contracts)

    bronze_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "bronze")
    silver_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "silver")
    quarantine_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "quarantine")

    if cfg.mode == "catalog":
        for schema in ("silver", "quarantine"):
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{schema}")

    specs = [
        ("gl", BRONZE_GL_SCHEMA, SILVER_GL_SCHEMA),
        ("ap", BRONZE_AP_SCHEMA, SILVER_AP_SCHEMA),
    ]
    return [
        process_dataset(
            spark, dataset,
            contracts["datasets"][dataset]["rules"],
            bronze_schema, silver_schema,
            bronze_cfg, silver_cfg, quarantine_cfg,
        )
        for dataset, bronze_schema, silver_schema in specs
    ]


def dq_score(results: Sequence[SilverResult]) -> float:
    """Share of source rows that passed every contract, as a percentage.

    Defined once, here, so the dashboard and the pipeline cannot disagree about
    what the number means. Denominator is bronze rows - the rows we received,
    not the rows we kept.
    """
    total = sum(r.bronze_rows for r in results)
    passed = sum(r.silver_rows for r in results)
    return round(100.0 * passed / total, 4) if total else 0.0


# =============================================================================
# CLI
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.silver",
        description="Apply data contracts, write silver and quarantine tables.",
    )
    parser.add_argument("--mode", choices=["path", "catalog"], default=None)
    parser.add_argument("--base-path", type=Path, default=None)
    args = parser.parse_args(argv)

    cfg = LakehouseConfig.detect()
    if args.mode:
        cfg = LakehouseConfig(mode=args.mode)
    if args.base_path:
        cfg = LakehouseConfig(cfg.mode, args.base_path, cfg.catalog, cfg.schema)

    results = run(cfg=cfg)

    print(f"Silver layer  (mode={cfg.mode})")
    for r in results:
        print(f"  {r.dataset:<4} bronze {r.bronze_rows:>5}  ->  "
              f"silver {r.silver_rows:>5}  quarantine {r.quarantine_rows:>3}  "
              f"({r.total_violations} violations)")
    print(f"  DQ score {dq_score(results)}%")
    print("  row conservation holds for every dataset")
    return 0


if __name__ == "__main__":
    sys.exit(main())
