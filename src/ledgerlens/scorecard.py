"""Day 5: the data-quality scorecard.

WHAT THIS IS FOR
----------------
The pipeline already knows its own quality - silver counts what it rejected and
why. This module turns that into two gold tables a dashboard can bind to, so
the scorecard is a *queryable artefact* rather than a number somebody read off
a notebook once and pasted into a slide.

WHERE THE NUMBERS COME FROM, AND WHY IT MATTERS
-----------------------------------------------
Per-rule counts are derived from the **quarantine table**, by exploding the
`_failed_rule_ids` the pipeline stamped onto each rejected row. They are not
recomputed by re-running the predicates.

That is a deliberate choice and the opposite of the obvious one. Re-running the
rules would produce a second opinion, and a scorecard whose numbers are a second
opinion can disagree with the pipeline that actually rejected the rows. Reading
the recorded ids means the scorecard reports what *happened*, and it cannot
drift, because it is reading the pipeline's own output.

The rule CATALOGUE - id, column, check type, description, and the SQL predicate
itself - does come from `quality.py`. So each row of the rule scorecard carries
the count and the exact logic that produced it, side by side. "GL_NONZERO_AMOUNT
rejected 2 rows" is an assertion; the predicate in the next column is the
evidence, and a controller can disagree with it without reading Python.

THE TWO GRAINS
--------------
    dq_scorecard        one row per dataset  - "is this fit to reconcile?"
    dq_rule_scorecard   one row per rule     - "what is wrong with it?"

Collapsing them into one table would force one of the two questions to be
answered by a filter, and filters are where dashboard definitions go to die.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Sequence

from .bronze import LakehouseConfig, get_spark, write_delta
from .config import load_contracts
from .gold import conform
from .quality import RULE_ID_SEPARATOR, predicate_for, reject_rules, sql_string
from .schemas import (
    GOLD_DQ_RULE_SCORECARD_SCHEMA,
    GOLD_DQ_SCORECARD_SCHEMA,
    to_spark_schema,
)
from .silver import read_delta

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

DATASET_TABLES = {"gl": "gl", "ap": "ap_subledger"}


# =============================================================================
# The rule catalogue - pure Python, so it is testable without a JVM
# =============================================================================
def rule_catalogue_rows(contracts: Dict[str, Any]) -> List[tuple]:
    """Every declared rule as a row, in (dataset, id) order.

    Includes rules that never fire. A scorecard listing only the rules that
    rejected something cannot distinguish "the data is clean" from "that check
    silently stopped running", and the second is the one that hurts.

    `description` is squashed to a single line because contracts.yaml uses YAML
    folded blocks, and a newline inside a dashboard cell renders as a broken row
    rather than as a paragraph.
    """
    rows: List[tuple] = []
    for dataset in sorted(contracts.get("datasets", {})):
        for rule in reject_rules(contracts["datasets"][dataset]["rules"]):
            rows.append(
                (
                    rule["id"],
                    dataset,
                    rule["column"],
                    rule["check"],
                    rule.get("severity", "reject"),
                    " ".join(str(rule.get("description", "")).split()),
                    predicate_for(rule),
                )
            )
    return rows


def catalogue_spark_schema():
    """Schema for the catalogue before violation counts are joined on."""
    return to_spark_schema(
        [c for c in GOLD_DQ_RULE_SCORECARD_SCHEMA if c.name != "rows_rejected"]
    )


# =============================================================================
# Violation counts, read back out of the quarantine table
# =============================================================================
def violations_sql(quarantine_tables: Sequence[str]) -> str:
    """SQL counting rejections per rule, from the ids the pipeline recorded.

    `explode` turns one quarantined row carrying two rule ids into two rows, so
    a row that breached two rules counts once against each - which is exactly
    why violations exceed quarantined rows, and why the two numbers must never
    be used interchangeably.

    `explode` is a generator, legal in a projection. The counting then happens
    in an outer aggregate over its output. Same two-step shape the uniqueness
    rules needed in silver, for the same reason: Spark is strict about what may
    appear inside an aggregate.

    The union is built from the table list rather than hard-coded so adding a
    third source dataset is a config change, not a SQL edit.
    """
    if not quarantine_tables:
        raise ValueError("No quarantine tables supplied - nothing to score.")

    separator = sql_string(f"[{RULE_ID_SEPARATOR}]")
    parts = [
        f"SELECT explode(split(_failed_rule_ids, {separator})) AS rule_id "
        f"FROM {table}"
        for table in quarantine_tables
    ]
    union = "\n  UNION ALL\n  ".join(parts)
    return (
        "SELECT rule_id, count(*) AS rows_rejected\n"
        f"FROM (\n  {union}\n)\n"
        "GROUP BY rule_id"
    )


def dq_score_expr(passed: str = "rows_passed",
                  received: str = "rows_received") -> str:
    """The DQ score, defined in exactly one place.

    Double arithmetic, then rounded to 4dp. Money is DECIMAL everywhere in this
    project because binary floating point cannot represent 0.01 exactly - but a
    percentage is a ratio for display, not a monetary amount, and computing it
    in double is what makes it byte-identical to the pandas oracle's
    `round(100.0 * passed / received, 4)`. Matching the oracle matters more here
    than a precision argument that does not apply.

    Guarded against a zero denominator: an empty extract should score 0 and be
    visible on the dashboard, not divide by zero and take the job down.
    """
    return (
        f"CASE WHEN {received} = 0 THEN 0.0 "
        f"ELSE round(100 * cast({passed} AS DOUBLE) / {received}, 4) END"
    )


# =============================================================================
# Builders
# =============================================================================
def build_rule_scorecard(
    spark: "SparkSession",
    contracts: Dict[str, Any],
    quarantine_tables: Sequence[str],
) -> "DataFrame":
    """Catalogue LEFT JOIN counts. Left, so silent rules keep their row."""
    from pyspark.sql import functions as F

    catalogue = spark.createDataFrame(
        rule_catalogue_rows(contracts), catalogue_spark_schema()
    )
    violations = spark.sql(violations_sql(quarantine_tables))

    # Joined on rule_id alone: ids are globally unique across datasets, asserted
    # by config._assert_rule_ids_unique at load time. Joining on the dataset too
    # would add a second chance for a naming mismatch to silently produce zeros.
    joined = catalogue.join(violations, on="rule_id", how="left").withColumn(
        "rows_rejected", F.expr("coalesce(rows_rejected, 0)")
    )
    return conform(joined, GOLD_DQ_RULE_SCORECARD_SCHEMA, "dq_rule_scorecard")


def build_scorecard(spark: "SparkSession", rows: Sequence[tuple]) -> "DataFrame":
    """Per-dataset headline numbers from counts already measured."""
    schema = to_spark_schema(
        [c for c in GOLD_DQ_SCORECARD_SCHEMA if c.name != "dq_score_pct"]
    )
    frame = spark.createDataFrame(list(rows), schema)
    scored = frame.selectExpr("*", f"{dq_score_expr()} AS dq_score_pct")
    return conform(scored, GOLD_DQ_SCORECARD_SCHEMA, "dq_scorecard")


# =============================================================================
# Pipeline
# =============================================================================
@dataclass
class ScorecardResult:
    rows_received: int
    rows_passed: int
    rows_quarantined: int
    rule_violations: int
    dq_score_pct: float
    by_rule: Dict[str, int] = field(default_factory=dict)
    targets: Dict[str, str] = field(default_factory=dict)

    @property
    def conserved(self) -> bool:
        return self.rows_passed + self.rows_quarantined == self.rows_received


def run(
    cfg: LakehouseConfig | None = None,
    spark: "SparkSession" | None = None,
    contracts: Dict[str, Any] | None = None,
) -> ScorecardResult:
    cfg = cfg or LakehouseConfig.detect()
    spark = spark or get_spark()
    contracts = contracts or load_contracts()

    is_path = cfg.mode == "path"
    bronze_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "bronze")
    silver_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "silver")
    quarantine_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "quarantine")
    gold_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "gold")

    if cfg.mode == "catalog":
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.gold")

    dataset_rows: List[tuple] = []
    for dataset, table in DATASET_TABLES.items():
        received = read_delta(spark, bronze_cfg.target(table), is_path).count()
        passed = read_delta(spark, silver_cfg.target(table), is_path).count()
        quarantined_df = read_delta(spark, quarantine_cfg.target(table), is_path)
        quarantined = quarantined_df.count()
        violations = int(
            quarantined_df.selectExpr(
                "coalesce(sum(_failed_rule_count), 0) AS total"
            ).first()["total"]
        )
        dataset_rows.append(
            (
                dataset,
                contracts["datasets"][dataset].get("label", dataset),
                received,
                passed,
                quarantined,
                violations,
            )
        )

    scorecard = build_scorecard(spark, dataset_rows)

    # The rule scorecard reads the quarantine tables through SQL, so they need
    # names the session can resolve. In path mode there are no table names, so
    # a temp view stands in - the generated SQL is identical either way, which
    # is the point of compiling to strings rather than to Column objects.
    quarantine_tables = []
    for dataset, table in DATASET_TABLES.items():
        if is_path:
            view = f"_ll_quarantine_{dataset}"
            read_delta(spark, quarantine_cfg.target(table), True).createOrReplaceTempView(view)
            quarantine_tables.append(view)
        else:
            quarantine_tables.append(quarantine_cfg.target(table))

    rule_scorecard = build_rule_scorecard(spark, contracts, quarantine_tables)

    targets = {}
    for name, df in (("dq_scorecard", scorecard),
                     ("dq_rule_scorecard", rule_scorecard)):
        target = gold_cfg.target(name)
        write_delta(df, target, "overwrite", is_path)
        targets[name] = target

    # Read back what landed, not the plan that produced it.
    written = read_delta(spark, targets["dq_scorecard"], is_path)
    totals = written.selectExpr(
        "sum(rows_received) AS received",
        "sum(rows_passed) AS passed",
        "sum(rows_quarantined) AS quarantined",
        "sum(rule_violations) AS violations",
    ).first()

    written_rules = read_delta(spark, targets["dq_rule_scorecard"], is_path)
    by_rule = {
        row["rule_id"]: int(row["rows_rejected"])
        for row in written_rules.collect()
    }

    result = ScorecardResult(
        rows_received=int(totals["received"]),
        rows_passed=int(totals["passed"]),
        rows_quarantined=int(totals["quarantined"]),
        rule_violations=int(totals["violations"]),
        # Overall score: sum the numerators and denominators. Averaging the
        # per-dataset scores would weight 940 GL rows equally with 969 AP rows
        # and produce a number that is not the share of rows that passed.
        dq_score_pct=round(
            100.0 * int(totals["passed"]) / int(totals["received"]), 4
        )
        if int(totals["received"])
        else 0.0,
        by_rule=by_rule,
        targets=targets,
    )

    if not result.conserved:
        raise RuntimeError(
            f"Row conservation failed across the scorecard: "
            f"{result.rows_passed} + {result.rows_quarantined} != "
            f"{result.rows_received}."
        )
    return result


# =============================================================================
# CLI
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.scorecard",
        description="Build the gold data-quality scorecard tables.",
    )
    parser.add_argument("--mode", choices=["path", "catalog"], default=None)
    parser.add_argument("--show-sql", action="store_true",
                        help="Print the violation-count SQL and exit.")
    args = parser.parse_args(argv)

    if args.show_sql:
        print(violations_sql(["quarantine.gl", "quarantine.ap_subledger"]))
        return 0

    cfg = LakehouseConfig(mode=args.mode) if args.mode else LakehouseConfig.detect()
    result = run(cfg=cfg)

    print(f"DQ scorecard  (mode={cfg.mode})")
    print(f"  rows received     {result.rows_received:>6}")
    print(f"  rows passed       {result.rows_passed:>6}")
    print(f"  rows quarantined  {result.rows_quarantined:>6}")
    print(f"  rule violations   {result.rule_violations:>6}")
    print(f"  DQ score          {result.dq_score_pct:>6}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
