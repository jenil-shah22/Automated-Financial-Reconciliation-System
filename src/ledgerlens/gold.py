"""Gold layer: the three tables anybody outside this repo actually reads.

WHAT GOLD IS FOR
----------------
Bronze preserved the evidence, silver applied the contract, recon found the
breaks. Gold shapes that result for consumption, and its contract is different
from every layer above it: gold is a **published interface**. A dashboard, a
controller and the data dictionary all bind to these column names, so the
schemas are declared in schemas.py and every table is projected through its
declaration. A column that is not declared cannot reach a gold table, and a
declared column that the transformation forgot to produce is an error at build
time rather than a blank tile on somebody's dashboard.

THE THREE TABLES, AND WHY THREE
-------------------------------
    recon_detail      one row per business key - the grain of the analysis
    recon_summary     counts and value by period and status - the scorecard
    recon_exceptions  the non-MATCHED keys, labelled and ranked - the worklist

Summary is a pure aggregate of detail and could be a view. It is materialised
because it is what a dashboard queries on every filter change, and because the
distinction it draws between NET and ABSOLUTE difference is a metric definition
that belongs in one place rather than in every chart that needs it.

Exceptions is not just `WHERE break_status <> 'MATCHED'`. It is the join to the
human-readable labels, and that join is the one place in the pipeline where a
cosmetic operation could silently delete a finding - see `_label_dimension`.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Sequence

from .bronze import LakehouseConfig, get_spark, write_delta
from .config import ALL_STATUSES, STATUS_MATCHED, load_contracts
from .recon import build as build_recon
from .recon import summarise
from .schemas import (
    GOLD_RECON_DETAIL_SCHEMA,
    GOLD_RECON_EXCEPTIONS_SCHEMA,
    GOLD_RECON_SUMMARY_SCHEMA,
    Column,
    to_sql_type,
)
from .silver import read_delta

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

UNKNOWN_LABEL = "(unlabelled)"


# =============================================================================
# Projection through the declared schema
# =============================================================================
def projection_for(schema: Sequence[Column]) -> List[str]:
    """SQL projecting a frame onto a declared gold schema, in declared order.

    Strings pass through untouched - silver already trimmed them, and trimming
    twice would imply this layer still doubts its input. Everything else is
    cast to its declared type, which for the amount columns means narrowing the
    aggregate back to DECIMAL(18,2). That narrowing is safe rather than
    hopeful: a key sums at most a handful of invoice lines, each contract-bound
    to +/- 1bn, so the total cannot approach the 10^16 the type allows.
    """
    out = []
    for col in schema:
        if col.dtype == "string":
            out.append(col.name)
        else:
            out.append(f"cast({col.name} AS {to_sql_type(col.dtype)}) AS {col.name}")
    return out


def conform(df: "DataFrame", schema: Sequence[Column], table: str) -> "DataFrame":
    """Project onto the declared schema, failing loudly on a missing column."""
    missing = [c.name for c in schema if c.name not in df.columns]
    if missing:
        raise ValueError(
            f"gold.{table} is declared with columns the transformation did not "
            f"produce: {missing}. Either the transformation or schemas.py is "
            f"wrong - a gold table cannot be published with a column missing."
        )
    return df.selectExpr(*projection_for(schema))


# =============================================================================
# recon_detail
# =============================================================================
def reporting_period_expr() -> str:
    """Which period a break belongs to when the two sides disagree.

    The GL period, falling back to the subledger's when there is no GL side.

    The GL is the book of record, so a timing difference is reported against
    the period whose close it affects - if March's books show an invoice April's
    subledger has not booked yet, that is March's reconciling item. Reporting it
    in April would move the break out of the period somebody is trying to close,
    which is the one period they need to see it in.

    The fallback is not cosmetic: MISSING_FROM_GL keys have no GL period at all,
    and those are the unrecorded liabilities. Leaving them NULL would drop the
    most important break type out of every period-filtered view on the
    dashboard.
    """
    return "coalesce(gl_fiscal_period, ap_fiscal_period)"


def build_detail(recon: "DataFrame") -> "DataFrame":
    """One row per business key, at the grain the analysis was performed at."""
    from pyspark.sql import functions as F

    detail = recon.withColumn("fiscal_period", F.expr(reporting_period_expr()))
    return conform(detail, GOLD_RECON_DETAIL_SCHEMA, "recon_detail")


# =============================================================================
# recon_summary
# =============================================================================
def summary_aggregations() -> Dict[str, str]:
    """Output column -> SQL aggregate for the scorecard.

    NET vs ABSOLUTE is a metric definition, not a naming preference, and
    conflating them is the most common way a reconciliation summary lies.

      net_amount_difference  sums the SIGNED differences. A key overstated by
                             5,000 and one understated by 5,000 net to zero.
                             This is the effect on the books.
      abs_amount_difference  sums the ABSOLUTE differences. Those same two keys
                             total 10,000. This is the size of the problem -
                             the number to quote as "value under investigation".

    A summary reporting only the net figure can show a clean period while ten
    thousand dollars of breaks sit under it. Both are here, named so they cannot
    be mistaken for each other, and defined exactly once.
    """
    return {
        "key_count": "count(*)",
        "gl_amount": "sum(coalesce(gl_amount, 0))",
        "ap_amount": "sum(coalesce(ap_amount, 0))",
        "net_amount_difference": "sum(amount_difference)",
        "abs_amount_difference": "sum(abs_amount_difference)",
    }


def build_summary(spark: "SparkSession", detail: "DataFrame") -> "DataFrame":
    """Counts and value by period and status, with the empty cells stated.

    The grid is DENSE: every observed period is crossed with all six statuses,
    and combinations that did not occur are written as zero rather than left
    out. Same reasoning as quality.py reporting rules that fired zero times -
    an absent row cannot tell "no duplicates in April" apart from "the duplicate
    branch stopped being reachable in April". It also keeps a dashboard's
    legend, colours and axis stable when a status empties out, instead of
    silently re-ordering the chart.

    Densifying cannot change any total: the added rows are zeros, and the build
    asserts that key_count still sums to the number of business keys.
    """
    from pyspark.sql import functions as F

    aggregations = summary_aggregations()
    observed = detail.groupBy("fiscal_period", "break_status").agg(
        *[F.expr(sql).alias(name) for name, sql in aggregations.items()]
    )

    periods = detail.select("fiscal_period").distinct()
    statuses = spark.createDataFrame(
        [(status,) for status in ALL_STATUSES], "break_status string"
    )
    # Explicit crossJoin: the implicit form is blocked by default in Spark, and
    # this one is intentional and tiny (periods x 6).
    grid = periods.crossJoin(statuses)

    filled = grid.join(observed, on=["fiscal_period", "break_status"], how="left")
    zeroed = filled.selectExpr(
        "fiscal_period",
        "break_status",
        *[f"coalesce({name}, 0) AS {name}" for name in aggregations],
    )
    return conform(zeroed, GOLD_RECON_SUMMARY_SCHEMA, "recon_summary")


# =============================================================================
# recon_exceptions
# =============================================================================
def _label_dimension(
    df: "DataFrame", code_column: str, name_column: str
) -> "DataFrame":
    """One label per code, deterministically.

    `min(name)` rather than `first(name)`: first() has no defined result when a
    code carries two spellings, so the gold table would change between runs on
    identical input. min() is an arbitrary tie-break, but it is the SAME
    arbitrary tie-break every time, and reproducibility is worth more here than
    picking the "right" name - a display label is not evidence.

    The aggregation also guarantees one row per code, which is what stops the
    enrichment join from fanning out and inventing exceptions. (A code carrying
    two different names is a real conformance defect; detecting it belongs in
    the DQ scorecard on day 5, not in a join that is trying to render a label.)
    """
    from pyspark.sql import functions as F

    return df.groupBy(code_column).agg(F.expr(f"min({name_column})").alias(name_column))


def exception_rank_expr() -> str:
    """Rank by exposure, with a deterministic tie-break.

    Materialised as a column because a Delta table has no inherent row order -
    sorting at write time does not survive a read, so "the top twenty breaks"
    has to be a value someone can filter on rather than a hope about row order.

    The business key is the tie-break so two breaks of identical value always
    rank in the same order; without it the ranks would shuffle between runs and
    "exception #7" would mean a different thing each morning.

    No PARTITION BY, so Spark moves the whole frame to one partition and says
    so in a warning. That is acceptable here and only here: the frame is the
    exception list, ~126 rows, and a global ranking is inherently un-partitioned.
    """
    return (
        "row_number() OVER (ORDER BY abs_amount_difference DESC, "
        "account_code, vendor_code, invoice_number)"
    )


def build_exceptions(
    detail: "DataFrame", gl_silver: "DataFrame", ap_silver: "DataFrame"
) -> "DataFrame":
    """Non-MATCHED keys, labelled for humans and ranked by exposure.

    The labels come from opposite sides: only the GL carries account_name, only
    the subledger carries vendor_name. So they are looked up from conformed
    dimensions built across ALL silver rows, not from the key's own rows - a
    MISSING_FROM_GL key has no GL row to read an account name from, and that is
    exactly the break type most worth labelling.

    Both joins are LEFT, and this is the important line in the module. An inner
    join would drop any exception whose code has no label, which means a
    cosmetic lookup would delete a finding - a break disappearing from the
    worklist because nobody maintained a vendor name. A missing label is
    rendered as "(unlabelled)" and the row survives. Enrichment must never
    change the population; the build asserts the row count is unchanged by it.
    """
    from pyspark.sql import functions as F

    accounts = _label_dimension(gl_silver, "account_code", "account_name")
    vendors = _label_dimension(ap_silver, "vendor_code", "vendor_name")

    exceptions = detail.filter(F.col("break_status") != STATUS_MATCHED)
    before = exceptions.count()

    labelled = (
        exceptions.join(accounts, on="account_code", how="left")
        .join(vendors, on="vendor_code", how="left")
        .withColumn("account_name",
                    F.expr(f"coalesce(account_name, '{UNKNOWN_LABEL}')"))
        .withColumn("vendor_name",
                    F.expr(f"coalesce(vendor_name, '{UNKNOWN_LABEL}')"))
    )

    after = labelled.count()
    if after != before:
        raise RuntimeError(
            f"Label enrichment changed the exception population: {before} keys "
            f"before, {after} after. A display attribute must never add or "
            f"remove a finding - check the label dimensions for duplicate codes."
        )

    ranked = labelled.withColumn("exception_rank", F.expr(exception_rank_expr()))
    return conform(ranked, GOLD_RECON_EXCEPTIONS_SCHEMA, "recon_exceptions").orderBy(
        "exception_rank"
    )


# =============================================================================
# Pipeline
# =============================================================================
@dataclass
class GoldResult:
    key_total: int
    counts: Dict[str, int]
    exception_rows: int
    summary_rows: int
    summary_key_total: int
    targets: Dict[str, str] = field(default_factory=dict)

    @property
    def summary_reconciles(self) -> bool:
        """The scorecard must add up to the detail it summarises."""
        return self.summary_key_total == self.key_total

    @property
    def exceptions_reconcile(self) -> bool:
        return self.exception_rows == self.key_total - self.counts.get(STATUS_MATCHED, 0)


def run(
    cfg: LakehouseConfig | None = None,
    spark: "SparkSession" | None = None,
    contracts: Dict[str, Any] | None = None,
) -> GoldResult:
    cfg = cfg or LakehouseConfig.detect()
    spark = spark or get_spark()
    contracts = contracts or load_contracts()

    is_path = cfg.mode == "path"
    silver_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "silver")
    gold_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "gold")

    if cfg.mode == "catalog":
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.gold")

    # Every frame below is lazy, and the controls scattered through this build
    # each force the recon chain to run again. Deliberately not cached: serverless
    # rejects explicit persistence (NOT_SUPPORTED_WITH_SERVERLESS) because it
    # manages its own, and at 1,885 silver rows the recomputation is not
    # measurable. Trading a portability bug for an optimisation the platform
    # already performs would be a bad deal.
    recon = build_recon(cfg=cfg, spark=spark, contracts=contracts)
    recon_result = summarise(recon, contracts)

    detail = build_detail(recon)
    summary = build_summary(spark, detail)
    exceptions = build_exceptions(
        detail,
        read_delta(spark, silver_cfg.target("gl"), is_path),
        read_delta(spark, silver_cfg.target("ap_subledger"), is_path),
    )

    targets = {}
    for table, df in (("recon_detail", detail),
                      ("recon_summary", summary),
                      ("recon_exceptions", exceptions)):
        target = gold_cfg.target(table)
        write_delta(df, target, "overwrite", is_path)
        targets[table] = target

    # Read the counts back from the written tables rather than from the frames
    # that produced them. The point of these controls is to prove what LANDED,
    # and a lazy DataFrame re-evaluated in place would only prove the plan
    # agrees with itself.
    written_summary = read_delta(spark, targets["recon_summary"], is_path)
    summary_key_total = int(
        written_summary.selectExpr("sum(key_count) AS total").first()["total"] or 0
    )

    result = GoldResult(
        key_total=recon_result.key_total,
        counts=recon_result.counts,
        exception_rows=read_delta(spark, targets["recon_exceptions"], is_path).count(),
        summary_rows=written_summary.count(),
        summary_key_total=summary_key_total,
        targets=targets,
    )

    if not result.summary_reconciles:
        raise RuntimeError(
            f"gold.recon_summary does not reconcile to gold.recon_detail: "
            f"{result.summary_key_total} keys summarised, {result.key_total} keys "
            f"in detail. An aggregate that does not tie to its own source is "
            f"worse than no aggregate."
        )
    if not result.exceptions_reconcile:
        raise RuntimeError(
            f"gold.recon_exceptions has {result.exception_rows} rows but detail "
            f"reports {result.key_total - result.counts.get(STATUS_MATCHED, 0)} "
            f"non-matched keys."
        )
    return result


# =============================================================================
# CLI
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.gold",
        description="Build the gold reconciliation tables.",
    )
    parser.add_argument("--mode", choices=["path", "catalog"], default=None)
    args = parser.parse_args(argv)

    cfg = LakehouseConfig(mode=args.mode) if args.mode else LakehouseConfig.detect()
    result = run(cfg=cfg)

    print(f"Gold layer  (mode={cfg.mode})")
    for status, count in result.counts.items():
        print(f"  {status:<24} {count:>6}")
    print(f"  {'-' * 24} {'-' * 6}")
    print(f"  {'business keys':<24} {result.key_total:>6}")
    print(f"  {'exceptions':<24} {result.exception_rows:>6}")
    print(f"  {'summary rows':<24} {result.summary_rows:>6}")
    for table, target in result.targets.items():
        print(f"  {table:<24} -> {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
