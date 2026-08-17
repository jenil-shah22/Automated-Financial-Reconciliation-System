"""The reconciliation engine: match GL against AP, classify every key once.

This is the analytical core. Everything else in the project is plumbing that
gets clean rows to this module and carries its output to a dashboard.

THE ORDER OF OPERATIONS IS THE WHOLE POINT
------------------------------------------
    aggregate each side to one row per business key
      carrying sum(amount), min(fiscal_period) AND row_count
    -> full outer join
    -> classify

Duplicates are detected on the AP side BEFORE the join, by counting rows during
the aggregation. Three realistic alternatives were tested against the generated
data and all three are wrong:

    implementation                              MATCHED  DUPLICATE  total keys
    correct                                         820         20         946
    aggregate with sum(), never count rows          820          0         946
    de-duplicate first, then join                   820         20         966
    join first, drop_duplicates to fix the fan-out  840          0         946

Two of the three preserve the total key count exactly. They lose every
duplicate - twenty possible double payments - and no row-count reconciliation
would notice, because the arithmetic still balances. That is what makes this
the most dangerous bug in the project: it is silent, and it hides the finding
with the most money attached to it.

The middle failure is the subtle one. `sum()` without `count(*)` produces a
perfectly correct total for a duplicated key and no way whatsoever to know the
total came from two rows. The row count is not a diagnostic extra; it is the
only evidence that duplication happened.

WHY THIS MODULE COMPILES SQL STRINGS
------------------------------------
Same reason as quality.py: local Spark does not run on the development machine,
so anything expressed as a `pyspark.sql.Column` object cannot be tested until
it reaches a cluster. Expressed as strings, the exact predicate behind every
status is asserted in CI with no JVM, and only execution needs Spark.

It also makes the classifier auditable. "40 amount mismatches" is an assertion;
`abs(amount_difference) > CAST(1.00 AS DECIMAL(18,2))` printed beside it is the
evidence, and a controller can disagree with it without reading Python.

RELATIONSHIP TO THE PANDAS ORACLE
---------------------------------
`validate.py::reconcile` is the specification, not a rough guide. It is a
separate implementation in a separate engine that shares no code with this one,
and the two must agree on all six counts. Where the engines have genuinely
different semantics - NULL comparison being the sharp one, see
`status_conditions` - the difference is called out in a comment and the reason
they still agree is stated rather than assumed.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Sequence

from .bronze import LakehouseConfig, get_spark
from .config import (
    ALL_STATUSES,
    STATUS_AMOUNT_MISMATCH,
    STATUS_DUPLICATE_IN_SUBLEDGER,
    STATUS_MATCHED,
    STATUS_MISSING_FROM_GL,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_TIMING_DIFFERENCE,
    load_contracts,
)
from .silver import read_delta

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

# Amounts are DECIMAL end to end. See `tolerance_literal` for why the tolerance
# has to be a decimal literal too, and what breaks if it is not.
DECIMAL_TYPE = "DECIMAL(18,2)"

GL_PREFIX = "gl"
AP_PREFIX = "ap"


# =============================================================================
# Step 1 - aggregate each side to one row per business key
# =============================================================================
def aggregation_exprs(prefix: str) -> Dict[str, str]:
    """Output column -> SQL aggregate, for one side of the reconciliation.

    `count(*)` is not optional and not decorative. Without it a key backed by
    two subledger rows is indistinguishable from a key backed by one, and
    DUPLICATE_IN_SUBLEDGER becomes undetectable - see the module docstring for
    what that costs. There is a test asserting this dictionary contains a row
    count, because the tempting simplification is to drop it.

    `min(fiscal_period)` rather than max: a duplicated or split key is reported
    in the period it FIRST appeared in, which is the period whose close it
    affects. Max would move a break into a later period the moment a second
    copy arrived, and a break that changes period on its own is unauditable.

    fiscal_period is a string, so min() is lexicographic - which is exactly
    chronological for zero-padded yyyy-mm, and is the reason the format is
    contract-enforced rather than merely conventional.
    """
    return {
        f"{prefix}_amount": "sum(amount)",
        f"{prefix}_fiscal_period": "min(fiscal_period)",
        f"{prefix}_row_count": "count(*)",
    }


def aggregate_side(
    df: "DataFrame", business_key: Sequence[str], prefix: str
) -> "DataFrame":
    """Collapse one side to exactly one row per business key."""
    from pyspark.sql import functions as F

    exprs = aggregation_exprs(prefix)
    return df.groupBy(*business_key).agg(
        *[F.expr(sql).alias(name) for name, sql in exprs.items()]
    )


# =============================================================================
# Step 2 - derived values, materialised before anything classifies on them
# =============================================================================
def tolerance_literal(tolerance: float) -> str:
    """The tolerance, as a DECIMAL literal.

    The cast is not ceremony. Spark promotes a DECIMAL-vs-DOUBLE comparison to
    DOUBLE, so an amount tolerance written as a floating point literal would
    quietly move the entire reconciliation onto binary floating point at exactly
    the boundary the tolerance defines. Amounts are DECIMAL(18,2) throughout
    precisely so that 101.00 vs 100.00 is decidable; comparing them against a
    double would undo that in the last line of the pipeline.
    """
    return f"CAST({tolerance:.2f} AS {DECIMAL_TYPE})"


def derived_exprs() -> Dict[str, str]:
    """Output column -> SQL, for the values the classifier reads.

    Computed in their own projection rather than inline in the CASE expression.
    Two reasons:

    1. A column aliased in a SELECT is not visible to its siblings in
       open-source Spark. Databricks does support lateral column aliases, so
       the one-step version would work on the cluster and fail on the
       documented local-Spark fallback - a portability bug that only appears
       where it is hardest to debug.
    2. `amount_difference` is read three times (the mismatch test, the stored
       signed value, the stored absolute value). Writing it once means the
       classifier and the reported number cannot drift apart.

    Row counts are coalesced to 0 here, immediately after the outer join, so
    that "no rows on this side" is a number the classifier can compare rather
    than a NULL that would poison every comparison it appears in.
    """
    gl_amount = f"coalesce({GL_PREFIX}_amount, 0)"
    ap_amount = f"coalesce({AP_PREFIX}_amount, 0)"
    return {
        f"{GL_PREFIX}_row_count": f"coalesce({GL_PREFIX}_row_count, 0)",
        f"{AP_PREFIX}_row_count": f"coalesce({AP_PREFIX}_row_count, 0)",
        # The side amounts stay NULL in the output (NULL means "this side has no
        # opinion", 0.00 would mean "this side posted nothing"), but a
        # DIFFERENCE has to be a number, so the missing side is zeroed for the
        # subtraction only.
        "amount_difference": f"({gl_amount} - {ap_amount})",
        "abs_amount_difference": f"abs({gl_amount} - {ap_amount})",
    }


# =============================================================================
# Step 3 - the precedence ladder
# =============================================================================
# The order the conditions below are evaluated in. Declared in code because
# changing it means changing what the conditions MEAN, not just what order they
# run in - see `status_conditions`. contracts.yaml publishes the same ladder for
# a human audience, and `assert_precedence_matches` refuses to run if the two
# disagree, so the documented order and the executed order cannot drift apart.
CLASSIFICATION_LADDER: List[str] = [
    STATUS_DUPLICATE_IN_SUBLEDGER,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_MISSING_FROM_GL,
    STATUS_AMOUNT_MISMATCH,
    STATUS_TIMING_DIFFERENCE,
    STATUS_MATCHED,
]


def status_conditions(tolerance: float) -> Dict[str, str]:
    """Status -> the SQL that is TRUE when a key belongs to it.

    These conditions are POSITIONAL. Each one is written assuming every
    condition above it in CLASSIFICATION_LADDER already failed, which is what
    "first match wins" means and is why the ladder is asserted rather than
    assumed. Read top to bottom:

    1. DUPLICATE_IN_SUBLEDGER - `ap_row_count > 1`
       Structural, and outranks everything. Duplication is a statement about
       how many rows exist, true regardless of amounts. A double-booking whose
       second copy was keyed slightly differently is still a double-booking;
       reporting it as an amount difference sends an analyst hunting a keying
       error instead of a duplicate payment.

    2/3. MISSING_FROM_* - one side absent. Reached only when ap_row_count <= 1,
       so these two are genuinely "one side, no counterpart".

    4. AMOUNT_MISMATCH - value beats timing. Timing differences are benign and
       self-correcting; amount differences are not. Filing a real value
       discrepancy under "timing" files it under "will fix itself next month".
       It will not.
       No presence test is needed here: rungs 2 and 3 removed every one-sided
       key, so anything reaching this line has both sides.

    5. TIMING_DIFFERENCE - amounts tie, periods differ.

       THE ENGINE DIFFERENCE WORTH KNOWING ABOUT. The pandas oracle writes
       `gl_period.fillna("") != ap_period.fillna("")`, comparing NULLs as
       values. SQL propagates: `NULL <> NULL` is NULL, which is not TRUE, so
       the row would fall through to MATCHED. The two engines agree only
       because rungs 2 and 3 guarantee both periods are non-NULL by the time
       this line is evaluated. That correctness argument depends entirely on
       the ladder order, which is the concrete reason the order is checked
       against contracts.yaml on every run instead of being trusted.

    6. MATCHED - the residual. Not a test, an ELSE: whatever survived all five.
    """
    tol = tolerance_literal(tolerance)
    return {
        STATUS_DUPLICATE_IN_SUBLEDGER: f"{AP_PREFIX}_row_count > 1",
        STATUS_MISSING_FROM_SUBLEDGER:
            f"{GL_PREFIX}_row_count > 0 AND {AP_PREFIX}_row_count = 0",
        STATUS_MISSING_FROM_GL:
            f"{AP_PREFIX}_row_count > 0 AND {GL_PREFIX}_row_count = 0",
        # Strictly greater than. At-tolerance is rounding, not a break:
        # 101.00 vs 100.00 is MATCHED, 101.01 is not.
        STATUS_AMOUNT_MISMATCH: f"abs(amount_difference) > {tol}",
        STATUS_TIMING_DIFFERENCE:
            f"{GL_PREFIX}_fiscal_period <> {AP_PREFIX}_fiscal_period",
    }


def assert_precedence_matches(precedence: Sequence[str]) -> None:
    """The published ladder must equal the compiled one.

    contracts.yaml declares `recon.status_precedence` so a controller can read
    the classification policy without reading Python. That declaration is only
    worth anything if it is the policy that actually executes. Since the
    conditions are positional, a reordered YAML would not produce a different
    classification - it would produce a WRONG one, quietly: put
    TIMING_DIFFERENCE above AMOUNT_MISMATCH and every mismatch that also shifted
    period gets filed as benign.

    Same pattern as `assert_patterns_are_anchored` in quality.py - an invariant
    the code depends on is enforced at the top of the run, not hoped for.
    """
    declared = list(precedence)
    if declared == CLASSIFICATION_LADDER:
        return

    missing = [s for s in CLASSIFICATION_LADDER if s not in declared]
    unknown = [s for s in declared if s not in CLASSIFICATION_LADDER]
    if missing or unknown:
        raise ValueError(
            "contracts.yaml recon.status_precedence does not cover the break "
            f"taxonomy - missing: {missing}, unknown: {unknown}"
        )
    raise ValueError(
        "contracts.yaml recon.status_precedence is in a different order than "
        "the classifier was written for. The conditions are positional (each "
        "assumes the ones above it failed), so reordering silently changes what "
        "they mean.\n"
        f"  declared: {declared}\n"
        f"  compiled: {CLASSIFICATION_LADDER}"
    )


def classification_expr(tolerance: float,
                        precedence: Sequence[str] | None = None) -> str:
    """Compile the ladder into one Spark SQL CASE expression.

    Emitted as a single expression rather than five chained `withColumn` calls
    so that "first match wins" is a property of the SQL itself - CASE stops at
    its first true branch - instead of a property of the order somebody happened
    to write the calls in.
    """
    ladder = list(precedence) if precedence is not None else CLASSIFICATION_LADDER
    assert_precedence_matches(ladder)

    conditions = status_conditions(tolerance)
    arms = " ".join(
        f"WHEN {conditions[status]} THEN '{status}'" for status in ladder[:-1]
    )
    return f"CASE {arms} ELSE '{ladder[-1]}' END"


# =============================================================================
# The reconciliation
# =============================================================================
def reconcile(
    gl: "DataFrame",
    ap: "DataFrame",
    business_key: Sequence[str],
    tolerance: float,
    precedence: Sequence[str] | None = None,
) -> "DataFrame":
    """Match, then classify. Mirrors validate.py::reconcile on a second engine."""
    from pyspark.sql import functions as F

    key = list(business_key)

    gl_agg = aggregate_side(gl, key, GL_PREFIX)
    ap_agg = aggregate_side(ap, key, AP_PREFIX)

    # FULL outer, never inner. A key present on only one side is not a row to
    # be discarded for lack of a counterpart - it IS the finding, and in the
    # MISSING_FROM_GL direction it is the unrecorded liability that matters
    # most. An inner join here would report a clean reconciliation by deleting
    # every problem in it.
    #
    # Joining on a list of names uses Spark's USING semantics, which emit one
    # coalesced key column per name rather than two. `assert_no_null_keys`
    # below verifies that rather than trusting it - the behaviour is only
    # observable on a cluster, and a silently uncoalesced key would produce
    # NULL business keys in gold.
    joined = gl_agg.join(ap_agg, on=key, how="full_outer")

    resolved = joined.selectExpr(
        *key,
        f"{GL_PREFIX}_amount",
        f"{AP_PREFIX}_amount",
        f"{GL_PREFIX}_fiscal_period",
        f"{AP_PREFIX}_fiscal_period",
        *[f"{sql} AS {name}" for name, sql in derived_exprs().items()],
    )

    return resolved.withColumn(
        "break_status", F.expr(classification_expr(tolerance, precedence))
    )


def assert_no_null_keys(df: "DataFrame", business_key: Sequence[str]) -> None:
    """No business key column may be NULL after the join.

    Guards the one thing in this module that cannot be checked without a
    cluster: whether the full outer join coalesced its key columns. If it did
    not, AP-only keys would carry NULL account/vendor/invoice values, every
    downstream group-by would collapse them together, and the gold tables would
    look plausible while being wrong. Cheap at this volume, and it fails loudly
    at the point of the mistake instead of three tables later.
    """
    predicate = " OR ".join(f"{col} IS NULL" for col in business_key)
    orphaned = df.filter(predicate).count()
    if orphaned:
        raise RuntimeError(
            f"{orphaned} reconciled key(s) have a NULL business key column. The "
            f"full outer join did not coalesce its key columns, so one-sided "
            f"keys lost their identity."
        )


def status_counts(recon: "DataFrame") -> Dict[str, int]:
    """Key count per status, including the statuses that did not occur.

    Zeros are reported for the same reason quality.py reports rules that fired
    zero times: a status that stops appearing is as much a signal as one that
    starts, and a result set containing only non-empty statuses cannot tell
    "there were none" apart from "that branch stopped being reachable".
    """
    from pyspark.sql import functions as F

    observed = {
        row["break_status"]: int(row["key_count"])
        for row in recon.groupBy("break_status")
        .agg(F.expr("count(*)").alias("key_count"))
        .collect()
    }
    unknown = set(observed) - set(ALL_STATUSES)
    if unknown:
        raise RuntimeError(f"Classifier produced statuses outside the taxonomy: {unknown}")
    return {status: observed.get(status, 0) for status in ALL_STATUSES}


# =============================================================================
# Pipeline
# =============================================================================
@dataclass
class ReconResult:
    key_total: int
    counts: Dict[str, int]
    tolerance: float
    business_key: List[str]

    @property
    def is_a_partition(self) -> bool:
        """Every key resolved to exactly one status - none lost, none doubled."""
        return sum(self.counts.values()) == self.key_total

    @property
    def exceptions(self) -> int:
        return self.key_total - self.counts.get(STATUS_MATCHED, 0)


def build(
    cfg: LakehouseConfig | None = None,
    spark: "SparkSession" | None = None,
    contracts: Dict[str, Any] | None = None,
) -> "DataFrame":
    """Read the silver tables and return the classified reconciliation."""
    cfg = cfg or LakehouseConfig.detect()
    spark = spark or get_spark()
    contracts = contracts or load_contracts()

    silver_cfg = LakehouseConfig(cfg.mode, cfg.base_path, cfg.catalog, "silver")
    is_path = cfg.mode == "path"

    gl = read_delta(spark, silver_cfg.target("gl"), is_path)
    ap = read_delta(spark, silver_cfg.target("ap_subledger"), is_path)

    recon_cfg = contracts["recon"]
    key = list(recon_cfg["business_key"])
    tolerance = float(recon_cfg["amount_tolerance_abs"])

    recon = reconcile(gl, ap, key, tolerance, recon_cfg["status_precedence"])
    assert_no_null_keys(recon, key)
    return recon


def summarise(recon: "DataFrame", contracts: Dict[str, Any]) -> ReconResult:
    counts = status_counts(recon)
    result = ReconResult(
        key_total=recon.count(),
        counts=counts,
        tolerance=float(contracts["recon"]["amount_tolerance_abs"]),
        business_key=list(contracts["recon"]["business_key"]),
    )
    if not result.is_a_partition:
        raise RuntimeError(
            f"The break taxonomy is not a partition: {result.key_total} keys "
            f"reconciled but the statuses account for {sum(counts.values())}. "
            f"Every key must resolve to exactly one status."
        )
    return result


# =============================================================================
# CLI
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.recon",
        description="Reconcile the silver tables and report the break counts.",
    )
    parser.add_argument("--mode", choices=["path", "catalog"], default=None)
    parser.add_argument("--show-sql", action="store_true",
                        help="Print the compiled classifier and exit.")
    args = parser.parse_args(argv)

    contracts = load_contracts()
    tolerance = float(contracts["recon"]["amount_tolerance_abs"])

    if args.show_sql:
        print(classification_expr(tolerance, contracts["recon"]["status_precedence"]))
        return 0

    cfg = LakehouseConfig(mode=args.mode) if args.mode else LakehouseConfig.detect()
    recon = build(cfg=cfg, contracts=contracts)
    result = summarise(recon, contracts)

    print(f"Reconciliation  (tolerance {result.tolerance:.2f}, "
          f"key {'+'.join(result.business_key)})")
    for status, count in result.counts.items():
        print(f"  {status:<24} {count:>6}")
    print(f"  {'-' * 24} {'-' * 6}")
    print(f"  {'business keys':<24} {result.key_total:>6}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
