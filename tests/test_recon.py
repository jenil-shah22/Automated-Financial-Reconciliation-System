"""Tests for the matcher and the break classifier.

These are the tests that matter most. Everything else in the project is
plumbing around this classification.

Two implementations are covered here. `validate.reconcile` is the pandas
oracle - the specification - and runs end to end in these tests.
`ledgerlens.recon` is the PySpark port, and only its *compiled SQL* is tested
here, because local Spark does not run on the development machine. That is a
stated limitation, not an oversight: the numeric equality of the two engines is
proven on Databricks by notebook 03, and the tests below pin the logic the
notebook then executes.
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
import pytest

from ledgerlens import recon as spark_recon
from ledgerlens.config import (
    ALL_STATUSES,
    STATUS_AMOUNT_MISMATCH,
    STATUS_DUPLICATE_IN_SUBLEDGER,
    STATUS_MATCHED,
    STATUS_MISSING_FROM_GL,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_TIMING_DIFFERENCE,
    load_contracts,
)
from ledgerlens.validate import reconcile

KEY = ["account_code", "vendor_code", "invoice_number"]
TOL = 1.00


def _side(rows):
    """Build a minimal GL or AP frame. rows = (invoice, amount, period)."""
    return pd.DataFrame(
        [
            {
                "account_code": "6010",
                "vendor_code": "V0001",
                "invoice_number": inv,
                "amount": f"{amount:.2f}",
                "fiscal_period": period,
            }
            for inv, amount, period in rows
        ]
    )


def _status(gl_rows, ap_rows, invoice="INV-2026-000001"):
    recon = reconcile(_side(gl_rows), _side(ap_rows), KEY, TOL)
    row = recon[recon["invoice_number"] == invoice]
    assert len(row) == 1, f"expected exactly one recon row, got {len(row)}"
    return row["break_status"].iloc[0]


# =============================================================================
# One case per status
# =============================================================================
def test_identical_rows_match():
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 100.00, "2026-03")],
    ) == STATUS_MATCHED


def test_amount_beyond_tolerance_is_a_mismatch():
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 150.00, "2026-03")],
    ) == STATUS_AMOUNT_MISMATCH


def test_same_amount_later_period_is_a_timing_difference():
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 100.00, "2026-04")],
    ) == STATUS_TIMING_DIFFERENCE


def test_gl_only_is_missing_from_subledger():
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000002", 100.00, "2026-03")],
    ) == STATUS_MISSING_FROM_SUBLEDGER


def test_ap_only_is_missing_from_gl():
    """The unrecorded liability - the break auditors chase."""
    assert _status(
        [("INV-2026-000002", 100.00, "2026-03")],
        [("INV-2026-000001", 100.00, "2026-03")],
    ) == STATUS_MISSING_FROM_GL


def test_two_ap_rows_on_one_key_is_a_duplicate():
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 100.00, "2026-03"),
         ("INV-2026-000001", 100.00, "2026-03")],
    ) == STATUS_DUPLICATE_IN_SUBLEDGER


# =============================================================================
# Tolerance boundary
# =============================================================================
@pytest.mark.parametrize(
    "ap_amount,expected",
    [
        (100.00, STATUS_MATCHED),          # exact
        (100.99, STATUS_MATCHED),          # inside
        (101.00, STATUS_MATCHED),          # exactly at the bound - inclusive
        (101.01, STATUS_AMOUNT_MISMATCH),  # first value outside
        (99.00, STATUS_MATCHED),           # symmetric below
        (98.99, STATUS_AMOUNT_MISMATCH),
    ],
)
def test_tolerance_is_inclusive_and_symmetric(ap_amount, expected):
    """Tolerance is `<=`, and it applies to the absolute difference.

    Pinned as a test because 'is 1.00 a break?' is exactly the kind of question
    that gets answered differently by two people six months apart.
    """
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", ap_amount, "2026-03")],
    ) == expected


# =============================================================================
# Precedence ladder
# =============================================================================
def test_duplicates_outrank_amount_mismatch():
    """Two AP rows with different amounts is still DUPLICATE, not MISMATCH.

    A double-booking where the second copy was keyed slightly differently is
    still a double-booking. Reporting it as an amount difference would send an
    analyst looking for a keying error instead of a duplicate payment.
    """
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 100.00, "2026-03"),
         ("INV-2026-000001", 900.00, "2026-03")],
    ) == STATUS_DUPLICATE_IN_SUBLEDGER


def test_duplicates_outrank_timing():
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 50.00, "2026-04"),
         ("INV-2026-000001", 50.00, "2026-04")],
    ) == STATUS_DUPLICATE_IN_SUBLEDGER


def test_amount_mismatch_outranks_timing():
    """Both amount AND period differ -> AMOUNT_MISMATCH.

    Timing differences are usually benign and self-correcting; amount
    differences are not. Classifying this pair as TIMING would file a real
    value discrepancy under 'will fix itself next month'.
    """
    assert _status(
        [("INV-2026-000001", 100.00, "2026-03")],
        [("INV-2026-000001", 500.00, "2026-04")],
    ) == STATUS_AMOUNT_MISMATCH


def test_three_ap_rows_still_resolve_to_one_key():
    recon = reconcile(
        _side([("INV-2026-000001", 100.00, "2026-03")]),
        _side([("INV-2026-000001", 100.00, "2026-03")] * 3),
        KEY, TOL,
    )
    assert len(recon) == 1
    assert recon["ap_row_count"].iloc[0] == 3


# =============================================================================
# The regression this project is most likely to suffer
# =============================================================================
def test_duplicate_does_not_strand_its_gl_counterpart():
    """Duplicates must be detected BEFORE the join.

    The bug this guards against: join first, then deduplicate. A GL row facing
    two AP rows fans out to two joined rows; dropping one loses the GL side and
    the key gets misfiled as MISSING_FROM_SUBLEDGER. Totals still look
    plausible, so nothing alerts - it just quietly invents an unsupported
    entry and hides a double payment.

    Here: one clean matched key plus one duplicated key. If the bug is present,
    MISSING_FROM_SUBLEDGER appears where it should not.
    """
    gl = _side([
        ("INV-2026-000001", 100.00, "2026-03"),
        ("INV-2026-000002", 200.00, "2026-03"),
    ])
    ap = _side([
        ("INV-2026-000001", 100.00, "2026-03"),
        ("INV-2026-000002", 200.00, "2026-03"),
        ("INV-2026-000002", 200.00, "2026-03"),
    ])
    counts = reconcile(gl, ap, KEY, TOL)["break_status"].value_counts().to_dict()

    assert counts.get(STATUS_MATCHED, 0) == 1
    assert counts.get(STATUS_DUPLICATE_IN_SUBLEDGER, 0) == 1
    assert counts.get(STATUS_MISSING_FROM_SUBLEDGER, 0) == 0
    assert sum(counts.values()) == 2


def test_every_key_gets_exactly_one_status():
    """The taxonomy is a partition: no key missing, no key counted twice."""
    gl = _side([
        ("INV-2026-000001", 100.00, "2026-03"),
        ("INV-2026-000002", 200.00, "2026-03"),
        ("INV-2026-000003", 300.00, "2026-03"),
        ("INV-2026-000005", 500.00, "2026-03"),
    ])
    ap = _side([
        ("INV-2026-000001", 100.00, "2026-03"),
        ("INV-2026-000002", 999.00, "2026-03"),
        ("INV-2026-000003", 300.00, "2026-04"),
        ("INV-2026-000004", 400.00, "2026-03"),
        ("INV-2026-000006", 600.00, "2026-03"),
        ("INV-2026-000006", 600.00, "2026-03"),
    ])
    recon = reconcile(gl, ap, KEY, TOL)

    # 6 distinct keys across both sides
    assert len(recon) == 6
    assert recon["break_status"].notna().all()
    assert recon["invoice_number"].nunique() == 6


def test_matching_is_on_the_full_business_key_not_invoice_alone():
    """Same invoice number under a different account is a different key.

    Invoice numbers are only unique within a vendor in the real world; the
    account dimension is what stops two unrelated postings from tying out by
    coincidence.
    """
    gl = pd.DataFrame([{
        "account_code": "6010", "vendor_code": "V0001",
        "invoice_number": "INV-2026-000001",
        "amount": "100.00", "fiscal_period": "2026-03",
    }])
    ap = pd.DataFrame([{
        "account_code": "6020", "vendor_code": "V0001",
        "invoice_number": "INV-2026-000001",
        "amount": "100.00", "fiscal_period": "2026-03",
    }])
    counts = reconcile(gl, ap, KEY, TOL)["break_status"].value_counts().to_dict()

    assert counts.get(STATUS_MISSING_FROM_SUBLEDGER, 0) == 1
    assert counts.get(STATUS_MISSING_FROM_GL, 0) == 1
    assert counts.get(STATUS_MATCHED, 0) == 0


# =============================================================================
# The precedence ladder, walked exhaustively
# =============================================================================
# The tests above are one case per status plus the interesting collisions. This
# section enumerates the ENTIRE reachable case space and checks the oracle
# against a literal transcription of the documented ladder.
#
# The transcription walks `recon.CLASSIFICATION_LADDER` - the same list the
# PySpark CASE expression is generated from - so if anybody reorders the Spark
# classifier, this test fails against the pandas oracle. That is the cheapest
# available substitute for running both engines side by side, which this machine
# cannot do.
@dataclass(frozen=True)
class Case:
    gl_count: int
    ap_count: int
    difference: float
    periods_differ: bool


# One lambda per rung, in the same shape as recon.status_conditions. Positional:
# each assumes every rung above it already failed.
_LADDER_PREDICATES = {
    STATUS_DUPLICATE_IN_SUBLEDGER: lambda c: c.ap_count > 1,
    STATUS_MISSING_FROM_SUBLEDGER: lambda c: c.gl_count > 0 and c.ap_count == 0,
    STATUS_MISSING_FROM_GL: lambda c: c.ap_count > 0 and c.gl_count == 0,
    STATUS_AMOUNT_MISMATCH: lambda c: abs(c.difference) > TOL,
    STATUS_TIMING_DIFFERENCE: lambda c: c.periods_differ,
}


def _expected_status(case: Case) -> str:
    for status in spark_recon.CLASSIFICATION_LADDER[:-1]:
        if _LADDER_PREDICATES[status](case):
            return status
    return spark_recon.CLASSIFICATION_LADDER[-1]


def _split(total: float, n: int):
    """Spread a key total across n rows, so the aggregate is what we intended."""
    if n == 0:
        return []
    each = round(total / n, 2)
    parts = [each] * n
    parts[0] = round(total - each * (n - 1), 2)
    return parts


_BASE = 100.00
_DELTAS = {"tie": 0.00, "within_tolerance": 0.50, "beyond_tolerance": 5.00}


@pytest.mark.parametrize("gl_count", [0, 1, 2])
@pytest.mark.parametrize("ap_count", [0, 1, 2, 3])
@pytest.mark.parametrize("delta_name", list(_DELTAS))
@pytest.mark.parametrize("periods_differ", [False, True])
def test_ladder_holds_across_the_whole_case_space(
    gl_count, ap_count, delta_name, periods_differ
):
    if gl_count == 0 and ap_count == 0:
        pytest.skip("a key exists because at least one side has it")

    delta = _DELTAS[delta_name]
    gl_total = _BASE if gl_count else 0.00
    ap_total = round(_BASE + delta, 2) if ap_count else 0.00
    ap_period = "2026-04" if periods_differ else "2026-03"

    gl = _side([("INV-2026-000001", amt, "2026-03") for amt in _split(gl_total, gl_count)])
    ap = _side([("INV-2026-000001", amt, ap_period) for amt in _split(ap_total, ap_count)])

    # An empty side still needs the key columns, or groupby has nothing to group.
    if gl.empty:
        gl = _side([("INV-2026-000002", 1.00, "2026-03")])
    if ap.empty:
        ap = _side([("INV-2026-000003", 1.00, "2026-03")])

    result = reconcile(gl, ap, KEY, TOL)
    row = result[result["invoice_number"] == "INV-2026-000001"]
    assert len(row) == 1

    case = Case(
        gl_count=gl_count,
        ap_count=ap_count,
        difference=round(gl_total - ap_total, 2),
        periods_differ=periods_differ,
    )
    assert row["break_status"].iloc[0] == _expected_status(case)


# =============================================================================
# The PySpark port: aggregation
# =============================================================================
def test_aggregation_carries_a_row_count():
    """The single most important assertion in this file.

    An aggregation that sums but does not count produces a perfectly correct
    total for a duplicated key and no way to know the total came from two rows.
    DUPLICATE_IN_SUBLEDGER becomes undetectable, the key count still comes to
    946, and no row-count reconciliation notices that twenty possible double
    payments vanished.

    The row count is not a diagnostic extra. It is the only evidence that
    duplication happened.
    """
    for prefix in ("gl", "ap"):
        exprs = spark_recon.aggregation_exprs(prefix)
        assert exprs[f"{prefix}_row_count"] == "count(*)"


def test_aggregation_sums_amounts_and_takes_the_earliest_period():
    exprs = spark_recon.aggregation_exprs("gl")
    assert exprs["gl_amount"] == "sum(amount)"
    # min, not max: a break is reported in the period it first appeared in -
    # the period whose close it affects. Max would move a break into a later
    # period the moment a second copy arrived.
    assert exprs["gl_fiscal_period"] == "min(fiscal_period)"


def test_aggregation_prefixes_do_not_collide():
    gl = set(spark_recon.aggregation_exprs("gl"))
    ap = set(spark_recon.aggregation_exprs("ap"))
    assert not (gl & ap), "both sides survive the join, so their columns must differ"


def test_no_window_function_inside_an_aggregate():
    """Guards a bug that is only reachable on a cluster.

    `count_if(count(*) OVER (...))` is a Spark parse error, invisible to
    string-level tests unless the structure is asserted. The day-3 build hit
    exactly this in the violation counter; the structural guard is repeated
    here so the recon aggregates cannot reintroduce it.
    """
    for prefix in ("gl", "ap"):
        for sql in spark_recon.aggregation_exprs(prefix).values():
            assert "OVER (" not in sql


def test_row_counts_are_coalesced_before_classification():
    """A full outer join produces NULL counts on the side that is absent.

    NULL poisons every comparison it appears in - `NULL > 1` is NULL, not
    FALSE - so the missing side must become a number before the classifier
    reads it, or one-sided keys would fall through every rung to MATCHED.
    """
    derived = spark_recon.derived_exprs()
    assert derived["gl_row_count"] == "coalesce(gl_row_count, 0)"
    assert derived["ap_row_count"] == "coalesce(ap_row_count, 0)"


def test_difference_treats_a_missing_side_as_zero():
    derived = spark_recon.derived_exprs()
    assert "coalesce(gl_amount, 0)" in derived["amount_difference"]
    assert "coalesce(ap_amount, 0)" in derived["amount_difference"]
    assert derived["abs_amount_difference"].startswith("abs(")


# =============================================================================
# The PySpark port: the compiled classifier
# =============================================================================
def test_tolerance_is_a_decimal_literal_not_a_float():
    """Spark promotes a DECIMAL-vs-DOUBLE comparison to DOUBLE.

    Amounts are DECIMAL(18,2) all the way through precisely so that 101.00 vs
    100.00 is decidable. Writing the tolerance as a floating point literal would
    undo that in the last line of the pipeline, at exactly the boundary the
    tolerance defines.
    """
    literal = spark_recon.tolerance_literal(1.0)
    assert literal == "CAST(1.00 AS DECIMAL(18,2))"


def test_tolerance_comparison_is_strictly_greater_than():
    """At-tolerance is rounding, not a break: 101.00 MATCHED, 101.01 not.

    The pandas side already pins this behaviourally. This pins the SQL, because
    'is 1.00 a break?' is exactly the question that gets answered differently by
    two people six months apart.
    """
    condition = spark_recon.status_conditions(1.0)[STATUS_AMOUNT_MISMATCH]
    assert condition == "abs(amount_difference) > CAST(1.00 AS DECIMAL(18,2))"
    assert ">=" not in condition


def test_duplicate_detection_reads_the_row_count():
    condition = spark_recon.status_conditions(1.0)[STATUS_DUPLICATE_IN_SUBLEDGER]
    assert condition == "ap_row_count > 1"


def test_matched_is_the_residual_and_has_no_condition():
    """MATCHED is what survived every test, not a test of its own."""
    conditions = spark_recon.status_conditions(1.0)
    assert STATUS_MATCHED not in conditions
    assert set(conditions) == set(ALL_STATUSES) - {STATUS_MATCHED}


def test_case_arms_are_emitted_in_precedence_order():
    sql = spark_recon.classification_expr(1.0)
    positions = [sql.index(f"'{status}'") for status in spark_recon.CLASSIFICATION_LADDER]
    assert positions == sorted(positions), "CASE arms are out of ladder order"


def test_duplicate_outranks_everything_in_the_sql():
    sql = spark_recon.classification_expr(1.0)
    assert sql.startswith(
        f"CASE WHEN ap_row_count > 1 THEN '{STATUS_DUPLICATE_IN_SUBLEDGER}'"
    )


def test_amount_outranks_timing_in_the_sql():
    """Value beats timing.

    Timing differences are benign and self-correcting; amount differences are
    not. If the timing arm came first, every mismatch that also shifted period
    would be filed under 'will fix itself next month'. It will not.
    """
    sql = spark_recon.classification_expr(1.0)
    assert sql.index(f"'{STATUS_AMOUNT_MISMATCH}'") < sql.index(
        f"'{STATUS_TIMING_DIFFERENCE}'"
    )


def test_every_status_appears_exactly_once():
    """The taxonomy is a partition, asserted on the generated SQL itself."""
    sql = spark_recon.classification_expr(1.0)
    for status in ALL_STATUSES:
        assert sql.count(f"'{status}'") == 1


def test_matched_is_the_else_branch():
    sql = spark_recon.classification_expr(1.0)
    assert sql.endswith(f"ELSE '{STATUS_MATCHED}' END")


# =============================================================================
# The published ladder must be the executed ladder
# =============================================================================
def test_contracts_yaml_declares_the_ladder_the_classifier_compiles():
    """contracts.yaml is a published policy, not decoration.

    A controller reads `recon.status_precedence` to understand how breaks are
    classified without reading Python. That is only worth something if it is the
    policy that actually runs.
    """
    contracts = load_contracts()
    assert (
        list(contracts["recon"]["status_precedence"])
        == spark_recon.CLASSIFICATION_LADDER
    )


def test_reordered_precedence_is_rejected():
    """The conditions are positional, so a reorder is wrong, not merely different.

    Put TIMING_DIFFERENCE above AMOUNT_MISMATCH and every mismatch that also
    shifted period gets classified as benign - silently, with the totals still
    summing to 946.
    """
    swapped = list(spark_recon.CLASSIFICATION_LADDER)
    i = swapped.index(STATUS_AMOUNT_MISMATCH)
    j = swapped.index(STATUS_TIMING_DIFFERENCE)
    swapped[i], swapped[j] = swapped[j], swapped[i]

    with pytest.raises(ValueError, match="different order"):
        spark_recon.classification_expr(1.0, swapped)


def test_precedence_missing_a_status_is_rejected():
    partial = [s for s in spark_recon.CLASSIFICATION_LADDER
               if s != STATUS_DUPLICATE_IN_SUBLEDGER]
    with pytest.raises(ValueError, match="does not cover"):
        spark_recon.classification_expr(1.0, partial)


def test_unknown_status_in_precedence_is_rejected():
    extra = list(spark_recon.CLASSIFICATION_LADDER) + ["PARTIAL_MATCH"]
    with pytest.raises(ValueError, match="does not cover"):
        spark_recon.classification_expr(1.0, extra)
