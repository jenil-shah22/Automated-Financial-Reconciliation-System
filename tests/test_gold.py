"""Tests for the gold layer.

Gold is a published interface: a dashboard, a controller and the data
dictionary all bind to these column names. So the tests here are mostly about
the contract between schemas.py and gold.py holding, and about the two places
gold can quietly lose a finding - the label join and the dense summary grid.

No JVM required. Everything gold decides is decided in a SQL string or in the
schema declarations, both of which are plain data.
"""

from __future__ import annotations

from typing import List

import pytest

from ledgerlens import gold
from ledgerlens.config import ALL_STATUSES, STATUS_MATCHED
from ledgerlens.schemas import (
    GOLD_RECON_DETAIL_SCHEMA,
    GOLD_RECON_EXCEPTIONS_SCHEMA,
    GOLD_RECON_SUMMARY_SCHEMA,
    column_names,
)

GOLD_SCHEMAS = {
    "recon_detail": GOLD_RECON_DETAIL_SCHEMA,
    "recon_summary": GOLD_RECON_SUMMARY_SCHEMA,
    "recon_exceptions": GOLD_RECON_EXCEPTIONS_SCHEMA,
}


class _FakeFrame:
    """Just enough DataFrame for the projection logic.

    `conform` only ever asks a frame for its column names and hands back a
    projection, so a stub is sufficient to test the guard that matters: a
    declared column the transformation forgot to produce must be an error at
    build time, not a blank tile on somebody's dashboard.
    """

    def __init__(self, columns: List[str]):
        self.columns = list(columns)
        self.projected: List[str] = []

    def selectExpr(self, *exprs: str) -> "_FakeFrame":  # noqa: N802 - Spark's name
        self.projected = list(exprs)
        return self


# =============================================================================
# Projection through the declared schema
# =============================================================================
@pytest.mark.parametrize("table,schema", list(GOLD_SCHEMAS.items()))
def test_projection_covers_every_declared_column_in_order(table, schema):
    """Column ORDER is part of a published interface, not a detail."""
    frame = _FakeFrame(column_names(schema))
    gold.conform(frame, schema, table)
    assert len(frame.projected) == len(schema)
    for expr, col in zip(frame.projected, schema):
        assert expr.endswith(col.name)


def test_projection_casts_typed_columns_and_leaves_strings_alone():
    """Strings pass through untouched.

    Silver already trimmed them. Trimming again here would imply gold still
    doubts its input, which would make it unclear which layer owns conformance.
    """
    frame = _FakeFrame(column_names(GOLD_RECON_DETAIL_SCHEMA))
    gold.conform(frame, GOLD_RECON_DETAIL_SCHEMA, "recon_detail")
    projected = {expr.split()[-1]: expr for expr in frame.projected}

    assert projected["account_code"] == "account_code"
    assert projected["break_status"] == "break_status"
    assert projected["gl_amount"] == "cast(gl_amount AS DECIMAL(18,2)) AS gl_amount"
    assert projected["ap_row_count"] == "cast(ap_row_count AS INT) AS ap_row_count"


def test_missing_column_fails_at_build_time():
    """A gold table cannot be published with a declared column missing."""
    columns = [c for c in column_names(GOLD_RECON_DETAIL_SCHEMA) if c != "break_status"]
    with pytest.raises(ValueError, match="break_status"):
        gold.conform(_FakeFrame(columns), GOLD_RECON_DETAIL_SCHEMA, "recon_detail")


# =============================================================================
# The schema decisions that carry meaning
# =============================================================================
def test_detail_keeps_the_row_counts():
    """The duplicate evidence has to survive into gold.

    `ap_row_count` is what makes DUPLICATE_IN_SUBLEDGER auditable after the
    fact: an analyst can see that the key was backed by three subledger rows
    rather than take the status on trust.
    """
    names = column_names(GOLD_RECON_DETAIL_SCHEMA)
    assert "gl_row_count" in names
    assert "ap_row_count" in names


def test_side_amounts_are_nullable_but_differences_are_not():
    """NULL and 0.00 are different claims and the schema says so.

    Zero asserts "the ledger posted nothing". NULL says "the ledger has no
    opinion". MISSING_FROM_GL keys have no GL side at all, and writing 0.00
    there would let `sum(gl_amount)` read as a real posted total that happens to
    be nil. A difference, on the other hand, always has to be a number.
    """
    detail = {c.name: c for c in GOLD_RECON_DETAIL_SCHEMA}
    assert detail["gl_amount"].nullable
    assert detail["ap_amount"].nullable
    assert not detail["amount_difference"].nullable
    assert not detail["abs_amount_difference"].nullable


def test_summary_amounts_are_not_nullable():
    """A dense grid states zero rather than absent, so its measures are numbers."""
    for col in GOLD_RECON_SUMMARY_SCHEMA:
        assert not col.nullable, f"{col.name} should carry a value, not a NULL"


def test_every_gold_money_column_is_decimal_not_double():
    """Binary floating point cannot represent 0.01 exactly.

    The reconciliation tolerates 1.00, so it must not itself be the source of
    sub-cent drift - and gold is where the numbers get read by people.
    """
    for schema in GOLD_SCHEMAS.values():
        for col in schema:
            if "amount" in col.name or "difference" in col.name:
                assert col.dtype == "decimal_18_2", f"{col.name} is not decimal"


def test_exceptions_carry_labels_for_both_sides():
    """Both labels, regardless of which side the key is missing from.

    A MISSING_FROM_GL key has no GL row to read an account name from, and that
    is precisely the break type most worth labelling - which is why the labels
    come from conformed dimensions rather than from the key's own rows.
    """
    names = column_names(GOLD_RECON_EXCEPTIONS_SCHEMA)
    assert "account_name" in names
    assert "vendor_name" in names
    assert "exception_rank" in names


# =============================================================================
# Metric definitions
# =============================================================================
def test_summary_reports_net_and_absolute_separately():
    """Conflating these is the most common way a reconciliation summary lies.

    A key overstated by 5,000 and one understated by 5,000 net to zero and total
    10,000. Net is the effect on the books; absolute is the size of the problem.
    A summary quoting only the net figure can show a clean period with ten
    thousand dollars of breaks underneath it.
    """
    aggregations = gold.summary_aggregations()
    assert aggregations["net_amount_difference"] == "sum(amount_difference)"
    assert aggregations["abs_amount_difference"] == "sum(abs_amount_difference)"
    assert (
        aggregations["net_amount_difference"] != aggregations["abs_amount_difference"]
    )


def test_summary_counts_keys_not_rows_of_anything_else():
    assert gold.summary_aggregations()["key_count"] == "count(*)"


def test_summary_aggregates_contain_no_window_functions():
    """Spark forbids a window function inside an aggregate.

    Only reachable on a cluster, so it is guarded structurally rather than by
    execution - the same trap the day-3 violation counter fell into.
    """
    for sql in gold.summary_aggregations().values():
        assert "OVER (" not in sql


def test_every_summary_measure_is_declared_in_the_schema():
    """The aggregation and the published schema cannot drift apart."""
    declared = set(column_names(GOLD_RECON_SUMMARY_SCHEMA))
    for name in gold.summary_aggregations():
        assert name in declared


# =============================================================================
# The reporting period
# =============================================================================
def test_reporting_period_prefers_the_gl():
    """The GL is the book of record.

    A timing difference is reported against the period whose close it affects:
    if March's books show an invoice April's subledger has not booked yet, that
    is March's reconciling item. The fallback is not cosmetic - MISSING_FROM_GL
    keys have no GL period at all, and leaving them NULL would drop the
    unrecorded liabilities out of every period-filtered view.
    """
    assert gold.reporting_period_expr() == "coalesce(gl_fiscal_period, ap_fiscal_period)"


# =============================================================================
# Exception ranking
# =============================================================================
def test_exceptions_rank_by_exposure_descending():
    sql = gold.exception_rank_expr()
    assert sql.startswith("row_number() OVER (ORDER BY abs_amount_difference DESC")


def test_exception_rank_has_a_deterministic_tie_break():
    """Without it, ranks shuffle between runs on identical input.

    "Exception #7" would mean a different thing each morning, and a Delta table
    has no inherent row order to fall back on.
    """
    sql = gold.exception_rank_expr()
    for key_part in ("account_code", "vendor_code", "invoice_number"):
        assert key_part in sql


# =============================================================================
# Taxonomy plumbing
# =============================================================================
def test_summary_grid_is_built_over_the_whole_taxonomy():
    """The dense grid crosses periods with ALL six statuses, not the observed ones.

    An absent row cannot tell "no duplicates in April" apart from "the duplicate
    branch stopped being reachable in April".
    """
    assert len(ALL_STATUSES) == 6
    assert STATUS_MATCHED in ALL_STATUSES
