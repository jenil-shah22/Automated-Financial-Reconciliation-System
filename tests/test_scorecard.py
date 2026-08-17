"""Tests for the data-quality scorecard.

No JVM required: the catalogue is built in pure Python and the violation count
is a SQL string, for the same reason the rest of the project works that way -
local Spark does not run on the development machine, so anything that can only
be checked on a cluster is effectively untested until it gets there.
"""

from __future__ import annotations

import pytest

from ledgerlens import scorecard
from ledgerlens.config import load_contracts
from ledgerlens.quality import predicate_for, reject_rules
from ledgerlens.schemas import GOLD_DQ_RULE_SCORECARD_SCHEMA, column_names

CONTRACTS = load_contracts()


def _catalogue():
    return scorecard.rule_catalogue_rows(CONTRACTS)


# =============================================================================
# The rule catalogue
# =============================================================================
def test_catalogue_includes_every_declared_rule():
    """Including the ones that never fire.

    A scorecard listing only rules that rejected something cannot distinguish
    "the data is clean" from "that check silently stopped running". The second
    is the one that hurts, and it looks identical to success.
    """
    expected = sum(
        len(reject_rules(ds["rules"])) for ds in CONTRACTS["datasets"].values()
    )
    assert len(_catalogue()) == expected


def test_catalogue_rule_ids_are_unique():
    """Ids are stamped onto quarantined rows, so a collision corrupts history."""
    ids = [row[0] for row in _catalogue()]
    assert len(ids) == len(set(ids))


def test_catalogue_carries_the_predicate_the_pipeline_evaluated():
    """The count and its evidence live in the same row.

    This is what makes the scorecard auditable rather than assertive: a
    controller reads "GL_NONZERO_AMOUNT rejected 2 rows" and the exact SQL that
    rejected them, without opening any Python.
    """
    by_id = {row[0]: row for row in _catalogue()}
    for dataset, ds in CONTRACTS["datasets"].items():
        for rule in reject_rules(ds["rules"]):
            row = by_id[rule["id"]]
            assert row[1] == dataset
            assert row[2] == rule["column"]
            assert row[3] == rule["check"]
            assert row[6] == predicate_for(rule)


def test_catalogue_descriptions_are_single_line():
    """contracts.yaml uses folded blocks; a newline breaks a dashboard cell."""
    for row in _catalogue():
        assert "\n" not in row[5]
        assert row[5].strip(), f"{row[0]} has no description"


def test_catalogue_shape_matches_the_declared_schema():
    """The tuple order and the schema order must agree, or createDataFrame lies.

    Spark applies a supplied schema POSITIONALLY - the same trap the bronze
    header check exists for. A reordered tuple would load cleanly and put the
    check type in the severity column.
    """
    declared = [c for c in column_names(GOLD_DQ_RULE_SCORECARD_SCHEMA)
                if c != "rows_rejected"]
    assert len(_catalogue()[0]) == len(declared)
    assert declared == ["rule_id", "dataset", "column_name", "check_type",
                        "severity", "description", "predicate_sql"]


# =============================================================================
# Violation counting
# =============================================================================
def test_violations_sql_explodes_the_recorded_rule_ids():
    """Counts come from what the pipeline RECORDED, not from a recomputation.

    Re-running the predicates would produce a second opinion, and a scorecard
    whose numbers are a second opinion can disagree with the pipeline that
    actually rejected the rows.
    """
    sql = scorecard.violations_sql(["quarantine.gl", "quarantine.ap_subledger"])
    assert "explode(split(_failed_rule_ids, '[|]'))" in sql
    assert "count(*) AS rows_rejected" in sql
    assert "GROUP BY rule_id" in sql


def test_violations_sql_unions_every_quarantine_table():
    sql = scorecard.violations_sql(["quarantine.gl", "quarantine.ap_subledger"])
    assert sql.count("UNION ALL") == 1
    assert "quarantine.gl" in sql
    assert "quarantine.ap_subledger" in sql


def test_violations_sql_scales_to_more_datasets():
    """Built from the table list, so a third source is config, not a SQL edit."""
    sql = scorecard.violations_sql(["a", "b", "c"])
    assert sql.count("UNION ALL") == 2


def test_violations_sql_refuses_an_empty_table_list():
    with pytest.raises(ValueError, match="nothing to score"):
        scorecard.violations_sql([])


def test_explode_is_not_nested_inside_an_aggregate():
    """Spark rejects a generator inside an aggregate at parse time.

    Same class of cluster-only failure as the window-inside-aggregate bug found
    on day 3, so it gets the same structural guard.
    """
    sql = scorecard.violations_sql(["quarantine.gl"])
    assert "count(explode" not in sql
    # explode must sit in the inner projection, the aggregate in the outer one.
    assert sql.index("count(*)") < sql.index("explode(")


# =============================================================================
# The DQ score, defined exactly once
# =============================================================================
def test_dq_score_denominator_is_rows_received():
    """Measuring against what survived would score 100% on any input."""
    assert "rows_received" in scorecard.dq_score_expr()


def test_dq_score_guards_a_zero_denominator():
    """An empty extract should score zero and stay visible, not kill the job.

    A failed upstream load or a period with no postings is a real scenario, and
    the dashboard needs to show it rather than go blank.
    """
    assert scorecard.dq_score_expr().startswith("CASE WHEN rows_received = 0 THEN 0.0")


def test_dq_score_uses_double_to_match_the_pandas_oracle():
    """Money is DECIMAL; a percentage is not money.

    The oracle computes `round(100.0 * passed / received, 4)` in Python floats.
    Matching that exactly matters more here than a precision argument that does
    not apply to a display ratio.
    """
    expr = scorecard.dq_score_expr()
    assert "cast(rows_passed AS DOUBLE)" in expr
    assert ", 4)" in expr


def test_dq_score_reproduces_the_verified_number():
    """1,885 of 1,909 rows is 98.7428% - the number verified on Databricks."""
    assert round(100.0 * 1885 / 1909, 4) == 98.7428
