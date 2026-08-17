"""Tests for the SQL rule compiler.

Every test here runs without a JVM. That is the point of compiling rules to SQL
strings rather than building Spark Column objects: the exact predicate for every
rule is asserted in CI with no cluster, and only execution needs Spark.

The golden-string assertions are deliberate. A predicate is the most
consequential text in the project - it decides which rows are allowed near the
company's numbers - so it is pinned exactly rather than pattern-matched.
"""

from __future__ import annotations

import re

import pytest

from ledgerlens.config import load_contracts
from ledgerlens.quality import (
    assert_patterns_are_anchored,
    assert_patterns_compile,
    describe,
    failed_rule_count_expr,
    failed_rule_ids_expr,
    flag_column,
    predicate_for,
    reject_rules,
    rule_count_exprs,
    rule_flag_exprs,
    sql_string,
)


def _rule(**kwargs):
    base = {"id": "R", "severity": "reject"}
    base.update(kwargs)
    return base


# =============================================================================
# Literals
# =============================================================================
def test_sql_string_escapes_single_quotes():
    assert sql_string("O'Brien") == r"'O\'Brien'"


def test_sql_string_escapes_backslashes():
    r"""Backslash is an escape character inside Spark SQL string literals.

    Without doubling, a regex containing \d would reach the regex engine as a
    bare 'd' and match the wrong thing - silently, and only for patterns that
    happen to use shorthand classes.
    """
    assert sql_string(r"^\d{4}$") == r"'^\\d{4}$'"


# =============================================================================
# One golden predicate per check type
# =============================================================================
def test_not_null_predicate():
    assert predicate_for(_rule(column="amount", check="not_null")) == (
        "(amount IS NULL OR trim(amount) = '')"
    )


def test_regex_predicate_skips_blanks_and_negates_the_match():
    assert predicate_for(
        _rule(column="vendor_code", check="regex", pattern="^V[0-9]{4}$")
    ) == (
        "(NOT (vendor_code IS NULL OR trim(vendor_code) = '') "
        "AND NOT (vendor_code RLIKE '^V[0-9]{4}$'))"
    )


def test_allowed_values_predicate():
    assert predicate_for(
        _rule(column="currency", check="allowed_values", values=["USD", "EUR"])
    ) == (
        "(NOT (currency IS NULL OR trim(currency) = '') "
        "AND currency NOT IN ('USD', 'EUR'))"
    )


def test_numeric_predicate_uses_try_cast():
    """try_cast, never cast.

    `cast` raises and kills the job, which would mean one malformed row stops
    the pipeline from reporting on the other 1,908 - the opposite of a
    quarantine layer's purpose.
    """
    sql = predicate_for(_rule(column="amount", check="numeric"))
    assert "try_cast(amount AS DECIMAL(18,2))" in sql
    assert "IS NULL" in sql
    assert sql.startswith("(NOT (amount IS NULL")


def test_non_zero_predicate_skips_unparseable_values():
    """'N/A' is the numeric check's problem, not non_zero's.

    Reporting it under both would count one defect twice and inflate the DQ
    score's denominator.
    """
    assert predicate_for(_rule(column="amount", check="non_zero")) == (
        "(try_cast(amount AS DECIMAL(18,2)) IS NOT NULL "
        "AND try_cast(amount AS DECIMAL(18,2)) = 0)"
    )


def test_numeric_range_predicate_covers_both_bounds():
    sql = predicate_for(
        _rule(column="amount", check="numeric_range", min=-100, max=100)
    )
    assert "< -100" in sql and "> 100" in sql
    assert "IS NOT NULL" in sql


def test_numeric_range_accepts_a_single_bound():
    only_max = predicate_for(_rule(column="a", check="numeric_range", max=10))
    assert "> 10" in only_max and "<" not in only_max

    only_min = predicate_for(_rule(column="a", check="numeric_range", min=0))
    assert "< 0" in only_min and ">" not in only_min


def test_numeric_range_without_bounds_raises():
    with pytest.raises(ValueError, match="needs min or max"):
        predicate_for(_rule(column="a", check="numeric_range"))


def test_unique_predicate_uses_a_window_and_excludes_blanks():
    """Uniqueness cannot be a row predicate - it needs to see other rows.

    Blanks are excluded because repeated empties are not_null's defect;
    counting them as duplicates too would report one problem twice.
    """
    assert predicate_for(_rule(column="ap_line_id", check="unique")) == (
        "(NOT (ap_line_id IS NULL OR trim(ap_line_id) = '') "
        "AND count(*) OVER (PARTITION BY ap_line_id) > 1)"
    )


def test_unknown_check_raises():
    with pytest.raises(ValueError, match="unknown check type"):
        predicate_for(_rule(column="a", check="nonsense"))


# =============================================================================
# Rule assembly
# =============================================================================
def test_reject_rules_are_sorted_by_id():
    """Must match the pandas engine, which emits failed ids alphabetically.

    If the two engines ordered ids differently, `_failed_rule_ids` would differ
    as a string on every multi-violation row and the differential comparison
    would be measuring formatting instead of logic.
    """
    rules = [_rule(id="B_X", column="a", check="not_null"),
             _rule(id="A_Y", column="a", check="not_null"),
             _rule(id="C_Z", column="a", check="not_null")]
    assert [r["id"] for r in reject_rules(rules)] == ["A_Y", "B_X", "C_Z"]


def test_warn_severity_rules_are_excluded():
    rules = [_rule(id="KEEP", column="a", check="not_null"),
             _rule(id="SKIP", column="a", check="not_null", severity="warn")]
    assert [r["id"] for r in reject_rules(rules)] == ["KEEP"]


def test_failed_rule_ids_uses_concat_ws_so_passing_rules_contribute_nothing():
    """concat_ws skips NULLs, so each rule appears only when it fires.

    Every rule is still evaluated - no short-circuiting - because a row
    breaching three rules is three tickets for three different people.
    """
    rules = [_rule(id="R_A", column="a", check="not_null"),
             _rule(id="R_B", column="b", check="not_null")]
    sql = failed_rule_ids_expr(rules)
    assert sql.startswith("concat_ws('|', ")
    assert "CASE WHEN (a IS NULL OR trim(a) = '') THEN 'R_A' END" in sql
    assert "CASE WHEN (b IS NULL OR trim(b) = '') THEN 'R_B' END" in sql


def test_failed_rule_ids_with_no_rules_is_an_empty_string():
    assert failed_rule_ids_expr([]) == "''"


def test_failed_rule_count_is_derived_from_the_id_list():
    """Derived, not counted separately, so the two can never disagree."""
    sql = failed_rule_count_expr()
    assert "_failed_rule_ids = '' THEN 0" in sql
    assert "size(split(_failed_rule_ids, '[|]'))" in sql


def test_flag_and_count_exprs_cover_every_reject_rule():
    contracts = load_contracts()
    for dataset in ("gl", "ap"):
        rules = contracts["datasets"][dataset]["rules"]
        expected = {r["id"] for r in reject_rules(rules)}
        assert set(rule_flag_exprs(rules)) == expected
        assert set(rule_count_exprs(rules)) == expected


def test_no_aggregate_ever_wraps_a_window_function():
    """The regression this two-step design exists for.

    `count_if(<predicate>)` is the obvious one-step form and works for every
    check type except `unique`, whose predicate contains
    `count(*) OVER (PARTITION BY ...)`. Spark rejects a window nested inside an
    aggregate:

        "It is not allowed to use a window function inside an aggregate
         function. Please use the inner window function in a sub-query."

    Windows are legal in a projection and illegal inside an aggregate, so the
    counting path projects flags first and aggregates second. This test pins
    that split - the bug is otherwise invisible without a running cluster,
    because the SQL is only rejected at parse time on Spark.
    """
    contracts = load_contracts()
    for dataset in ("gl", "ap"):
        rules = contracts["datasets"][dataset]["rules"]

        # The aggregate step must reference only projected flag columns.
        for rule_id, sql in rule_count_exprs(rules).items():
            assert "OVER (" not in sql, f"{rule_id} aggregates a window function"
            assert sql == f"count_if(`{flag_column(rule_id)}`)"

        # ...and at least one predicate really does use a window, otherwise
        # this test would pass vacuously if `unique` were ever dropped.
        assert any("OVER (" in sql for sql in rule_flag_exprs(rules).values())


def test_flag_column_names_cannot_collide_with_source_columns():
    """Flags are projected alongside nothing else, but the prefix keeps them
    unambiguous if that ever changes."""
    from ledgerlens.schemas import AP_SOURCE_COLUMNS, GL_SOURCE_COLUMNS

    source = set(GL_SOURCE_COLUMNS) | set(AP_SOURCE_COLUMNS)
    contracts = load_contracts()
    for dataset in ("gl", "ap"):
        for rule in reject_rules(contracts["datasets"][dataset]["rules"]):
            assert flag_column(rule["id"]) not in source


# =============================================================================
# Invariants over the real contracts file
# =============================================================================
def test_every_real_rule_compiles():
    """Smoke test over contracts.yaml - a rule that will not compile is a rule
    that cannot protect anything, and finding that out on a cluster is slow."""
    contracts = load_contracts()
    for ds in contracts["datasets"].values():
        for rule in ds["rules"]:
            sql = predicate_for(rule)
            assert sql and rule["column"] in sql


def test_all_regex_patterns_are_anchored():
    """The RLIKE trap, enforced rather than trusted.

    Spark's RLIKE is a SEARCH, not a full match: 'INV-2026-000001-JUNK' would
    satisfy an unanchored invoice pattern. The pandas oracle uses fullmatch, so
    an unanchored pattern would make the two engines disagree for a reason that
    looks like nothing at all.
    """
    assert_patterns_are_anchored(load_contracts())


def test_unanchored_pattern_is_rejected():
    contracts = {"datasets": {"gl": {"rules": [
        {"id": "GL_FMT_X", "column": "x", "check": "regex", "pattern": "[0-9]{4}"}
    ]}}}
    with pytest.raises(ValueError, match="anchored"):
        assert_patterns_are_anchored(contracts)


def test_partially_anchored_pattern_is_rejected():
    contracts = {"datasets": {"gl": {"rules": [
        {"id": "GL_FMT_X", "column": "x", "check": "regex", "pattern": "^[0-9]{4}"}
    ]}}}
    with pytest.raises(ValueError, match="anchored"):
        assert_patterns_are_anchored(contracts)


def test_all_patterns_compile_as_regex():
    assert_patterns_compile(load_contracts())


def test_invalid_pattern_is_reported_with_its_rule_id():
    contracts = {"datasets": {"gl": {"rules": [
        {"id": "GL_FMT_BAD", "column": "x", "check": "regex", "pattern": "^[unclosed$"}
    ]}}}
    with pytest.raises(ValueError, match="GL_FMT_BAD"):
        assert_patterns_compile(contracts)


# =============================================================================
# Cross-engine agreement on rule identity
# =============================================================================
def test_sql_engine_and_pandas_engine_cover_the_same_rules():
    """Both engines must act on exactly the same rule set.

    They are separate implementations, so the thing to pin is that neither
    silently ignores a rule the other enforces.
    """
    from ledgerlens.validate import apply_contracts
    import pandas as pd

    contracts = load_contracts()
    for dataset, columns in (("gl", None), ("ap", None)):
        rules = contracts["datasets"][dataset]["rules"]
        sql_ids = {r["id"] for r in reject_rules(rules)}

        empty = pd.DataFrame({c: pd.Series(dtype=str)
                              for c in {r["column"] for r in rules}})
        _, _, pandas_violations = apply_contracts(empty, rules)
        assert sql_ids == set(pandas_violations)


# =============================================================================
# Documentation output
# =============================================================================
def test_describe_renders_every_rule_with_its_sql():
    contracts = load_contracts()
    rules = contracts["datasets"]["ap"]["rules"]
    text = describe(rules)
    for rule in reject_rules(rules):
        assert rule["id"] in text
    assert "sql    :" in text and "intent :" in text
