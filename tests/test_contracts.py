"""Tests for the contract rule engine and the contracts file itself."""

from __future__ import annotations

import pandas as pd
import pytest

from ledgerlens.config import load_contracts
from ledgerlens.validate import apply_contracts, evaluate_rule


# =============================================================================
# The contracts file
# =============================================================================
def test_rule_ids_are_unique_across_datasets():
    """Ids are stamped onto quarantined rows; a collision makes them useless."""
    contracts = load_contracts()
    ids = [
        rule["id"]
        for ds in contracts["datasets"].values()
        for rule in ds["rules"]
    ]
    assert len(ids) == len(set(ids))


def test_no_uniqueness_rule_on_the_business_key():
    """Guard rail against the most tempting wrong fix in this project.

    Someone reviewing the AP data will see repeated business keys and reach for
    a `unique` contract. That would quarantine the duplicates and destroy the
    DUPLICATE_IN_SUBLEDGER break before recon ever sees it - turning a
    detected double-payment into a silently deleted row.

    Uniqueness belongs on the surrogate key. Duplication on the business key is
    a finding, not a defect.
    """
    contracts = load_contracts()
    business_key = set(contracts["recon"]["business_key"])

    for ds_name, ds in contracts["datasets"].items():
        for rule in ds["rules"]:
            if rule["check"] == "unique":
                assert rule["column"] not in business_key, (
                    f"{rule['id']} makes the business key unique, which would "
                    f"quarantine duplicates instead of reporting them"
                )
            assert rule["column"] == ds["primary_key"] or rule["check"] != "unique"


def test_every_rule_resolves_its_references():
    """pattern_ref / values_ref must resolve, or the rule silently does nothing."""
    contracts = load_contracts()
    for ds in contracts["datasets"].values():
        for rule in ds["rules"]:
            if rule["check"] == "regex":
                assert rule.get("pattern"), f"{rule['id']} has no pattern"
            if rule["check"] == "allowed_values":
                assert rule.get("values"), f"{rule['id']} has no values"


def test_status_precedence_covers_the_whole_taxonomy():
    from ledgerlens.config import ALL_STATUSES

    contracts = load_contracts()
    assert set(contracts["recon"]["status_precedence"]) == set(ALL_STATUSES)


# =============================================================================
# Rule engine semantics
# =============================================================================
def _rule(**kwargs):
    base = {"id": "TEST_RULE", "severity": "reject"}
    base.update(kwargs)
    return base


def test_not_null_treats_blank_and_whitespace_as_missing():
    df = pd.DataFrame({"amount": ["10.00", "", "   ", "0.00"]})
    mask = evaluate_rule(df, _rule(column="amount", check="not_null"))
    assert list(mask) == [False, True, True, False]


def test_value_checks_skip_nulls():
    """One missing amount is ONE defect, not four.

    If non_zero and numeric_range also fired on a null, the DQ scorecard would
    report three problems where a human sees one, and the 'most violated rule'
    chart would be dominated by knock-on effects instead of causes.
    """
    df = pd.DataFrame({"amount": ["", "  "]})
    for check, extra in [
        ("numeric", {}),
        ("non_zero", {}),
        ("numeric_range", {"min": 0, "max": 100}),
    ]:
        mask = evaluate_rule(df, _rule(column="amount", check=check, **extra))
        assert not mask.any(), f"{check} should skip nulls"


def test_non_zero_and_range_skip_unparseable_values():
    """'N/A' is the numeric check's problem, not non_zero's."""
    df = pd.DataFrame({"amount": ["N/A"]})
    assert evaluate_rule(df, _rule(column="amount", check="numeric")).iloc[0]
    assert not evaluate_rule(df, _rule(column="amount", check="non_zero")).iloc[0]
    assert not evaluate_rule(
        df, _rule(column="amount", check="numeric_range", min=0, max=1)
    ).iloc[0]


def test_unique_flags_every_row_in_a_duplicate_group():
    """Both copies are quarantined, not 'keep the first'.

    Nothing in the data says which of two rows sharing a surrogate key is the
    real one. Keeping either is a guess; quarantining both is a fact.
    """
    df = pd.DataFrame({"id": ["A", "B", "A", "C"]})
    mask = evaluate_rule(df, _rule(column="id", check="unique"))
    assert list(mask) == [True, False, True, False]


def test_unique_does_not_flag_repeated_blanks():
    df = pd.DataFrame({"id": ["", "", "A"]})
    mask = evaluate_rule(df, _rule(column="id", check="unique"))
    assert not mask.any()


def test_regex_uses_full_match_not_search():
    """'2026-03-extra' must fail. A partial match would let junk through."""
    rule = _rule(column="p", check="regex", pattern="^[0-9]{4}-[0-9]{2}$")
    df = pd.DataFrame({"p": ["2026-03", "2026-03-extra", "x2026-03"]})
    assert list(evaluate_rule(df, rule)) == [False, True, True]


def test_numeric_range_is_inclusive_at_the_bounds():
    rule = _rule(column="a", check="numeric_range", min=0, max=100)
    df = pd.DataFrame({"a": ["0", "100", "-0.01", "100.01"]})
    assert list(evaluate_rule(df, rule)) == [False, False, True, True]


def test_unknown_check_type_raises():
    with pytest.raises(ValueError, match="unknown check type"):
        evaluate_rule(pd.DataFrame({"a": ["1"]}), _rule(column="a", check="wat"))


def test_rule_referencing_a_missing_column_raises():
    """A typo in contracts.yaml must be loud, not a rule that never fires."""
    with pytest.raises(KeyError, match="missing column"):
        evaluate_rule(pd.DataFrame({"a": ["1"]}), _rule(column="b", check="not_null"))


# =============================================================================
# Quarantine behaviour
# =============================================================================
def test_quarantined_row_carries_all_failed_rule_ids():
    """A row breaching two rules is two tickets for two people."""
    rules = [
        _rule(id="R_NN", column="amount", check="not_null"),
        _rule(id="R_DOM", column="currency", check="allowed_values", values=["USD"]),
    ]
    df = pd.DataFrame({"amount": ["", "10.00"], "currency": ["USDD", "USD"]})
    clean, quarantined, violations = apply_contracts(df, rules)

    assert len(clean) == 1
    assert len(quarantined) == 1
    assert quarantined["failed_rule_ids"].iloc[0] == "R_DOM|R_NN"
    assert quarantined["failed_rule_count"].iloc[0] == 2
    assert violations == {"R_NN": 1, "R_DOM": 1}


def test_no_row_is_ever_silently_dropped():
    """clean + quarantined must reconstruct the input exactly."""
    rules = [_rule(id="R_NN", column="amount", check="not_null")]
    df = pd.DataFrame({"amount": ["1.00", "", "2.00", ""]})
    clean, quarantined, _ = apply_contracts(df, rules)
    assert len(clean) + len(quarantined) == len(df)


def test_empty_input_produces_empty_output_not_an_exception():
    """A real scenario: a failed upstream job, or a period with no postings.

    The pipeline should report '0 rows, 0 defects' rather than crash - an empty
    extract is a fact about the business, not a malformed file.
    """
    rules = [_rule(id="R_NN", column="amount", check="not_null")]
    empty = pd.DataFrame({"amount": pd.Series(dtype=str)})
    clean, quarantined, violations = apply_contracts(empty, rules)

    assert len(clean) == 0
    assert len(quarantined) == 0
    assert violations == {"R_NN": 0}


def test_warn_severity_rules_do_not_quarantine():
    rules = [_rule(id="R_WARN", column="amount", check="not_null", severity="warn")]
    df = pd.DataFrame({"amount": ["", "1.00"]})
    clean, quarantined, _ = apply_contracts(df, rules)
    assert len(clean) == 2 and len(quarantined) == 0
