"""Tests for the matcher and the break classifier.

These are the tests that matter most. Everything else in the project is
plumbing around this classification.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ledgerlens.config import (
    STATUS_AMOUNT_MISMATCH,
    STATUS_DUPLICATE_IN_SUBLEDGER,
    STATUS_MATCHED,
    STATUS_MISSING_FROM_GL,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_TIMING_DIFFERENCE,
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
