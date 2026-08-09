"""Synthetic GL + AP subledger generator with planted breaks.

WHY THIS FILE EXISTS
--------------------
A reconciliation engine that finds 37 breaks is useless unless you can prove
37 is the right answer. Real data cannot prove that - nobody knows the truth.
So we manufacture the truth first: every break in the output was deliberately
planted, and the count of each kind is written to a control manifest.

The pipeline is then judged by one question: does it rediscover exactly what
was planted? Not approximately. Exactly.

HOW BREAKS ARE PLANTED
----------------------
Every invoice is a single conceptual fact. It is assigned exactly one `fate`,
and that fate decides what gets emitted to each side:

    fate                      GL rows   AP rows
    MATCHED                      1         1      (identical, or within tolerance)
    AMOUNT_MISMATCH              1         1      (AP amount moved beyond tolerance)
    TIMING_DIFFERENCE            1         1      (amounts tie, AP period is later)
    MISSING_FROM_SUBLEDGER       1         0
    MISSING_FROM_GL              0         1
    DUPLICATE_IN_SUBLEDGER       1        2-3

Because fate is assigned before any row is written, the expected counts are
bookkeeping, not analysis. The generator never inspects its own output to
decide what the answer is - that would be circular.

WHY DEFECTS ONLY LAND ON SINGLE-SIDED KEYS
------------------------------------------
Quarantined rows never reach reconciliation. If we corrupted the AP row of a
MATCHED key, quarantine would delete it and the key would legitimately become
MISSING_FROM_GL - so a "data quality" decision would silently manufacture an
"unrecorded liability". The manifest could still describe that, but it would
couple two independent concerns and make every count harder to reason about.

Instead, defects are planted only on MISSING_FROM_SUBLEDGER (GL-only) and
MISSING_FROM_GL (AP-only) rows. Quarantine then simply shrinks those two
populations by a stated amount, and the manifest records the arithmetic:

    expected MISSING_FROM_GL = planted AP-only - quarantined AP-only

This is also realistic: a line that is malformed enough to fail a contract is
often exactly the line that never made it into the ledger.

Usage
-----
    python -m ledgerlens.generate_data
    python -m ledgerlens.generate_data --seed 42 --out-dir data/raw
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import platform
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from . import __version__
from .config import (
    ALL_STATUSES,
    AP_CSV,
    GL_CSV,
    MANIFEST_JSON,
    PLANTED_LEDGER_CSV,
    RAW_DIR,
    STATUS_AMOUNT_MISMATCH,
    STATUS_DUPLICATE_IN_SUBLEDGER,
    STATUS_MATCHED,
    STATUS_MISSING_FROM_GL,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_TIMING_DIFFERENCE,
    load_contracts,
)

# =============================================================================
# Generation parameters
# =============================================================================
DEFAULT_SEED = 42

# Fiscal periods covered. TIMING_DIFFERENCE rows push their AP side one period
# later, so the AP file can contain one period beyond this list.
PERIODS: List[str] = ["2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06"]

# How many invoices get each fate. These numbers ARE the expected break counts
# (before quarantine shrinks the two single-sided populations).
FATE_COUNTS: Dict[str, int] = {
    STATUS_MATCHED: 820,
    STATUS_AMOUNT_MISMATCH: 40,
    STATUS_TIMING_DIFFERENCE: 35,
    STATUS_MISSING_FROM_SUBLEDGER: 25,
    STATUS_MISSING_FROM_GL: 30,
    STATUS_DUPLICATE_IN_SUBLEDGER: 20,
}

# Deliberate edge cases, planted to prove the classifier's precedence ladder
# rather than just its happy path.
N_MATCHED_WITH_ROUNDING = 60  # |diff| <= tolerance -> must still be MATCHED
N_MISMATCH_ALSO_SHIFTED = 5   # amount AND period differ -> AMOUNT_MISMATCH wins
N_DUPLICATE_TRIPLETS = 4      # 3 AP rows on one key, not 2
N_DUPLICATE_UNEQUAL = 5       # duplicate copies with different amounts

# One extract timestamp per source system. Constant so the output is
# byte-for-byte reproducible from the seed alone.
GL_EXTRACT_TS = "2026-08-01T02:15:00Z"
AP_EXTRACT_TS = "2026-08-01T02:41:00Z"

GL_SOURCE_SYSTEM = "GLCORE"
AP_SOURCE_SYSTEM = "APHUB"

# Amount distribution. Lognormal because invoice values are heavily
# right-skewed in practice: many small ones, a few very large.
AMOUNT_LOG_MEAN = 8.0
AMOUNT_LOG_SIGMA = 1.15
AMOUNT_MIN = 50.00
AMOUNT_MAX = 400_000.00

# =============================================================================
# Reference data - all fictional
# =============================================================================
ACCOUNTS: List[Tuple[str, str]] = [
    ("5010", "Cost of Goods Sold - Materials"),
    ("6010", "Professional Fees"),
    ("6020", "IT and Software"),
    ("6030", "Facilities and Rent"),
    ("6040", "Travel and Entertainment"),
    ("6050", "Marketing and Advertising"),
    ("7010", "Utilities"),
]

DEPARTMENTS: List[Tuple[str, str]] = [
    ("D100", "Finance"),
    ("D200", "Engineering"),
    ("D300", "Sales"),
    ("D400", "Operations"),
    ("D500", "Marketing"),
]

PAYMENT_STATUSES: List[str] = ["OPEN", "PAID", "PARTIAL", "ON_HOLD"]
PAYMENT_STATUS_WEIGHTS: List[float] = [0.34, 0.45, 0.11, 0.10]

# Vendor names are assembled from invented word lists so that nothing here can
# be mistaken for a real supplier.
_VENDOR_PREFIX = [
    "Northwind", "Halcyon", "Brightvale", "Kestrel", "Ironwood", "Silverpine",
    "Cobalt", "Meridian", "Larkspur", "Quarryside", "Thornbury", "Elmgate",
    "Vantage", "Foxglove", "Ambergate", "Stonecrest", "Wexford", "Marlowe",
    "Pinnacle", "Ridgeway",
]
_VENDOR_MIDDLE = [
    "Industrial", "Logistics", "Facilities", "Digital", "Technical",
    "Commercial", "Regional", "Integrated", "Precision", "Allied",
]
_VENDOR_SUFFIX = ["Supplies", "Services", "Partners", "Systems", "Group", "Works"]

N_VENDORS = 60


# =============================================================================
# Small helpers
# =============================================================================
def _add_months(period: str, n: int) -> str:
    """'2026-06' + 1 -> '2026-07'. Fiscal periods, not dates."""
    year, month = (int(p) for p in period.split("-"))
    total = (year * 12 + (month - 1)) + n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def _random_date_in_period(rng: np.random.Generator, period: str) -> date:
    """A uniformly random calendar day inside a fiscal period."""
    year, month = (int(p) for p in period.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, int(rng.integers(1, last_day + 1)))


def _money(value: float) -> str:
    """Format as a fixed 2dp string.

    Amounts are written as strings, never floats, so the raw CSV is stable and
    free of binary-float artefacts like 1234.5700000000001. Bronze is supposed
    to be 'exactly as received'; letting a float repr leak in would undermine
    that on day one.
    """
    return f"{value:.2f}"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write with LF endings so file hashes match across Windows and Linux."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, lineterminator="\n", encoding="utf-8")


# =============================================================================
# Planted defect specification
# =============================================================================
@dataclass(frozen=True)
class DefectSpec:
    """One kind of deliberate data-quality defect.

    `expected_rule_ids` is a DECLARATION, not a measurement. We state up front
    which contract rules this mutation should trip. The verifier runs the real
    rule engine and must reproduce exactly these ids. If the declaration is
    wrong, validation fails loudly - which is the entire point.
    """

    name: str
    dataset: str
    count: int
    mutate: Callable[[Dict[str, Any]], None]
    expected_rule_ids: Tuple[str, ...]
    note: str


def _defect_specs() -> List[DefectSpec]:
    """The catalogue of planted defects.

    Note what is deliberately absent: no defect corrupts a business key on a
    two-sided row, and no defect creates a duplicate BUSINESS key. Duplicate
    business keys are a reconciliation finding, not a data defect.
    """

    def set_field(field_name: str, value: Any) -> Callable[[Dict[str, Any]], None]:
        def _apply(row: Dict[str, Any]) -> None:
            row[field_name] = value

        return _apply

    def set_fields(**kwargs: Any) -> Callable[[Dict[str, Any]], None]:
        def _apply(row: Dict[str, Any]) -> None:
            row.update(kwargs)

        return _apply

    return [
        # ---- GL defects (planted on GL-only rows) --------------------------
        DefectSpec(
            "gl_null_amount", "gl", 2,
            set_field("amount", ""),
            ("GL_NN_AMOUNT",),
            "Export dropped the value column. Not_null catches it; the "
            "numeric/non-zero/range checks skip nulls so this is ONE defect.",
        ),
        DefectSpec(
            "gl_zero_amount", "gl", 2,
            set_field("amount", "0.00"),
            ("GL_NONZERO_AMOUNT",),
            "Zero-value posting - carries no accounting meaning.",
        ),
        DefectSpec(
            "gl_bad_fiscal_period", "gl", 2,
            set_field("fiscal_period", "2026/04"),
            ("GL_FMT_FISCAL_PERIOD",),
            "Locale-dependent separator from a spreadsheet round-trip.",
        ),
        DefectSpec(
            "gl_unknown_account", "gl", 2,
            set_field("account_code", "9999"),
            ("GL_DOM_ACCOUNT_CODE",),
            "Account not in the chart of accounts - a suspense/clearing code "
            "that escaped into the extract.",
        ),
        DefectSpec(
            "gl_bad_department", "gl", 1,
            set_field("department_code", "d100 "),
            ("GL_FMT_DEPARTMENT_CODE", "GL_DOM_DEPARTMENT_CODE"),
            "Lowercased with trailing whitespace. Trips BOTH format and "
            "domain, because those are genuinely independent assertions - "
            "this row proves quarantine records rule ids as a LIST.",
        ),
        DefectSpec(
            "gl_null_amount_and_bad_currency", "gl", 1,
            set_fields(amount="", currency="USDD"),
            ("GL_NN_AMOUNT", "GL_DOM_CURRENCY"),
            "Two unrelated defects on one row - proves rules are evaluated "
            "independently rather than short-circuiting on first failure.",
        ),
        # ---- AP defects (planted on AP-only rows) --------------------------
        DefectSpec(
            "ap_null_amount", "ap", 2,
            set_field("amount", ""),
            ("AP_NN_AMOUNT",),
            "Missing invoice value.",
        ),
        DefectSpec(
            "ap_zero_amount", "ap", 2,
            set_field("amount", "0.00"),
            ("AP_NONZERO_AMOUNT",),
            "Zero-value invoice line.",
        ),
        DefectSpec(
            "ap_non_numeric_amount", "ap", 1,
            set_field("amount", "N/A"),
            ("AP_NUM_AMOUNT",),
            "Literal text in a numeric column. Not_null PASSES (the cell is "
            "not empty) - which is exactly why a separate numeric check "
            "exists. Non-zero and range then skip, since an unparseable value "
            "cannot be meaningfully compared.",
        ),
        DefectSpec(
            "ap_bad_currency", "ap", 2,
            set_field("currency", "US$"),
            ("AP_DOM_CURRENCY",),
            "Symbol instead of ISO code.",
        ),
        DefectSpec(
            "ap_bad_vendor_code", "ap", 2,
            set_field("vendor_code", "v12"),
            ("AP_FMT_VENDOR_CODE",),
            "Malformed vendor code - and it is part of the business key, so "
            "letting it through would silently create a phantom break.",
        ),
        DefectSpec(
            "ap_bad_fiscal_period", "ap", 1,
            set_field("fiscal_period", "26-05"),
            ("AP_FMT_FISCAL_PERIOD",),
            "Two-digit year.",
        ),
        DefectSpec(
            "ap_absurd_amount", "ap", 1,
            set_field("amount", "999000000000.00"),
            ("AP_RANGE_AMOUNT",),
            "Overflow / units error. Parses fine and is non-zero, so only the "
            "range check can catch it.",
        ),
        DefectSpec(
            "ap_unknown_payment_status", "ap", 1,
            set_field("payment_status", "CLOSED"),
            ("AP_DOM_PAYMENT_STATUS",),
            "Workflow state from an upstream system version we do not know.",
        ),
        # Handled specially in _apply_defects because it needs two rows to
        # cooperate: row B is given row A's surrogate key.
        DefectSpec(
            "ap_duplicate_line_id", "ap", 2,
            lambda row: None,
            ("AP_UNIQ_LINE_ID",),
            "Repeated SURROGATE key - a broken extract, not a double booking. "
            "Contrast with DUPLICATE_IN_SUBLEDGER, which repeats the BUSINESS "
            "key across two distinct line ids and must reach recon.",
        ),
    ]


# =============================================================================
# Generator
# =============================================================================
@dataclass
class GenerationResult:
    gl: pd.DataFrame
    ap: pd.DataFrame
    planted: pd.DataFrame
    manifest: Dict[str, Any]
    quarantine_by_rule: Dict[str, Dict[str, int]] = field(default_factory=dict)


def _build_vendors(rng: np.random.Generator) -> List[Dict[str, str]]:
    vendors = []
    for i in range(N_VENDORS):
        name = " ".join(
            [
                _VENDOR_PREFIX[i % len(_VENDOR_PREFIX)],
                str(rng.choice(_VENDOR_MIDDLE)),
                str(rng.choice(_VENDOR_SUFFIX)),
            ]
        )
        vendors.append({"vendor_code": f"V{i + 1:04d}", "vendor_name": name})
    return vendors


def _build_invoices(rng: np.random.Generator, n: int) -> List[Dict[str, Any]]:
    """The conceptual facts, before either side is written.

    Each invoice is the truth. GL and AP rows are two imperfect *views* of it.
    """
    vendors = _build_vendors(rng)

    amounts = rng.lognormal(AMOUNT_LOG_MEAN, AMOUNT_LOG_SIGMA, size=n)
    amounts = np.clip(amounts, AMOUNT_MIN, AMOUNT_MAX).round(2)

    vendor_idx = rng.integers(0, len(vendors), size=n)
    account_idx = rng.integers(0, len(ACCOUNTS), size=n)
    dept_idx = rng.integers(0, len(DEPARTMENTS), size=n)
    period_idx = rng.integers(0, len(PERIODS), size=n)

    invoices: List[Dict[str, Any]] = []
    for i in range(n):
        period = PERIODS[int(period_idx[i])]
        vendor = vendors[int(vendor_idx[i])]
        account_code, account_name = ACCOUNTS[int(account_idx[i])]
        dept_code, dept_name = DEPARTMENTS[int(dept_idx[i])]

        invoices.append(
            {
                "seq": i + 1,
                "invoice_number": f"INV-2026-{i + 1:06d}",
                "vendor_code": vendor["vendor_code"],
                "vendor_name": vendor["vendor_name"],
                "account_code": account_code,
                "account_name": account_name,
                "department_code": dept_code,
                "department_name": dept_name,
                "fiscal_period": period,
                "amount": float(amounts[i]),
            }
        )
    return invoices


def _assign_fates(
    rng: np.random.Generator, invoices: List[Dict[str, Any]]
) -> None:
    """Shuffle, then slice. Every invoice gets exactly one fate."""
    order = rng.permutation(len(invoices))
    cursor = 0
    for fate, count in FATE_COUNTS.items():
        for idx in order[cursor : cursor + count]:
            invoices[int(idx)]["fate"] = fate
        cursor += count
    assert cursor == len(invoices), "fate counts must exhaust the invoice list"


def _emit_rows(
    rng: np.random.Generator, invoices: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Turn fated invoices into GL rows and AP rows."""
    gl_rows: List[Dict[str, Any]] = []
    ap_rows: List[Dict[str, Any]] = []

    # Which of the edge-case treatments each invoice receives. Decided by
    # position within its fate group so the choice is deterministic.
    matched_seen = 0
    mismatch_seen = 0
    duplicate_seen = 0

    gl_counter = 0
    ap_counter = 0

    def new_gl_row(inv: Dict[str, Any], period: str, amount: float) -> Dict[str, Any]:
        nonlocal gl_counter
        gl_counter += 1
        posting = _random_date_in_period(rng, period)
        return {
            "gl_entry_id": f"GL{gl_counter:07d}",
            "posting_date": posting.isoformat(),
            "fiscal_period": period,
            "account_code": inv["account_code"],
            "account_name": inv["account_name"],
            "department_code": inv["department_code"],
            "department_name": inv["department_name"],
            "vendor_code": inv["vendor_code"],
            "invoice_number": inv["invoice_number"],
            "amount": _money(amount),
            "currency": "USD",
            "source_system": GL_SOURCE_SYSTEM,
            "extract_ts": GL_EXTRACT_TS,
        }

    def new_ap_row(inv: Dict[str, Any], period: str, amount: float) -> Dict[str, Any]:
        nonlocal ap_counter
        ap_counter += 1
        invoice_dt = _random_date_in_period(rng, period)
        # Invoices are frequently dated a few days before the period they land
        # in - a real cut-off artefact, and the reason fiscal_period is the
        # authoritative field for reporting rather than any date column.
        invoice_dt = invoice_dt - timedelta(days=int(rng.integers(0, 6)))
        terms = int(rng.choice([30, 45, 60]))
        return {
            "ap_line_id": f"AP{ap_counter:07d}",
            "invoice_date": invoice_dt.isoformat(),
            "due_date": (invoice_dt + timedelta(days=terms)).isoformat(),
            "fiscal_period": period,
            "vendor_code": inv["vendor_code"],
            "vendor_name": inv["vendor_name"],
            "account_code": inv["account_code"],
            "invoice_number": inv["invoice_number"],
            "amount": _money(amount),
            "currency": "USD",
            "payment_status": str(
                rng.choice(PAYMENT_STATUSES, p=PAYMENT_STATUS_WEIGHTS)
            ),
            "payment_terms_days": str(terms),
            "source_system": AP_SOURCE_SYSTEM,
            "extract_ts": AP_EXTRACT_TS,
        }

    for inv in invoices:
        fate = inv["fate"]
        period = inv["fiscal_period"]
        amount = inv["amount"]
        inv["gl_amount"] = None
        inv["ap_amount"] = None
        inv["ap_period"] = None
        inv["edge_case"] = ""

        if fate == STATUS_MATCHED:
            # Most matched pairs tie exactly. A deliberate minority differ by
            # sub-tolerance rounding noise - if the classifier ever reports
            # those as AMOUNT_MISMATCH, the tolerance logic is broken.
            ap_amount = amount
            if matched_seen < N_MATCHED_WITH_ROUNDING:
                drift = round(float(rng.uniform(0.01, 0.99)), 2)
                ap_amount = round(amount + (drift if matched_seen % 2 == 0 else -drift), 2)
                inv["edge_case"] = "sub_tolerance_rounding"
            matched_seen += 1
            gl_rows.append(new_gl_row(inv, period, amount))
            ap_rows.append(new_ap_row(inv, period, ap_amount))
            inv["gl_amount"], inv["ap_amount"], inv["ap_period"] = amount, ap_amount, period

        elif fate == STATUS_AMOUNT_MISMATCH:
            # Move the AP amount strictly beyond tolerance. Mixed direction and
            # magnitude: small keying slips and large partial payments.
            delta = round(float(rng.uniform(2.0, 0.25 * amount + 50.0)), 2)
            delta = max(delta, 2.0)
            ap_amount = round(amount + (delta if mismatch_seen % 2 == 0 else -delta), 2)
            ap_period = period
            if mismatch_seen < N_MISMATCH_ALSO_SHIFTED:
                # Amount AND period both differ. The precedence ladder says
                # AMOUNT_MISMATCH outranks TIMING_DIFFERENCE - these rows are
                # the only thing that actually tests that.
                ap_period = _add_months(period, 1)
                inv["edge_case"] = "amount_and_period_both_differ"
            mismatch_seen += 1
            gl_rows.append(new_gl_row(inv, period, amount))
            ap_rows.append(new_ap_row(inv, ap_period, ap_amount))
            inv["gl_amount"], inv["ap_amount"], inv["ap_period"] = amount, ap_amount, ap_period

        elif fate == STATUS_TIMING_DIFFERENCE:
            # Amounts tie; the subledger booked it one period later.
            ap_period = _add_months(period, 1)
            gl_rows.append(new_gl_row(inv, period, amount))
            ap_rows.append(new_ap_row(inv, ap_period, amount))
            inv["gl_amount"], inv["ap_amount"], inv["ap_period"] = amount, amount, ap_period

        elif fate == STATUS_MISSING_FROM_SUBLEDGER:
            gl_rows.append(new_gl_row(inv, period, amount))
            inv["gl_amount"] = amount

        elif fate == STATUS_MISSING_FROM_GL:
            ap_rows.append(new_ap_row(inv, period, amount))
            inv["ap_amount"], inv["ap_period"] = amount, period

        elif fate == STATUS_DUPLICATE_IN_SUBLEDGER:
            copies = 3 if duplicate_seen < N_DUPLICATE_TRIPLETS else 2
            gl_rows.append(new_gl_row(inv, period, amount))
            for c in range(copies):
                copy_amount = amount
                if c > 0 and duplicate_seen < N_DUPLICATE_UNEQUAL:
                    # A real double-payment is often not a byte-identical copy.
                    # These must STILL classify as DUPLICATE, not
                    # AMOUNT_MISMATCH - duplicates outrank value checks.
                    copy_amount = round(amount + float(rng.uniform(5.0, 200.0)), 2)
                    inv["edge_case"] = "duplicate_with_unequal_amounts"
                ap_rows.append(new_ap_row(inv, period, copy_amount))
            inv["ap_copies"] = copies
            duplicate_seen += 1
            inv["gl_amount"], inv["ap_amount"], inv["ap_period"] = amount, amount, period

        else:  # pragma: no cover - defended by _assign_fates
            raise ValueError(f"unknown fate {fate!r}")

    return gl_rows, ap_rows


def _apply_defects(
    gl_rows: List[Dict[str, Any]],
    ap_rows: List[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, int]], Dict[str, int], List[Dict[str, Any]]]:
    """Corrupt a stated number of single-sided rows.

    Returns (expected violations per rule per dataset, defective row counts,
    a per-row defect ledger).
    """
    specs = _defect_specs()

    # Candidate pools: rows whose business key exists on ONE side only.
    # Ordered by surrogate key so target selection is deterministic.
    gl_keys_in_ap = {
        (r["account_code"], r["vendor_code"], r["invoice_number"]) for r in ap_rows
    }
    ap_keys_in_gl = {
        (r["account_code"], r["vendor_code"], r["invoice_number"]) for r in gl_rows
    }
    gl_pool = [
        r for r in gl_rows
        if (r["account_code"], r["vendor_code"], r["invoice_number"]) not in gl_keys_in_ap
    ]
    ap_pool = [
        r for r in ap_rows
        if (r["account_code"], r["vendor_code"], r["invoice_number"]) not in ap_keys_in_gl
    ]

    by_rule: Dict[str, Dict[str, int]] = {"gl": {}, "ap": {}}
    defective_rows: Dict[str, int] = {"gl": 0, "ap": 0}
    ledger: List[Dict[str, Any]] = []
    cursors = {"gl": 0, "ap": 0}
    pools = {"gl": gl_pool, "ap": ap_pool}

    for spec in specs:
        pool = pools[spec.dataset]
        start = cursors[spec.dataset]
        end = start + spec.count
        if end > len(pool):
            raise RuntimeError(
                f"Not enough single-sided {spec.dataset.upper()} rows to plant "
                f"'{spec.name}': need {end}, pool has {len(pool)}. Increase "
                f"FATE_COUNTS for the corresponding MISSING_FROM_* population."
            )
        targets = pool[start:end]
        cursors[spec.dataset] = end

        if spec.name == "ap_duplicate_line_id":
            # Needs two rows to cooperate: give the second row the first's id.
            targets[1]["ap_line_id"] = targets[0]["ap_line_id"]
        else:
            for row in targets:
                spec.mutate(row)

        for row in targets:
            defective_rows[spec.dataset] += 1
            for rid in spec.expected_rule_ids:
                by_rule[spec.dataset][rid] = by_rule[spec.dataset].get(rid, 0) + 1
            ledger.append(
                {
                    "dataset": spec.dataset,
                    "defect_name": spec.name,
                    "surrogate_key": row.get("gl_entry_id") or row.get("ap_line_id"),
                    "expected_rule_ids": "|".join(spec.expected_rule_ids),
                    "note": spec.note,
                }
            )

    return by_rule, defective_rows, ledger


def generate(seed: int = DEFAULT_SEED, out_dir: Path | None = None) -> GenerationResult:
    """Generate both sources plus the control manifest."""
    out_dir = Path(out_dir) if out_dir is not None else RAW_DIR
    rng = np.random.default_rng(seed)

    n_invoices = sum(FATE_COUNTS.values())
    invoices = _build_invoices(rng, n_invoices)
    _assign_fates(rng, invoices)
    gl_rows, ap_rows = _emit_rows(rng, invoices)

    by_rule, defective_rows, defect_ledger = _apply_defects(gl_rows, ap_rows)

    gl_df = pd.DataFrame(gl_rows)
    ap_df = pd.DataFrame(ap_rows)

    # --- Expected break counts -------------------------------------------
    # Bookkeeping, not analysis. The only adjustment is quarantine shrinking
    # the two single-sided populations.
    expected_breaks = dict(FATE_COUNTS)
    expected_breaks[STATUS_MISSING_FROM_SUBLEDGER] -= defective_rows["gl"]
    expected_breaks[STATUS_MISSING_FROM_GL] -= defective_rows["ap"]
    expected_breaks = {s: expected_breaks[s] for s in ALL_STATUSES}

    for status, count in expected_breaks.items():
        if count < 0:
            raise RuntimeError(f"Negative expected count for {status} - check defect counts")

    # --- Planted ledger (the human-readable answer key) -------------------
    planted_df = pd.DataFrame(
        [
            {
                "invoice_number": inv["invoice_number"],
                "account_code": inv["account_code"],
                "vendor_code": inv["vendor_code"],
                "planted_fate": inv["fate"],
                "gl_fiscal_period": inv["fiscal_period"],
                "ap_fiscal_period": inv["ap_period"] or "",
                "gl_amount": _money(inv["gl_amount"]) if inv["gl_amount"] is not None else "",
                "ap_amount": _money(inv["ap_amount"]) if inv["ap_amount"] is not None else "",
                "ap_copies": inv.get("ap_copies", 1 if inv["ap_amount"] is not None else 0),
                "edge_case": inv["edge_case"],
            }
            for inv in invoices
        ]
    ).sort_values("invoice_number", kind="stable")

    _write_csv(gl_df, out_dir / GL_CSV.name)
    _write_csv(ap_df, out_dir / AP_CSV.name)
    _write_csv(planted_df, out_dir / PLANTED_LEDGER_CSV.name)

    # --- Manifest ---------------------------------------------------------
    contracts = load_contracts()
    tolerance = float(contracts["recon"]["amount_tolerance_abs"])

    total_rows = len(gl_df) + len(ap_df)
    total_quarantined = defective_rows["gl"] + defective_rows["ap"]
    total_violations = sum(by_rule["gl"].values()) + sum(by_rule["ap"].values())

    manifest: Dict[str, Any] = {
        "schema_version": 1,
        "generator_version": __version__,
        "seed": seed,
        "environment": {
            "python": platform.python_version(),
            "numpy": np.__version__,
            "pandas": pd.__version__,
        },
        "parameters": {
            "invoices": n_invoices,
            "periods": PERIODS,
            "amount_tolerance_abs": tolerance,
            "business_key": contracts["recon"]["business_key"],
            "fate_counts": FATE_COUNTS,
        },
        "row_counts": {
            "gl_raw": len(gl_df),
            "ap_raw": len(ap_df),
            "total_raw": total_rows,
            "gl_after_quarantine": len(gl_df) - defective_rows["gl"],
            "ap_after_quarantine": len(ap_df) - defective_rows["ap"],
        },
        "expected_quarantine": {
            "gl_rows": defective_rows["gl"],
            "ap_rows": defective_rows["ap"],
            "total_rows": total_quarantined,
            # Violations exceed rows because some rows breach two rules.
            "total_violations": total_violations,
            "by_rule": {
                "gl": dict(sorted(by_rule["gl"].items())),
                "ap": dict(sorted(by_rule["ap"].items())),
            },
        },
        "expected_dq_score_pct": round(
            100.0 * (total_rows - total_quarantined) / total_rows, 4
        ),
        "expected_breaks": expected_breaks,
        "expected_key_total": sum(expected_breaks.values()),
        "expected_edge_cases": {
            "matched_within_tolerance": N_MATCHED_WITH_ROUNDING,
            "amount_mismatch_with_period_shift": N_MISMATCH_ALSO_SHIFTED,
            "duplicate_triplets": N_DUPLICATE_TRIPLETS,
            "duplicate_with_unequal_amounts": N_DUPLICATE_UNEQUAL,
        },
        "planted_defects": defect_ledger,
        "source_files": {
            "gl": {
                "name": GL_CSV.name,
                "rows": len(gl_df),
                "sha256": _sha256(out_dir / GL_CSV.name),
            },
            "ap": {
                "name": AP_CSV.name,
                "rows": len(ap_df),
                "sha256": _sha256(out_dir / AP_CSV.name),
            },
        },
    }

    manifest_path = out_dir / MANIFEST_JSON.name
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")

    return GenerationResult(
        gl=gl_df, ap=ap_df, planted=planted_df,
        manifest=manifest, quarantine_by_rule=by_rule,
    )


# =============================================================================
# CLI
# =============================================================================
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.generate_data",
        description="Generate synthetic GL + AP sources with planted breaks.",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--out-dir", type=Path, default=RAW_DIR)
    args = parser.parse_args(argv)

    result = generate(seed=args.seed, out_dir=args.out_dir)
    m = result.manifest

    print(f"LedgerLens data generator  (seed={m['seed']})")
    print(f"  output          {args.out_dir}")
    print(f"  gl.csv          {m['row_counts']['gl_raw']:>6} rows")
    print(f"  ap_subledger    {m['row_counts']['ap_raw']:>6} rows")
    print()
    print("  planted breaks (expected after quarantine)")
    for status, count in m["expected_breaks"].items():
        print(f"    {status:<24} {count:>6}")
    print(f"    {'-' * 24} {'-' * 6}")
    print(f"    {'business keys':<24} {m['expected_key_total']:>6}")
    print()
    q = m["expected_quarantine"]
    print("  planted defects")
    print(f"    {'rows quarantined':<24} {q['total_rows']:>6}")
    print(f"    {'rule violations':<24} {q['total_violations']:>6}")
    print(f"    {'DQ score':<24} {m['expected_dq_score_pct']:>6}%")
    print()
    print(f"  manifest        {args.out_dir / MANIFEST_JSON.name}")
    print("  next: python -m ledgerlens.validate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
