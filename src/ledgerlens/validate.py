"""Independent verification: does the data contain exactly what was planted?

WHY A SECOND IMPLEMENTATION
---------------------------
This module deliberately shares NO code with generate_data.py. It reads only
the two CSVs, contracts.yaml, and the manifest's expected COUNTS - never the
per-invoice answer key. It then rebuilds the quarantine and the reconciliation
from scratch in pandas and asserts the numbers agree.

That is differential testing. If the generator's bookkeeping and an
independent reconstruction of the same data reach the same six numbers, the
odds that both are wrong in the same direction are low.

Honest limitation, stated because a reviewer will think of it anyway: both
implementations have the same author, so this catches implementation slips,
not a shared misunderstanding of the spec. That is what tests/ and the written
break taxonomy are for.

This is also the pandas ORACLE for the PySpark pipeline built on days 3-4:
silver.py and recon.py must land on identical numbers, on a different engine.

Usage
-----
    python -m ledgerlens.validate
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from .config import (
    ALL_STATUSES,
    AP_CSV,
    GL_CSV,
    MANIFEST_JSON,
    RAW_DIR,
    STATUS_AMOUNT_MISMATCH,
    STATUS_DUPLICATE_IN_SUBLEDGER,
    STATUS_MATCHED,
    STATUS_MISSING_FROM_GL,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_TIMING_DIFFERENCE,
    VALIDATION_REPORT_JSON,
    load_contracts,
)

QUARANTINE_RULE_SEPARATOR = "|"


# =============================================================================
# Contract rule engine (pandas reference implementation)
# =============================================================================
def _is_blank(series: pd.Series) -> pd.Series:
    """Null means: absent, or present but empty/whitespace.

    CSV has no concept of null, so an empty cell arrives as "". Treating
    whitespace as blank too, because a padded empty string from a fixed-width
    export is the same defect wearing a different hat.
    """
    return series.isna() | (series.astype(str).str.strip() == "")


def _to_numeric(series: pd.Series) -> pd.Series:
    """Coerce to float; unparseable and blank both become NaN."""
    return pd.to_numeric(series.astype(str).str.strip(), errors="coerce")


def evaluate_rule(df: pd.DataFrame, rule: Dict[str, Any]) -> pd.Series:
    """Return a boolean mask: True where this rule is VIOLATED.

    Null semantics: every check except not_null skips blank values, so one
    missing amount produces one defect rather than four. See the header of
    contracts.yaml for why that matters to the DQ score.
    """
    column = rule["column"]
    check = rule["check"]

    if column not in df.columns:
        raise KeyError(f"Rule {rule['id']} references missing column '{column}'")

    values = df[column]
    blank = _is_blank(values)

    if check == "not_null":
        return blank

    if check == "unique":
        # Blanks are not duplicates of each other - that is not_null's job.
        dup = values.duplicated(keep=False) & ~blank
        return dup

    if check == "regex":
        pattern = re.compile(rule["pattern"])
        matched = values.astype(str).map(lambda v: bool(pattern.fullmatch(v)))
        return (~matched) & ~blank

    if check == "allowed_values":
        allowed = set(rule["values"])
        return (~values.astype(str).isin(allowed)) & ~blank

    if check == "numeric":
        return _to_numeric(values).isna() & ~blank

    if check == "non_zero":
        numeric = _to_numeric(values)
        # Unparseable values skip this check - numeric's job to report those.
        return numeric.notna() & (numeric == 0)

    if check == "numeric_range":
        numeric = _to_numeric(values)
        low = float(rule.get("min", -np.inf))
        high = float(rule.get("max", np.inf))
        return numeric.notna() & ((numeric < low) | (numeric > high))

    raise ValueError(f"Rule {rule['id']}: unknown check type '{check}'")


def apply_contracts(
    df: pd.DataFrame, rules: List[Dict[str, Any]]
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, int]]:
    """Split a raw dataframe into (clean, quarantined, violations per rule).

    Every rule is evaluated against every row - no short-circuiting. A row that
    breaches three rules carries all three ids, because "fix the amount" and
    "fix the currency" are two different tickets for two different people.
    """
    violations_by_rule: Dict[str, int] = {}
    # One list of failed rule ids per row, built by accumulating masks.
    rule_hits: List[pd.Series] = []

    for rule in rules:
        if rule.get("severity", "reject") != "reject":
            continue
        mask = evaluate_rule(df, rule)
        violations_by_rule[rule["id"]] = int(mask.sum())
        rule_hits.append(mask.rename(rule["id"]))

    hits = pd.concat(rule_hits, axis=1) if rule_hits else pd.DataFrame(index=df.index)

    def collect(row: pd.Series) -> str:
        return QUARANTINE_RULE_SEPARATOR.join(sorted(row.index[row.values]))

    failed_rule_ids = hits.apply(collect, axis=1) if len(hits.columns) else pd.Series(
        "", index=df.index
    )
    is_bad = failed_rule_ids != ""

    quarantined = df[is_bad].copy()
    quarantined["failed_rule_ids"] = failed_rule_ids[is_bad]
    quarantined["failed_rule_count"] = quarantined["failed_rule_ids"].str.count(
        re.escape(QUARANTINE_RULE_SEPARATOR)
    ) + 1

    clean = df[~is_bad].copy()
    return clean, quarantined, violations_by_rule


# =============================================================================
# Reconciliation (pandas reference implementation)
# =============================================================================
def reconcile(
    gl: pd.DataFrame,
    ap: pd.DataFrame,
    business_key: Sequence[str],
    tolerance: float,
) -> pd.DataFrame:
    """Match GL against AP and classify every business key exactly once.

    THE ORDER OF OPERATIONS HERE IS THE WHOLE POINT.

    Duplicates are detected on the AP side BEFORE the join. If you join first
    and look for duplicates afterwards, a GL row facing two AP rows fans out
    into two joined rows; deduplicating those loses the GL counterpart and it
    gets misfiled as MISSING_FROM_SUBLEDGER. That is the single easiest bug to
    introduce in this project, and it is silent - the totals still look
    plausible.
    """
    key = list(business_key)

    gl = gl.copy()
    ap = ap.copy()
    gl["amount_num"] = _to_numeric(gl["amount"])
    ap["amount_num"] = _to_numeric(ap["amount"])

    # ---- Step 1: aggregate each side to one row per business key ---------
    gl_agg = (
        gl.groupby(key, dropna=False)
        .agg(
            gl_amount=("amount_num", "sum"),
            gl_fiscal_period=("fiscal_period", "min"),
            gl_row_count=("amount_num", "size"),
        )
        .reset_index()
    )

    ap_agg = (
        ap.groupby(key, dropna=False)
        .agg(
            ap_amount=("amount_num", "sum"),
            ap_fiscal_period=("fiscal_period", "min"),
            ap_row_count=("amount_num", "size"),
        )
        .reset_index()
    )

    # ---- Step 2: full outer join on the business key ---------------------
    recon = gl_agg.merge(ap_agg, on=key, how="outer", indicator=True)

    recon["gl_row_count"] = recon["gl_row_count"].fillna(0).astype(int)
    recon["ap_row_count"] = recon["ap_row_count"].fillna(0).astype(int)

    gl_present = recon["gl_row_count"] > 0
    ap_present = recon["ap_row_count"] > 0
    ap_duplicated = recon["ap_row_count"] > 1

    amount_diff = recon["gl_amount"].fillna(0.0) - recon["ap_amount"].fillna(0.0)
    within_tolerance = amount_diff.abs() <= tolerance
    period_differs = (
        recon["gl_fiscal_period"].fillna("") != recon["ap_fiscal_period"].fillna("")
    )

    # ---- Step 3: classify, first match wins ------------------------------
    # Mirrors recon.status_precedence in contracts.yaml. np.select evaluates
    # top-down, which is what makes "first match wins" true rather than hoped.
    recon["break_status"] = np.select(
        [
            ap_duplicated,                                  # structural, outranks all
            gl_present & ~ap_present,
            ap_present & ~gl_present,
            ~within_tolerance,                              # value beats timing
            within_tolerance & period_differs,
        ],
        [
            STATUS_DUPLICATE_IN_SUBLEDGER,
            STATUS_MISSING_FROM_SUBLEDGER,
            STATUS_MISSING_FROM_GL,
            STATUS_AMOUNT_MISMATCH,
            STATUS_TIMING_DIFFERENCE,
        ],
        default=STATUS_MATCHED,
    )

    recon["amount_difference"] = amount_diff.round(2)
    recon["abs_amount_difference"] = amount_diff.abs().round(2)
    return recon.drop(columns=["_merge"])


# =============================================================================
# Verification harness
# =============================================================================
@dataclass
class Check:
    name: str
    expected: Any
    actual: Any

    @property
    def passed(self) -> bool:
        return self.expected == self.actual


def _read_raw(path: Path) -> pd.DataFrame:
    """Read a source file with zero inference.

    dtype=str and keep_default_na=False mean an empty cell stays "" instead of
    becoming NaN, and "2026/04" is never quietly reinterpreted. Inference is
    how silent corruption enters a pipeline - the raw layer must be a faithful
    copy of the bytes we were handed.
    """
    return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False)


def run_validation(raw_dir: Path | None = None) -> Dict[str, Any]:
    raw_dir = Path(raw_dir) if raw_dir is not None else RAW_DIR
    contracts = load_contracts()

    with open(raw_dir / MANIFEST_JSON.name, "r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    gl_raw = _read_raw(raw_dir / GL_CSV.name)
    ap_raw = _read_raw(raw_dir / AP_CSV.name)

    gl_clean, gl_quar, gl_viol = apply_contracts(
        gl_raw, contracts["datasets"]["gl"]["rules"]
    )
    ap_clean, ap_quar, ap_viol = apply_contracts(
        ap_raw, contracts["datasets"]["ap"]["rules"]
    )

    recon = reconcile(
        gl_clean,
        ap_clean,
        contracts["recon"]["business_key"],
        float(contracts["recon"]["amount_tolerance_abs"]),
    )
    status_counts = recon["break_status"].value_counts().to_dict()

    checks: List[Check] = [
        Check("gl raw rows", manifest["row_counts"]["gl_raw"], len(gl_raw)),
        Check("ap raw rows", manifest["row_counts"]["ap_raw"], len(ap_raw)),
        Check("gl rows quarantined",
              manifest["expected_quarantine"]["gl_rows"], len(gl_quar)),
        Check("ap rows quarantined",
              manifest["expected_quarantine"]["ap_rows"], len(ap_quar)),
        Check("total rows quarantined",
              manifest["expected_quarantine"]["total_rows"],
              len(gl_quar) + len(ap_quar)),
        Check("total rule violations",
              manifest["expected_quarantine"]["total_violations"],
              int(gl_quar["failed_rule_count"].sum() + ap_quar["failed_rule_count"].sum())),
        Check("gl rows surviving to silver",
              manifest["row_counts"]["gl_after_quarantine"], len(gl_clean)),
        Check("ap rows surviving to silver",
              manifest["row_counts"]["ap_after_quarantine"], len(ap_clean)),
    ]

    # Per-rule violation counts. Rules expected to fire zero times are checked
    # too - a rule that suddenly starts rejecting rows is as much a signal as
    # one that stops.
    for dataset, observed in (("gl", gl_viol), ("ap", ap_viol)):
        expected_map = manifest["expected_quarantine"]["by_rule"][dataset]
        for rule_id in [r["id"] for r in contracts["datasets"][dataset]["rules"]]:
            checks.append(
                Check(
                    f"rule {rule_id}",
                    expected_map.get(rule_id, 0),
                    observed.get(rule_id, 0),
                )
            )

    for status in ALL_STATUSES:
        checks.append(
            Check(f"break {status}",
                  manifest["expected_breaks"][status],
                  int(status_counts.get(status, 0)))
        )

    checks.append(
        Check("business keys reconciled", manifest["expected_key_total"], len(recon))
    )

    # Structural invariant, independent of the manifest: the taxonomy is a
    # partition. Every key gets exactly one status and no key escapes.
    checks.append(
        Check("statuses are a partition", len(recon), int(sum(status_counts.values())))
    )
    checks.append(
        Check("no unknown statuses", 0,
              int(len(set(status_counts) - set(ALL_STATUSES))))
    )

    # Every quarantined row must carry at least one rule id - "never silently
    # drop a row" is only true if the reason travels with the row.
    unexplained = int(
        (gl_quar["failed_rule_ids"] == "").sum() + (ap_quar["failed_rule_ids"] == "").sum()
    )
    checks.append(Check("quarantined rows without a rule id", 0, unexplained))

    dq_score = round(
        100.0
        * (len(gl_clean) + len(ap_clean))
        / (len(gl_raw) + len(ap_raw)),
        4,
    )
    checks.append(Check("DQ score %", manifest["expected_dq_score_pct"], dq_score))

    report = {
        "passed": all(c.passed for c in checks),
        "checks_total": len(checks),
        "checks_failed": sum(1 for c in checks if not c.passed),
        "dq_score_pct": dq_score,
        "observed_breaks": {s: int(status_counts.get(s, 0)) for s in ALL_STATUSES},
        "observed_quarantine": {
            "gl_rows": len(gl_quar),
            "ap_rows": len(ap_quar),
            "by_rule": {"gl": gl_viol, "ap": ap_viol},
        },
        "checks": [
            {"name": c.name, "expected": c.expected, "actual": c.actual,
             "passed": c.passed}
            for c in checks
        ],
    }
    return report


def _print_report(report: Dict[str, Any]) -> None:
    print("LedgerLens independent verification")
    print("=" * 62)

    failed = [c for c in report["checks"] if not c["passed"]]
    interesting = [
        c for c in report["checks"]
        if not c["name"].startswith("rule ") or c["expected"] or c["actual"]
    ]

    for c in interesting:
        mark = "ok  " if c["passed"] else "FAIL"
        print(f"  [{mark}] {c['name']:<42} {str(c['actual']):>10}")

    silent_rules = len(report["checks"]) - len(interesting)
    if silent_rules:
        print(f"  [ok  ] {'rules with 0 expected violations':<42} {silent_rules:>10}")

    print("-" * 62)
    if report["passed"]:
        print(f"  PASS - {report['checks_total']} checks, 0 failures")
        print(f"  DQ score {report['dq_score_pct']}%")
    else:
        print(f"  FAIL - {report['checks_failed']}/{report['checks_total']} checks failed")
        for c in failed:
            print(f"        {c['name']}: expected {c['expected']}, got {c['actual']}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.validate",
        description="Verify generated data against the control manifest.",
    )
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    parser.add_argument("--report", type=Path, default=VALIDATION_REPORT_JSON)
    args = parser.parse_args(argv)

    report = run_validation(args.raw_dir)
    _print_report(report)

    args.report.parent.mkdir(parents=True, exist_ok=True)
    with open(args.report, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")

    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
