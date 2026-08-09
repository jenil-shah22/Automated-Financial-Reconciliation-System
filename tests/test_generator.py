"""Tests for the generator, the manifest, and end-to-end verification.

The last section holds the negative controls. A verification suite that cannot
be made to fail is not evidence of anything, so these tests deliberately break
the data and assert that validation notices.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pandas as pd
import pytest

from ledgerlens import generate_data
from ledgerlens.config import ALL_STATUSES, load_contracts
from ledgerlens.validate import run_validation


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


# =============================================================================
# Determinism
# =============================================================================
def test_same_seed_produces_byte_identical_files(tmp_path):
    """Reproducibility is the whole basis for the manifest being trustworthy.

    If two runs at seed 42 differed, the committed manifest would describe a
    dataset nobody else could regenerate, and 'the counts match' would be
    unfalsifiable.
    """
    a, b = tmp_path / "a", tmp_path / "b"
    generate_data.generate(seed=42, out_dir=a)
    generate_data.generate(seed=42, out_dir=b)

    for name in ("gl.csv", "ap_subledger.csv", "planted_breaks.csv"):
        assert _sha(a / name) == _sha(b / name), f"{name} is not reproducible"


def test_different_seed_produces_different_data(tmp_path):
    """Guards against a seed that is accepted but silently ignored."""
    a, b = tmp_path / "a", tmp_path / "b"
    generate_data.generate(seed=42, out_dir=a)
    generate_data.generate(seed=7, out_dir=b)
    assert _sha(a / "gl.csv") != _sha(b / "gl.csv")


def test_break_counts_are_seed_independent(tmp_path):
    """Different data, same planted structure.

    Counts come from FATE_COUNTS, not from whatever the random draw happened
    to produce - so a reviewer can regenerate at any seed and still verify.
    """
    out = tmp_path / "s7"
    generate_data.generate(seed=7, out_dir=out)
    report = run_validation(out)
    assert report["passed"], [c for c in report["checks"] if not c["passed"]]


def test_manifest_records_file_hashes(generated, manifest):
    """The manifest must describe the files that actually shipped."""
    for key, name in (("gl", "gl.csv"), ("ap", "ap_subledger.csv")):
        assert manifest["source_files"][key]["sha256"] == _sha(generated / name)


# =============================================================================
# Manifest self-consistency
# =============================================================================
def test_expected_breaks_cover_the_whole_taxonomy(manifest):
    assert set(manifest["expected_breaks"]) == set(ALL_STATUSES)


def test_break_counts_sum_to_the_key_total(manifest):
    assert sum(manifest["expected_breaks"].values()) == manifest["expected_key_total"]


def test_violations_are_at_least_the_quarantined_row_count(manifest):
    """Rows breaching two rules make violations exceed rows - never the reverse."""
    q = manifest["expected_quarantine"]
    assert q["total_violations"] >= q["total_rows"]
    assert q["gl_rows"] + q["ap_rows"] == q["total_rows"]


def test_quarantine_arithmetic_is_stated_not_assumed(manifest):
    """MISSING_FROM_* = planted - quarantined, explicitly.

    This is the one place the DQ layer and the recon layer interact, so the
    relationship is pinned rather than left to be rediscovered.
    """
    fates = manifest["parameters"]["fate_counts"]
    q = manifest["expected_quarantine"]
    breaks = manifest["expected_breaks"]

    assert breaks["MISSING_FROM_SUBLEDGER"] == fates["MISSING_FROM_SUBLEDGER"] - q["gl_rows"]
    assert breaks["MISSING_FROM_GL"] == fates["MISSING_FROM_GL"] - q["ap_rows"]


def test_defects_only_target_single_sided_keys(generated):
    """The invariant that keeps DQ and recon independent.

    Every quarantined row's business key must be absent from the other side.
    If a defect ever landed on a two-sided key, quarantine would manufacture a
    phantom MISSING_FROM_* break and the manifest arithmetic above would be a
    lie.
    """
    contracts = load_contracts()
    key = contracts["recon"]["business_key"]

    gl = pd.read_csv(generated / "gl.csv", dtype=str, keep_default_na=False)
    ap = pd.read_csv(generated / "ap_subledger.csv", dtype=str, keep_default_na=False)

    from ledgerlens.validate import apply_contracts

    _, gl_quar, _ = apply_contracts(gl, contracts["datasets"]["gl"]["rules"])
    _, ap_quar, _ = apply_contracts(ap, contracts["datasets"]["ap"]["rules"])

    gl_keys = set(map(tuple, gl[key].values))
    ap_keys = set(map(tuple, ap[key].values))

    for _, row in gl_quar.iterrows():
        assert tuple(row[k] for k in key) not in ap_keys
    for _, row in ap_quar.iterrows():
        assert tuple(row[k] for k in key) not in gl_keys


def test_every_planted_defect_declares_its_rule_ids(manifest):
    contracts = load_contracts()
    known = {
        rule["id"]
        for ds in contracts["datasets"].values()
        for rule in ds["rules"]
    }
    assert manifest["planted_defects"], "no defects planted"
    for defect in manifest["planted_defects"]:
        ids = defect["expected_rule_ids"].split("|")
        assert ids and all(i in known for i in ids), defect


# =============================================================================
# Generated data shape
# =============================================================================
def test_row_counts_match_the_manifest(generated, manifest):
    gl = pd.read_csv(generated / "gl.csv", dtype=str, keep_default_na=False)
    ap = pd.read_csv(generated / "ap_subledger.csv", dtype=str, keep_default_na=False)
    assert len(gl) == manifest["row_counts"]["gl_raw"]
    assert len(ap) == manifest["row_counts"]["ap_raw"]


def test_gl_business_keys_are_unique(generated):
    """GL has one row per invoice by construction.

    Worth pinning: if GL ever gained duplicates, DUPLICATE_IN_SUBLEDGER would
    stop meaning 'duplicated on the subledger side' and the taxonomy would need
    a seventh status.
    """
    contracts = load_contracts()
    key = contracts["recon"]["business_key"]
    gl = pd.read_csv(generated / "gl.csv", dtype=str, keep_default_na=False)
    assert not gl.duplicated(subset=key).any()


def test_duplicate_ap_rows_have_distinct_surrogate_keys(generated):
    """A double booking repeats the BUSINESS key, never the line id.

    This is the distinction that lets AP_UNIQ_LINE_ID coexist with the
    DUPLICATE_IN_SUBLEDGER break instead of cancelling it.
    """
    contracts = load_contracts()
    key = contracts["recon"]["business_key"]
    ap = pd.read_csv(generated / "ap_subledger.csv", dtype=str, keep_default_na=False)

    dup_keys = ap[ap.duplicated(subset=key, keep=False)]
    assert len(dup_keys) > 0
    # The only repeated line ids in the file are the two planted by the
    # ap_duplicate_line_id defect, which sit on AP-only keys.
    assert not dup_keys.duplicated(subset=["ap_line_id"]).any()


def test_amounts_are_written_as_fixed_2dp_strings(generated):
    """Bronze is 'exactly as received'; float repr artefacts must not appear."""
    ap = pd.read_csv(generated / "ap_subledger.csv", dtype=str, keep_default_na=False)
    numeric = ap[~ap["amount"].isin(["", "N/A"])]["amount"]
    assert numeric.str.match(r"^-?\d+\.\d{2}$").all()


# =============================================================================
# End-to-end
# =============================================================================
def test_validation_passes_on_generated_data(generated):
    report = run_validation(generated)
    failures = [c for c in report["checks"] if not c["passed"]]
    assert report["passed"], failures


def test_validation_checks_every_declared_rule(generated):
    """Rules expected to fire zero times are still asserted.

    A rule that suddenly starts rejecting rows is as much a signal as one that
    stops, so silence is verified rather than assumed.
    """
    contracts = load_contracts()
    all_ids = {
        rule["id"]
        for ds in contracts["datasets"].values()
        for rule in ds["rules"]
    }
    report = run_validation(generated)
    checked = {
        c["name"].removeprefix("rule ")
        for c in report["checks"]
        if c["name"].startswith("rule ")
    }
    assert all_ids == checked


# =============================================================================
# Negative controls - prove the verifier can fail
# =============================================================================
@pytest.fixture
def corrupted(generated, tmp_path):
    """A writable copy of the generated data, for deliberate sabotage."""
    target = tmp_path / "corrupt"
    shutil.copytree(generated, target)
    return target


def _fails(raw_dir: Path, *, expect_check: str) -> None:
    report = run_validation(raw_dir)
    assert not report["passed"], "verification should have failed but passed"
    failed = {c["name"] for c in report["checks"] if not c["passed"]}
    assert any(expect_check in name for name in failed), (
        f"expected a failure mentioning {expect_check!r}, got {sorted(failed)}"
    )


def test_dropping_a_gl_row_is_detected(corrupted):
    """The classic silent pipeline failure: a row goes missing in transit."""
    path = corrupted / "gl.csv"
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.iloc[1:].to_csv(path, index=False, lineterminator="\n")
    _fails(corrupted, expect_check="gl raw rows")


def test_duplicating_an_ap_row_is_detected(corrupted):
    """Double-counting is as dangerous as dropping, and less visible."""
    path = corrupted / "ap_subledger.csv"
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    pd.concat([df, df.iloc[[0]]]).to_csv(path, index=False, lineterminator="\n")
    _fails(corrupted, expect_check="ap raw rows")


def test_repairing_a_planted_defect_is_detected(corrupted):
    """Cleaning bad data outside the pipeline breaks the control.

    If someone 'helpfully' fixes a source file, the quarantine count no longer
    matches the manifest. That is intended: the manifest describes the data
    that was generated, not the data someone wished for.
    """
    path = corrupted / "ap_subledger.csv"
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.loc[df["currency"] == "US$", "currency"] = "USD"
    df.to_csv(path, index=False, lineterminator="\n")
    _fails(corrupted, expect_check="AP_DOM_CURRENCY")


def test_converting_a_match_into_a_break_is_detected(corrupted):
    """Move one amount beyond tolerance; MATCHED and AMOUNT_MISMATCH both shift."""
    path = corrupted / "ap_subledger.csv"
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.loc[0, "amount"] = "999999.00"
    df.to_csv(path, index=False, lineterminator="\n")
    _fails(corrupted, expect_check="break")


def test_tampering_with_the_manifest_is_detected(corrupted):
    """The manifest is an assertion, not a description that follows the data."""
    import json

    path = corrupted / "control_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["expected_breaks"]["MISSING_FROM_GL"] += 1
    path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    _fails(corrupted, expect_check="break MISSING_FROM_GL")
