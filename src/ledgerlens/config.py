"""Paths and contract loading.

Kept deliberately tiny. Everything that another module might need to agree on -
where the repo root is, where raw data lands, how contracts.yaml is parsed -
lives here so there is exactly one answer.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# config.py sits at <root>/src/ledgerlens/config.py, so the root is 3 levels up.
# LEDGERLENS_ROOT lets Databricks point at a workspace/DBFS path instead.
PROJECT_ROOT = Path(
    os.environ.get("LEDGERLENS_ROOT", Path(__file__).resolve().parents[2])
)

CONFIG_DIR = PROJECT_ROOT / "config"
CONTRACTS_PATH = CONFIG_DIR / "contracts.yaml"

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"

GL_CSV = RAW_DIR / "gl.csv"
AP_CSV = RAW_DIR / "ap_subledger.csv"
MANIFEST_JSON = RAW_DIR / "control_manifest.json"
PLANTED_LEDGER_CSV = RAW_DIR / "planted_breaks.csv"
VALIDATION_REPORT_JSON = DATA_DIR / "validation_report.json"

# ---------------------------------------------------------------------------
# Break taxonomy - the closed set of statuses a business key can resolve to.
# Declared here rather than in contracts.yaml because it is code-level truth:
# adding a status means writing classification logic, not editing config.
# ---------------------------------------------------------------------------
STATUS_MATCHED = "MATCHED"
STATUS_AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
STATUS_TIMING_DIFFERENCE = "TIMING_DIFFERENCE"
STATUS_MISSING_FROM_SUBLEDGER = "MISSING_FROM_SUBLEDGER"
STATUS_MISSING_FROM_GL = "MISSING_FROM_GL"
STATUS_DUPLICATE_IN_SUBLEDGER = "DUPLICATE_IN_SUBLEDGER"

ALL_STATUSES: List[str] = [
    STATUS_MATCHED,
    STATUS_AMOUNT_MISMATCH,
    STATUS_TIMING_DIFFERENCE,
    STATUS_MISSING_FROM_SUBLEDGER,
    STATUS_MISSING_FROM_GL,
    STATUS_DUPLICATE_IN_SUBLEDGER,
]


def load_contracts(path: Path | str | None = None) -> Dict[str, Any]:
    """Load contracts.yaml and resolve `pattern_ref` / `values_ref` indirection.

    Rules in the YAML refer to shared patterns and reference lists by name so
    that, say, the vendor-code regex is written once and reused by both
    datasets. Resolving here means every consumer sees fully materialised
    rules and no consumer has to know the indirection exists.
    """
    path = Path(path) if path is not None else CONTRACTS_PATH
    with open(path, "r", encoding="utf-8") as fh:
        contracts = yaml.safe_load(fh)

    patterns = contracts.get("patterns", {})
    reference = contracts.get("reference", {})

    for ds_name, ds in contracts.get("datasets", {}).items():
        for rule in ds.get("rules", []):
            if "pattern_ref" in rule:
                ref = rule["pattern_ref"]
                if ref not in patterns:
                    raise KeyError(
                        f"{ds_name}.{rule['id']}: unknown pattern_ref '{ref}'"
                    )
                rule["pattern"] = patterns[ref]
            if "values_ref" in rule:
                ref = rule["values_ref"]
                if ref not in reference:
                    raise KeyError(
                        f"{ds_name}.{rule['id']}: unknown values_ref '{ref}'"
                    )
                rule["values"] = reference[ref]

    _assert_rule_ids_unique(contracts)
    return contracts


def _assert_rule_ids_unique(contracts: Dict[str, Any]) -> None:
    """Rule ids are stamped onto quarantined rows, so collisions are fatal."""
    seen: Dict[str, str] = {}
    for ds_name, ds in contracts.get("datasets", {}).items():
        for rule in ds.get("rules", []):
            rid = rule["id"]
            if rid in seen:
                raise ValueError(
                    f"Duplicate rule id '{rid}' in datasets "
                    f"'{seen[rid]}' and '{ds_name}'"
                )
            seen[rid] = ds_name


def all_rule_ids(contracts: Dict[str, Any], dataset: str) -> List[str]:
    """Every rule id declared for a dataset, in file order."""
    return [r["id"] for r in contracts["datasets"][dataset]["rules"]]
