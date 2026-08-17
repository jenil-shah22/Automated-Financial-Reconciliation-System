"""Generate the data dictionary from the schema registry.

WHY THIS IS GENERATED AND NOT WRITTEN
-------------------------------------
A hand-written data dictionary is correct on the day it is written and wrong
within a month. The failure is silent: nobody diffs prose against code, so the
document keeps confidently describing a column that was renamed in March.

Here the descriptions live on the `Column` objects in `schemas.py` - the same
objects that generate the Spark `StructType` and the SQL DDL. One declaration
produces the table, the DDL and the documentation, so they cannot disagree.
`tests/test_docs.py` regenerates this file and fails if the committed copy is
stale, which turns "keep the docs updated" from a discipline into a build error.

Usage
-----
    python -m ledgerlens.docs_gen          # write docs/data_dictionary.md
    python -m ledgerlens.docs_gen --check  # fail if the committed file is stale
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from .config import PROJECT_ROOT
from .schemas import SCHEMAS, Column, to_sql_type

DOCS_DIR = PROJECT_ROOT / "docs"
DATA_DICTIONARY = DOCS_DIR / "data_dictionary.md"

# Presentation order and prose for each layer. Ordered by the path a row takes
# through the lakehouse, because that is the order a reader needs them in.
LAYERS: list[tuple[str, str, str]] = [
    (
        "bronze_gl",
        "bronze.gl",
        "General ledger, exactly as received. Every column is `STRING` on "
        "purpose: casting at ingest would turn the planted `\"N/A\"` into "
        "`NULL` before the contract engine sees it, so the row would be "
        "quarantined as *missing value* instead of *text in a numeric column* "
        "— the wrong diagnosis, pointing at the wrong upstream fix, with the "
        "original bytes gone.",
    ),
    (
        "bronze_ap",
        "bronze.ap_subledger",
        "AP subledger, exactly as received. Same all-`STRING` rule as "
        "`bronze.gl`.",
    ),
    (
        "silver_gl",
        "silver.gl",
        "General ledger after the contract passed and types were applied. "
        "Casting happens *after* the contract, never before — so a cast "
        "failure here is a bug in the contract, not bad data.",
    ),
    (
        "silver_ap",
        "silver.ap_subledger",
        "AP subledger after the contract passed and types were applied.",
    ),
    (
        "quarantine_gl",
        "quarantine.gl",
        "GL rows that failed the contract, with the reason attached. Stays "
        "untyped: these rows failed validation, so they cannot be assumed "
        "castable, and typing this table would mean the rows most needing "
        "investigation are the ones that fail to load.",
    ),
    (
        "quarantine_ap",
        "quarantine.ap_subledger",
        "AP rows that failed the contract, with the reason attached.",
    ),
    (
        "gold_recon_detail",
        "gold.recon_detail",
        "One row per business key — the grain the reconciliation was performed "
        "at. Every key carries exactly one `break_status`.",
    ),
    (
        "gold_recon_summary",
        "gold.recon_summary",
        "Counts and value by period and status. A **dense** grid: every "
        "observed period is crossed with all six statuses and combinations "
        "that did not occur are written as zero, so an absent row cannot mean "
        "both *none this period* and *that branch stopped firing*.",
    ),
    (
        "gold_recon_exceptions",
        "gold.recon_exceptions",
        "The non-`MATCHED` keys, labelled with vendor and account names and "
        "ranked by exposure. The analyst worklist.",
    ),
    (
        "gold_dq_scorecard",
        "gold.dq_scorecard",
        "One row per source dataset: how many rows arrived, how many passed, "
        "and the resulting DQ score.",
    ),
    (
        "gold_dq_rule_scorecard",
        "gold.dq_rule_scorecard",
        "One row per contract rule, carrying both the count of rows it "
        "rejected and the exact SQL predicate that rejected them.",
    ),
]

HEADER = """# Data dictionary

<!-- GENERATED FILE - DO NOT EDIT BY HAND.
     Produced by `python -m ledgerlens.docs_gen` from the column descriptions in
     src/ledgerlens/schemas.py. Edit the descriptions there; regenerating is
     checked by tests/test_docs.py, which fails if this file is stale. -->

Every column in every layer, with its type and what it means.

The descriptions here are not a parallel document. They are the `description`
field of the `Column` objects in `schemas.py` — the same objects that generate
the Spark `StructType` and the SQL DDL. One declaration produces the table, the
DDL and this page, so they cannot drift apart.

**Underscore-prefixed columns are pipeline metadata**, not source data. They are
prefixed so they can never collide with a column an upstream system adds later.
"""

FOOTER = """
---

## Conventions that apply everywhere

| Convention | Reason |
|---|---|
| Money is `DECIMAL(18,2)`, never `DOUBLE` | Binary floating point cannot represent `0.01` exactly. A reconciliation tolerating `1.00` must not itself be the source of sub-cent drift. |
| `fiscal_period` is a `STRING`, never a `DATE` | An accounting period closes on a decision, not a calendar boundary. Typing it as a date invites `date_trunc('month', ...)` downstream, which would silently reclassify every cut-off entry and manufacture timing differences that do not exist. |
| Codes stay `STRING` | Leading zeros are meaningful and arithmetic on an account code is never a valid operation. |
| `NULL` and `0.00` are different claims | `0.00` asserts *the ledger posted nothing*. `NULL` says *the ledger has no opinion*. Only differences coalesce a missing side to zero, because a difference has to be a number. |
| Nothing is inferred | Schema inference reads a sample and guesses, and the guess changes silently when the data changes. |

*Synthetic demonstration project. All data is fictional and does not represent
any real company, client, employee, vendor, or financial system.*
"""


def _escape(text: str) -> str:
    """Pipes would break the markdown table; backticks in prose are fine."""
    return text.replace("|", "\\|").strip()


def render_table(columns: Sequence[Column]) -> str:
    lines = [
        "| Column | Type | Null | Description |",
        "|---|---|---|---|",
    ]
    for col in columns:
        nullable = "yes" if col.nullable else "**no**"
        lines.append(
            f"| `{col.name}` | `{to_sql_type(col.dtype)}` | {nullable} | "
            f"{_escape(col.description)} |"
        )
    return "\n".join(lines)


def render() -> str:
    """Build the whole document."""
    missing = [name for name, _, _ in LAYERS if name not in SCHEMAS]
    if missing:
        raise KeyError(f"docs_gen references unknown schemas: {missing}")
    undocumented = [name for name in SCHEMAS if name not in {n for n, _, _ in LAYERS}]
    if undocumented:
        raise KeyError(
            f"These schemas are registered but not in the data dictionary: "
            f"{undocumented}. Every published table must be documented."
        )

    parts = [HEADER]
    for key, table, blurb in LAYERS:
        parts.append(f"\n---\n\n## `{table}`\n\n{blurb}\n\n{render_table(SCHEMAS[key])}\n")
    parts.append(FOOTER)
    return "\n".join(parts).rstrip() + "\n"


def write(path: Path | None = None) -> Path:
    path = Path(path) if path is not None else DATA_DICTIONARY
    path.parent.mkdir(parents=True, exist_ok=True)
    # newline="\n" so the file hashes identically on Windows and Linux - the
    # staleness test compares content, and CRLF would make it fail on one OS.
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(render())
    return path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m ledgerlens.docs_gen",
        description="Generate docs/data_dictionary.md from schemas.py.",
    )
    parser.add_argument("--check", action="store_true",
                        help="Exit non-zero if the committed file is stale.")
    parser.add_argument("--path", type=Path, default=DATA_DICTIONARY)
    args = parser.parse_args(argv)

    if args.check:
        if not args.path.exists():
            print(f"MISSING {args.path} - run python -m ledgerlens.docs_gen")
            return 1
        current = args.path.read_text(encoding="utf-8")
        if current != render():
            print(f"STALE {args.path} - run python -m ledgerlens.docs_gen")
            return 1
        print(f"up to date  {args.path}")
        return 0

    path = write(args.path)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
