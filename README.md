# LedgerLens — Automated Financial Reconciliation System

**A GL-to-subledger reconciliation lakehouse with data contracts, a row-level
quarantine layer, and a verifiable break taxonomy.**

> **Status: v0.1 in progress — Days 1–3 complete and verified on Databricks.**
> Every number below came from an actual run. Bronze and Silver have been
> executed against Delta on Databricks serverless, and the PySpark
> implementation reproduces the independent pandas oracle exactly. Unbuilt work
> is in [Roadmap](#roadmap) and is not claimed as a feature.

---

## The problem

Every company with a finance function keeps two records of the same money: the
**general ledger** (the official summarised books) and the **subledger** (the
detailed feeder system — here, accounts payable, one row per vendor invoice).

They are supposed to agree. They never fully do.

At month-end someone exports both to Excel, VLOOKUPs them together, eyeballs
the differences, and emails the exceptions. It is slow, unauditable, and
silently wrong the moment a row is dropped or double-counted. Nobody can prove
afterwards what was actually checked.

LedgerLens replaces that with a controlled pipeline: it enforces a data
contract on every row, quarantines failures with the reason attached, matches
the two sides, and classifies every disagreement into a specific break type.

---

## Architecture

```
Source CSVs (GL + AP subledger)
        |
     BRONZE      raw Delta tables, exactly as received, nothing cleaned
        |
  [ data contracts - config/contracts.yaml ]
        |                                  \  fails
     SILVER      typed, conformed, valid     QUARANTINE (row + rule ids)
        |                                          |
  RECONCILIATION ENGINE                       DQ SCORECARD
        |                                          |
     GOLD  detail / summary / exceptions  <-->  DASHBOARD
```

Medallion (bronze → silver → gold) exists for one reason: **never overwrite raw
data.** If a transformation turns out to be wrong six months later, you replay
from bronze rather than re-requesting an extract nobody can reproduce.

---

## Break taxonomy

Every business key resolves to **exactly one** status.

| Status | Meaning | Why it matters |
|---|---|---|
| `MATCHED` | Both sides agree within tolerance | Nothing to do |
| `AMOUNT_MISMATCH` | Both present, amounts differ beyond tolerance | Keying error or partial payment |
| `TIMING_DIFFERENCE` | Amounts tie, subledger booked in a later period | Cut-off issue, usually self-correcting |
| `MISSING_FROM_SUBLEDGER` | In GL, not in subledger | Possibly an unsupported entry |
| `MISSING_FROM_GL` | In subledger, not in GL | **Unrecorded liability** — the company owes money its books don't show |
| `DUPLICATE_IN_SUBLEDGER` | One GL row facing 2+ subledger rows on the same key | Possible double payment |

**Matching key:** `account_code` + `vendor_code` + `invoice_number`
**Amount tolerance:** absolute difference ≤ 1.00 is rounding, not a break.

Three design rules that are easy to get wrong:

1. **A duplicated key is `DUPLICATE_IN_SUBLEDGER`, never `MATCHED`** — even
   when one of the two copies would tie out perfectly.
2. **Duplicates are detected before the join, not after.** See
   [Why the manifest exists](#why-the-manifest-exists).
3. **Precedence is explicit.** When a key qualifies for more than one status,
   the ladder in `contracts.yaml` decides: duplicates outrank presence checks,
   presence outranks amount, amount outranks timing. A key that is both an
   amount mismatch *and* a period shift is an `AMOUNT_MISMATCH` — filing it
   under "timing" would mean filing a real value discrepancy under "will fix
   itself next month".

---

## How correctness is proven

The data is synthetic, so **the answer is known before the pipeline runs.**

`generate_data.py` assigns every invoice exactly one *fate* — matched,
duplicated, missing from one side, and so on — and writes the resulting counts
to `data/raw/control_manifest.json`. The counts are bookkeeping, not analysis:
the generator never inspects its own output to decide what the answer is.

`validate.py` then reads only the two CSVs, `contracts.yaml`, and the manifest's
expected *counts* — never the per-invoice answer key — and rebuilds the
quarantine and the reconciliation from scratch. It shares no code with the
generator.

### Verification results

From `python -m ledgerlens.validate` on seed 42:

| Check | Expected | Actual |
|---|---|---|
| GL rows ingested | 940 | 940 |
| AP rows ingested | 969 | 969 |
| Rows quarantined | 24 | 24 |
| Rule violations (some rows breach two rules) | 26 | 26 |
| Rows surviving to silver | 1,885 | 1,885 |
| DQ score | 98.7428% | 98.7428% |
| `MATCHED` | 820 | 820 |
| `AMOUNT_MISMATCH` | 40 | 40 |
| `TIMING_DIFFERENCE` | 35 | 35 |
| `MISSING_FROM_SUBLEDGER` | 15 | 15 |
| `MISSING_FROM_GL` | 16 | 16 |
| `DUPLICATE_IN_SUBLEDGER` | 20 | 20 |
| Business keys reconciled | 946 | 946 |
| Quarantined rows with no rule id | 0 | 0 |

**56 checks, 0 failures.** Every one of the 34 contract rules is asserted,
including the 21 expected to fire zero times — a rule that suddenly starts
rejecting rows is as much a signal as one that stops.

### Cross-engine verification

The pipeline is implemented **twice**: once in pandas as the reference oracle,
once in PySpark as the production path. They share no code. Running the PySpark
silver layer on Databricks serverless against the same manifest:

| | pandas oracle | PySpark on Databricks |
|---|---|---|
| GL rows quarantined | 10 | **10** |
| GL rule violations | 12 | **12** |
| AP rows quarantined | 14 | **14** |
| AP rule violations | 14 | **14** |
| Rows surviving to silver | 1,885 | **1,885** |
| DQ score | 98.7428% | **98.7428%** |

Two independent implementations, two engines, two languages, identical numbers.

The GL asymmetry is the interesting column: **10 rows produce 12 violations**,
because two rows breach two rules each — a malformed department code that trips
both the format *and* the domain check, and a row carrying both a null amount
and a bad currency. An engine that short-circuited on first failure would report
10 and 10, and the totals would still look entirely plausible.

### Why the manifest exists

A reconciliation engine that reports 37 breaks is useless unless you can show
37 is the right answer. Row counts are not enough. Here are three wrong
implementations of the matcher, each a mistake a competent person actually
makes, run against the same generated data:

| Implementation | `MATCHED` | `DUPLICATE_IN_SUBLEDGER` | Total keys |
|---|---|---|---|
| Correct | 820 | 20 | 946 |
| Aggregate subledger with `sum()`, never count rows | 820 | **0** | 946 |
| Remove duplicates, then join | 820 | 20 | **966** |
| Join first, `drop_duplicates` to clean the fan-out | **840** | **0** | 946 |

**Two of the three preserve the total key count exactly.** They lose every
duplicate — twenty possible double payments — and nothing in a row-count
reconciliation would notice. The third silently converts twenty duplicates into
`MISSING_FROM_SUBLEDGER`, inventing twenty unsupported entries.

Only the planted-break manifest catches all three. That is the argument for
building the control before building the pipeline.

---

## Data quality: contracts and quarantine

Rules live in [`config/contracts.yaml`](config/contracts.yaml), not in code, so
a controller can disagree with one without reading Python. Rule ids are
permanent and are stamped onto every quarantined row, so *"why was this row
rejected?"* is answerable months later from the data alone.

Three decisions worth stating:

**Nulls skip checks rather than failing them.** Every check except `not_null`
is null-tolerant. Otherwise one missing amount would trip `not_null`,
`numeric`, `non_zero` and `numeric_range` and report four defects for one
problem — inflating the scorecard's denominator and filling the "most violated
rule" chart with knock-on effects instead of causes. One defect, one rule.

**Uniqueness is enforced on the surrogate key, never the business key.** A
`unique` contract on `account + vendor + invoice` would quarantine duplicates
and destroy the `DUPLICATE_IN_SUBLEDGER` break before recon ever sees it —
turning a detected double payment into a silently deleted row. Duplication on
the business key is a *finding*; duplication on the line id is a *defect*.
There is a test that fails if anyone ever "fixes" this.

**No row is silently dropped.** `clean + quarantined` always reconstructs the
input exactly, and every quarantined row carries the full list of rule ids that
rejected it — a row breaching two rules is two tickets for two different people.

---

## Repository layout

```
ledgerlens/
├── config/contracts.yaml          DQ rules with stable ids, tolerance, precedence
├── src/ledgerlens/
│   ├── config.py                  paths, contract loading, the status taxonomy
│   ├── schemas.py                 explicit schemas for every layer, never inferred
│   ├── generate_data.py           synthetic sources + planted breaks + manifest
│   ├── bronze.py                  lossless CSV -> Delta ingest with lineage
│   ├── quality.py                 compiles contracts.yaml into Spark SQL predicates
│   ├── silver.py                  contract enforcement + row-level quarantine
│   └── validate.py                independent verifier (pandas oracle)
├── notebooks/                     Databricks wrappers (thin - logic lives in src/)
├── tests/                         113 pytest tests incl. negative controls
└── data/raw/                      generated CSVs (gitignored) + control manifest
```

Notebooks are deliberately **thin wrappers**. Logic lives in `src/` because a
notebook cannot be unit-tested and cannot be reviewed properly in a pull
request.

---

## Running it

```bash
pip install -e .
```

```bash
python -m ledgerlens.generate_data
```

```bash
python -m ledgerlens.validate
```

```bash
pytest
```

The generator is deterministic: seed 42 produces byte-identical files on any
machine, and there is a test asserting it. Break counts are seed-independent —
regenerate at any seed and verification still passes — so a reviewer can check
the work without trusting the committed data.

---

## The Bronze layer

Bronze is not a staging area you clean things in. It is the **evidence locker**:
its only job is to make the raw extract queryable and replayable without
altering it. Three rules, each one a rule because breaking it is tempting:

| Rule | How it is enforced |
|---|---|
| **No casting** — every column lands as `STRING` | Declared in `schemas.py`; tested |
| **No filtering** — bronze rows == file rows | Ingest *raises* if they differ |
| **No renaming or reordering** | Header checked against the contract before the read |

**Why no casting?** If bronze cast `amount` to decimal, the planted `"N/A"`
becomes `NULL` at ingest — before the contract engine sees it. The row is then
quarantined as *"missing value"* instead of *"text in a numeric column"*: the
wrong diagnosis, pointing at the wrong upstream fix, with the original bytes
gone. Bronze preserves evidence; silver applies judgment.

**Why check the header separately?** Spark applies a supplied schema
**positionally** and ignores what the header says. A reordered upstream extract
would load cleanly, put vendor codes into the invoice field, and produce a
reconciliation where every number is wrong but plausible. It is the cheapest
control in the pipeline and it catches the most expensive class of failure.

Three lineage columns are added, all underscore-prefixed so they can never
collide with a source column: `_ingested_at`, `_source_file`, `_batch_id`.

---

## The Silver layer

Bronze preserved the evidence; silver applies judgment. It decides which rows
are fit to reconcile, records why the others were not, and **only then** applies
types.

**Rules are compiled to SQL, not to Spark `Column` objects.** Each rule in
`contracts.yaml` becomes a Spark SQL expression that is TRUE when the rule is
violated. Three reasons:

1. **Testable without a JVM** — the exact predicate for all 34 rules is asserted
   in CI with no cluster. Only execution needs Spark.
2. **Auditable** — *"GL_NONZERO_AMOUNT rejected 2 rows"* is an assertion; the
   generated SQL printed beside it is the evidence. A `Column` object is opaque.
3. **Portable** — the same string runs in a Databricks SQL dashboard, so the DQ
   scorecard queries the logic the pipeline enforced instead of a hand-written
   re-implementation that can drift.

**The `RLIKE` trap.** Spark's `RLIKE` is a *search*, not a full match —
`'INV-2026-000001-JUNK'` satisfies an unanchored invoice pattern. The pandas
oracle uses `fullmatch`. The two only agree because every pattern is anchored,
so that invariant is *enforced* by `assert_patterns_are_anchored` and a test,
not trusted. An unanchored pattern would make the engines disagree for a reason
that looks like nothing at all.

**Row conservation.** `silver_rows + quarantine_rows == bronze_rows`, asserted
on every run. A failing row is not deleted, it is filed — with the full list of
rule ids that rejected it, because a row breaching three rules is three tickets
for three different people.

---

## Testing

113 tests. The ones that carry weight:

- **Precedence ladder** — one test per status, plus every ordering conflict
  (duplicate vs mismatch, duplicate vs timing, mismatch vs timing).
- **Tolerance boundary** — 101.00 against 100.00 is `MATCHED`; 101.01 is a
  break. Pinned because "is exactly 1.00 a break?" is the kind of question two
  people answer differently six months apart.
- **Negative controls** — drop a row, duplicate a row, repair a planted defect,
  convert a match into a break, tamper with the manifest. Each must make
  verification *fail*. A verification suite that cannot be made to fail is not
  evidence of anything.
- **Structural invariants** — the taxonomy is a partition; defects only ever
  land on single-sided keys.

---

## Limitations

Stated first because a reviewer will find them anyway.

- **Small dataset.** 1,909 source rows. Enough to exercise every code path,
  nowhere near enough to say anything about performance at scale.
- **Exact-key matching only.** No fuzzy matching. A real subledger has invoice
  numbers that differ by a leading zero or a stray hyphen, and this pipeline
  would report those as two separate breaks.
- **Single currency.** All amounts are USD and the contract enforces it. No FX
  conversion or revaluation.
- **One-to-one matching.** A GL row facing two *legitimately different*
  subledger lines (a genuine split invoice) is reported as a duplicate. The
  taxonomy has no `PARTIAL_MATCH` status.
- **Duplicates on AP-only keys are untested.** Two subledger rows on a key with
  no GL counterpart would classify as `DUPLICATE_IN_SUBLEDGER`; that case is
  not planted, so the behaviour is defined but unverified.
- **No CDC or incremental load.** Every run is a full refresh.
- **The verifier shares an author with the generator.** Differential testing
  catches implementation slips, not a shared misunderstanding of the spec.
  That is what the written break taxonomy and the unit tests are for.
- **The local Spark path is unverified.** Bronze and Silver run on Databricks,
  but local Spark will not start on the development machine: Netty cannot open
  a loopback connection inside the JVM (`failed to create a child event loop`),
  which is endpoint-security software blocking `java.exe`, not a pipeline fault
  — Python loopback on the same machine works fine. `LakehouseConfig(mode="path")`
  is therefore a documented fallback, not a tested one.
- **Some Spark errors are only reachable on a cluster.** Compiling rules to SQL
  makes the predicates unit-testable without a JVM, but Spark rejects some
  constructs at *parse* time — a window function nested inside an aggregate was
  found this way, not by tests. That class of bug is now guarded structurally
  (no aggregate expression may contain `OVER (`), which is the best a
  JVM-free test can do.
- **The reconciliation engine is still pandas-only.** Break counts quoted above
  come from the oracle; the PySpark port is Day 4.
- **Not yet built:** everything from Day 4 onward — see below.

---

## Roadmap

Marked honestly: none of this is built yet.

| Stage | Scope | Status |
|---|---|---|
| Day 1 | Scaffold, generator, planted breaks, manifest, contracts, verifier | **Done** |
| Day 2 | Explicit schemas, Bronze ingest to Delta | **Done — 1,909 rows ingested losslessly** |
| Day 3 | Silver layer in PySpark, contract enforcement, quarantine tables | **Done — matches the pandas oracle exactly** |
| Day 4 | Reconciliation engine + gold tables in PySpark | Not started |
| Day 5 | DQ scorecard, Databricks SQL dashboard | Not started |
| Day 6 | Data dictionary, metric definitions, runbook | Not started |
| v0.2 | Variance & driver analysis (rate / volume / mix waterfall) | Not started |
| v0.3 | Anomaly detection (z-score, Isolation Forest) | Not started |
| v0.4 | Break aging and SLA breach tracking | Not started |
| v0.5 | Databricks Workflows orchestration, GitHub Actions CI | Not started |
| v0.6 | Tableau Public dashboard, lineage diagram, RBAC matrix | Not started |

Days 3 and 4 rebuild the silver and reconciliation logic in PySpark. `validate.py`
becomes the pandas **oracle** for that work: two independent implementations, on
two different engines, must land on the same six numbers.

---

## A note on the data

**All data in this project is synthetic**, generated from a fixed seed. Vendor
names are assembled from invented word lists. Nothing here represents any real
company, client, employee, vendor, invoice, amount, or financial system.
