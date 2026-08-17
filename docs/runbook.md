# Runbook

How to run LedgerLens, what breaks, and what to do about it.

Written for the person on call at 07:00 who did not build this. Every failure
mode listed below has actually happened during development.

---

## What this pipeline does

Reconciles a general ledger against an AP subledger, classifies every business
key into one of six break types, and publishes the result plus a data-quality
scorecard as gold Delta tables.

**Full refresh, every run.** There is no CDC and no incremental load — every run
replaces every table. That is a stated limitation, not an oversight: incremental
load needs a watermark the source extract does not carry.

---

## Prerequisites

| | |
|---|---|
| Compute | Databricks **serverless** (Free Edition is sufficient) |
| Catalog | `workspace` |
| Schemas | `raw`, `bronze`, `silver`, `quarantine`, `gold` — created by the notebooks if absent |
| Volume | `/Volumes/workspace/raw/landing` containing `gl.csv`, `ap_subledger.csv`, `control_manifest.json` |
| Repo | Cloned as a Databricks **Git folder** |

> **Never upload `planted_breaks.csv` to the volume.** It is the per-invoice
> answer key. The pipeline must not be able to see it, or the verification proves
> nothing.

---

## Running it

### Locally (generation, verification, tests)

```bash
python -m ledgerlens.generate_data
```

```bash
python -m ledgerlens.validate
```

```bash
pytest
```

```bash
python -m ledgerlens.docs_gen
```

If the package is not pip-installed:

```bash
$env:PYTHONPATH="C:\Users\Jenil Shah\Desktop\Ledger\src"
```

**Nothing above needs Spark.** That is by design — see *Known limitations*.

### On Databricks — run in order

| # | Notebook | Produces | Runtime |
|---|---|---|---|
| 1 | `01_bronze_ingest` | `bronze.gl`, `bronze.ap_subledger` | ~1 min |
| 2 | `02_silver_contracts` | `silver.*`, `quarantine.*` | ~2 min |
| 3 | `03_reconciliation` | `gold.recon_detail`, `gold.recon_summary`, `gold.recon_exceptions` | ~2 min |
| 4 | `04_dq_scorecard` | `gold.dq_scorecard`, `gold.dq_rule_scorecard` | ~1 min |

Each notebook asserts its own results against `control_manifest.json` and
**fails loudly** rather than publishing wrong numbers quietly.

Then build the dashboard from [`dashboard_layout.md`](dashboard_layout.md) and
[`dashboard_queries.sql`](dashboard_queries.sql).

---

## Expected output — the numbers to check against

If any of these differ, **stop and investigate**. Do not publish.

```
SOURCE          970 invoices -> 940 GL rows + 969 AP rows = 1,909 rows
QUARANTINE      24 rows, 26 rule violations
                  GL: 10 rows / 12 violations   (two rows breach two rules each)
                  AP: 14 rows / 14 violations
SILVER          1,885 rows (GL 930 + AP 955)
DQ SCORE        98.7428%

BREAKS (946 business keys)
  MATCHED                 820
  AMOUNT_MISMATCH          40
  TIMING_DIFFERENCE        35
  MISSING_FROM_SUBLEDGER   15
  MISSING_FROM_GL          16
  DUPLICATE_IN_SUBLEDGER   20
  EXCEPTIONS              126
```

`15` and `16` are `planted − quarantined`: 25 GL-only rows planted, 10
quarantined; 30 AP-only rows planted, 14 quarantined. That subtraction is the
seam between the DQ layer and the recon layer.

---

## Failure modes

Ordered by how often they actually happened.

### `ModuleNotFoundError: No module named 'ledgerlens'`

The notebook's `REPO_ROOT` does not match where the Git folder actually lives.
Free Edition often clones under `/Workspace/Users/<you>/...` rather than
`/Workspace/Repos/...`.

**Fix:** edit `REPO_ROOT` at the top of the notebook, or run the notebook from
inside the repo folder so the `../src` fallback resolves.

---

### A bug you already fixed still appears

Databricks caches imported modules in `sys.modules`. A Git pull updates the file
on disk, but the running Python process never re-reads it.

**Fix:** **Run → Clear state and run all.** Every notebook starts with
`%load_ext autoreload` / `%autoreload 2`, but autoreload only refreshes modules
already imported — it does not reliably handle *brand new* modules that were
never in `sys.modules`.

---

### The notebook does not appear in Databricks after a merge

Databricks Git folders **never auto-pull**. A merged PR on GitHub does not reach
the workspace on its own.

**Fix:** click the branch name at the top of the Git folder, confirm it says
`main`, click **Pull**. If the pull is refused, something in the folder was
edited in place — discard those edits from the same dialog.

---

### `NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE`

Something called `.cache()` or `.persist()`. Serverless manages its own caching
and rejects explicit persistence.

**Fix:** remove the call. At this data volume it buys nothing anyway. See
*Serverless rules* below.

---

### `It is not allowed to use a window function inside an aggregate function`

An aggregate contains `OVER (` — for example `count_if(count(*) OVER (...))` from
the `unique` rules.

Windows are legal in a **projection**, illegal inside an **aggregate**.

**Fix:** split into two steps — project the predicate to a boolean flag, then
aggregate the flags. `quality.rule_flag_exprs` → `quality.rule_count_exprs`
already does this; follow the same shape for anything new. Guarded by a test that
rejects `OVER (` inside any aggregate expression.

---

### `PARSE_SYNTAX_ERROR at or near '||'` in a `%sql` cell

`identifier(:catalog || '.' || :schema || '.gl')`. Databricks `identifier()`
requires a constant string and rejects `||` on parameter markers.

**Fix:** call `spark.sql(f"USE CATALOG {CATALOG}")` once, then use plain two-part
names (`silver.gl`). Reads better anyway.

---

### `contracts.yaml recon.status_precedence is in a different order…`

Someone reordered the precedence ladder in `contracts.yaml`.

This is a **hard stop by design**. The classifier's conditions are positional —
each assumes every condition above it already failed — so a reorder does not
produce a different classification, it produces a silently wrong one. Put
`TIMING_DIFFERENCE` above `AMOUNT_MISMATCH` and every mismatch that also shifted
period is filed as benign, while the totals still sum to 946.

**Fix:** restore the declared order, or change
`recon.CLASSIFICATION_LADDER` *and* the conditions together, deliberately.

---

### `Row conservation failed for <dataset>`

`silver_rows + quarantine_rows ≠ bronze_rows`. A row went missing.

**This is the most serious failure in the list.** It means the "never silently
drop a row" guarantee broke.

**Fix:** do not work around it. Compare bronze against the source file, then
check whether a filter or a join in `silver.py` is dropping rows. Escalate before
publishing anything.

---

### `<dataset> source header does not match the contract`

The upstream extract renamed, reordered or dropped a column.

**This control is working, not failing.** Spark applies a supplied schema
*positionally* and ignores the header — without this check, a reordered extract
loads cleanly, puts vendor codes in the invoice field, and produces a
reconciliation where every number is wrong but plausible.

**Fix:** get the extract corrected upstream. Only update
`schemas.GL_SOURCE_COLUMNS` / `AP_SOURCE_COLUMNS` if the change is intentional
and agreed — and regenerate the control manifest if it is.

---

### `docs/data_dictionary.md is stale`

`schemas.py` changed and the dictionary was not regenerated.

**Fix:**

```bash
python -m ledgerlens.docs_gen
```

---

### Break counts differ from the manifest

The reconciliation logic changed, or the data did.

**Check in this order:**

1. Did the source files change? The manifest records a SHA-256 for each.
2. Did quarantine counts change? `MISSING_FROM_*` are `planted − quarantined`,
   so a DQ change moves them legitimately.
3. Did the precedence ladder or tolerance change in `contracts.yaml`?
4. Run `python -m ledgerlens.validate` — the pandas oracle is independent of the
   Spark path. If pandas agrees with the manifest and Spark does not, the bug is
   in the Spark port.

**The one to suspect first:** `DUPLICATE_IN_SUBLEDGER` dropping to 0 while the
total key count stays at 946. That is the signature of duplicate detection moving
to *after* the join, and it is silent — the arithmetic still balances.

---

## Serverless rules for any new Spark code

- ❌ No `.cache()` / `.persist()`
- ❌ No `sparkContext`, no RDDs, no session `.config()`
- ❌ No `identifier(:param || …)` in `%sql`
- ❌ No window function inside an aggregate
- ❌ No generator (`explode`) inside an aggregate
- ✅ `get_spark()`'s early return on `getActiveSession()` is **load-bearing** — it
  keeps the local-only config unreachable on serverless. Do not move
  configuration above that guard.

---

## Known limitations

- **Local Spark does not run on the development machine.** Netty fails with
  `failed to create a child event loop` → `Unable to establish loopback
  connection`. Python opens loopback sockets fine on the same machine, so it is
  endpoint security blocking `java.exe`. Already ruled out: tool sandbox,
  `java.io.tmpdir`, IPv4/Netty flags. **Do not re-debug this** — all Spark
  execution happens on Databricks.

  Consequence, and the reason the codebase looks the way it does: every rule,
  predicate and classifier compiles to a **SQL string** rather than a
  `pyspark.sql.Column`, so the logic is unit-testable with no JVM. Only execution
  needs a cluster.

- **`LakehouseConfig(mode="path")` is a documented fallback, not a tested one.**
  It cannot be exercised locally for the reason above.

- **Some Spark errors are only reachable on a cluster.** Parse-time rejections
  are invisible to string-level tests. Guarded structurally where possible.

- **Small dataset.** 1,909 source rows exercise every code path and say nothing
  about performance at scale.

- **No orchestration.** Notebooks are run by hand, in order. Databricks Workflows
  is on the roadmap at v0.5.

---

## Escalation

This is a portfolio project with one maintainer, so the honest escalation path is
short. In a real deployment the columns below would carry names.

| Symptom | Owner in a real deployment |
|---|---|
| Source file missing or malformed header | Upstream data engineering |
| A contract rule rejects rows it should not | Financial Control (contract steward) |
| Row conservation failure, cast failure in silver | Pipeline owner — treat as a code bug |
| Break counts disagree with the manifest | Pipeline owner |
| Serverless / platform errors | Platform team |

Contract ownership is declared in `config/contracts.yaml` under `metadata`:
owner **Data Engineering**, steward **Financial Control**.

---

*Synthetic demonstration project. All data is fictional and does not represent
any real company, client, employee, vendor, or financial system.*
