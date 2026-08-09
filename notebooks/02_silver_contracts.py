# Databricks notebook source
# MAGIC %md
# MAGIC # LedgerLens — 02 · Silver + Quarantine
# MAGIC
# MAGIC Applies `config/contracts.yaml` to the bronze tables. Rows that pass are
# MAGIC typed and written to **silver**. Rows that fail are written to
# MAGIC **quarantine**, each carrying the rule ids that rejected it.
# MAGIC
# MAGIC ### The two things this layer guarantees
# MAGIC
# MAGIC **1. No row is silently dropped.** Every bronze row leaves through
# MAGIC exactly one door: `silver_rows + quarantine_rows == bronze_rows`, asserted
# MAGIC on every run. A failing row is not deleted, it is *filed*.
# MAGIC
# MAGIC **2. Casting happens after the contract, never before.** A cast is
# MAGIC destructive on bad data — it turns `"N/A"` into `NULL` and erases the
# MAGIC difference between *absent* and *unreadable*. By the time a row is cast,
# MAGIC the contract has guaranteed the cast will succeed. A cast failure here is
# MAGIC a bug in the contract, not bad data.
# MAGIC
# MAGIC ### Expected result, from the control manifest
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Rows quarantined | **24** (GL 10, AP 14) |
# MAGIC | Rule violations | **26** — two rows breach two rules each |
# MAGIC | Rows surviving to silver | **1,885** |
# MAGIC | DQ score | **98.7428%** |

# COMMAND ----------

import os
import sys

REPO_ROOT = "/Workspace/Repos/ledgerlens"  # adjust if cloned elsewhere

for candidate in (f"{REPO_ROOT}/src", "../src", "./src"):
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from ledgerlens import quality, silver
from ledgerlens.bronze import LakehouseConfig
from ledgerlens.config import load_contracts

contracts = load_contracts()

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("raw_path", "/Volumes/workspace/raw/landing", "Raw volume path")

CATALOG = dbutils.widgets.get("catalog")
RAW_PATH = dbutils.widgets.get("raw_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The rules, as SQL
# MAGIC
# MAGIC Rules are compiled from YAML into Spark SQL predicates that are TRUE when
# MAGIC the rule is **violated**. Printing them matters: *"GL_NONZERO_AMOUNT
# MAGIC rejected 2 rows"* is an assertion, and the SQL beside it is the evidence.
# MAGIC A controller can read this and disagree without reading any Python.
# MAGIC
# MAGIC Note the null handling. Every check except `not_null` skips blanks, and
# MAGIC `non_zero` / `numeric_range` also skip values that do not parse. One
# MAGIC missing amount is **one** defect, not four — otherwise the DQ score's
# MAGIC denominator inflates and the "most violated rule" chart fills up with
# MAGIC knock-on effects instead of causes.

# COMMAND ----------

print(quality.describe(contracts["datasets"]["ap"]["rules"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight: every regex must be anchored
# MAGIC
# MAGIC Spark's `RLIKE` is a **search**, not a full match — `'INV-2026-000001-JUNK'`
# MAGIC would satisfy an unanchored invoice pattern. The pandas reference
# MAGIC implementation uses `fullmatch`, so an unanchored pattern would make the
# MAGIC two engines disagree for a reason nobody would think to look for.
# MAGIC
# MAGIC This is checked before any data is touched, so the job fails in two
# MAGIC seconds rather than halfway through a write.

# COMMAND ----------

quality.assert_patterns_compile(contracts)
quality.assert_patterns_are_anchored(contracts)
print("All regex patterns compile and are anchored at both ends.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

cfg = LakehouseConfig(mode="catalog", catalog=CATALOG)
results = silver.run(cfg=cfg, spark=spark, contracts=contracts)

for r in results:
    print(f"{r.dataset:<4} bronze {r.bronze_rows:>5} -> silver {r.silver_rows:>5} "
          f"| quarantine {r.quarantine_rows:>3} ({r.total_violations} violations)")

print(f"\nDQ score: {silver.dq_score(results)}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control: does the pipeline rediscover what was planted?
# MAGIC
# MAGIC This is the cell that matters. The manifest was written by the generator
# MAGIC before this code existed, and it records the expected count for **every**
# MAGIC rule — including the 21 expected to fire zero times. A rule that suddenly
# MAGIC starts rejecting rows is as much a signal as one that stops.

# COMMAND ----------

import json

with open(f"{RAW_PATH}/control_manifest.json") as fh:
    manifest = json.load(fh)

expected_q = manifest["expected_quarantine"]
by_dataset = {r.dataset: r for r in results}

checks = []
checks.append(("gl rows quarantined", expected_q["gl_rows"], by_dataset["gl"].quarantine_rows))
checks.append(("ap rows quarantined", expected_q["ap_rows"], by_dataset["ap"].quarantine_rows))
checks.append(("total rows quarantined", expected_q["total_rows"],
               sum(r.quarantine_rows for r in results)))
checks.append(("total rule violations", expected_q["total_violations"],
               sum(r.total_violations for r in results)))
checks.append(("rows surviving to silver",
               manifest["row_counts"]["gl_after_quarantine"]
               + manifest["row_counts"]["ap_after_quarantine"],
               sum(r.silver_rows for r in results)))
checks.append(("DQ score %", manifest["expected_dq_score_pct"], silver.dq_score(results)))

# Every declared rule, including the silent ones.
for dataset in ("gl", "ap"):
    expected_map = expected_q["by_rule"][dataset]
    observed = by_dataset[dataset].violations
    for rule_id in sorted(observed):
        checks.append((f"rule {rule_id}", expected_map.get(rule_id, 0), observed[rule_id]))

failed = [(n, e, a) for n, e, a in checks if e != a]
for name, exp, act in checks:
    if exp or act or name in {c[0] for c in failed}:
        print(f"  [{'ok  ' if exp == act else 'FAIL'}] {name:<40} {act:>10}")

print(f"\n  {len(checks) - len(failed)}/{len(checks)} checks passed")
assert not failed, f"Silver does not match the control manifest: {failed}"
print("  Silver and quarantine match the control manifest exactly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Row conservation
# MAGIC
# MAGIC The "never silently drop a row" principle, stated as arithmetic.

# COMMAND ----------

for r in results:
    print(f"  {r.dataset}: {r.silver_rows} + {r.quarantine_rows} "
          f"= {r.silver_rows + r.quarantine_rows} (bronze had {r.bronze_rows}) "
          f"{'OK' if r.conserved else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The quarantine table
# MAGIC
# MAGIC This is what makes the layer auditable rather than merely defensive. Each
# MAGIC row keeps its original untouched strings plus the rule ids that rejected
# MAGIC it — so *"why was this row rejected?"* is answerable months later from the
# MAGIC data alone, with no access to the code that rejected it.
# MAGIC
# MAGIC Look for the rows with `_failed_rule_count = 2`. A row breaching two
# MAGIC rules is two tickets for two different people, which is why the column is
# MAGIC a **list** and not a single value.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _failed_rule_ids, _failed_rule_count, ap_line_id, invoice_number,
# MAGIC        amount, currency, fiscal_period, vendor_code, payment_status
# MAGIC FROM identifier(:catalog || '.quarantine.ap_subledger')
# MAGIC ORDER BY _failed_rule_count DESC, _failed_rule_ids;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which rule rejected the most rows. The DQ scorecard is built on this.
# MAGIC SELECT _failed_rule_ids AS rule_combination,
# MAGIC        count(*) AS rows_rejected
# MAGIC FROM (
# MAGIC   SELECT _failed_rule_ids FROM identifier(:catalog || '.quarantine.gl')
# MAGIC   UNION ALL
# MAGIC   SELECT _failed_rule_ids FROM identifier(:catalog || '.quarantine.ap_subledger')
# MAGIC )
# MAGIC GROUP BY _failed_rule_ids
# MAGIC ORDER BY rows_rejected DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver is typed
# MAGIC
# MAGIC `amount` is `DECIMAL(18,2)`, not `DOUBLE`. Binary floating point cannot
# MAGIC represent `0.01` exactly, and summing thousands of invoice amounts in a
# MAGIC double accumulates error. A reconciliation that tolerates `1.00` must not
# MAGIC itself be the source of sub-cent drift.
# MAGIC
# MAGIC `fiscal_period` stays a **string**. An accounting period is a stated
# MAGIC attribute that closes on a decision, not a truncated timestamp. Typing it
# MAGIC as a date invites `date_trunc('month', posting_date)` downstream, which
# MAGIC would silently reclassify every cut-off entry and manufacture timing
# MAGIC differences that do not exist.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE identifier(:catalog || '.silver.ap_subledger');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** `03_reconciliation` — match GL against AP on
# MAGIC `account_code + vendor_code + invoice_number`, classify every business key
# MAGIC into exactly one of six break types, and write the gold tables.
# MAGIC
# MAGIC Expected: **MATCHED 820 · AMOUNT_MISMATCH 40 · TIMING_DIFFERENCE 35 ·
# MAGIC MISSING_FROM_SUBLEDGER 15 · MISSING_FROM_GL 16 ·
# MAGIC DUPLICATE_IN_SUBLEDGER 20 = 946 business keys.**
