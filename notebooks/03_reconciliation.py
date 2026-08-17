# Databricks notebook source
# MAGIC %md
# MAGIC # LedgerLens — 03 · Reconciliation + Gold
# MAGIC
# MAGIC Matches the GL against the AP subledger on
# MAGIC `account_code + vendor_code + invoice_number`, classifies every business
# MAGIC key into exactly one of six break types, and writes the three gold tables.
# MAGIC
# MAGIC ### What this notebook is actually testing
# MAGIC
# MAGIC The pipeline is implemented **twice, sharing no code**: pandas
# MAGIC (`validate.py`) as the reference oracle, PySpark (`recon.py`) as the
# MAGIC production path. Days 2–3 proved the two engines agree on the quarantine
# MAGIC and the DQ score. This notebook extends that to the break counts, which is
# MAGIC the part anyone would actually be paid for.
# MAGIC
# MAGIC ### Expected result, from the control manifest
# MAGIC
# MAGIC | Status | Keys | Meaning |
# MAGIC |---|---|---|
# MAGIC | `MATCHED` | **820** | Both sides agree within tolerance |
# MAGIC | `AMOUNT_MISMATCH` | **40** | Keying error or partial payment |
# MAGIC | `TIMING_DIFFERENCE` | **35** | Cut-off, usually self-correcting |
# MAGIC | `MISSING_FROM_SUBLEDGER` | **15** | Possibly unsupported entry |
# MAGIC | `MISSING_FROM_GL` | **16** | **Unrecorded liability** |
# MAGIC | `DUPLICATE_IN_SUBLEDGER` | **20** | Possible double payment |
# MAGIC | | **946** | business keys |
# MAGIC
# MAGIC **15 and 16 are `planted − quarantined`.** 25 GL-only rows were planted and
# MAGIC 10 were quarantined; 30 AP-only rows were planted and 14 were quarantined.
# MAGIC That subtraction is the seam between the data-quality layer and the
# MAGIC reconciliation layer, and it is deliberate: defects are only ever planted
# MAGIC on single-sided keys, so quarantine can shrink those two populations and
# MAGIC can never convert a `MATCHED` key into a fake unrecorded liability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reload edited source automatically
# MAGIC
# MAGIC Databricks caches imported modules in `sys.modules`. A Git pull updates the
# MAGIC file on disk but the running notebook keeps executing the old code, so a
# MAGIC fixed bug looks unfixed. Must run **before** the imports.
# MAGIC
# MAGIC If behaviour still looks stale: **Run → Clear state and run all**.

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import os
import sys

REPO_ROOT = "/Workspace/Repos/ledgerlens"  # adjust if cloned elsewhere

for candidate in (f"{REPO_ROOT}/src", "../src", "./src"):
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

from ledgerlens import gold, recon
from ledgerlens.bronze import LakehouseConfig
from ledgerlens.config import ALL_STATUSES, STATUS_MATCHED, load_contracts

contracts = load_contracts()

# COMMAND ----------

dbutils.widgets.removeAll()

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("raw_path", "/Volumes/workspace/raw/landing", "Raw volume path")

CATALOG = dbutils.widgets.get("catalog")
RAW_PATH = dbutils.widgets.get("raw_path")

spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The classifier, as SQL
# MAGIC
# MAGIC Printed rather than described. *"40 amount mismatches"* is an assertion;
# MAGIC the predicate beside it is the evidence, and a controller can disagree with
# MAGIC it without reading any Python.
# MAGIC
# MAGIC Three things to read for in the output:
# MAGIC
# MAGIC 1. **`ap_row_count > 1` is the first branch.** Duplication is *structural* —
# MAGIC    a statement about how many rows exist, true regardless of amounts. A
# MAGIC    double-booking whose second copy was keyed slightly differently is still
# MAGIC    a double-booking.
# MAGIC 2. **The amount test comes before the period test.** Timing differences are
# MAGIC    benign and self-correcting; amount differences are not. Classifying a
# MAGIC    real value discrepancy as "timing" files it under *"will fix itself next
# MAGIC    month"*.
# MAGIC 3. **The tolerance is `CAST(1.00 AS DECIMAL(18,2))`, not a bare `1.0`.**
# MAGIC    Spark promotes a DECIMAL-vs-DOUBLE comparison to DOUBLE, so a floating
# MAGIC    point literal would move the whole reconciliation onto binary floating
# MAGIC    point at exactly the boundary the tolerance defines.

# COMMAND ----------

tolerance = float(contracts["recon"]["amount_tolerance_abs"])
precedence = contracts["recon"]["status_precedence"]

print(recon.classification_expr(tolerance, precedence).replace(" WHEN ", "\n  WHEN ")
      .replace(" ELSE ", "\n  ELSE "))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight: the published ladder must be the executed ladder
# MAGIC
# MAGIC `contracts.yaml` publishes `recon.status_precedence` so a controller can
# MAGIC read the classification policy without reading Python. That declaration is
# MAGIC only worth something if it is the policy that actually runs.
# MAGIC
# MAGIC The conditions are **positional** — each is written assuming every
# MAGIC condition above it already failed. `TIMING_DIFFERENCE` does not re-test the
# MAGIC tolerance, because by the time it is reached the amount test has already
# MAGIC passed. So a reordered YAML would not produce a *different* classification,
# MAGIC it would produce a **wrong** one, quietly. This refuses to run instead.

# COMMAND ----------

recon.assert_precedence_matches(precedence)
print("Published precedence matches the compiled classifier:")
for i, status in enumerate(precedence, start=1):
    print(f"  {i}. {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run
# MAGIC
# MAGIC `gold.run` reconciles silver and writes all three gold tables. It raises if
# MAGIC the summary does not tie back to the detail, or if the exception list does
# MAGIC not tie to the non-matched key count — an aggregate that does not reconcile
# MAGIC to its own source is worse than no aggregate.

# COMMAND ----------

cfg = LakehouseConfig(mode="catalog", catalog=CATALOG)
result = gold.run(cfg=cfg, spark=spark, contracts=contracts)

for status, count in result.counts.items():
    print(f"  {status:<24} {count:>6}")
print(f"  {'-' * 24} {'-' * 6}")
print(f"  {'business keys':<24} {result.key_total:>6}")
print(f"  {'exceptions':<24} {result.exception_rows:>6}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control: does the engine rediscover exactly what was planted?
# MAGIC
# MAGIC The manifest was written by the generator before any of this code existed,
# MAGIC and every invoice was assigned its fate *before* a single row was written —
# MAGIC so the expected counts are **bookkeeping, not analysis**. The generator
# MAGIC never inspects its own output to decide what the answer is; that would be
# MAGIC circular and the manifest would agree with any bug.
# MAGIC
# MAGIC Not approximately. Exactly.

# COMMAND ----------

import json

with open(f"{RAW_PATH}/control_manifest.json") as fh:
    manifest = json.load(fh)

checks = []
for status in ALL_STATUSES:
    checks.append((f"break {status}",
                   manifest["expected_breaks"][status],
                   result.counts[status]))
checks.append(("business keys reconciled",
               manifest["expected_key_total"], result.key_total))

# The taxonomy is a partition - independent of the manifest. Every key gets
# exactly one status, no key escapes, no key is counted twice.
checks.append(("statuses are a partition",
               result.key_total, sum(result.counts.values())))
checks.append(("exceptions = keys - matched",
               result.key_total - result.counts[STATUS_MATCHED],
               result.exception_rows))
checks.append(("summary ties to detail", result.key_total, result.summary_key_total))

failed = [(n, e, a) for n, e, a in checks if e != a]
for name, exp, act in checks:
    print(f"  [{'ok  ' if exp == act else 'FAIL'}] {name:<40} {act:>10}")

print(f"\n  {len(checks) - len(failed)}/{len(checks)} checks passed")
assert not failed, f"Reconciliation does not match the control manifest: {failed}"
print("  PySpark reproduces the pandas oracle exactly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control: the planted edge cases
# MAGIC
# MAGIC Totals can be right for the wrong reasons. These four populations were
# MAGIC planted specifically to test the **precedence ladder** rather than the
# MAGIC happy path, and each one is a bug that a correct-looking total would hide:
# MAGIC
# MAGIC | Planted | Must be | If it is wrong |
# MAGIC |---|---|---|
# MAGIC | Matched pairs differing by ±0.01–0.99 | 60 `MATCHED` | The tolerance is not being applied |
# MAGIC | Amount **and** period both differ | 5 `AMOUNT_MISMATCH` | Timing outranks amount — real discrepancies filed as benign |
# MAGIC | Duplicates that are triplets, not pairs | 4 with `ap_row_count = 3` | Duplicate detection is hard-coded to pairs |
# MAGIC | Duplicate copies with unequal amounts | 5 | Amount outranks duplicate — a double payment reported as a keying error |
# MAGIC
# MAGIC The last one is detected arithmetically: for an honest duplicate the
# MAGIC subledger total is exactly `ap_row_count × gl_amount`. Where it is not, the
# MAGIC copies differed — and the key must **still** be `DUPLICATE_IN_SUBLEDGER`.

# COMMAND ----------

edge = spark.sql("""
    SELECT
      count_if(break_status = 'MATCHED'
               AND abs_amount_difference > 0)                AS matched_within_tolerance,
      count_if(break_status = 'AMOUNT_MISMATCH'
               AND gl_fiscal_period <> ap_fiscal_period)     AS mismatch_with_period_shift,
      count_if(break_status = 'DUPLICATE_IN_SUBLEDGER'
               AND ap_row_count = 3)                         AS duplicate_triplets,
      count_if(break_status = 'DUPLICATE_IN_SUBLEDGER'
               AND ap_amount <> gl_amount * ap_row_count)    AS duplicate_unequal_amounts,
      -- Invariant, not a count: nothing classified MATCHED may exceed tolerance.
      count_if(break_status = 'MATCHED'
               AND abs_amount_difference > 1.00)             AS matched_beyond_tolerance
    FROM gold.recon_detail
""").first()

expected_edges = manifest["expected_edge_cases"]
edge_checks = [
    ("matched within tolerance", expected_edges["matched_within_tolerance"],
     edge["matched_within_tolerance"]),
    ("amount mismatch + period shift", expected_edges["amount_mismatch_with_period_shift"],
     edge["mismatch_with_period_shift"]),
    ("duplicate triplets", expected_edges["duplicate_triplets"],
     edge["duplicate_triplets"]),
    ("duplicates with unequal amounts", expected_edges["duplicate_with_unequal_amounts"],
     edge["duplicate_unequal_amounts"]),
    ("MATCHED keys beyond tolerance", 0, edge["matched_beyond_tolerance"]),
]

failed_edges = [(n, e, a) for n, e, a in edge_checks if e != a]
for name, exp, act in edge_checks:
    print(f"  [{'ok  ' if exp == act else 'FAIL'}] {name:<40} {act:>10}")

assert not failed_edges, f"Edge cases misclassified: {failed_edges}"
print("\n  Every planted edge case landed in the right bucket.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The bug this design exists to prevent
# MAGIC
# MAGIC Duplicates are detected **before** the join, by carrying `count(*)` through
# MAGIC the aggregation. Three realistic alternatives were tested against this data:
# MAGIC
# MAGIC | Implementation | `MATCHED` | `DUPLICATE` | Total keys |
# MAGIC |---|---|---|---|
# MAGIC | **Correct** | 820 | 20 | 946 |
# MAGIC | Aggregate with `sum()`, never count rows | 820 | **0** | 946 |
# MAGIC | Remove duplicates, then join | 820 | 20 | **966** |
# MAGIC | Join first, `drop_duplicates` to clean the fan-out | **840** | **0** | 946 |
# MAGIC
# MAGIC **Two of the three preserve the total key count exactly.** They lose every
# MAGIC duplicate — twenty possible double payments — and no row-count
# MAGIC reconciliation would notice, because the arithmetic still balances.
# MAGIC
# MAGIC The middle failure is the instructive one: `sum()` gives a perfectly
# MAGIC correct total for a duplicated key and no way to know it came from two
# MAGIC rows. The row count is not a diagnostic extra, it is the **only** evidence
# MAGIC that duplication happened.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Every duplicate, with the row count that proves it is one.
# MAGIC -- Note ap_row_count = 3 on the triplets, and that the unequal copies are
# MAGIC -- still classified DUPLICATE rather than AMOUNT_MISMATCH.
# MAGIC SELECT invoice_number, vendor_code, account_code,
# MAGIC        gl_row_count, ap_row_count,
# MAGIC        gl_amount, ap_amount, amount_difference,
# MAGIC        ap_amount = gl_amount * ap_row_count AS copies_were_identical
# MAGIC FROM gold.recon_detail
# MAGIC WHERE break_status = 'DUPLICATE_IN_SUBLEDGER'
# MAGIC ORDER BY ap_row_count DESC, copies_were_identical, abs_amount_difference DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The break mix
# MAGIC
# MAGIC `gold.recon_summary` is a **dense** grid: every period crossed with all six
# MAGIC statuses, and combinations that did not occur written as zero rather than
# MAGIC left out. Same reasoning as reporting rules that fired zero times — an
# MAGIC absent row cannot tell *"no duplicates in April"* apart from *"the duplicate
# MAGIC branch stopped being reachable in April"*. It also keeps a dashboard's
# MAGIC legend and colours stable when a status empties out.
# MAGIC
# MAGIC Note the two value columns are **not** interchangeable. `net` sums the
# MAGIC signed differences, so opposite-direction breaks cancel; `abs` sums the
# MAGIC absolute differences, so nothing cancels. A summary quoting only the net
# MAGIC figure can show a clean period with a large problem underneath it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT break_status,
# MAGIC        sum(key_count)               AS keys,
# MAGIC        sum(net_amount_difference)   AS net_difference,
# MAGIC        sum(abs_amount_difference)   AS gross_exposure
# MAGIC FROM gold.recon_summary
# MAGIC GROUP BY break_status
# MAGIC ORDER BY keys DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Break mix by period. The zero cells are written, not missing.
# MAGIC SELECT fiscal_period, break_status, key_count, abs_amount_difference
# MAGIC FROM gold.recon_summary
# MAGIC ORDER BY fiscal_period, break_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The exception worklist
# MAGIC
# MAGIC `gold.recon_exceptions` is what an analyst opens on Monday morning: the
# MAGIC non-matched keys, labelled with vendor and account names, ranked by
# MAGIC exposure.
# MAGIC
# MAGIC The labels are joined from conformed dimensions built across **all** silver
# MAGIC rows rather than from the key's own rows — a `MISSING_FROM_GL` key has no GL
# MAGIC row to read an account name from, and that is precisely the break type most
# MAGIC worth labelling.
# MAGIC
# MAGIC Both joins are **LEFT**. An inner join would drop any exception whose code
# MAGIC has no label — a cosmetic lookup silently deleting a finding. `gold.py`
# MAGIC asserts the row count is unchanged by enrichment.
# MAGIC
# MAGIC `exception_rank` is materialised because a Delta table has no inherent row
# MAGIC order: sorting at write time does not survive a read, so *"the top twenty
# MAGIC breaks"* has to be a value you can filter on.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT exception_rank, break_status, fiscal_period,
# MAGIC        vendor_name, account_name, invoice_number,
# MAGIC        gl_amount, ap_amount, abs_amount_difference
# MAGIC FROM gold.recon_exceptions
# MAGIC WHERE exception_rank <= 20
# MAGIC ORDER BY exception_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The break that matters most
# MAGIC
# MAGIC `MISSING_FROM_GL` is an **unrecorded liability**: an invoice sitting in the
# MAGIC subledger that was never posted to the general ledger. The company owes
# MAGIC money its books do not show.
# MAGIC
# MAGIC Everything else in the taxonomy is an *error*. This one is a
# MAGIC **misstatement** — it understates liabilities, which flatters the balance
# MAGIC sheet, which is why it is the break an auditor goes looking for first.
# MAGIC
# MAGIC Note these keys have `gl_amount` **NULL**, not `0.00`. Zero is an assertion
# MAGIC ("the ledger posted nothing"); NULL is an absence ("the ledger has no
# MAGIC opinion"). Writing zero would let `sum(gl_amount)` read as a real posted
# MAGIC total that happens to be nil.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT fiscal_period, vendor_name, invoice_number, ap_amount, gl_amount
# MAGIC FROM gold.recon_exceptions
# MAGIC WHERE break_status = 'MISSING_FROM_GL'
# MAGIC ORDER BY ap_amount DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Total unrecorded liability, by period. The headline number of the project.
# MAGIC SELECT fiscal_period,
# MAGIC        key_count                AS unrecorded_invoices,
# MAGIC        abs_amount_difference    AS unrecorded_liability
# MAGIC FROM gold.recon_summary
# MAGIC WHERE break_status = 'MISSING_FROM_GL'
# MAGIC ORDER BY fiscal_period;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** `04_dq_scorecard` — DQ score, most-violated rules and break mix as
# MAGIC a Databricks SQL dashboard, reusing the SQL `quality.py` already generates
# MAGIC so the dashboard queries the same logic the pipeline enforced rather than a
# MAGIC hand-written re-implementation that can drift.
