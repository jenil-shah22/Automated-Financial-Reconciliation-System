# Databricks notebook source
# MAGIC %md
# MAGIC # LedgerLens — 04 · Data Quality Scorecard
# MAGIC
# MAGIC Turns what the pipeline already knows about its own quality into two gold
# MAGIC tables a dashboard can bind to.
# MAGIC
# MAGIC | Table | Grain | Answers |
# MAGIC |---|---|---|
# MAGIC | `gold.dq_scorecard` | one row per dataset | *Is this data fit to reconcile?* |
# MAGIC | `gold.dq_rule_scorecard` | one row per contract rule | *What is wrong with it?* |
# MAGIC
# MAGIC ### Where the numbers come from — and why that is the interesting part
# MAGIC
# MAGIC Per-rule counts are read **back out of the quarantine table**, by exploding
# MAGIC the `_failed_rule_ids` the pipeline stamped onto each rejected row. They are
# MAGIC *not* recomputed by re-running the rules.
# MAGIC
# MAGIC That is deliberate, and it is the opposite of the obvious choice.
# MAGIC Re-running the predicates would produce a **second opinion**, and a
# MAGIC scorecard whose numbers are a second opinion can disagree with the pipeline
# MAGIC that actually rejected the rows. Reading the recorded ids means the
# MAGIC scorecard reports what *happened*.
# MAGIC
# MAGIC The rule **catalogue** — id, column, check type, description and the SQL
# MAGIC predicate — does come from `quality.py`. So every row carries the count and
# MAGIC the exact logic that produced it, side by side.
# MAGIC
# MAGIC ### Expected result, from the control manifest
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | Rows received | **1,909** |
# MAGIC | Rows passed | **1,885** |
# MAGIC | Rows quarantined | **24** |
# MAGIC | Rule violations | **26** — two rows breach two rules each |
# MAGIC | DQ score | **98.7428%** |

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

from ledgerlens import scorecard
from ledgerlens.bronze import LakehouseConfig
from ledgerlens.config import load_contracts

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
# MAGIC ## The counting query
# MAGIC
# MAGIC `explode` turns one quarantined row carrying two rule ids into two rows, so
# MAGIC a row that breached two rules counts once against each. **That is why
# MAGIC violations (26) exceed quarantined rows (24)**, and why the two numbers
# MAGIC must never be used interchangeably.
# MAGIC
# MAGIC Note the shape: `explode` sits in the inner projection and the aggregate in
# MAGIC the outer one. Spark rejects a generator nested inside an aggregate at
# MAGIC *parse* time — the same class of cluster-only failure as the
# MAGIC window-inside-aggregate bug found on day 3, so it gets the same structural
# MAGIC guard in the tests.

# COMMAND ----------

print(scorecard.violations_sql(["quarantine.gl", "quarantine.ap_subledger"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

cfg = LakehouseConfig(mode="catalog", catalog=CATALOG)
result = scorecard.run(cfg=cfg, spark=spark, contracts=contracts)

print(f"  rows received     {result.rows_received:>6}")
print(f"  rows passed       {result.rows_passed:>6}")
print(f"  rows quarantined  {result.rows_quarantined:>6}")
print(f"  rule violations   {result.rule_violations:>6}")
print(f"  DQ score          {result.dq_score_pct:>6}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control: every rule, including the silent ones
# MAGIC
# MAGIC The manifest records the expected count for **every** declared rule, not
# MAGIC just the ones expected to fire. A rule that suddenly starts rejecting rows
# MAGIC is as much a signal as one that stops — and a scorecard showing only
# MAGIC non-zero rules cannot tell *"clean"* apart from *"that check silently
# MAGIC stopped running"*.

# COMMAND ----------

import json

with open(f"{RAW_PATH}/control_manifest.json") as fh:
    manifest = json.load(fh)

expected_q = manifest["expected_quarantine"]

checks = [
    ("rows received", manifest["row_counts"]["total_raw"], result.rows_received),
    ("rows passed",
     manifest["row_counts"]["gl_after_quarantine"]
     + manifest["row_counts"]["ap_after_quarantine"],
     result.rows_passed),
    ("rows quarantined", expected_q["total_rows"], result.rows_quarantined),
    ("rule violations", expected_q["total_violations"], result.rule_violations),
    ("DQ score %", manifest["expected_dq_score_pct"], result.dq_score_pct),
    ("row conservation", result.rows_received,
     result.rows_passed + result.rows_quarantined),
]

# Every declared rule, whether or not it fired.
for dataset in ("gl", "ap"):
    expected_map = expected_q["by_rule"][dataset]
    for rule in contracts["datasets"][dataset]["rules"]:
        rid = rule["id"]
        checks.append((f"rule {rid}", expected_map.get(rid, 0),
                       result.by_rule.get(rid, 0)))

failed = [(n, e, a) for n, e, a in checks if e != a]
silent = 0
for name, exp, act in checks:
    if name.startswith("rule ") and not exp and not act:
        silent += 1
        continue
    print(f"  [{'ok  ' if exp == act else 'FAIL'}] {name:<40} {act:>10}")
print(f"  [ok  ] {'rules with 0 expected violations':<40} {silent:>10}")

print(f"\n  {len(checks) - len(failed)}/{len(checks)} checks passed")
assert not failed, f"Scorecard does not match the control manifest: {failed}"
print("  The scorecard reproduces the control manifest exactly.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The scorecard, as a table
# MAGIC
# MAGIC **Do not average these two scores to get an overall figure.** Averaging
# MAGIC weights 940 GL rows equally with 969 AP rows and produces a number that is
# MAGIC not the share of rows that passed. Sum the numerators and denominators
# MAGIC instead — the query below does, and `docs/metric_definitions.md` states it
# MAGIC once so no chart has to rediscover it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dataset, label, rows_received, rows_passed, rows_quarantined,
# MAGIC        rule_violations, dq_score_pct
# MAGIC FROM gold.dq_scorecard
# MAGIC ORDER BY dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The overall score. Numerators and denominators summed, never averaged.
# MAGIC SELECT sum(rows_received)                                          AS rows_received,
# MAGIC        sum(rows_passed)                                            AS rows_passed,
# MAGIC        round(100 * sum(rows_passed) / sum(rows_received), 4)       AS dq_score_pct
# MAGIC FROM gold.dq_scorecard;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Most violated rules — with the evidence attached
# MAGIC
# MAGIC `predicate_sql` is the exact expression the pipeline evaluated. *"This rule
# MAGIC rejected 3 rows"* is an assertion; the predicate beside it is the proof, and
# MAGIC a controller can disagree with it without opening any Python.
# MAGIC
# MAGIC This is also why the dashboard cannot drift from the pipeline: it is not
# MAGIC re-implementing the rules, it is displaying them.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT rule_id, dataset, column_name, check_type, rows_rejected, predicate_sql
# MAGIC FROM gold.dq_rule_scorecard
# MAGIC WHERE rows_rejected > 0
# MAGIC ORDER BY rows_rejected DESC, rule_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The silent rules. Not noise: this list going up is a regression signal.
# MAGIC SELECT count(*) AS rules_that_fired_zero_times
# MAGIC FROM gold.dq_rule_scorecard
# MAGIC WHERE rows_rejected = 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## The row that proves rules are evaluated independently
# MAGIC
# MAGIC Two GL rows breach **two** rules each: a department code lowercased with
# MAGIC trailing whitespace (`"d100 "`) trips both *format* and *domain*, and one
# MAGIC row carries both a null amount and a bad currency.
# MAGIC
# MAGIC An engine that short-circuited on first failure would report 10 rows / 10
# MAGIC violations and still look entirely plausible. It reports 10 / 12 because
# MAGIC every rule is evaluated against every row — a row breaching two rules is two
# MAGIC tickets for two different people.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _dataset, _failed_rule_ids, _failed_rule_count, count(*) AS rows
# MAGIC FROM (
# MAGIC   SELECT _dataset, _failed_rule_ids, _failed_rule_count FROM quarantine.gl
# MAGIC   UNION ALL
# MAGIC   SELECT _dataset, _failed_rule_ids, _failed_rule_count FROM quarantine.ap_subledger
# MAGIC )
# MAGIC GROUP BY _dataset, _failed_rule_ids, _failed_rule_count
# MAGIC ORDER BY _failed_rule_count DESC, rows DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** build the dashboard. Every tile's query is in
# MAGIC `docs/dashboard_queries.sql`, and the layout — which tile, which
# MAGIC visualisation, and what each one is there to answer — is in
# MAGIC `docs/dashboard_layout.md`.
# MAGIC
# MAGIC Nothing in the dashboard re-implements a definition. Every metric it shows
# MAGIC is already computed in a gold table, defined once in
# MAGIC `docs/metric_definitions.md`.
