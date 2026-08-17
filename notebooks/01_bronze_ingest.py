# Databricks notebook source
# MAGIC %md
# MAGIC # LedgerLens — 01 · Bronze Ingest
# MAGIC
# MAGIC Lands `gl.csv` and `ap_subledger.csv` as Delta tables, **unchanged**.
# MAGIC
# MAGIC This notebook is a thin wrapper. All logic lives in
# MAGIC `src/ledgerlens/bronze.py` so it is unit-testable and reviewable in a
# MAGIC pull request — a notebook that contains the logic can be neither.
# MAGIC
# MAGIC ### What bronze guarantees
# MAGIC
# MAGIC | Guarantee | How it is enforced |
# MAGIC |---|---|
# MAGIC | Nothing is cast | Every column lands as `STRING` (see `schemas.py`) |
# MAGIC | Nothing is filtered | Ingest raises if bronze row count ≠ file row count |
# MAGIC | Nothing is renamed or reordered | Header is checked against the contract before the read |
# MAGIC | Every row is traceable | `_ingested_at`, `_source_file`, `_batch_id` |
# MAGIC
# MAGIC **Why no casting?** If bronze cast `amount` to decimal, the planted
# MAGIC value `"N/A"` would become `NULL` at ingest — before the contract engine
# MAGIC ever saw it. The row would then be quarantined as *"missing value"*
# MAGIC instead of *"text in a numeric column"*: the wrong diagnosis, pointing at
# MAGIC the wrong upstream fix, with the original bytes gone. Bronze preserves
# MAGIC evidence; silver applies judgment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Expects this repo cloned into the workspace (Workspace → Repos → Add Repo).
# MAGIC The path below is added to `sys.path` so `ledgerlens` imports like any
# MAGIC other package.

# COMMAND ----------

import os
import sys

REPO_ROOT = "/Workspace/Repos/ledgerlens"  # adjust if cloned elsewhere

for candidate in (f"{REPO_ROOT}/src", "../src", "./src"):
    if os.path.isdir(candidate) and candidate not in sys.path:
        sys.path.insert(0, candidate)

import ledgerlens
from ledgerlens import bronze, schemas

print("ledgerlens", ledgerlens.__version__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC `raw_path` is the Unity Catalog **volume** holding the two CSVs. Upload
# MAGIC them with Catalog → your schema → Create volume → Upload.
# MAGIC
# MAGIC A volume rather than DBFS root because DBFS root is deprecated for new
# MAGIC workspaces and is not governed — a volume gets the same permission model
# MAGIC as a table, which is the whole point of putting a governance layer in
# MAGIC this project.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("bronze_schema", "bronze", "Bronze schema")
dbutils.widgets.text(
    "raw_path", "/Volumes/workspace/raw/landing", "Raw volume path"
)

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
RAW_PATH = dbutils.widgets.get("raw_path")

print(f"catalog={CATALOG}  schema={BRONZE_SCHEMA}  raw={RAW_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm the source files are where we think they are
# MAGIC
# MAGIC Cheap, and it turns a confusing Spark stack trace twenty minutes from now
# MAGIC into an obvious error right here.

# COMMAND ----------

from pathlib import Path

for name in ("gl.csv", "ap_subledger.csv", "control_manifest.json"):
    full = f"{RAW_PATH}/{name}"
    exists = os.path.exists(f"/dbfs{full}") or os.path.exists(full)
    print(f"  {'found  ' if exists else 'MISSING'} {full}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The declared schema
# MAGIC
# MAGIC Printed rather than assumed. Schemas are **never inferred** — inference
# MAGIC reads a sample and guesses, and the guess changes silently when the data
# MAGIC changes. A column that inferred as integer last month infers as string
# MAGIC this month because one row arrived with a comma in it, and every
# MAGIC downstream comparison quietly starts returning false. Nothing crashes,
# MAGIC which is what makes it dangerous.

# COMMAND ----------

print("BRONZE GL")
print(schemas.to_ddl(schemas.BRONZE_GL_SCHEMA))
print()
print("BRONZE AP")
print(schemas.to_ddl(schemas.BRONZE_AP_SCHEMA))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest

# COMMAND ----------

cfg = bronze.LakehouseConfig(
    mode="catalog", catalog=CATALOG, schema=BRONZE_SCHEMA
)

results = bronze.run(raw_dir=Path(RAW_PATH), cfg=cfg, spark=spark)

for r in results:
    print(f"{r.dataset:<4} {r.bronze_rows:>6} rows -> {r.target}  (batch {r.batch_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Control: bronze must be lossless
# MAGIC
# MAGIC `bronze.run` already raises if a dataset lost rows, so reaching this cell
# MAGIC means the counts matched. We re-state them against the **control
# MAGIC manifest** as well, so the check is against the independently recorded
# MAGIC expectation rather than against the file we just read.

# COMMAND ----------

import json

with open(f"{RAW_PATH}/control_manifest.json") as fh:
    manifest = json.load(fh)

expected = {
    "gl": manifest["row_counts"]["gl_raw"],
    "ap": manifest["row_counts"]["ap_raw"],
}

ok = True
for r in results:
    match = r.bronze_rows == expected[r.dataset]
    ok &= match
    print(f"  [{'ok  ' if match else 'FAIL'}] {r.dataset}: "
          f"expected {expected[r.dataset]}, bronze has {r.bronze_rows}")

assert ok, "Bronze row counts do not match the control manifest"
print("\nBronze ingest is lossless and matches the manifest.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect
# MAGIC
# MAGIC Note the planted defects are **still here**, untouched: `"N/A"` in a
# MAGIC numeric column, `"US$"` as a currency, `"2026/04"` as a period. Bronze is
# MAGIC supposed to contain them. They are removed at silver, with a rule id
# MAGIC attached to each rejection.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM identifier(:catalog || '.' || :bronze_schema || '.gl') LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The defects that survived ingest, exactly as received.
# MAGIC SELECT ap_line_id, invoice_number, amount, currency, fiscal_period, payment_status
# MAGIC FROM identifier(:catalog || '.' || :bronze_schema || '.ap_subledger')
# MAGIC WHERE amount = '' OR amount = 'N/A' OR currency <> 'USD'
# MAGIC    OR fiscal_period NOT RLIKE '^[0-9]{4}-(0[1-9]|1[0-2])$'
# MAGIC ORDER BY ap_line_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta time travel
# MAGIC
# MAGIC The reason medallion works at all. Bronze is never overwritten in a way
# MAGIC that loses history — every write is a new table version, so a bad load is
# MAGIC reversible and "what did the data look like when we closed March?" is an
# MAGIC answerable question rather than an apology.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY identifier(:catalog || '.' || :bronze_schema || '.gl');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** `02_silver_contracts` — enforce `config/contracts.yaml`, write
# MAGIC the silver tables and the quarantine tables. Expected outcome, from the
# MAGIC control manifest: **24 rows quarantined, 26 rule violations, 1,885 rows
# MAGIC surviving to silver, DQ score 98.7428%.**
