# Data dictionary

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


---

## `bronze.gl`

General ledger, exactly as received. Every column is `STRING` on purpose: casting at ingest would turn the planted `"N/A"` into `NULL` before the contract engine sees it, so the row would be quarantined as *missing value* instead of *text in a numeric column* — the wrong diagnosis, pointing at the wrong upstream fix, with the original bytes gone.

| Column | Type | Null | Description |
|---|---|---|---|
| `gl_entry_id` | `STRING` | yes | Surrogate key of the GL posting. Unique within a load. |
| `posting_date` | `STRING` | yes | Date the entry hit the books. Informational - fiscal_period is authoritative for reporting. |
| `fiscal_period` | `STRING` | yes | Accounting period yyyy-mm. Authoritative for all period reporting. |
| `account_code` | `STRING` | yes | Chart-of-accounts code. Part of the business key. |
| `account_name` | `STRING` | yes | Human-readable account label, for display only. |
| `department_code` | `STRING` | yes | Cost centre the expense is classified to. |
| `department_name` | `STRING` | yes | Human-readable department label, for display only. |
| `vendor_code` | `STRING` | yes | Supplier identifier. Part of the business key. |
| `invoice_number` | `STRING` | yes | Vendor invoice reference. Part of the business key. |
| `amount` | `STRING` | yes | Posted amount in currency units. |
| `currency` | `STRING` | yes | ISO currency code. v0.1 is single-currency (USD). |
| `source_system` | `STRING` | yes | System of record the row was extracted from. |
| `extract_ts` | `STRING` | yes | When the source extract was taken. Proves which extract was reconciled. |
| `_ingested_at` | `TIMESTAMP` | **no** | When this row was written to bronze. |
| `_source_file` | `STRING` | **no** | Absolute path of the file this row came from. The first question asked about any suspect row is 'where did it come from'. |
| `_batch_id` | `STRING` | **no** | Identifier of the ingest run. Makes a bad load reversible as a set. |


---

## `bronze.ap_subledger`

AP subledger, exactly as received. Same all-`STRING` rule as `bronze.gl`.

| Column | Type | Null | Description |
|---|---|---|---|
| `ap_line_id` | `STRING` | yes | Surrogate key of the subledger line. Unique within a load. |
| `invoice_date` | `STRING` | yes | Date the vendor issued the invoice. May fall in a different month than fiscal_period - that is a normal cut-off artefact. |
| `due_date` | `STRING` | yes | Payment due date. Drives aging (roadmap v0.4). |
| `fiscal_period` | `STRING` | yes | Accounting period yyyy-mm. Authoritative for all period reporting. |
| `vendor_code` | `STRING` | yes | Supplier identifier. Part of the business key. |
| `vendor_name` | `STRING` | yes | Human-readable vendor label, for display only. |
| `account_code` | `STRING` | yes | Chart-of-accounts code. Part of the business key. |
| `invoice_number` | `STRING` | yes | Vendor invoice reference. Part of the business key. |
| `amount` | `STRING` | yes | Invoiced amount in currency units. |
| `currency` | `STRING` | yes | ISO currency code. v0.1 is single-currency (USD). |
| `payment_status` | `STRING` | yes | Workflow state: OPEN, PAID, PARTIAL, ON_HOLD. |
| `payment_terms_days` | `STRING` | yes | Agreed payment terms in days. |
| `source_system` | `STRING` | yes | System of record the row was extracted from. |
| `extract_ts` | `STRING` | yes | When the source extract was taken. |
| `_ingested_at` | `TIMESTAMP` | **no** | When this row was written to bronze. |
| `_source_file` | `STRING` | **no** | Absolute path of the file this row came from. The first question asked about any suspect row is 'where did it come from'. |
| `_batch_id` | `STRING` | **no** | Identifier of the ingest run. Makes a bad load reversible as a set. |


---

## `silver.gl`

General ledger after the contract passed and types were applied. Casting happens *after* the contract, never before — so a cast failure here is a bug in the contract, not bad data.

| Column | Type | Null | Description |
|---|---|---|---|
| `gl_entry_id` | `STRING` | yes | Surrogate key of the GL posting. Unique within a load. |
| `posting_date` | `DATE` | yes | Date the entry hit the books. Informational - fiscal_period is authoritative for reporting. |
| `fiscal_period` | `STRING` | **no** | Accounting period yyyy-mm. Authoritative for all period reporting. |
| `account_code` | `STRING` | **no** | Chart-of-accounts code. Part of the business key. |
| `account_name` | `STRING` | yes | Human-readable account label, for display only. |
| `department_code` | `STRING` | yes | Cost centre the expense is classified to. |
| `department_name` | `STRING` | yes | Human-readable department label, for display only. |
| `vendor_code` | `STRING` | **no** | Supplier identifier. Part of the business key. |
| `invoice_number` | `STRING` | **no** | Vendor invoice reference. Part of the business key. |
| `amount` | `DECIMAL(18,2)` | **no** | Posted amount in currency units. |
| `currency` | `STRING` | yes | ISO currency code. v0.1 is single-currency (USD). |
| `source_system` | `STRING` | yes | System of record the row was extracted from. |
| `extract_ts` | `TIMESTAMP` | yes | When the source extract was taken. Proves which extract was reconciled. |
| `_ingested_at` | `TIMESTAMP` | **no** | When this row was written to bronze. |
| `_source_file` | `STRING` | **no** | Absolute path of the file this row came from. The first question asked about any suspect row is 'where did it come from'. |
| `_batch_id` | `STRING` | **no** | Identifier of the ingest run. Makes a bad load reversible as a set. |


---

## `silver.ap_subledger`

AP subledger after the contract passed and types were applied.

| Column | Type | Null | Description |
|---|---|---|---|
| `ap_line_id` | `STRING` | yes | Surrogate key of the subledger line. Unique within a load. |
| `invoice_date` | `DATE` | yes | Date the vendor issued the invoice. May fall in a different month than fiscal_period - that is a normal cut-off artefact. |
| `due_date` | `DATE` | yes | Payment due date. Drives aging (roadmap v0.4). |
| `fiscal_period` | `STRING` | **no** | Accounting period yyyy-mm. Authoritative for all period reporting. |
| `vendor_code` | `STRING` | **no** | Supplier identifier. Part of the business key. |
| `vendor_name` | `STRING` | yes | Human-readable vendor label, for display only. |
| `account_code` | `STRING` | **no** | Chart-of-accounts code. Part of the business key. |
| `invoice_number` | `STRING` | **no** | Vendor invoice reference. Part of the business key. |
| `amount` | `DECIMAL(18,2)` | **no** | Invoiced amount in currency units. |
| `currency` | `STRING` | yes | ISO currency code. v0.1 is single-currency (USD). |
| `payment_status` | `STRING` | yes | Workflow state: OPEN, PAID, PARTIAL, ON_HOLD. |
| `payment_terms_days` | `INT` | yes | Agreed payment terms in days. |
| `source_system` | `STRING` | yes | System of record the row was extracted from. |
| `extract_ts` | `TIMESTAMP` | yes | When the source extract was taken. |
| `_ingested_at` | `TIMESTAMP` | **no** | When this row was written to bronze. |
| `_source_file` | `STRING` | **no** | Absolute path of the file this row came from. The first question asked about any suspect row is 'where did it come from'. |
| `_batch_id` | `STRING` | **no** | Identifier of the ingest run. Makes a bad load reversible as a set. |


---

## `quarantine.gl`

GL rows that failed the contract, with the reason attached. Stays untyped: these rows failed validation, so they cannot be assumed castable, and typing this table would mean the rows most needing investigation are the ones that fail to load.

| Column | Type | Null | Description |
|---|---|---|---|
| `gl_entry_id` | `STRING` | yes | Surrogate key of the GL posting. Unique within a load. |
| `posting_date` | `STRING` | yes | Date the entry hit the books. Informational - fiscal_period is authoritative for reporting. |
| `fiscal_period` | `STRING` | yes | Accounting period yyyy-mm. Authoritative for all period reporting. |
| `account_code` | `STRING` | yes | Chart-of-accounts code. Part of the business key. |
| `account_name` | `STRING` | yes | Human-readable account label, for display only. |
| `department_code` | `STRING` | yes | Cost centre the expense is classified to. |
| `department_name` | `STRING` | yes | Human-readable department label, for display only. |
| `vendor_code` | `STRING` | yes | Supplier identifier. Part of the business key. |
| `invoice_number` | `STRING` | yes | Vendor invoice reference. Part of the business key. |
| `amount` | `STRING` | yes | Posted amount in currency units. |
| `currency` | `STRING` | yes | ISO currency code. v0.1 is single-currency (USD). |
| `source_system` | `STRING` | yes | System of record the row was extracted from. |
| `extract_ts` | `STRING` | yes | When the source extract was taken. Proves which extract was reconciled. |
| `_ingested_at` | `TIMESTAMP` | **no** | When this row was written to bronze. |
| `_source_file` | `STRING` | **no** | Absolute path of the file this row came from. The first question asked about any suspect row is 'where did it come from'. |
| `_batch_id` | `STRING` | **no** | Identifier of the ingest run. Makes a bad load reversible as a set. |
| `_quarantined_at` | `TIMESTAMP` | **no** | When the row was rejected. |
| `_dataset` | `STRING` | **no** | Source dataset: 'gl' or 'ap'. |
| `_failed_rule_ids` | `STRING` | **no** | Pipe-separated contract rule ids that rejected this row. Plural: a row can breach several rules and each is a separate fix. |
| `_failed_rule_count` | `INT` | **no** | How many rules this row breached. |


---

## `quarantine.ap_subledger`

AP rows that failed the contract, with the reason attached.

| Column | Type | Null | Description |
|---|---|---|---|
| `ap_line_id` | `STRING` | yes | Surrogate key of the subledger line. Unique within a load. |
| `invoice_date` | `STRING` | yes | Date the vendor issued the invoice. May fall in a different month than fiscal_period - that is a normal cut-off artefact. |
| `due_date` | `STRING` | yes | Payment due date. Drives aging (roadmap v0.4). |
| `fiscal_period` | `STRING` | yes | Accounting period yyyy-mm. Authoritative for all period reporting. |
| `vendor_code` | `STRING` | yes | Supplier identifier. Part of the business key. |
| `vendor_name` | `STRING` | yes | Human-readable vendor label, for display only. |
| `account_code` | `STRING` | yes | Chart-of-accounts code. Part of the business key. |
| `invoice_number` | `STRING` | yes | Vendor invoice reference. Part of the business key. |
| `amount` | `STRING` | yes | Invoiced amount in currency units. |
| `currency` | `STRING` | yes | ISO currency code. v0.1 is single-currency (USD). |
| `payment_status` | `STRING` | yes | Workflow state: OPEN, PAID, PARTIAL, ON_HOLD. |
| `payment_terms_days` | `STRING` | yes | Agreed payment terms in days. |
| `source_system` | `STRING` | yes | System of record the row was extracted from. |
| `extract_ts` | `STRING` | yes | When the source extract was taken. |
| `_ingested_at` | `TIMESTAMP` | **no** | When this row was written to bronze. |
| `_source_file` | `STRING` | **no** | Absolute path of the file this row came from. The first question asked about any suspect row is 'where did it come from'. |
| `_batch_id` | `STRING` | **no** | Identifier of the ingest run. Makes a bad load reversible as a set. |
| `_quarantined_at` | `TIMESTAMP` | **no** | When the row was rejected. |
| `_dataset` | `STRING` | **no** | Source dataset: 'gl' or 'ap'. |
| `_failed_rule_ids` | `STRING` | **no** | Pipe-separated contract rule ids that rejected this row. Plural: a row can breach several rules and each is a separate fix. |
| `_failed_rule_count` | `INT` | **no** | How many rules this row breached. |


---

## `gold.recon_detail`

One row per business key — the grain the reconciliation was performed at. Every key carries exactly one `break_status`.

| Column | Type | Null | Description |
|---|---|---|---|
| `fiscal_period` | `STRING` | **no** | Reporting period for the key: the GL period, falling back to the AP period when the key has no GL side. The GL is the book of record, so a timing difference is reported in the period whose close it affects. |
| `account_code` | `STRING` | **no** | Business key part 1 of 3. |
| `vendor_code` | `STRING` | **no** | Business key part 2 of 3. |
| `invoice_number` | `STRING` | **no** | Business key part 3 of 3. |
| `break_status` | `STRING` | **no** | Exactly one of the six statuses in the break taxonomy. The taxonomy is a partition: every key gets one status, no key escapes. |
| `gl_amount` | `DECIMAL(18,2)` | yes | Sum of GL amounts on this key. NULL - not zero - when the key has no GL side. |
| `ap_amount` | `DECIMAL(18,2)` | yes | Sum of subledger amounts on this key. NULL - not zero - when the key has no subledger side. |
| `amount_difference` | `DECIMAL(18,2)` | **no** | gl_amount - ap_amount, with a missing side treated as zero for the subtraction. Signed: positive means the GL carries more. |
| `abs_amount_difference` | `DECIMAL(18,2)` | **no** | Absolute value of amount_difference. This is the exposure the break represents, and what exceptions are ranked by. |
| `gl_fiscal_period` | `STRING` | yes | Earliest GL period on the key. NULL when the key has no GL side. |
| `ap_fiscal_period` | `STRING` | yes | Earliest subledger period on the key. NULL when the key has no subledger side. |
| `gl_row_count` | `INT` | **no** | GL rows behind this key. Zero for a subledger-only key. |
| `ap_row_count` | `INT` | **no** | Subledger rows behind this key. Greater than one is what makes a key DUPLICATE_IN_SUBLEDGER - which is why the aggregation carries a row count and not just a sum. |


---

## `gold.recon_summary`

Counts and value by period and status. A **dense** grid: every observed period is crossed with all six statuses and combinations that did not occur are written as zero, so an absent row cannot mean both *none this period* and *that branch stopped firing*.

| Column | Type | Null | Description |
|---|---|---|---|
| `fiscal_period` | `STRING` | **no** | Reporting period, as per gold.recon_detail. |
| `break_status` | `STRING` | **no** | One of the six statuses. |
| `key_count` | `BIGINT` | **no** | Business keys with this status in this period. Zero rows are materialised on purpose - see gold.py. |
| `gl_amount` | `DECIMAL(18,2)` | **no** | Total GL value of those keys. Zero when there are none. |
| `ap_amount` | `DECIMAL(18,2)` | **no** | Total subledger value of those keys. Zero when there are none. |
| `net_amount_difference` | `DECIMAL(18,2)` | **no** | Sum of the SIGNED differences. Opposite-direction breaks cancel, so this is the effect on the books, not the size of the problem. |
| `abs_amount_difference` | `DECIMAL(18,2)` | **no** | Sum of the ABSOLUTE differences. Nothing cancels, so this is the gross exposure - the number to quote as 'value under investigation'. Distinct from net_amount_difference and never interchangeable. |


---

## `gold.recon_exceptions`

The non-`MATCHED` keys, labelled with vendor and account names and ranked by exposure. The analyst worklist.

| Column | Type | Null | Description |
|---|---|---|---|
| `exception_rank` | `INT` | **no** | Rank by abs_amount_difference descending, business key ascending as the tie-break. Materialised because a Delta table has no inherent row order - an ORDER BY at write time does not survive a read. |
| `fiscal_period` | `STRING` | **no** | Reporting period, as per gold.recon_detail. |
| `break_status` | `STRING` | **no** | One of the five non-MATCHED statuses. |
| `account_code` | `STRING` | **no** | Business key part 1 of 3. |
| `account_name` | `STRING` | yes | Account label, joined from the GL. Display only - never a join key. |
| `vendor_code` | `STRING` | **no** | Business key part 2 of 3. |
| `vendor_name` | `STRING` | yes | Vendor label, joined from the subledger. Display only. |
| `invoice_number` | `STRING` | **no** | Business key part 3 of 3. |
| `gl_amount` | `DECIMAL(18,2)` | yes | As per gold.recon_detail. |
| `ap_amount` | `DECIMAL(18,2)` | yes | As per gold.recon_detail. |
| `amount_difference` | `DECIMAL(18,2)` | **no** | As per gold.recon_detail. |
| `abs_amount_difference` | `DECIMAL(18,2)` | **no** | As per gold.recon_detail. |
| `gl_fiscal_period` | `STRING` | yes | As per gold.recon_detail. |
| `ap_fiscal_period` | `STRING` | yes | As per gold.recon_detail. |
| `gl_row_count` | `INT` | **no** | As per gold.recon_detail. |
| `ap_row_count` | `INT` | **no** | As per gold.recon_detail. |


---

## `gold.dq_scorecard`

One row per source dataset: how many rows arrived, how many passed, and the resulting DQ score.

| Column | Type | Null | Description |
|---|---|---|---|
| `dataset` | `STRING` | **no** | Source dataset: 'gl' or 'ap'. |
| `label` | `STRING` | **no** | Human-readable dataset name, from contracts.yaml. |
| `rows_received` | `BIGINT` | **no** | Rows that arrived in bronze. The DQ denominator is what we were SENT, not what we kept - measuring against what survived would score 100% on any input. |
| `rows_passed` | `BIGINT` | **no** | Rows that satisfied every contract rule. |
| `rows_quarantined` | `BIGINT` | **no** | Rows rejected. rows_passed + rows_quarantined = rows_received, asserted on every run: no row is ever silently dropped. |
| `rule_violations` | `BIGINT` | **no** | Total rule breaches. EXCEEDS rows_quarantined when a row breaches more than one rule - which is the point of recording rule ids as a list. Never use this as a row count. |
| `dq_score_pct` | `DECIMAL(7,4)` | **no** | 100 * rows_passed / rows_received, to 4dp. Per dataset. To get an overall score, sum the numerators and denominators - averaging the per-dataset scores weights a small dataset equally with a large one. |


---

## `gold.dq_rule_scorecard`

One row per contract rule, carrying both the count of rows it rejected and the exact SQL predicate that rejected them.

| Column | Type | Null | Description |
|---|---|---|---|
| `rule_id` | `STRING` | **no** | Permanent contract rule id. Stamped onto every row it rejects, so ids are never recycled - a reused id would silently change the meaning of historical quarantine records. |
| `dataset` | `STRING` | **no** | Dataset the rule applies to. |
| `column_name` | `STRING` | **no** | Column the rule tests. |
| `check_type` | `STRING` | **no** | Kind of assertion: not_null, unique, regex, allowed_values, numeric, non_zero, numeric_range. |
| `severity` | `STRING` | **no** | 'reject' quarantines the row; 'warn' would let it through flagged. |
| `description` | `STRING` | **no** | The rule's intent, in the words of whoever wrote the contract. |
| `predicate_sql` | `STRING` | **no** | The exact Spark SQL the pipeline evaluated, TRUE where violated. This is what makes the scorecard evidence rather than assertion: the count and the logic that produced it sit in the same row, and the dashboard cannot drift from the pipeline because it is reading the pipeline's own predicate. |
| `rows_rejected` | `BIGINT` | **no** | Rows this rule rejected. Zero-firing rules are kept: a rule that stops firing is as much a signal as one that starts, and a list of only non-zero rules cannot tell 'clean' from 'that check silently stopped running'. |


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
