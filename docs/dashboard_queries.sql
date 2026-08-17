-- =============================================================================
-- LedgerLens - dashboard queries
-- =============================================================================
-- One query per dashboard tile. Paste each into its own dataset/visualisation.
-- Tile numbers match docs/dashboard_layout.md.
--
-- BEFORE YOU START, run this once in the SQL editor so every query below can
-- use plain two-part names (gold.recon_detail rather than workspace.gold....):
--
--     USE CATALOG workspace;
--
-- Databricks `identifier()` needs a constant string and rejects `||` on
-- parameter markers, so parameterising the catalog inside each query is not
-- worth the syntax fight. Set it once at the session level instead.
--
-- -----------------------------------------------------------------------------
-- THE RULE THESE QUERIES FOLLOW
-- -----------------------------------------------------------------------------
-- No query below computes a definition. Every metric is already materialised in
-- a gold table, defined exactly once in docs/metric_definitions.md. These
-- queries filter, sort and total - they never decide what a number means.
--
-- That is not tidiness. A dashboard that re-derives "DQ score" or "break value"
-- in SQL creates a second definition, and the version on the dashboard is the
-- one people believe. When the two drift, the pipeline is right and the
-- dashboard is trusted.
-- =============================================================================


-- =============================================================================
-- TILE 1 - Headline counters
-- Viz: Counter (one per metric, or one query feeding four counter tiles)
-- =============================================================================
-- Read left to right: how much did we reconcile, how much broke, how much is it
-- worth, and how clean was the input.
--
-- gross_exposure sums ABSOLUTE differences. Opposite-direction breaks do not
-- cancel, so this is the value under investigation - not the effect on the
-- books. See TILE 5 for why that distinction matters.
SELECT
    (SELECT count(*) FROM gold.recon_detail)                       AS business_keys,
    (SELECT count(*) FROM gold.recon_exceptions)                   AS exceptions,
    (SELECT round(sum(abs_amount_difference), 2)
     FROM gold.recon_exceptions)                                   AS gross_exposure,
    (SELECT round(100 * sum(rows_passed) / sum(rows_received), 4)
     FROM gold.dq_scorecard)                                       AS dq_score_pct;


-- =============================================================================
-- TILE 2 - Break mix
-- Viz: Bar chart. X = break_status, Y = keys. Sort descending.
-- =============================================================================
-- The shape of the reconciliation in one picture. MATCHED dominates by design;
-- if it ever does not, something upstream broke rather than something here.
--
-- Do NOT add `WHERE key_count > 0`. The summary is a dense grid on purpose, and
-- a status dropping off the axis because it hit zero is exactly the signal the
-- chart exists to show.
SELECT break_status,
       sum(key_count)              AS keys,
       round(sum(abs_amount_difference), 2) AS gross_exposure
FROM gold.recon_summary
GROUP BY break_status
ORDER BY keys DESC;


-- =============================================================================
-- TILE 3 - Break mix excluding MATCHED
-- Viz: Bar chart (or donut). X = break_status, Y = keys.
-- =============================================================================
-- Tile 2 with the 820 matched keys removed, because on a linear axis they flatten
-- every other bar into the baseline. Same data, different question: "of the
-- things that broke, what kind of broken are they?"
SELECT break_status,
       sum(key_count)                       AS keys,
       round(sum(abs_amount_difference), 2) AS gross_exposure
FROM gold.recon_summary
WHERE break_status <> 'MATCHED'
GROUP BY break_status
ORDER BY keys DESC;


-- =============================================================================
-- TILE 4 - Breaks by period
-- Viz: Stacked bar. X = fiscal_period, Y = keys, colour = break_status.
-- =============================================================================
-- Trend over the six periods. A period whose stack changes shape is worth a
-- question even when the total is flat.
SELECT fiscal_period,
       break_status,
       key_count,
       abs_amount_difference AS gross_exposure
FROM gold.recon_summary
WHERE break_status <> 'MATCHED'
ORDER BY fiscal_period, break_status;


-- =============================================================================
-- TILE 5 - Net vs gross by status
-- Viz: Table, or a grouped bar with two series.
-- =============================================================================
-- THE MOST IMPORTANT TILE ON THE DASHBOARD, and the one most likely to be cut
-- for being unglamorous.
--
--   net   sums the SIGNED differences - overstatements and understatements
--         cancel. This is the effect on the books.
--   gross sums the ABSOLUTE differences - nothing cancels. This is the size of
--         the problem.
--
-- A period can show a near-zero net while carrying a large gross. Reporting only
-- net would show a clean reconciliation sitting on top of real, unresolved
-- breaks - which is precisely the failure a reconciliation exists to prevent.
SELECT break_status,
       sum(key_count)                        AS keys,
       round(sum(net_amount_difference), 2)  AS net_effect_on_books,
       round(sum(abs_amount_difference), 2)  AS gross_exposure
FROM gold.recon_summary
WHERE break_status <> 'MATCHED'
GROUP BY break_status
ORDER BY gross_exposure DESC;


-- =============================================================================
-- TILE 6 - Unrecorded liability by period
-- Viz: Bar chart. X = fiscal_period, Y = unrecorded_liability.
-- =============================================================================
-- The headline finding of the whole project.
--
-- MISSING_FROM_GL means an invoice sits in the subledger that was never posted
-- to the general ledger: the company owes money its books do not show.
-- Everything else in the taxonomy is an ERROR. This one is a MISSTATEMENT - it
-- understates liabilities, which flatters the balance sheet, which is why an
-- auditor goes looking for it first.
SELECT fiscal_period,
       key_count                              AS unrecorded_invoices,
       round(abs_amount_difference, 2)        AS unrecorded_liability
FROM gold.recon_summary
WHERE break_status = 'MISSING_FROM_GL'
ORDER BY fiscal_period;


-- =============================================================================
-- TILE 7 - Top exceptions worklist
-- Viz: Table. Leave it unsorted in the UI - exception_rank already carries the
--      order, and a Delta table has no inherent row order to rely on.
-- =============================================================================
-- What an analyst actually opens on Monday morning.
SELECT exception_rank,
       break_status,
       fiscal_period,
       vendor_name,
       account_name,
       invoice_number,
       gl_amount,
       ap_amount,
       abs_amount_difference AS exposure
FROM gold.recon_exceptions
WHERE exception_rank <= 25
ORDER BY exception_rank;


-- =============================================================================
-- TILE 8 - Duplicates, with the evidence
-- Viz: Table.
-- =============================================================================
-- ap_row_count is what makes a duplicate provable rather than asserted: the key
-- was backed by two or three subledger rows, and you can see it.
--
-- copies_were_identical distinguishes a clean double-booking from one where the
-- second copy was keyed slightly differently. Both are duplicates. A pipeline
-- that classified the second kind as AMOUNT_MISMATCH would send an analyst
-- hunting a keying error instead of a duplicate payment.
SELECT invoice_number,
       vendor_code,
       account_code,
       ap_row_count                                   AS subledger_rows,
       gl_amount,
       ap_amount,
       ap_amount = gl_amount * ap_row_count           AS copies_were_identical
FROM gold.recon_detail
WHERE break_status = 'DUPLICATE_IN_SUBLEDGER'
ORDER BY ap_row_count DESC, copies_were_identical, abs_amount_difference DESC;


-- =============================================================================
-- TILE 9 - DQ score by dataset
-- Viz: Counter per dataset, or a small table.
-- =============================================================================
-- NEVER average these two scores to get an overall figure. Averaging weights 940
-- GL rows equally with 969 AP rows and produces a number that is not the share
-- of rows that passed. Sum the numerators and denominators - TILE 1 does.
SELECT dataset,
       label,
       rows_received,
       rows_passed,
       rows_quarantined,
       rule_violations,
       dq_score_pct
FROM gold.dq_scorecard
ORDER BY dataset;


-- =============================================================================
-- TILE 10 - Most violated rules
-- Viz: Horizontal bar. Y = rule_id, X = rows_rejected.
-- =============================================================================
-- Which contract rule is costing the most rows. Drives the upstream conversation:
-- these are tickets for whoever owns the source system, not for whoever owns the
-- pipeline.
SELECT rule_id,
       dataset,
       column_name,
       check_type,
       rows_rejected
FROM gold.dq_rule_scorecard
WHERE rows_rejected > 0
ORDER BY rows_rejected DESC, rule_id;


-- =============================================================================
-- TILE 11 - The rule catalogue, with its SQL
-- Viz: Table. Put it at the bottom; it is reference, not a headline.
-- =============================================================================
-- Every declared rule, whether or not it fired, with the exact predicate the
-- pipeline evaluated.
--
-- This tile is what turns the dashboard from a set of claims into evidence. A
-- controller can read a rule's intent, see the SQL that enforces it and the
-- number of rows it rejected, all in one row, without opening any Python.
--
-- Zero-firing rules stay listed on purpose: a rule that stops firing looks
-- exactly like clean data, and this is the only place the difference is visible.
SELECT rule_id,
       dataset,
       column_name,
       check_type,
       severity,
       rows_rejected,
       description,
       predicate_sql
FROM gold.dq_rule_scorecard
ORDER BY rows_rejected DESC, dataset, rule_id;


-- =============================================================================
-- TILE 12 - Rows breaching more than one rule
-- Viz: Table. Small, and worth the space.
-- =============================================================================
-- Two rows breach two rules each, which is why 24 quarantined rows produce 26
-- violations. An engine that short-circuited on first failure would report 24/24
-- and look entirely plausible.
--
-- It also demonstrates why _failed_rule_ids is a LIST: a row breaching two rules
-- is two tickets for two different people.
SELECT _dataset          AS dataset,
       _failed_rule_ids  AS rules_breached,
       _failed_rule_count AS rules_count,
       count(*)          AS rows
FROM (
    SELECT _dataset, _failed_rule_ids, _failed_rule_count FROM quarantine.gl
    UNION ALL
    SELECT _dataset, _failed_rule_ids, _failed_rule_count FROM quarantine.ap_subledger
)
GROUP BY _dataset, _failed_rule_ids, _failed_rule_count
ORDER BY rules_count DESC, rows DESC;


-- =============================================================================
-- OPTIONAL - filter widget backing query
-- =============================================================================
-- If you add a period filter to the dashboard, back it with this rather than a
-- hard-coded list, so a new period appears automatically.
SELECT DISTINCT fiscal_period
FROM gold.recon_summary
ORDER BY fiscal_period;
