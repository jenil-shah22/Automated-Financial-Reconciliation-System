# Metric definitions

Every number this project reports, defined **exactly once**.

## Why this file exists

Not for tidiness. A metric defined in two places is a metric with two values,
and the day they disagree is the day somebody makes a decision on the wrong one.

The specific failure this prevents: an analyst needs "total break value", writes
`sum(amount_difference)` in a chart because it is the obvious column, and now the
dashboard reports a number that cancels overstatements against understatements
while the pipeline reports one that does not. Both are defensible. Only one
answers the question that was asked. Without this file, nobody notices for
months.

**The rule:** every metric below is computed once, in code, and materialised into
a gold table. Dashboards filter, sort and total. They never derive.

---

## Reconciliation metrics

### Business key

`account_code` + `vendor_code` + `invoice_number`.

The unit of reconciliation. One key = one thing being reconciled, regardless of
how many ledger rows sit behind it.

Invoice number alone is not enough: invoice numbers are only unique *within a
vendor* in the real world, and the account dimension is what stops two unrelated
postings from tying out by coincidence.

**Exact match only.** No fuzzy matching in v0.1 — a leading zero or a stray
hyphen produces two separate breaks rather than one match.

---

### Amount tolerance

**1.00, absolute, inclusive.**

A difference of `≤ 1.00` is rounding, not a break. `101.00` against `100.00` is
`MATCHED`; `101.01` is `AMOUNT_MISMATCH`.

Chosen because the two source systems round to 2dp independently and sub-unit
drift carries no accounting meaning. Declared in `config/contracts.yaml` as
`recon.amount_tolerance_abs`, so changing it is a config change with a visible
diff, not a number edited in three files.

*Implementation note:* the tolerance is compared as `DECIMAL(18,2)`, never as a
float. Spark promotes a DECIMAL-vs-DOUBLE comparison to DOUBLE, so a floating
point literal would put the comparison back on binary floating point at exactly
the boundary the tolerance defines.

---

### Break status

Exactly one of six values per business key. The taxonomy is a **partition**:
every key gets one status, no key escapes, no key is counted twice. Asserted on
every run.

| Status | Definition | Commercial meaning |
|---|---|---|
| `MATCHED` | Both sides present, difference ≤ tolerance, periods equal | Nothing to do |
| `AMOUNT_MISMATCH` | Both sides present, difference > tolerance | Keying error or partial payment |
| `TIMING_DIFFERENCE` | Both sides present, difference ≤ tolerance, periods differ | Cut-off; usually self-correcting |
| `MISSING_FROM_SUBLEDGER` | GL rows present, no subledger rows | Possibly unsupported entry |
| `MISSING_FROM_GL` | Subledger rows present, no GL rows | **Unrecorded liability** |
| `DUPLICATE_IN_SUBLEDGER` | More than one subledger row on the key | Possible double payment |

Assigned by a **precedence ladder**, first match wins, declared in
`contracts.yaml` as `recon.status_precedence` and asserted against the compiled
classifier on every run. The order is load-bearing, not cosmetic: the conditions
are positional, so each assumes every condition above it already failed.

Two orderings in particular:

- **Duplicates outrank everything.** Duplication is *structural* — a statement
  about how many rows exist, true regardless of amounts. A double-booking whose
  second copy was keyed slightly differently is still a double-booking.
- **Amount outranks timing.** Timing differences are benign and self-correcting;
  amount differences are not. Classifying a real value discrepancy as "timing"
  files it under *will fix itself next month*.

---

### `amount_difference` (signed)

```
coalesce(gl_amount, 0) - coalesce(ap_amount, 0)
```

Positive means the GL carries more. A missing side is treated as zero **for the
subtraction only** — the stored `gl_amount` / `ap_amount` stay `NULL`, because
`0.00` asserts *the ledger posted nothing* while `NULL` says *the ledger has no
opinion*, and those are different claims.

---

### `abs_amount_difference`

`abs(amount_difference)`. The **exposure** a single break represents, and what
the exception worklist is ranked by.

---

### Net vs gross difference — read this one carefully

At key grain the two are trivially related. **At any aggregate grain they are
different metrics and are never interchangeable.**

| Metric | Column in `gold.recon_summary` | Definition | What it tells you |
|---|---|---|---|
| **Net** | `net_amount_difference` | `sum(amount_difference)` | Effect on the books. Overstatements cancel understatements. |
| **Gross** | `abs_amount_difference` | `sum(abs_amount_difference)` | Size of the problem. Nothing cancels. |

A key overstated by 5,000 and one understated by 5,000 give **net 0** and
**gross 10,000**.

- Quote **gross** for *"value under investigation"*, *"total break value"*,
  anything an analyst is being asked to work through.
- Quote **net** for *"effect on the reported position"*.

A summary quoting only net can show a clean period sitting on top of ten thousand
dollars of unresolved breaks. That is the failure a reconciliation exists to
prevent, which is why both columns are materialised and named so they cannot be
confused.

---

### Exception

Any business key whose status is not `MATCHED`.

```
exceptions = business_keys - MATCHED
```

Materialised as `gold.recon_exceptions`, and the row count is asserted equal to
that subtraction on every run.

---

### `exception_rank`

`row_number()` over `abs_amount_difference` descending, with the business key
ascending as a tie-break.

Materialised as a column rather than applied at query time, because a Delta table
has **no inherent row order** — sorting at write time does not survive a read. So
*"the top twenty breaks"* has to be a value you can filter on.

The tie-break is not decoration: without it, two breaks of identical value would
shuffle between runs and *"exception #7"* would mean something different each
morning.

---

### Unrecorded liability

```
gross exposure of MISSING_FROM_GL keys
```

An invoice sitting in the subledger that was never posted to the general ledger —
the company owes money its books do not show.

Called out separately because it is the only **misstatement** in the taxonomy.
Everything else is an *error*. This one understates liabilities, which flatters
the balance sheet, which is why an auditor looks for it first.

---

## Data quality metrics

### DQ score

```
100 × rows_passed / rows_received
```

The share of received rows that satisfied **every** contract rule, to 4 decimal
places.

**Denominator is `rows_received`** — the rows bronze was handed, not the rows
that survived. Measuring against survivors would score 100% on any input, which
is the most common way a data-quality metric becomes meaningless.

**Computed in double, then rounded.** Money is `DECIMAL` everywhere in this
project, but a percentage is a display ratio, not a monetary amount, and double
arithmetic is what makes this byte-identical to the pandas oracle.

> **Aggregating across datasets: sum the numerators and denominators.**
> ```sql
> round(100 * sum(rows_passed) / sum(rows_received), 4)   -- correct
> avg(dq_score_pct)                                        -- WRONG
> ```
> Averaging the per-dataset scores weights 940 GL rows equally with 969 AP rows
> and produces a number that is not the share of rows that passed.

Verified value on seed 42: **98.7428%** (1,885 of 1,909).

---

### Rows quarantined vs rule violations

**Not the same number, and never interchangeable.**

| Metric | Definition | Verified value |
|---|---|---|
| `rows_quarantined` | Rows that failed at least one rule | **24** |
| `rule_violations` | Total rule breaches across those rows | **26** |

Violations exceed rows because **two rows breach two rules each** — a department
code lowercased with trailing whitespace trips both *format* and *domain*, and
one row carries both a null amount and a bad currency.

That gap is deliberate evidence. An engine that short-circuited on first failure
would report 24 / 24 and look entirely plausible. Rules are evaluated
independently because a row breaching two rules is **two tickets for two
different people**.

`rule_violations` must never be used as a row count.

---

### Rows rejected, per rule

`gold.dq_rule_scorecard.rows_rejected` — count of rows a given rule appears on in
the quarantine table.

Derived from the `_failed_rule_ids` the pipeline **recorded**, not by re-running
the predicates. Re-running would produce a second opinion, and a scorecard whose
numbers are a second opinion can disagree with the pipeline that actually
rejected the rows.

Rules that fired zero times keep their row. A rule that stops firing looks
identical to clean data, and this is the only place the difference is visible.

---

### One defect, one rule

Every check except `not_null` **skips** blank values, and `non_zero` /
`numeric_range` additionally skip values that do not parse.

Without this, one missing amount would trip `not_null` + `numeric` + `non_zero` +
`numeric_range` — four defects for one problem. That inflates the DQ denominator
and fills the "most violated rule" chart with knock-on effects instead of causes,
sending someone to fix the wrong thing.

---

## Row conservation

Not a reported metric — an **invariant**, asserted on every run:

```
silver_rows + quarantine_rows = bronze_rows
```

"Never silently drop a row", written as arithmetic the pipeline checks. A failure
raises rather than warns.

---

## Where each metric is computed

| Metric | Defined in | Materialised in |
|---|---|---|
| Business key, tolerance, precedence | `config/contracts.yaml` | — |
| Break status | `src/ledgerlens/recon.py` | `gold.recon_detail` |
| Signed / absolute difference | `src/ledgerlens/recon.py` | `gold.recon_detail` |
| Net vs gross by period | `src/ledgerlens/gold.py` | `gold.recon_summary` |
| Exception rank | `src/ledgerlens/gold.py` | `gold.recon_exceptions` |
| DQ score | `src/ledgerlens/scorecard.py` | `gold.dq_scorecard` |
| Rows rejected per rule | `src/ledgerlens/scorecard.py` | `gold.dq_rule_scorecard` |
| Rule predicates | `src/ledgerlens/quality.py` | `gold.dq_rule_scorecard.predicate_sql` |

Every one of these is also implemented a second time, independently, in
`src/ledgerlens/validate.py` (pandas), and the two implementations are asserted
to agree. See the README's cross-engine verification table.

---

*Synthetic demonstration project. All data is fictional and does not represent
any real company, client, employee, vendor, or financial system.*
