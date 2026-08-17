# Dashboard layout

The build instructions for the LedgerLens dashboard. Every query referenced here
lives in [`dashboard_queries.sql`](dashboard_queries.sql), numbered to match.

This is the one part of the project that cannot be committed as code — a
Databricks dashboard is built in a UI and stored in the workspace, not in the
repo. So the *specification* is committed instead: what each tile shows, which
query backs it, and what question it answers. Anyone can rebuild the dashboard
from this file.

---

## Before you start

Run once in the SQL editor, then leave the session open:

```sql
USE CATALOG workspace;
```

Every query then uses plain two-part names (`gold.recon_detail`). Databricks
`identifier()` requires a constant string and rejects `||` on parameter markers,
so parameterising the catalog inside each query is not worth the syntax fight.

**Prerequisite:** notebooks `01` → `04` have all been run, so `gold.recon_detail`,
`gold.recon_summary`, `gold.recon_exceptions`, `gold.dq_scorecard` and
`gold.dq_rule_scorecard` exist.

---

## The shape of the page

Three bands, top to bottom, in the order a reader needs them:

```
┌────────────────────────────────────────────────────────────────┐
│  BAND 1 — "how bad is it?"                                     │
│  ┌──────────┬──────────┬──────────────┬──────────┐             │
│  │ 946      │ 126      │ gross        │ 98.7428% │   TILE 1    │
│  │ keys     │ excepts  │ exposure     │ DQ score │             │
│  └──────────┴──────────┴──────────────┴──────────┘             │
├────────────────────────────────────────────────────────────────┤
│  BAND 2 — "what kind of bad, and where?"                       │
│  ┌────────────────────────┬─────────────────────────┐          │
│  │ Break mix (excl.       │ Breaks by period,       │          │
│  │ MATCHED)      TILE 3   │ stacked        TILE 4   │          │
│  ├────────────────────────┼─────────────────────────┤          │
│  │ Net vs gross  TILE 5   │ Unrecorded              │          │
│  │ by status              │ liability      TILE 6   │          │
│  └────────────────────────┴─────────────────────────┘          │
├────────────────────────────────────────────────────────────────┤
│  BAND 3 — "what do I actually do about it?"                    │
│  ┌────────────────────────────────────────────────┐            │
│  │ Top 25 exceptions worklist            TILE 7   │            │
│  ├────────────────────────────────────────────────┤            │
│  │ Duplicates, with evidence             TILE 8   │            │
│  ├──────────────────────┬─────────────────────────┤            │
│  │ DQ by dataset TILE 9 │ Most violated   TILE 10 │            │
│  ├──────────────────────┴─────────────────────────┤            │
│  │ Rule catalogue + SQL                 TILE 11   │            │
│  │ Rows breaching 2+ rules              TILE 12   │            │
│  └────────────────────────────────────────────────┘            │
└────────────────────────────────────────────────────────────────┘
```

The ordering is the argument: **a number, then its shape, then the action.** A
dashboard that opens with a table of 126 rows makes the reader do the
summarising. A dashboard that never gets to the worklist is decoration.

---

## Tile specifications

| # | Title | Query | Visualisation | The question it answers |
|---|---|---|---|---|
| 1 | Headline counters | TILE 1 | Counter ×4 | How much was reconciled, how much broke, what is it worth, how clean was the input |
| 2 | Break mix *(optional)* | TILE 2 | Bar | The full taxonomy including MATCHED — use only if you want the 820 visible |
| 3 | Break mix, exceptions only | TILE 3 | Bar or donut | Of the things that broke, what kind of broken are they |
| 4 | Breaks by period | TILE 4 | Stacked bar | Is any period getting worse |
| 5 | Net vs gross by status | TILE 5 | Table or grouped bar | Is a small net difference hiding a large gross one |
| 6 | Unrecorded liability | TILE 6 | Bar | The headline finding — money owed that the books do not show |
| 7 | Exception worklist | TILE 7 | Table | What an analyst works on first |
| 8 | Duplicates, with evidence | TILE 8 | Table | Which double payments, and were the copies identical |
| 9 | DQ score by dataset | TILE 9 | Table or counters | Which source system is sending bad data |
| 10 | Most violated rules | TILE 10 | Horizontal bar | Which contract rule costs the most rows |
| 11 | Rule catalogue + SQL | TILE 11 | Table | The evidence layer — every rule, its intent, its SQL, its count |
| 12 | Rows breaching 2+ rules | TILE 12 | Table | Proof that rules are evaluated independently |

---

## Decisions worth defending

These are the choices a reviewer is most likely to ask about.

**Tile 3 exists because Tile 2 is unreadable.** 820 matched keys against 40, 35,
20, 16 and 15 flattens every break bar into the baseline on a linear axis. The
fix is a second chart with a different question, not a log scale — a log axis on
a business dashboard is a chart that requires an explanation, and a chart that
requires an explanation has already failed.

**Tile 5 is the one to keep when space runs out.** Net and gross difference are
different metrics, and reporting only net can show a clean period sitting on top
of real unresolved breaks. That is exactly the failure a reconciliation exists to
prevent, so the tile that makes it visible earns its space over anything
prettier.

**Tile 6 gets its own tile despite being one bar.** `MISSING_FROM_GL` is the only
*misstatement* in the taxonomy — everything else is an error. It understates
liabilities, which flatters the balance sheet. Burying it inside the break mix
would give the most important finding the same visual weight as a rounding
difference.

**Tile 11 looks like clutter and is not.** Showing the enforcing SQL next to the
rejection count is what makes the dashboard *evidence* rather than *assertion*. A
controller can disagree with a rule without reading Python, which is the entire
reason the rules live in `contracts.yaml` and compile to SQL strings.

**Zero rows stay visible.** The break-mix queries never filter `key_count > 0`,
and the rule catalogue keeps rules that fired zero times. A status or a rule
dropping off a chart because it hit zero looks identical to that check silently
ceasing to run, and the second is the one that hurts.

**No query computes a definition.** Every metric shown is already materialised in
a gold table and defined once in [`metric_definitions.md`](metric_definitions.md).
The queries filter, sort and total — they never decide what a number means. A
dashboard that re-derives "DQ score" in SQL creates a second definition, and the
version on the dashboard is the one people believe.

---

## Honest note on the platform

Databricks **Free Edition** has limited dashboard functionality compared with a
paid workspace. If the AI/BI dashboard builder is unavailable or restricted, the
fallback is a notebook with the same queries and `display()` charts — the queries
are identical either way, because all of them run against gold tables rather than
against notebook state.

If it lands as a notebook rather than a true Databricks SQL dashboard, say so.
The queries, the layout reasoning and the metric discipline are the transferable
work; which surface renders them is a licensing detail, and claiming a dashboard
that is actually a notebook is the kind of small overstatement that costs
credibility when someone asks to see it.

A Tableau Public version is on the roadmap at v0.6 and would be the stronger
public artefact, since it is linkable without a Databricks login.

---

*Synthetic demonstration project. All data is fictional and does not represent
any real company, client, employee, vendor, or financial system.*
