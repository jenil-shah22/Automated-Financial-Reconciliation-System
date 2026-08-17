"""Explicit schemas for every layer. Nothing is ever inferred.

WHY EXPLICIT SCHEMAS
--------------------
Schema inference reads a sample of the data and guesses. That guess is not
stable: it depends on which rows were sampled, and it changes silently when
the data changes. A column that inferred as integer last month infers as
string this month because one row arrived with a comma in it - and every
downstream comparison quietly starts returning false.

The failure mode is worse than a crash, because nothing crashes.

WHY BRONZE IS ALL STRINGS
-------------------------
Every bronze column is StringType, including amounts and dates. This looks
lazy. It is the opposite.

Bronze's contract is "exactly as received". If bronze casts `amount` to
decimal, the planted value "N/A" becomes NULL at ingest - before the contract
engine ever sees it. The row would then be quarantined by AP_NN_AMOUNT
("missing value") instead of AP_NUM_AMOUNT ("text in a numeric column"), which
is the wrong diagnosis pointing at the wrong upstream fix. And the original
bytes are gone, so nobody can ever prove what actually arrived.

Casting is a *silver* concern, performed only after the contract has ruled the
value acceptable. Bronze preserves evidence; silver applies judgment.

SINGLE SOURCE OF TRUTH
----------------------
Schemas are declared here as plain Python data, not as Spark objects, for two
reasons: this module imports without a JVM (so schema tests run anywhere), and
one declaration generates the Spark StructType, the SQL DDL, and the CSV header
contract - which cannot drift apart because they are the same object.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple

# =============================================================================
# Column specification
# =============================================================================
@dataclass(frozen=True)
class Column:
    name: str
    dtype: str  # logical type name, mapped to Spark/SQL below
    nullable: bool
    description: str


# Logical type -> (Spark type constructor name, SQL DDL type).
# DECIMAL(18,2) rather than DOUBLE for money: binary floating point cannot
# represent 0.01 exactly, so summing thousands of invoice amounts in a double
# accumulates error. A reconciliation that tolerates 1.00 must not be the thing
# introducing sub-cent drift.
_TYPE_MAP: Dict[str, Tuple[str, str]] = {
    "string": ("StringType", "STRING"),
    "date": ("DateType", "DATE"),
    "timestamp": ("TimestampType", "TIMESTAMP"),
    "decimal_18_2": ("DecimalType", "DECIMAL(18,2)"),
    # A percentage is not money. It is a ratio rounded for display, so it gets
    # its own narrow type rather than borrowing the money type and implying a
    # currency amount. 999.9999 is plenty of headroom for a 0-100 score.
    "decimal_7_4": ("DecimalType", "DECIMAL(7,4)"),
    "int": ("IntegerType", "INT"),
    "long": ("LongType", "BIGINT"),
    "boolean": ("BooleanType", "BOOLEAN"),
}


# =============================================================================
# Source contract - the exact CSV header we expect to receive
# =============================================================================
# Order matters. If the upstream extract reorders or renames a column, we want
# a loud failure at ingest, not a silent mis-mapping of vendor_code into
# invoice_number. See `assert_header_matches`.
GL_SOURCE_COLUMNS: List[str] = [
    "gl_entry_id",
    "posting_date",
    "fiscal_period",
    "account_code",
    "account_name",
    "department_code",
    "department_name",
    "vendor_code",
    "invoice_number",
    "amount",
    "currency",
    "source_system",
    "extract_ts",
]

AP_SOURCE_COLUMNS: List[str] = [
    "ap_line_id",
    "invoice_date",
    "due_date",
    "fiscal_period",
    "vendor_code",
    "vendor_name",
    "account_code",
    "invoice_number",
    "amount",
    "currency",
    "payment_status",
    "payment_terms_days",
    "source_system",
    "extract_ts",
]


# =============================================================================
# Bronze - every column a string, plus ingestion lineage
# =============================================================================
_GL_DESCRIPTIONS: Dict[str, str] = {
    "gl_entry_id": "Surrogate key of the GL posting. Unique within a load.",
    "posting_date": "Date the entry hit the books. Informational - fiscal_period is authoritative for reporting.",
    "fiscal_period": "Accounting period yyyy-mm. Authoritative for all period reporting.",
    "account_code": "Chart-of-accounts code. Part of the business key.",
    "account_name": "Human-readable account label, for display only.",
    "department_code": "Cost centre the expense is classified to.",
    "department_name": "Human-readable department label, for display only.",
    "vendor_code": "Supplier identifier. Part of the business key.",
    "invoice_number": "Vendor invoice reference. Part of the business key.",
    "amount": "Posted amount in currency units.",
    "currency": "ISO currency code. v0.1 is single-currency (USD).",
    "source_system": "System of record the row was extracted from.",
    "extract_ts": "When the source extract was taken. Proves which extract was reconciled.",
}

_AP_DESCRIPTIONS: Dict[str, str] = {
    "ap_line_id": "Surrogate key of the subledger line. Unique within a load.",
    "invoice_date": "Date the vendor issued the invoice. May fall in a different month than fiscal_period - that is a normal cut-off artefact.",
    "due_date": "Payment due date. Drives aging (roadmap v0.4).",
    "fiscal_period": "Accounting period yyyy-mm. Authoritative for all period reporting.",
    "vendor_code": "Supplier identifier. Part of the business key.",
    "vendor_name": "Human-readable vendor label, for display only.",
    "account_code": "Chart-of-accounts code. Part of the business key.",
    "invoice_number": "Vendor invoice reference. Part of the business key.",
    "amount": "Invoiced amount in currency units.",
    "currency": "ISO currency code. v0.1 is single-currency (USD).",
    "payment_status": "Workflow state: OPEN, PAID, PARTIAL, ON_HOLD.",
    "payment_terms_days": "Agreed payment terms in days.",
    "source_system": "System of record the row was extracted from.",
    "extract_ts": "When the source extract was taken.",
}

# Lineage columns added at ingest. Prefixed with an underscore so they can
# never collide with a source column name, now or after an upstream change.
INGESTION_COLUMNS: List[Column] = [
    Column("_ingested_at", "timestamp", False,
           "When this row was written to bronze."),
    Column("_source_file", "string", False,
           "Absolute path of the file this row came from. The first question "
           "asked about any suspect row is 'where did it come from'."),
    Column("_batch_id", "string", False,
           "Identifier of the ingest run. Makes a bad load reversible as a set."),
]


def _bronze_columns(source_columns: Sequence[str],
                    descriptions: Dict[str, str]) -> List[Column]:
    """Bronze mirrors the source exactly, as strings, plus lineage."""
    return [
        Column(name, "string", True, descriptions[name]) for name in source_columns
    ] + INGESTION_COLUMNS


BRONZE_GL_SCHEMA: List[Column] = _bronze_columns(GL_SOURCE_COLUMNS, _GL_DESCRIPTIONS)
BRONZE_AP_SCHEMA: List[Column] = _bronze_columns(AP_SOURCE_COLUMNS, _AP_DESCRIPTIONS)


# =============================================================================
# Silver - typed, conformed, contract-approved
# =============================================================================
# Types are applied here and nowhere earlier. Every column below survived the
# contract, so the cast is guaranteed to succeed - a cast failure in silver is
# a bug in the contract, not bad data, and should be treated as such.
_SILVER_TYPES: Dict[str, str] = {
    "posting_date": "date",
    "invoice_date": "date",
    "due_date": "date",
    "amount": "decimal_18_2",
    "payment_terms_days": "int",
    "extract_ts": "timestamp",
}


def _silver_columns(bronze: Sequence[Column]) -> List[Column]:
    out: List[Column] = []
    for col in bronze:
        if col.name.startswith("_"):
            out.append(col)
            continue
        dtype = _SILVER_TYPES.get(col.name, "string")
        # Contract-guaranteed non-null in silver: the business key and amount
        # all have not_null rules, so anything reaching silver has them.
        nullable = col.name not in {
            "account_code", "vendor_code", "invoice_number", "amount", "fiscal_period",
        }
        out.append(Column(col.name, dtype, nullable, col.description))
    return out


SILVER_GL_SCHEMA: List[Column] = _silver_columns(BRONZE_GL_SCHEMA)
SILVER_AP_SCHEMA: List[Column] = _silver_columns(BRONZE_AP_SCHEMA)


# =============================================================================
# Quarantine - the rejected rows, with the reason attached
# =============================================================================
# Quarantine keeps every source column as a STRING, exactly as bronze had it.
# The whole point of a quarantined row is that it failed the contract, so it
# cannot be assumed castable. Typing this table would mean the rows that most
# need investigating are the ones that fail to load.
QUARANTINE_METADATA: List[Column] = [
    Column("_quarantined_at", "timestamp", False, "When the row was rejected."),
    Column("_dataset", "string", False, "Source dataset: 'gl' or 'ap'."),
    Column("_failed_rule_ids", "string", False,
           "Pipe-separated contract rule ids that rejected this row. Plural: a "
           "row can breach several rules and each is a separate fix."),
    Column("_failed_rule_count", "int", False, "How many rules this row breached."),
]


def quarantine_schema(bronze: Sequence[Column]) -> List[Column]:
    return [Column(c.name, "string", True, c.description) for c in bronze
            if not c.name.startswith("_")] + INGESTION_COLUMNS + QUARANTINE_METADATA


QUARANTINE_GL_SCHEMA: List[Column] = quarantine_schema(BRONZE_GL_SCHEMA)
QUARANTINE_AP_SCHEMA: List[Column] = quarantine_schema(BRONZE_AP_SCHEMA)


# =============================================================================
# Gold - the reconciliation output, and the only layer anyone else reads
# =============================================================================
# Gold is declared here for the same reason bronze and silver are: it is the
# layer a dashboard, a controller and the data dictionary all consume, so its
# shape is a published interface rather than whatever the last transformation
# happened to emit. gold.py projects through these declarations, so a column
# that is not declared here cannot reach a gold table.
#
# NULL vs 0.00 IN THE AMOUNT COLUMNS - a deliberate distinction
#   `gl_amount` is NULL when a key has no GL side at all. It is NOT 0.00.
#   Zero is an assertion ("the ledger posted nothing"); NULL is an absence
#   ("the ledger has no opinion"). MISSING_FROM_GL is the unrecorded-liability
#   break, and writing 0.00 there would let `sum(gl_amount)` read as a real
#   posted total that happens to be zero.
#   `amount_difference` is the exception: a difference has to be a number, so
#   the missing side is coalesced to zero *for the subtraction only*. The
#   stored side amounts keep the distinction.
GOLD_RECON_DETAIL_SCHEMA: List[Column] = [
    Column("fiscal_period", "string", False,
           "Reporting period for the key: the GL period, falling back to the AP "
           "period when the key has no GL side. The GL is the book of record, so "
           "a timing difference is reported in the period whose close it affects."),
    Column("account_code", "string", False, "Business key part 1 of 3."),
    Column("vendor_code", "string", False, "Business key part 2 of 3."),
    Column("invoice_number", "string", False, "Business key part 3 of 3."),
    Column("break_status", "string", False,
           "Exactly one of the six statuses in the break taxonomy. The taxonomy "
           "is a partition: every key gets one status, no key escapes."),
    Column("gl_amount", "decimal_18_2", True,
           "Sum of GL amounts on this key. NULL - not zero - when the key has no "
           "GL side."),
    Column("ap_amount", "decimal_18_2", True,
           "Sum of subledger amounts on this key. NULL - not zero - when the key "
           "has no subledger side."),
    Column("amount_difference", "decimal_18_2", False,
           "gl_amount - ap_amount, with a missing side treated as zero for the "
           "subtraction. Signed: positive means the GL carries more."),
    Column("abs_amount_difference", "decimal_18_2", False,
           "Absolute value of amount_difference. This is the exposure the break "
           "represents, and what exceptions are ranked by."),
    Column("gl_fiscal_period", "string", True,
           "Earliest GL period on the key. NULL when the key has no GL side."),
    Column("ap_fiscal_period", "string", True,
           "Earliest subledger period on the key. NULL when the key has no "
           "subledger side."),
    Column("gl_row_count", "int", False,
           "GL rows behind this key. Zero for a subledger-only key."),
    Column("ap_row_count", "int", False,
           "Subledger rows behind this key. Greater than one is what makes a key "
           "DUPLICATE_IN_SUBLEDGER - which is why the aggregation carries a row "
           "count and not just a sum."),
]

GOLD_RECON_SUMMARY_SCHEMA: List[Column] = [
    Column("fiscal_period", "string", False, "Reporting period, as per gold.recon_detail."),
    Column("break_status", "string", False, "One of the six statuses."),
    Column("key_count", "long", False,
           "Business keys with this status in this period. Zero rows are "
           "materialised on purpose - see gold.py."),
    Column("gl_amount", "decimal_18_2", False,
           "Total GL value of those keys. Zero when there are none."),
    Column("ap_amount", "decimal_18_2", False,
           "Total subledger value of those keys. Zero when there are none."),
    Column("net_amount_difference", "decimal_18_2", False,
           "Sum of the SIGNED differences. Opposite-direction breaks cancel, so "
           "this is the effect on the books, not the size of the problem."),
    Column("abs_amount_difference", "decimal_18_2", False,
           "Sum of the ABSOLUTE differences. Nothing cancels, so this is the "
           "gross exposure - the number to quote as 'value under investigation'. "
           "Distinct from net_amount_difference and never interchangeable."),
]

GOLD_RECON_EXCEPTIONS_SCHEMA: List[Column] = [
    Column("exception_rank", "int", False,
           "Rank by abs_amount_difference descending, business key ascending as "
           "the tie-break. Materialised because a Delta table has no inherent row "
           "order - an ORDER BY at write time does not survive a read."),
    Column("fiscal_period", "string", False, "Reporting period, as per gold.recon_detail."),
    Column("break_status", "string", False, "One of the five non-MATCHED statuses."),
    Column("account_code", "string", False, "Business key part 1 of 3."),
    Column("account_name", "string", True,
           "Account label, joined from the GL. Display only - never a join key."),
    Column("vendor_code", "string", False, "Business key part 2 of 3."),
    Column("vendor_name", "string", True,
           "Vendor label, joined from the subledger. Display only."),
    Column("invoice_number", "string", False, "Business key part 3 of 3."),
    Column("gl_amount", "decimal_18_2", True, "As per gold.recon_detail."),
    Column("ap_amount", "decimal_18_2", True, "As per gold.recon_detail."),
    Column("amount_difference", "decimal_18_2", False, "As per gold.recon_detail."),
    Column("abs_amount_difference", "decimal_18_2", False, "As per gold.recon_detail."),
    Column("gl_fiscal_period", "string", True, "As per gold.recon_detail."),
    Column("ap_fiscal_period", "string", True, "As per gold.recon_detail."),
    Column("gl_row_count", "int", False, "As per gold.recon_detail."),
    Column("ap_row_count", "int", False, "As per gold.recon_detail."),
]


# =============================================================================
# Gold - the data-quality scorecard
# =============================================================================
# Two tables, at two different grains, because they answer two different
# questions. "Is the data fit to reconcile?" is a per-dataset question.
# "What is wrong with it?" is a per-rule question. Collapsing them into one
# table would force one of the two to be answered by a filter.
GOLD_DQ_SCORECARD_SCHEMA: List[Column] = [
    Column("dataset", "string", False, "Source dataset: 'gl' or 'ap'."),
    Column("label", "string", False,
           "Human-readable dataset name, from contracts.yaml."),
    Column("rows_received", "long", False,
           "Rows that arrived in bronze. The DQ denominator is what we were "
           "SENT, not what we kept - measuring against what survived would "
           "score 100% on any input."),
    Column("rows_passed", "long", False, "Rows that satisfied every contract rule."),
    Column("rows_quarantined", "long", False,
           "Rows rejected. rows_passed + rows_quarantined = rows_received, "
           "asserted on every run: no row is ever silently dropped."),
    Column("rule_violations", "long", False,
           "Total rule breaches. EXCEEDS rows_quarantined when a row breaches "
           "more than one rule - which is the point of recording rule ids as a "
           "list. Never use this as a row count."),
    Column("dq_score_pct", "decimal_7_4", False,
           "100 * rows_passed / rows_received, to 4dp. Per dataset. To get an "
           "overall score, sum the numerators and denominators - averaging the "
           "per-dataset scores weights a small dataset equally with a large one."),
]

GOLD_DQ_RULE_SCORECARD_SCHEMA: List[Column] = [
    Column("rule_id", "string", False,
           "Permanent contract rule id. Stamped onto every row it rejects, so "
           "ids are never recycled - a reused id would silently change the "
           "meaning of historical quarantine records."),
    Column("dataset", "string", False, "Dataset the rule applies to."),
    Column("column_name", "string", False, "Column the rule tests."),
    Column("check_type", "string", False,
           "Kind of assertion: not_null, unique, regex, allowed_values, "
           "numeric, non_zero, numeric_range."),
    Column("severity", "string", False,
           "'reject' quarantines the row; 'warn' would let it through flagged."),
    Column("description", "string", False,
           "The rule's intent, in the words of whoever wrote the contract."),
    Column("predicate_sql", "string", False,
           "The exact Spark SQL the pipeline evaluated, TRUE where violated. "
           "This is what makes the scorecard evidence rather than assertion: "
           "the count and the logic that produced it sit in the same row, and "
           "the dashboard cannot drift from the pipeline because it is reading "
           "the pipeline's own predicate."),
    Column("rows_rejected", "long", False,
           "Rows this rule rejected. Zero-firing rules are kept: a rule that "
           "stops firing is as much a signal as one that starts, and a list of "
           "only non-zero rules cannot tell 'clean' from 'that check silently "
           "stopped running'."),
]


# =============================================================================
# Conversions
# =============================================================================
def to_sql_type(dtype: str) -> str:
    """Logical type name -> SQL type. One lookup table, used by every layer."""
    if dtype not in _TYPE_MAP:
        raise KeyError(f"Unknown logical dtype '{dtype}'")
    return _TYPE_MAP[dtype][1]


def to_spark_schema(columns: Sequence[Column]):
    """Build a Spark StructType. Imported lazily so this module needs no JVM."""
    from pyspark.sql import types as T

    fields = []
    for col in columns:
        spark_name, _ = _TYPE_MAP[col.dtype]
        if col.dtype == "decimal_18_2":
            spark_type: Any = T.DecimalType(18, 2)
        elif col.dtype == "decimal_7_4":
            spark_type = T.DecimalType(7, 4)
        else:
            spark_type = getattr(T, spark_name)()
        fields.append(T.StructField(col.name, spark_type, col.nullable))
    return T.StructType(fields)


def to_ddl(columns: Sequence[Column]) -> str:
    """Emit SQL DDL, for `CREATE TABLE` and for the data dictionary."""
    parts = []
    for col in columns:
        _, sql_type = _TYPE_MAP[col.dtype]
        null = "" if col.nullable else " NOT NULL"
        parts.append(f"  {col.name} {sql_type}{null}")
    return ",\n".join(parts)


def column_names(columns: Sequence[Column]) -> List[str]:
    return [c.name for c in columns]


def assert_header_matches(actual: Sequence[str], expected: Sequence[str],
                          dataset: str) -> None:
    """Fail loudly if the source file is not the shape we contracted for.

    This is the cheapest control in the entire pipeline and it catches the most
    expensive class of failure: an upstream extract that silently reorders or
    renames columns. Without it, a positional read maps vendor_code into
    invoice_number and every downstream number is wrong but plausible.
    """
    actual, expected = list(actual), list(expected)
    if actual == expected:
        return

    missing = [c for c in expected if c not in actual]
    unexpected = [c for c in actual if c not in expected]
    detail = []
    if missing:
        detail.append(f"missing columns: {missing}")
    if unexpected:
        detail.append(f"unexpected columns: {unexpected}")
    if not detail:
        detail.append(f"column ORDER changed: got {actual}, expected {expected}")

    raise ValueError(
        f"{dataset} source header does not match the contract - " + "; ".join(detail)
    )


# Registry so notebooks and tests can look layers up by name.
SCHEMAS: Dict[str, List[Column]] = {
    "bronze_gl": BRONZE_GL_SCHEMA,
    "bronze_ap": BRONZE_AP_SCHEMA,
    "silver_gl": SILVER_GL_SCHEMA,
    "silver_ap": SILVER_AP_SCHEMA,
    "quarantine_gl": QUARANTINE_GL_SCHEMA,
    "quarantine_ap": QUARANTINE_AP_SCHEMA,
    "gold_recon_detail": GOLD_RECON_DETAIL_SCHEMA,
    "gold_recon_summary": GOLD_RECON_SUMMARY_SCHEMA,
    "gold_recon_exceptions": GOLD_RECON_EXCEPTIONS_SCHEMA,
    "gold_dq_scorecard": GOLD_DQ_SCORECARD_SCHEMA,
    "gold_dq_rule_scorecard": GOLD_DQ_RULE_SCORECARD_SCHEMA,
}
