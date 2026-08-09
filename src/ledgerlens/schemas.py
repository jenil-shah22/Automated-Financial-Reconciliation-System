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
# Conversions
# =============================================================================
def to_spark_schema(columns: Sequence[Column]):
    """Build a Spark StructType. Imported lazily so this module needs no JVM."""
    from pyspark.sql import types as T

    fields = []
    for col in columns:
        spark_name, _ = _TYPE_MAP[col.dtype]
        if col.dtype == "decimal_18_2":
            spark_type: Any = T.DecimalType(18, 2)
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
}
