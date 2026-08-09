"""Tests for the schema registry and the source header contract.

None of these need a JVM. Schemas are declared as plain data precisely so that
the rules about them can be verified anywhere, including in CI without Spark.
"""

from __future__ import annotations

import pandas as pd
import pytest

from ledgerlens import schemas
from ledgerlens.config import load_contracts
from ledgerlens.schemas import (
    AP_SOURCE_COLUMNS,
    BRONZE_AP_SCHEMA,
    BRONZE_GL_SCHEMA,
    GL_SOURCE_COLUMNS,
    QUARANTINE_AP_SCHEMA,
    QUARANTINE_GL_SCHEMA,
    SILVER_AP_SCHEMA,
    SILVER_GL_SCHEMA,
    assert_header_matches,
    column_names,
    to_ddl,
)


# =============================================================================
# The rule that defines bronze
# =============================================================================
@pytest.mark.parametrize("schema", [BRONZE_GL_SCHEMA, BRONZE_AP_SCHEMA])
def test_every_bronze_column_is_a_string(schema):
    """Bronze preserves evidence; silver applies judgment.

    If bronze cast `amount` to decimal, the planted "N/A" would become NULL at
    ingest and the row would be quarantined as 'missing value' instead of
    'text in a numeric column' - the wrong diagnosis, pointing at the wrong
    upstream fix, with the original bytes gone.
    """
    for col in schema:
        if col.name.startswith("_"):
            continue
        assert col.dtype == "string", f"{col.name} is typed in bronze"


@pytest.mark.parametrize("schema", [BRONZE_GL_SCHEMA, BRONZE_AP_SCHEMA])
def test_bronze_adds_only_underscore_prefixed_lineage(schema):
    """Added columns must be unable to collide with a source column, ever."""
    source = set(GL_SOURCE_COLUMNS) | set(AP_SOURCE_COLUMNS)
    added = [c.name for c in schema if c.name not in source]
    assert added == ["_ingested_at", "_source_file", "_batch_id"]
    assert all(name.startswith("_") for name in added)


@pytest.mark.parametrize("schema", [QUARANTINE_GL_SCHEMA, QUARANTINE_AP_SCHEMA])
def test_quarantine_keeps_everything_as_strings(schema):
    """A quarantined row failed the contract, so it cannot be assumed castable.

    Typing this table would mean the rows that most need investigating are
    exactly the ones that fail to load.
    """
    for col in schema:
        if col.name in {"_ingested_at", "_quarantined_at", "_failed_rule_count"}:
            continue
        assert col.dtype == "string", f"{col.name} is typed in quarantine"


def test_quarantine_records_rule_ids_and_a_count(schema=QUARANTINE_AP_SCHEMA):
    names = column_names(schema)
    assert "_failed_rule_ids" in names
    assert "_failed_rule_count" in names
    assert "_dataset" in names


# =============================================================================
# Silver typing
# =============================================================================
def test_amounts_are_decimal_not_double():
    """Binary floating point cannot represent 0.01 exactly.

    Summing thousands of invoice amounts in a double accumulates error. A
    reconciliation that tolerates 1.00 must not itself be the source of
    sub-cent drift.
    """
    for schema in (SILVER_GL_SCHEMA, SILVER_AP_SCHEMA):
        amount = next(c for c in schema if c.name == "amount")
        assert amount.dtype == "decimal_18_2"


def test_silver_types_the_columns_that_need_it():
    gl = {c.name: c.dtype for c in SILVER_GL_SCHEMA}
    ap = {c.name: c.dtype for c in SILVER_AP_SCHEMA}
    assert gl["posting_date"] == "date"
    assert ap["invoice_date"] == "date"
    assert ap["due_date"] == "date"
    assert ap["payment_terms_days"] == "int"
    # Codes stay strings: leading zeros are meaningful and arithmetic on an
    # account code is never a valid operation.
    assert gl["account_code"] == "string"
    assert ap["vendor_code"] == "string"


def test_fiscal_period_stays_a_string_not_a_date():
    """A fiscal period is a stated attribute, not a truncated timestamp.

    Periods close on a decision, not on a calendar boundary. Storing this as a
    date invites `date_trunc('month', posting_date)` downstream, which would
    silently reclassify every cut-off entry and manufacture timing differences
    that do not exist.
    """
    for schema in (SILVER_GL_SCHEMA, SILVER_AP_SCHEMA):
        period = next(c for c in schema if c.name == "fiscal_period")
        assert period.dtype == "string"


def test_business_key_and_amount_are_not_nullable_in_silver():
    """Everything in silver passed a not_null contract, so the schema says so."""
    contracts = load_contracts()
    key = set(contracts["recon"]["business_key"]) | {"amount", "fiscal_period"}
    for schema in (SILVER_GL_SCHEMA, SILVER_AP_SCHEMA):
        for col in schema:
            if col.name in key:
                assert not col.nullable, f"{col.name} should be NOT NULL in silver"


def test_silver_and_bronze_have_the_same_columns():
    """Silver types columns; it does not add or drop them."""
    for bronze, silver in ((BRONZE_GL_SCHEMA, SILVER_GL_SCHEMA),
                           (BRONZE_AP_SCHEMA, SILVER_AP_SCHEMA)):
        assert column_names(bronze) == column_names(silver)


# =============================================================================
# The schema registry agrees with reality
# =============================================================================
def test_declared_source_columns_match_the_generated_files(generated):
    """The schema contract and the generator must not drift apart.

    If someone adds a column to the generator and forgets the schema, this
    fails here rather than at ingest on Databricks twenty minutes later.
    """
    gl = pd.read_csv(generated / "gl.csv", nrows=0)
    ap = pd.read_csv(generated / "ap_subledger.csv", nrows=0)
    assert list(gl.columns) == GL_SOURCE_COLUMNS
    assert list(ap.columns) == AP_SOURCE_COLUMNS


def test_every_contract_rule_targets_a_declared_column():
    """A rule on a column that does not exist is a rule that never fires."""
    contracts = load_contracts()
    for ds_name, source in (("gl", GL_SOURCE_COLUMNS), ("ap", AP_SOURCE_COLUMNS)):
        for rule in contracts["datasets"][ds_name]["rules"]:
            assert rule["column"] in source, (
                f"{rule['id']} targets '{rule['column']}', which is not a "
                f"{ds_name} source column"
            )


def test_every_column_has_a_description():
    """The data dictionary is generated from these, so blanks are not allowed."""
    for name, schema in schemas.SCHEMAS.items():
        for col in schema:
            assert col.description.strip(), f"{name}.{col.name} has no description"


# =============================================================================
# Header contract
# =============================================================================
def test_header_matching_accepts_the_exact_contract():
    assert_header_matches(GL_SOURCE_COLUMNS, GL_SOURCE_COLUMNS, "gl") is None


def test_reordered_header_is_rejected():
    """The failure this control exists for.

    Spark applies a supplied schema POSITIONALLY and ignores the header. A
    swapped pair of columns would load cleanly, put vendor codes in the invoice
    field, and produce a reconciliation where every number is wrong but
    plausible. Nothing else in the pipeline would notice.
    """
    swapped = list(GL_SOURCE_COLUMNS)
    i, j = swapped.index("vendor_code"), swapped.index("invoice_number")
    swapped[i], swapped[j] = swapped[j], swapped[i]

    with pytest.raises(ValueError, match="ORDER changed"):
        assert_header_matches(swapped, GL_SOURCE_COLUMNS, "gl")


def test_missing_column_is_reported_by_name():
    partial = [c for c in AP_SOURCE_COLUMNS if c != "payment_status"]
    with pytest.raises(ValueError, match="payment_status"):
        assert_header_matches(partial, AP_SOURCE_COLUMNS, "ap")


def test_unexpected_column_is_reported_by_name():
    extra = list(AP_SOURCE_COLUMNS) + ["approval_workflow_id"]
    with pytest.raises(ValueError, match="approval_workflow_id"):
        assert_header_matches(extra, AP_SOURCE_COLUMNS, "ap")


# =============================================================================
# DDL generation
# =============================================================================
def test_ddl_renders_types_and_nullability():
    ddl = to_ddl(SILVER_AP_SCHEMA)
    assert "amount DECIMAL(18,2) NOT NULL" in ddl
    assert "invoice_date DATE" in ddl
    assert "payment_terms_days INT" in ddl
    assert "_batch_id STRING NOT NULL" in ddl


def test_ddl_covers_every_column():
    for schema in schemas.SCHEMAS.values():
        ddl = to_ddl(schema)
        assert ddl.count("\n") + 1 == len(schema)
