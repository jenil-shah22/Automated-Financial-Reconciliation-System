"""Tests for bronze ingest logic that does not need a JVM.

The Spark-dependent parts are exercised on Databricks by
`notebooks/01_bronze_ingest.py`, which asserts bronze row counts against the
control manifest. What is tested here is everything that can be wrong *before*
Spark is involved - target resolution, environment detection, and the row
counting that the losslessness control depends on.

Deliberate: `_count_source_rows` must never be replaced with `df.count()`. The
control compares what Spark loaded against what the file contains, so it has to
be measured without Spark - otherwise a read that silently dropped rows would
simply agree with itself.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from ledgerlens.bronze import LakehouseConfig, _count_source_rows


# =============================================================================
# Target resolution
# =============================================================================
def test_catalog_mode_builds_a_three_part_name():
    cfg = LakehouseConfig(mode="catalog", catalog="ledgerlens", schema="bronze")
    assert cfg.target("gl") == "ledgerlens.bronze.gl"


def test_path_mode_builds_a_directory(tmp_path):
    cfg = LakehouseConfig(mode="path", base_path=tmp_path, schema="bronze")
    assert cfg.target("gl") == str(tmp_path / "bronze" / "gl")


def test_detect_returns_catalog_mode_on_databricks(monkeypatch):
    """Databricks sets DATABRICKS_RUNTIME_VERSION; nothing else does.

    The point of detection is that the same code runs in both places without a
    branch in the caller - the brief lists local PySpark as the fallback if
    Databricks Free Edition is unavailable, and that has to be a swap rather
    than a rewrite.
    """
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "15.4.x-scala2.12")
    assert LakehouseConfig.detect().mode == "catalog"


def test_detect_returns_path_mode_locally(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert LakehouseConfig.detect().mode == "path"


# =============================================================================
# The losslessness control
# =============================================================================
def test_source_row_count_excludes_the_header(tmp_path):
    path = tmp_path / "f.csv"
    path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    assert _count_source_rows(path) == 2


def test_header_only_file_counts_as_zero_rows(tmp_path):
    path = tmp_path / "f.csv"
    path.write_text("a,b\n", encoding="utf-8")
    assert _count_source_rows(path) == 0


def test_empty_file_does_not_produce_a_negative_count(tmp_path):
    path = tmp_path / "f.csv"
    path.write_text("", encoding="utf-8")
    assert _count_source_rows(path) == 0


def test_row_count_matches_the_manifest(generated, manifest):
    """The control the ingest assertion is built on."""
    assert _count_source_rows(generated / "gl.csv") == manifest["row_counts"]["gl_raw"]
    assert _count_source_rows(generated / "ap_subledger.csv") == manifest["row_counts"]["ap_raw"]


# =============================================================================
# Spark-dependent conversion, skipped where there is no JVM
# =============================================================================
pyspark = pytest.importorskip("pyspark", reason="pyspark not installed")


def test_spark_schema_matches_the_declaration():
    """to_spark_schema is a pure type mapping - no session, no JVM needed."""
    from pyspark.sql import types as T

    from ledgerlens.schemas import BRONZE_GL_SCHEMA, SILVER_AP_SCHEMA, to_spark_schema

    bronze = to_spark_schema(BRONZE_GL_SCHEMA)
    assert len(bronze.fields) == len(BRONZE_GL_SCHEMA)
    # Every source column is a string in bronze; only _ingested_at is not.
    assert isinstance(bronze["amount"].dataType, T.StringType)
    assert isinstance(bronze["posting_date"].dataType, T.StringType)
    assert isinstance(bronze["_ingested_at"].dataType, T.TimestampType)

    silver = to_spark_schema(SILVER_AP_SCHEMA)
    assert silver["amount"].dataType == T.DecimalType(18, 2)
    assert isinstance(silver["invoice_date"].dataType, T.DateType)
    assert isinstance(silver["fiscal_period"].dataType, T.StringType)
    assert not silver["amount"].nullable
