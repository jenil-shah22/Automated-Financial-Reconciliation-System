"""Tests for the generated documentation.

The point of these is narrow and worth stating: they turn "keep the docs
updated" from a discipline into a build error. A hand-maintained data
dictionary is correct on the day it is written and silently wrong a month
later, because nobody diffs prose against code.
"""

from __future__ import annotations

from ledgerlens import docs_gen
from ledgerlens.schemas import SCHEMAS, column_names


def test_committed_data_dictionary_is_not_stale():
    """Fails if schemas.py changed and the dictionary was not regenerated.

    Fix: `python -m ledgerlens.docs_gen`.
    """
    assert docs_gen.DATA_DICTIONARY.exists(), (
        "docs/data_dictionary.md is missing - run python -m ledgerlens.docs_gen"
    )
    committed = docs_gen.DATA_DICTIONARY.read_text(encoding="utf-8")
    assert committed == docs_gen.render(), (
        "docs/data_dictionary.md is stale - run python -m ledgerlens.docs_gen"
    )


def test_every_registered_schema_is_documented():
    """A published table with no entry in the dictionary is undocumented data.

    `render()` raises rather than quietly omitting it, so this asserts the guard
    is wired up rather than re-implementing it.
    """
    documented = {name for name, _, _ in docs_gen.LAYERS}
    assert documented == set(SCHEMAS)


def test_every_column_appears_in_the_document():
    rendered = docs_gen.render()
    for schema in SCHEMAS.values():
        for name in column_names(schema):
            assert f"`{name}`" in rendered, f"{name} is missing from the dictionary"


def test_document_warns_against_hand_editing():
    """It is generated, and a reader needs to know before they edit it."""
    rendered = docs_gen.render()
    assert "GENERATED FILE" in rendered
    assert "docs_gen" in rendered
