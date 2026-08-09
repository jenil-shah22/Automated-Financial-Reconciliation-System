"""The contract rule engine, compiled to SQL.

WHY SQL STRINGS RATHER THAN SPARK COLUMN OBJECTS
------------------------------------------------
Every rule in contracts.yaml is compiled into a Spark SQL boolean expression
that evaluates to TRUE when the rule is VIOLATED. Three reasons this beats
building `pyspark.sql.Column` objects directly:

1. **It is testable without a JVM.** Compilation is pure string manipulation,
   so the exact predicate for every rule is asserted in CI with no cluster and
   no Spark session. Only execution needs Spark.
2. **It is auditable.** The generated SQL can be printed and handed to a
   controller. "GL_NONZERO_AMOUNT rejected 2 rows" is an assertion; the SQL
   next to it is the evidence. A Column object is opaque.
3. **It is portable.** The same string runs in a Databricks SQL dashboard as
   in a notebook, so the DQ scorecard queries the same logic the pipeline
   enforced, rather than a hand-written re-implementation that can drift.

THE RLIKE TRAP
--------------
Spark's `RLIKE` is a *search*, not a full match - `'2026-03-extra' RLIKE
'[0-9]{4}-[0-9]{2}'` is TRUE. The pandas reference implementation uses
`fullmatch`. The two only agree because every pattern in contracts.yaml is
anchored with ^ and $, so `assert_patterns_are_anchored` enforces that
invariant rather than trusting it. An unanchored pattern would silently accept
junk in Spark while the pandas oracle rejected it, and the two engines would
disagree for a reason nobody would think to look for.

NULL SEMANTICS
--------------
Mirrors the pandas engine exactly: every check except `not_null` skips blank
values, and `non_zero` / `numeric_range` additionally skip values that do not
parse. One defect, one rule. See the header of contracts.yaml for why.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence

# Amounts are compared as DECIMAL, never DOUBLE. Binary floating point cannot
# represent 0.01 exactly; a reconciliation that tolerates 1.00 must not be the
# thing introducing sub-cent drift.
NUMERIC_CAST = "DECIMAL(18,2)"

RULE_ID_SEPARATOR = "|"


# =============================================================================
# SQL literal helpers
# =============================================================================
def sql_string(value: str) -> str:
    """Quote a Python string as a Spark SQL literal.

    Backslash is an escape character inside Spark SQL string literals, so it
    must be doubled - otherwise a regex containing \\d would arrive at the
    regex engine as a bare 'd' and silently match the wrong thing.
    """
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _blank(column: str) -> str:
    """Null means absent, or present but empty/whitespace.

    CSV has no concept of null, so an empty cell arrives as ''. A padded empty
    string from a fixed-width export is the same defect wearing a different hat.
    """
    return f"({column} IS NULL OR trim({column}) = '')"


def _parsed(column: str) -> str:
    """The value as a number, or NULL if it does not parse.

    try_cast rather than cast: cast raises and kills the job, which would mean
    one bad row prevents the pipeline from reporting on the other 1,908.
    """
    return f"try_cast({column} AS {NUMERIC_CAST})"


# =============================================================================
# Rule compilation
# =============================================================================
def predicate_for(rule: Dict[str, Any]) -> str:
    """Compile one rule into a Spark SQL expression, TRUE where VIOLATED."""
    column = rule["column"]
    check = rule["check"]
    blank = _blank(column)

    if check == "not_null":
        return blank

    if check == "unique":
        # A row predicate cannot see other rows, so uniqueness needs a window.
        # Blanks are excluded: repeated empties are not_null's problem, and
        # treating them as duplicates would double-report one defect.
        return (
            f"(NOT {blank} AND "
            f"count(*) OVER (PARTITION BY {column}) > 1)"
        )

    if check == "regex":
        # Anchored patterns only - see the RLIKE trap in the module docstring.
        return f"(NOT {blank} AND NOT ({column} RLIKE {sql_string(rule['pattern'])}))"

    if check == "allowed_values":
        values = ", ".join(sql_string(v) for v in rule["values"])
        return f"(NOT {blank} AND {column} NOT IN ({values}))"

    if check == "numeric":
        return f"(NOT {blank} AND {_parsed(column)} IS NULL)"

    if check == "non_zero":
        # Unparseable values skip this check - that is `numeric`'s job to
        # report, and reporting it twice would inflate the DQ denominator.
        return f"({_parsed(column)} IS NOT NULL AND {_parsed(column)} = 0)"

    if check == "numeric_range":
        low = rule.get("min")
        high = rule.get("max")
        bounds = []
        if low is not None:
            bounds.append(f"{_parsed(column)} < {low}")
        if high is not None:
            bounds.append(f"{_parsed(column)} > {high}")
        if not bounds:
            raise ValueError(f"Rule {rule['id']}: numeric_range needs min or max")
        return f"({_parsed(column)} IS NOT NULL AND ({' OR '.join(bounds)}))"

    raise ValueError(f"Rule {rule['id']}: unknown check type '{check}'")


def reject_rules(rules: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rules that quarantine, sorted by id.

    Sorted because the pandas reference implementation emits failed rule ids
    alphabetically. The two engines must produce byte-identical
    `_failed_rule_ids` strings or the differential test is comparing noise.
    """
    return sorted(
        (r for r in rules if r.get("severity", "reject") == "reject"),
        key=lambda r: r["id"],
    )


def failed_rule_ids_expr(rules: Sequence[Dict[str, Any]]) -> str:
    """A SQL expression producing the pipe-separated list of breached rule ids.

    `concat_ws` skips NULLs, so each rule contributes its id only when its
    predicate is TRUE. Every rule is evaluated - no short-circuiting - because a
    row breaching three rules is three separate tickets for three different
    people, and stopping at the first would hide two of them.
    """
    parts = [
        f"CASE WHEN {predicate_for(rule)} THEN {sql_string(rule['id'])} END"
        for rule in reject_rules(rules)
    ]
    if not parts:
        return "''"
    return f"concat_ws({sql_string(RULE_ID_SEPARATOR)}, {', '.join(parts)})"


def failed_rule_count_expr(ids_column: str = "_failed_rule_ids") -> str:
    """Count of breached rules, derived from the id list so they cannot disagree."""
    return (
        f"CASE WHEN {ids_column} = '' THEN 0 "
        f"ELSE size(split({ids_column}, {sql_string('[|]')})) END"
    )


def rule_violation_exprs(rules: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    """Per-rule counting expressions, for the DQ scorecard.

    Keyed by rule id so the scorecard reports 'GL_NONZERO_AMOUNT: 2' rather
    than an anonymous total. A rule that suddenly starts firing is as much a
    signal as one that stops, which is why zero-count rules stay in the output.
    """
    return {
        rule["id"]: f"count_if({predicate_for(rule)})"
        for rule in reject_rules(rules)
    }


# =============================================================================
# Invariants
# =============================================================================
def assert_patterns_are_anchored(contracts: Dict[str, Any]) -> None:
    """Every regex must be anchored at both ends.

    Spark RLIKE searches rather than full-matches. An unanchored pattern would
    accept 'INV-2026-000001-JUNK' in Spark while the pandas oracle rejected it,
    and the engines would disagree for a reason that looks like nothing.
    """
    offenders = []
    for ds_name, ds in contracts.get("datasets", {}).items():
        for rule in ds.get("rules", []):
            if rule["check"] != "regex":
                continue
            pattern = rule["pattern"]
            if not (pattern.startswith("^") and pattern.endswith("$")):
                offenders.append(f"{ds_name}.{rule['id']}: {pattern!r}")

    if offenders:
        raise ValueError(
            "Regex patterns must be anchored with ^ and $ because Spark RLIKE "
            "is a search, not a full match. Offenders: " + "; ".join(offenders)
        )


def assert_patterns_compile(contracts: Dict[str, Any]) -> None:
    """A pattern that does not compile is a rule that cannot fire."""
    for ds_name, ds in contracts.get("datasets", {}).items():
        for rule in ds.get("rules", []):
            if rule["check"] == "regex":
                try:
                    re.compile(rule["pattern"])
                except re.error as exc:
                    raise ValueError(
                        f"{ds_name}.{rule['id']} has an invalid pattern: {exc}"
                    ) from exc


def describe(rules: Sequence[Dict[str, Any]]) -> str:
    """Render every rule and its SQL, for the notebook and the data dictionary.

    Printing this is how a reviewer checks the rules mean what the descriptions
    claim, without reading any Python.
    """
    lines = []
    for rule in reject_rules(rules):
        lines.append(f"{rule['id']}")
        lines.append(f"    column : {rule['column']}  ({rule['check']})")
        desc = " ".join(str(rule.get("description", "")).split())
        if desc:
            lines.append(f"    intent : {desc}")
        lines.append(f"    sql    : {predicate_for(rule)}")
        lines.append("")
    return "\n".join(lines)
