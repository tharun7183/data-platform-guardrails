from __future__ import annotations
from pyspark.sql import DataFrame, functions as F

def check_non_null(df: DataFrame, col: str) -> tuple[bool, str]:
    bad = df.filter(F.col(col).isNull()).count()
    return (bad == 0, f"{col}: null_count={bad}")

def check_allowed_values(df: DataFrame, col: str, allowed: list[str]) -> tuple[bool, str]:
    bad = df.filter(~F.col(col).isin(allowed)).count()
    return (bad == 0, f"{col}: bad_values_count={bad}, allowed={allowed}")

def check_no_duplicates(df: DataFrame, key: str) -> tuple[bool, str]:
    total = df.count()
    distinct = df.select(key).distinct().count()
    dup = total - distinct
    return (dup == 0, f"{key}: duplicate_count={dup}")

def check_drop_rate(input_rows: int, curated_rows: int, max_drop_pct: float = 5.0) -> tuple[bool, str]:
    if input_rows <= 0:
        return (False, "input_rows=0 (unexpected)")
    drop = max(0, input_rows - curated_rows)
    drop_pct = (drop / input_rows) * 100.0
    passed = drop_pct <= max_drop_pct
    return (passed, f"drop_pct={drop_pct:.2f} (threshold={max_drop_pct:.2f})")

def run_quality_suite(df: DataFrame, curated_rows: int | None = None) -> list[tuple[str, bool, str]]:
    allowed_event_types = ["view", "click", "vote", "comment", "signup"]
    input_rows = df.count()

    checks = [
        ("non_null_user_id",) + check_non_null(df, "user_id"),
        ("non_null_event_id",) + check_non_null(df, "event_id"),
        ("allowed_event_type",) + check_allowed_values(df, "event_type", allowed_event_types),
        ("no_duplicate_event_id",) + check_no_duplicates(df, "event_id"),
    ]
    if curated_rows is not None:
        passed, details = check_drop_rate(input_rows, curated_rows, max_drop_pct=5.0)
        checks.append(("drop_rate_guardrail", passed, details))

    return [(name, passed, details) for (name, passed, details) in checks]
