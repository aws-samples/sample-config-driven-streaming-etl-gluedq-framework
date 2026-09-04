"""
YAML -> DQDL compiler for native AWS Glue Data Quality.

Pure-Python module (no pyspark/awsglue imports) so the compilation logic is
unit-testable off-Glue. Used by glue_dq_analyzer.py at runtime.

DQDL reference: https://docs.aws.amazon.com/glue/latest/dg/dqdl.html
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("DQDLCompiler")

# Quality metrics supported by the YAML -> DQDL compiler
SUPPORTED_METRICS = {"completeness", "uniqueness", "compliance", "size"}


def build_dqdl_rules(checks: List[Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Compile YAML quality checks into DQDL rule strings.

    Returns a list of (dqdl_rule, source_check) pairs so evaluation outcomes
    can be mapped back to their originating check (for severity handling).

    Mapping:
      completeness + threshold -> Completeness "col" >= threshold
      uniqueness  + threshold  -> Uniqueness "col" >= threshold
      compliance  + pattern    -> ColumnValues "col" matches "pattern"
                                  (+ optional "with threshold >= X")
      size        + threshold  -> RowCount >= threshold
    """
    rules: List[Tuple[str, Dict[str, Any]]] = []

    for check in checks or []:
        metric = str(check.get("metric", "")).lower()
        column = check.get("column")
        threshold = check.get("threshold")

        if metric not in SUPPORTED_METRICS:
            logger.warning("Skipping unsupported quality metric: %s", metric)
            continue

        if metric == "completeness" and column is not None and threshold is not None:
            rules.append((f'Completeness "{column}" >= {threshold}', check))

        elif metric == "uniqueness" and column is not None and threshold is not None:
            rules.append((f'Uniqueness "{column}" >= {threshold}', check))

        elif metric == "compliance" and column is not None:
            pattern = check.get("pattern")
            if not pattern:
                logger.warning(
                    "Skipping compliance check on '%s': requires 'pattern'", column
                )
                continue
            rule = f'ColumnValues "{column}" matches "{pattern}"'
            if threshold is not None:
                rule += f" with threshold >= {threshold}"
            rules.append((rule, check))

        elif metric == "size" and threshold is not None:
            rules.append((f"RowCount >= {int(threshold)}", check))

        else:
            logger.warning(
                "Skipping malformed quality check (metric=%s column=%s threshold=%s)",
                metric, column, threshold
            )

    return rules


def build_dqdl_ruleset(checks: List[Dict[str, Any]]) -> Optional[str]:
    """Build a complete DQDL ruleset string, or None if no valid rules."""
    rules = build_dqdl_rules(checks)
    if not rules:
        return None
    return "Rules = [" + ", ".join(rule for rule, _ in rules) + "]"
