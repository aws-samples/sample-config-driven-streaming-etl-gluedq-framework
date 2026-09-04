"""
Config Validator for the Reusable Config-Driven Streaming ETL Framework.

Validates tables.yaml configuration files before deployment or compilation,
catching schema errors, invalid references, and semantic issues.

Usage:
    python -m src.config_validator --config <path>
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Set


@dataclass
class ValidationError:
    """Represents a validation error found in the config."""
    table: str       # table name or "_global" for top-level errors
    field: str       # field path (e.g., "settings.checkpoint_location")
    message: str     # human-readable error message
    severity: str    # "error" or "warning"


@dataclass
class ValidationWarning:
    """Represents a validation warning found in the config."""
    table: str
    field: str
    message: str


@dataclass
class ValidationResult:
    """Result of config validation."""
    valid: bool                          # True if zero errors
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationWarning] = field(default_factory=list)


# Built-in sets for validation
BUILTIN_DQ_RULES = {"range", "allowed_values", "regex", "not_null", "unique", "length"}
BUILTIN_TRANSFORMS = {"trim", "lower", "upper", "round", "mask_pii", "cast", "default_value", "rename"}
# Quality metrics supported by the YAML -> DQDL compiler (native Glue DQ):
#   completeness -> Completeness, uniqueness -> Uniqueness,
#   compliance -> ColumnValues matches, size -> RowCount
VALID_QUALITY_METRICS = {"completeness", "uniqueness", "compliance", "size"}
# Metrics that operate on a specific column (size/RowCount is table-level).
COLUMN_REQUIRED_QUALITY_METRICS = {"completeness", "uniqueness", "compliance"}
VALID_SCHEMA_TYPES = {
    "string", "varchar", "text",
    "integer", "int", "long", "bigint",
    "double", "float", "real",
    "timestamp", "date", "boolean",
}
# Regex for decimal(p,s) type
DECIMAL_PATTERN = re.compile(r"^decimal\(\d+,\s*\d+\)$", re.IGNORECASE)


def _is_valid_schema_type(type_str: str) -> bool:
    """Check if a schema type string is valid."""
    lower = type_str.lower().strip()
    if lower in VALID_SCHEMA_TYPES:
        return True
    if DECIMAL_PATTERN.match(lower):
        return True
    return False


class ConfigValidator:
    """Validates tables.yaml configuration files."""

    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Main entry point. Validates the entire config and collects ALL errors
        without short-circuiting on the first error.

        Returns a ValidationResult with valid=True only if zero errors found.
        """
        errors: List[ValidationError] = []
        warnings: List[ValidationWarning] = []

        # Validate top-level structure
        if "settings" not in config:
            errors.append(ValidationError(
                "_global", "settings", "Missing required 'settings' section", "error"
            ))
        if "tables" not in config:
            errors.append(ValidationError(
                "_global", "tables", "Missing required 'tables' section", "error"
            ))
            return ValidationResult(valid=False, errors=errors, warnings=warnings)

        # Validate settings
        settings = config.get("settings", {})
        if settings is not None:
            for required_key in ["checkpoint_location", "quarantine_path", "delta_bucket"]:
                if required_key not in settings:
                    errors.append(ValidationError(
                        "_global", f"settings.{required_key}",
                        f"Missing required setting: {required_key}", "error"
                    ))

        # Collect all names/topics for uniqueness checks
        table_names: List[str] = []
        topic_names: List[str] = []

        tables = config.get("tables", [])
        if not isinstance(tables, list):
            errors.append(ValidationError(
                "_global", "tables", "'tables' must be a list", "error"
            ))
            return ValidationResult(valid=False, errors=errors, warnings=warnings)

        for table in tables:
            name = table.get("name", "<unnamed>")
            is_internal = table.get("internal", False)

            # Check required fields for non-internal tables
            if not table.get("name"):
                errors.append(ValidationError(
                    name, "name", "Table name is required", "error"
                ))

            # Uniqueness: table names
            if name in table_names:
                errors.append(ValidationError(
                    name, "name", f"Duplicate table name: {name}", "error"
                ))
            table_names.append(name)

            # Uniqueness: topic names
            topic = table.get("topic", f"cdc-{name}")
            if topic in topic_names:
                errors.append(ValidationError(
                    name, "topic", f"Duplicate topic: {topic}", "error"
                ))
            topic_names.append(topic)

            if not table.get("schema"):
                errors.append(ValidationError(
                    name, "schema", "Schema is required", "error"
                ))
                continue

            if not is_internal and not table.get("primary_key"):
                errors.append(ValidationError(
                    name, "primary_key",
                    "Primary key is required for non-internal tables", "error"
                ))

            # Validate schema
            schema_errors = self.validate_schema(table.get("schema", []), name)
            errors.extend(schema_errors)

            # Build schema column set for reference validation
            schema_columns = {col["name"] for col in table.get("schema", []) if "name" in col}

            # Validate primary key exists in schema
            pk = table.get("primary_key")
            if pk and pk not in schema_columns:
                errors.append(ValidationError(
                    name, "primary_key",
                    f"Primary key '{pk}' not found in schema", "error"
                ))

            # Validate DQ rules
            dq_rules = table.get("dq_rules")
            if dq_rules is not None:
                dq_errors = self.validate_dq_rules(dq_rules, schema_columns, name)
                errors.extend(dq_errors)
            else:
                warnings.append(ValidationWarning(
                    name, "dq_rules", "No DQ rules defined for this table"
                ))

            # Validate transforms
            transforms = table.get("transforms")
            if transforms is not None:
                tx_errors = self.validate_transforms(transforms, schema_columns, name)
                errors.extend(tx_errors)
            else:
                warnings.append(ValidationWarning(
                    name, "transforms", "No transforms defined for this table"
                ))

            # Validate quality checks (compiled to native Glue DQ DQDL rules)
            quality_checks = table.get("quality_checks")
            if quality_checks is not None:
                quality_errors = self.validate_quality_checks(quality_checks, schema_columns, name)
                errors.extend(quality_errors)
            else:
                warnings.append(ValidationWarning(
                    name, "quality_checks", "No quality checks defined for this table"
                ))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def validate_table(self, table_config: Dict) -> List[ValidationError]:
        """
        Per-table validation. Validates a single table config in isolation.
        Returns a list of ValidationErrors found.
        """
        errors: List[ValidationError] = []
        name = table_config.get("name", "<unnamed>")
        is_internal = table_config.get("internal", False)

        if not table_config.get("name"):
            errors.append(ValidationError(name, "name", "Table name is required", "error"))

        if not table_config.get("schema"):
            errors.append(ValidationError(name, "schema", "Schema is required", "error"))
            return errors

        if not is_internal and not table_config.get("primary_key"):
            errors.append(ValidationError(
                name, "primary_key",
                "Primary key is required for non-internal tables", "error"
            ))

        schema_errors = self.validate_schema(table_config.get("schema", []), name)
        errors.extend(schema_errors)

        schema_columns = {col["name"] for col in table_config.get("schema", []) if "name" in col}

        pk = table_config.get("primary_key")
        if pk and pk not in schema_columns:
            errors.append(ValidationError(
                name, "primary_key",
                f"Primary key '{pk}' not found in schema", "error"
            ))

        if table_config.get("dq_rules") is not None:
            errors.extend(self.validate_dq_rules(
                table_config["dq_rules"], schema_columns, name
            ))

        if table_config.get("transforms") is not None:
            errors.extend(self.validate_transforms(
                table_config["transforms"], schema_columns, name
            ))

        if table_config.get("quality_checks") is not None:
            errors.extend(self.validate_quality_checks(
                table_config["quality_checks"], schema_columns, name
            ))

        return errors

    def validate_dq_rules(
        self, rules: List[Dict], schema_columns: Set[str], table_name: str = "<unknown>"
    ) -> List[ValidationError]:
        """Validate DQ rules against built-in set and schema columns."""
        errors: List[ValidationError] = []
        rule_ids: List[str] = []

        for rule in rules:
            rid = rule.get("id", "<no-id>")

            # Duplicate rule ID check
            if rid in rule_ids:
                errors.append(ValidationError(
                    table_name, f"dq_rules.{rid}",
                    f"Duplicate rule ID: {rid}", "error"
                ))
            rule_ids.append(rid)

            # Validate rule type
            rule_type = rule.get("type")
            if rule_type not in BUILTIN_DQ_RULES:
                errors.append(ValidationError(
                    table_name, f"dq_rules.{rid}",
                    f"Unknown DQ rule type: {rule_type}", "error"
                ))

            # Validate column exists in schema
            col = rule.get("column")
            if col and col not in schema_columns:
                errors.append(ValidationError(
                    table_name, f"dq_rules.{rid}",
                    f"Column '{col}' not in schema", "error"
                ))

        return errors

    def validate_transforms(
        self, transforms: List[Dict], schema_columns: Set[str], table_name: str = "<unknown>"
    ) -> List[ValidationError]:
        """Validate transforms against built-in set and schema columns."""
        errors: List[ValidationError] = []
        tx_ids: List[str] = []

        for tx in transforms:
            tid = tx.get("id", "<no-id>")

            # Duplicate transform ID check
            if tid in tx_ids:
                errors.append(ValidationError(
                    table_name, f"transforms.{tid}",
                    f"Duplicate transform ID: {tid}", "error"
                ))
            tx_ids.append(tid)

            # Validate transform type
            tx_type = tx.get("type")
            if tx_type not in BUILTIN_TRANSFORMS:
                errors.append(ValidationError(
                    table_name, f"transforms.{tid}",
                    f"Unknown transform type: {tx_type}", "error"
                ))

            # Validate column exists in schema
            col = tx.get("column")
            if col and col not in schema_columns:
                errors.append(ValidationError(
                    table_name, f"transforms.{tid}",
                    f"Column '{col}' not in schema", "error"
                ))

        return errors

    def validate_quality_checks(
        self, checks: List[Dict], schema_columns: Set[str] = None,
        table_name: str = "<unknown>"
    ) -> List[ValidationError]:
        """
        Validate quality checks that compile to native Glue DQ DQDL rules.
        Verifies the metric name, that column-based metrics reference a real
        schema column, that compliance checks carry a 'pattern', and that
        thresholds are numeric where required — so misconfigured checks are
        caught before deployment rather than at runtime.
        """
        errors: List[ValidationError] = []
        schema_columns = schema_columns or set()

        for check in checks:
            metric = check.get("metric")

            # Metric name must be one the DQDL compiler supports.
            if metric not in VALID_QUALITY_METRICS:
                errors.append(ValidationError(
                    table_name, "quality_checks",
                    f"Unknown quality metric: {metric}", "error"
                ))
                continue

            # Column-based metrics require a column that exists in the schema;
            # 'size' (DQDL RowCount) is table-level and takes no column.
            col = check.get("column")
            if metric in COLUMN_REQUIRED_QUALITY_METRICS:
                if not col:
                    errors.append(ValidationError(
                        table_name, "quality_checks",
                        f"Quality metric '{metric}' requires a 'column'", "error"
                    ))
                elif schema_columns and col not in schema_columns:
                    errors.append(ValidationError(
                        table_name, "quality_checks",
                        f"Column '{col}' not in schema", "error"
                    ))

            # compliance compiles to `ColumnValues "col" matches "pattern"`,
            # so 'pattern' is mandatory and 'threshold' optional.
            if metric == "compliance" and not check.get("pattern"):
                errors.append(ValidationError(
                    table_name, "quality_checks",
                    "Quality metric 'compliance' requires a 'pattern' (regex)",
                    "error"
                ))

            # completeness/uniqueness/size require a numeric threshold.
            threshold_required = metric != "compliance"
            if "threshold" not in check:
                if threshold_required:
                    errors.append(ValidationError(
                        table_name, "quality_checks",
                        f"Quality check for metric '{metric}' is missing 'threshold'",
                        "error"
                    ))
            else:
                threshold = check.get("threshold")
                if isinstance(threshold, bool) or not isinstance(threshold, (int, float)):
                    errors.append(ValidationError(
                        table_name, "quality_checks",
                        f"Quality 'threshold' for metric '{metric}' must be numeric",
                        "error"
                    ))

        return errors

    def validate_schema(
        self, schema: List[Dict], table_name: str = "<unknown>"
    ) -> List[ValidationError]:
        """Validate schema column definitions for valid types."""
        errors: List[ValidationError] = []

        for col_def in schema:
            col_name = col_def.get("name", "<unnamed>")
            col_type = col_def.get("type", "")

            if not col_type:
                errors.append(ValidationError(
                    table_name, f"schema.{col_name}",
                    f"Column '{col_name}' is missing a type", "error"
                ))
                continue

            if not _is_valid_schema_type(col_type):
                errors.append(ValidationError(
                    table_name, f"schema.{col_name}",
                    f"Invalid schema type '{col_type}' for column '{col_name}'", "error"
                ))

        return errors


def main():
    """CLI entry point for config validation."""
    parser = argparse.ArgumentParser(
        description="Validate a tables.yaml configuration file"
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to the tables.yaml configuration file"
    )
    args = parser.parse_args()

    import os
    if not os.path.exists(args.config):
        print(f"Error: Config file not found: {args.config}")
        sys.exit(1)

    import yaml
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    validator = ConfigValidator()
    result = validator.validate(config)

    if result.warnings:
        print(f"Warnings ({len(result.warnings)}):")
        for w in result.warnings:
            print(f"  [{w.table}] {w.field}: {w.message}")
        print()

    if result.valid:
        print(f"Validation passed (0 errors, {len(result.warnings)} warnings)")
        sys.exit(0)
    else:
        print(f"Validation failed ({len(result.errors)} errors, {len(result.warnings)} warnings):")
        for e in result.errors:
            print(f"  [{e.table}] {e.field}: {e.message}")
        sys.exit(1)


if __name__ == "__main__":
    main()
