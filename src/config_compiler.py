"""
Config Compiler for the Reusable Config-Driven Streaming ETL Framework.

Generates derived artifacts from a validated tables.yaml configuration:
- DMS table mappings JSON (selection + object-mapping rules)
- RDS DDL SQL (CREATE TABLE, foreign keys, indexes)
- CloudFormation parameters JSON

Usage:
    python -m src.config_compiler --config <path> --output <dir>
"""

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class CompileResult:
    """Result of config compilation with paths to generated artifacts."""
    dms_mappings_path: str
    ddl_path: str
    cfn_params_path: str
    topic_list: List[str]


# PostgreSQL type mapping from config schema types
TYPE_MAP = {
    "integer": "INTEGER",
    "int": "INTEGER",
    "string": "VARCHAR(255)",
    "varchar": "VARCHAR(255)",
    "text": "TEXT",
    "double": "DOUBLE PRECISION",
    "float": "DOUBLE PRECISION",
    "real": "REAL",
    "long": "BIGINT",
    "bigint": "BIGINT",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
}

# Regex for decimal(p,s) type
DECIMAL_PATTERN = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.IGNORECASE)


def _map_sql_type(col_type: str) -> str:
    """Map a config schema type string to a PostgreSQL DDL type."""
    lower = col_type.lower().strip()
    if lower in TYPE_MAP:
        return TYPE_MAP[lower]
    match = DECIMAL_PATTERN.match(lower)
    if match:
        return "DECIMAL({},{})".format(match.group(1), match.group(2))
    return "VARCHAR(255)"


def _format_default(value, col_type):
    # type: (Any, str) -> str
    """Format a default value for DDL based on column type."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        if value.upper() == "NOW()":
            return "NOW()"
        return "'{}'".format(value)
    return "'{}'".format(value)


class ConfigCompiler:
    """Compiles a validated tables.yaml config into derived artifacts."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._schema_name = config.get("source_database", {}).get("schema_name", "public")
        self._tables = [
            t for t in config.get("tables", [])
            if not t.get("internal", False)
        ]

    def compile_all(self, output_dir: str) -> CompileResult:
        """
        Generate all artifacts and write them to output_dir.

        Creates the output directory if it doesn't exist.
        Returns a CompileResult with paths to generated files.
        """
        os.makedirs(output_dir, exist_ok=True)

        # Generate artifacts
        dms_mappings = self.compile_dms_table_mappings()
        ddl = self.compile_rds_ddl()
        cfn_params = self.compile_cfn_parameters()
        topic_list = self.compile_kafka_topic_list()

        # Write files
        dms_path = os.path.join(output_dir, "dms_table_mappings.json")
        ddl_path = os.path.join(output_dir, "create_tables.sql")
        cfn_path = os.path.join(output_dir, "cfn_parameters.json")

        with open(dms_path, "w") as f:
            json.dump(dms_mappings, f, indent=2, sort_keys=False)
            f.write("\n")

        with open(ddl_path, "w") as f:
            f.write(ddl)

        with open(cfn_path, "w") as f:
            json.dump(cfn_params, f, indent=2, sort_keys=True)
            f.write("\n")

        return CompileResult(
            dms_mappings_path=dms_path,
            ddl_path=ddl_path,
            cfn_params_path=cfn_path,
            topic_list=topic_list,
        )

    def compile_dms_table_mappings(self) -> Dict:
        """
        Generate DMS table mappings with one selection rule and one
        object-mapping rule per non-internal table.

        Rule IDs are sequential starting from 1.
        """
        rules = []
        rule_id = 1

        for table in self._tables:
            name = table["name"]
            topic = table.get("topic", "cdc-{}".format(name))

            # Selection rule
            rules.append({
                "rule-type": "selection",
                "rule-id": str(rule_id),
                "rule-name": "select-{}".format(name),
                "object-locator": {
                    "schema-name": self._schema_name,
                    "table-name": name,
                },
                "rule-action": "include",
            })
            rule_id += 1

            # Object-mapping rule
            mapping_name = name.title().replace("_", "") + "Mapping"
            rules.append({
                "rule-type": "object-mapping",
                "rule-id": str(rule_id),
                "rule-name": mapping_name,
                "rule-action": "map-record-to-record",
                "kafka-target-topic": topic,
                "object-locator": {
                    "schema-name": self._schema_name,
                    "table-name": name,
                },
            })
            rule_id += 1

        return {"rules": rules}

    def compile_rds_ddl(self) -> str:
        """
        Generate PostgreSQL DDL with CREATE TABLE IF NOT EXISTS statements,
        foreign keys, and indexes for all non-internal tables.
        """
        ddl_statements = []
        index_statements = []

        for table in self._tables:
            name = table["name"]
            pk = table.get("primary_key")
            columns = []

            for col_def in table.get("schema", []):
                col_name = col_def["name"]
                col_type = col_def.get("type", "string").lower().strip()
                nullable = col_def.get("nullable", True)
                default = col_def.get("default")
                unique = col_def.get("unique", False)

                # Integer primary key columns become SERIAL PRIMARY KEY
                if col_name == pk and col_type in ("integer", "int"):
                    sql_type = "SERIAL PRIMARY KEY"
                else:
                    sql_type = _map_sql_type(col_type)
                    if not nullable:
                        sql_type += " NOT NULL"
                    if unique:
                        sql_type += " UNIQUE"
                    if default is not None:
                        sql_type += " DEFAULT {}".format(
                            _format_default(default, col_type)
                        )

                columns.append("    {} {}".format(col_name, sql_type))

            # Foreign key constraints
            for fk in table.get("foreign_keys", []):
                ref = fk["references"]
                columns.append(
                    "    FOREIGN KEY ({}) REFERENCES {}({})".format(
                        fk["column"], ref["table"], ref["column"]
                    )
                )

            ddl = "CREATE TABLE IF NOT EXISTS {} (\n".format(name)
            ddl += ",\n".join(columns)
            ddl += "\n);\n"
            ddl_statements.append(ddl)

            # Indexes
            for idx in table.get("indexes", []):
                idx_cols = ", ".join(idx["columns"])
                idx_name = "idx_{}_{}".format(name, "_".join(idx["columns"]))
                unique_kw = "UNIQUE " if idx.get("unique") else ""
                index_statements.append(
                    "CREATE {}INDEX IF NOT EXISTS {} ON {}({});".format(
                        unique_kw, idx_name, name, idx_cols
                    )
                )

        full_ddl = "\n".join(ddl_statements)
        if index_statements:
            full_ddl += "\n" + "\n".join(index_statements) + "\n"

        return full_ddl

    def compile_cfn_parameters(self) -> Dict[str, str]:
        """Generate CloudFormation parameter overrides."""
        return {
            "EnvironmentName": self.config.get("_stack_name", "dq-etl"),
        }

    def compile_kafka_topic_list(self) -> List[str]:
        """Extract the list of Kafka topics from config (with auto-defaults)."""
        return [
            t.get("topic", "cdc-{}".format(t["name"]))
            for t in self._tables
        ]


def main():
    """CLI entry point for config compilation."""
    parser = argparse.ArgumentParser(
        description="Compile a tables.yaml configuration into derived artifacts"
    )
    parser.add_argument(
        "--config", required=True,
        help="Path to the tables.yaml configuration file",
    )
    parser.add_argument(
        "--output", required=True,
        help="Output directory for generated artifacts",
    )
    args = parser.parse_args()

    # Check config file exists
    if not os.path.exists(args.config):
        print("Error: Config file not found: {}".format(args.config))
        sys.exit(1)

    import yaml
    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    # Validate config first
    from src.config_validator import ConfigValidator

    validator = ConfigValidator()
    result = validator.validate(config)

    if result.warnings:
        print("Warnings ({}):".format(len(result.warnings)))
        for w in result.warnings:
            print("  [{}] {}: {}".format(w.table, w.field, w.message))
        print()

    if not result.valid:
        print("Validation failed ({} errors):".format(len(result.errors)))
        for e in result.errors:
            print("  [{}] {}: {}".format(e.table, e.field, e.message))
        sys.exit(1)

    print("Validating config... OK ({} errors, {} warnings)".format(
        len(result.errors), len(result.warnings)
    ))

    # Compile all artifacts in one pass
    compiler = ConfigCompiler(config)
    compile_result = compiler.compile_all(args.output)

    # Print summary
    non_internal = [t for t in config.get("tables", []) if not t.get("internal", False)]
    fk_count = sum(len(t.get("foreign_keys", [])) for t in non_internal)
    idx_count = sum(len(t.get("indexes", [])) for t in non_internal)

    print("Compiling DMS table mappings... {} tables \u2192 {} rules".format(
        len(non_internal), len(non_internal) * 2
    ))
    print("Compiling RDS DDL... {} tables, {} foreign key(s), {} index(es)".format(
        len(non_internal), fk_count, idx_count
    ))
    print("Compiling CFn parameters... done")

    print("\nOutput:")
    print("  {}".format(compile_result.dms_mappings_path))
    print("  {}".format(compile_result.ddl_path))
    print("  {}".format(compile_result.cfn_params_path))


if __name__ == "__main__":
    main()
