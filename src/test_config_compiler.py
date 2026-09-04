"""
Unit tests for ConfigCompiler.

Tests cover:
- Task 4.1: Class structure and CompileResult dataclass
- Task 4.2: DMS table mappings generation
- Task 4.3: RDS DDL generation
- Task 4.4: CFn parameters generation and CLI entry point
"""

import json
import os
import subprocess
import sys
import tempfile

import pytest
import yaml

from src.config_compiler import CompileResult, ConfigCompiler, _format_default, _map_sql_type


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _minimal_config(tables=None, source_database=None, settings=None):
    """Build a minimal valid config dict for testing."""
    cfg = {
        "settings": settings or {
            "checkpoint_location": "s3://bucket/checkpoints/",
            "quarantine_path": "s3://bucket/quarantine/",
            "delta_bucket": "my-delta-bucket",
        },
        "tables": tables if tables is not None else [
            {
                "name": "orders",
                "primary_key": "id",
                "schema": [
                    {"name": "id", "type": "integer", "nullable": False},
                    {"name": "customer", "type": "string", "nullable": False},
                    {"name": "total", "type": "double", "nullable": True},
                ],
            }
        ],
    }
    if source_database is not None:
        cfg["source_database"] = source_database
    return cfg


# ===========================================================================
# Task 4.1 — Class structure
# ===========================================================================

class TestClassStructure:
    """Verify CompileResult dataclass and ConfigCompiler interface."""

    def test_compile_result_fields(self):
        cr = CompileResult(
            dms_mappings_path="/a/dms.json",
            ddl_path="/a/ddl.sql",
            cfn_params_path="/a/cfn.json",
            topic_list=["t1", "t2"],
        )
        assert cr.dms_mappings_path == "/a/dms.json"
        assert cr.ddl_path == "/a/ddl.sql"
        assert cr.cfn_params_path == "/a/cfn.json"
        assert cr.topic_list == ["t1", "t2"]

    def test_compiler_init_stores_config(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        assert compiler.config is cfg

    def test_compiler_filters_internal_tables(self):
        cfg = _minimal_config(tables=[
            {"name": "users", "primary_key": "id", "schema": [{"name": "id", "type": "integer"}]},
            {"name": "dq_metrics", "internal": True, "primary_key": None,
             "schema": [{"name": "table_name", "type": "string"}]},
        ])
        compiler = ConfigCompiler(cfg)
        assert len(compiler._tables) == 1
        assert compiler._tables[0]["name"] == "users"

    def test_compiler_default_schema_name(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        assert compiler._schema_name == "public"

    def test_compiler_custom_schema_name(self):
        cfg = _minimal_config(source_database={"schema_name": "myschema"})
        compiler = ConfigCompiler(cfg)
        assert compiler._schema_name == "myschema"

    def test_compile_all_creates_output_dir_and_files(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, "nested", "output")
            result = compiler.compile_all(out)
            assert os.path.isfile(result.dms_mappings_path)
            assert os.path.isfile(result.ddl_path)
            assert os.path.isfile(result.cfn_params_path)
            assert isinstance(result.topic_list, list)


# ===========================================================================
# Task 4.2 — DMS table mappings generation
# ===========================================================================

class TestDmsTableMappings:
    """Verify DMS selection + object-mapping rule generation."""

    def test_single_table_produces_two_rules(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        assert len(mappings["rules"]) == 2

    def test_rule_ids_sequential_from_one(self):
        cfg = _minimal_config(tables=[
            {"name": "a", "primary_key": "id", "schema": [{"name": "id", "type": "integer"}]},
            {"name": "b", "primary_key": "id", "schema": [{"name": "id", "type": "integer"}]},
        ])
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        ids = [r["rule-id"] for r in mappings["rules"]]
        assert ids == ["1", "2", "3", "4"]

    def test_selection_rule_format(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        sel = mappings["rules"][0]
        assert sel["rule-type"] == "selection"
        assert sel["rule-id"] == "1"
        assert sel["rule-name"] == "select-orders"
        assert sel["object-locator"]["schema-name"] == "public"
        assert sel["object-locator"]["table-name"] == "orders"
        assert sel["rule-action"] == "include"

    def test_object_mapping_rule_format(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        obj = mappings["rules"][1]
        assert obj["rule-type"] == "object-mapping"
        assert obj["rule-id"] == "2"
        assert obj["rule-name"] == "OrdersMapping"
        assert obj["rule-action"] == "map-record-to-record"
        assert obj["kafka-target-topic"] == "cdc-orders"
        assert obj["object-locator"]["schema-name"] == "public"
        assert obj["object-locator"]["table-name"] == "orders"

    def test_auto_default_topic(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        obj = mappings["rules"][1]
        assert obj["kafka-target-topic"] == "cdc-orders"

    def test_explicit_topic_used(self):
        cfg = _minimal_config(tables=[
            {"name": "orders", "primary_key": "id", "topic": "my-custom-topic",
             "schema": [{"name": "id", "type": "integer"}]},
        ])
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        obj = mappings["rules"][1]
        assert obj["kafka-target-topic"] == "my-custom-topic"

    def test_custom_schema_name_in_rules(self):
        cfg = _minimal_config(source_database={"schema_name": "sales"})
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        for rule in mappings["rules"]:
            assert rule["object-locator"]["schema-name"] == "sales"

    def test_internal_tables_excluded(self):
        cfg = _minimal_config(tables=[
            {"name": "users", "primary_key": "id", "schema": [{"name": "id", "type": "integer"}]},
            {"name": "dq_metrics", "internal": True, "primary_key": None,
             "schema": [{"name": "table_name", "type": "string"}]},
        ])
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        table_names = [r["object-locator"]["table-name"] for r in mappings["rules"]]
        assert "dq_metrics" not in table_names
        assert len(mappings["rules"]) == 2

    def test_n_tables_produce_2n_rules(self):
        tables = [
            {"name": "t{}".format(i), "primary_key": "id",
             "schema": [{"name": "id", "type": "integer"}]}
            for i in range(5)
        ]
        cfg = _minimal_config(tables=tables)
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        assert len(mappings["rules"]) == 10

    def test_mapping_name_for_underscored_table(self):
        cfg = _minimal_config(tables=[
            {"name": "vehicle_telemetry", "primary_key": "id",
             "schema": [{"name": "id", "type": "integer"}]},
        ])
        compiler = ConfigCompiler(cfg)
        mappings = compiler.compile_dms_table_mappings()
        obj = mappings["rules"][1]
        assert obj["rule-name"] == "VehicleTelemetryMapping"


# ===========================================================================
# Task 4.3 — RDS DDL generation
# ===========================================================================

class TestRdsDdl:
    """Verify DDL generation: types, constraints, foreign keys, indexes."""

    def test_create_table_statement(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "CREATE TABLE IF NOT EXISTS orders" in ddl

    def test_serial_primary_key_for_integer_pk(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "id SERIAL PRIMARY KEY" in ddl

    def test_type_mapping_string(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "customer VARCHAR(255)" in ddl

    def test_type_mapping_double(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "total DOUBLE PRECISION" in ddl

    def test_not_null_constraint(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "customer VARCHAR(255) NOT NULL" in ddl

    def test_nullable_column_no_not_null(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        # total is nullable=True, should NOT have NOT NULL
        lines = [l.strip() for l in ddl.split("\n")]
        total_line = [l for l in lines if l.startswith("total")]
        assert len(total_line) == 1
        assert "NOT NULL" not in total_line[0]

    def test_unique_constraint(self):
        cfg = _minimal_config(tables=[
            {"name": "users", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "email", "type": "string", "nullable": False, "unique": True},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "email VARCHAR(255) NOT NULL UNIQUE" in ddl

    def test_default_value_string(self):
        cfg = _minimal_config(tables=[
            {"name": "items", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "status", "type": "string", "nullable": True, "default": "active"},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "DEFAULT 'active'" in ddl

    def test_default_value_numeric(self):
        cfg = _minimal_config(tables=[
            {"name": "items", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "score", "type": "double", "nullable": True, "default": 100.0},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "DEFAULT 100.0" in ddl

    def test_default_value_boolean(self):
        cfg = _minimal_config(tables=[
            {"name": "items", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "active", "type": "boolean", "nullable": True, "default": False},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "DEFAULT FALSE" in ddl

    def test_foreign_key_clause(self):
        cfg = _minimal_config(tables=[
            {"name": "orders", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "user_id", "type": "integer", "nullable": False},
            ], "foreign_keys": [
                {"column": "user_id", "references": {"table": "users", "column": "id"}},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "FOREIGN KEY (user_id) REFERENCES users(id)" in ddl

    def test_index_statement(self):
        cfg = _minimal_config(tables=[
            {"name": "events", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "created_at", "type": "timestamp", "nullable": True},
            ], "indexes": [
                {"columns": ["created_at"]},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);" in ddl

    def test_unique_index(self):
        cfg = _minimal_config(tables=[
            {"name": "events", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "code", "type": "string", "nullable": False},
            ], "indexes": [
                {"columns": ["code"], "unique": True},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_events_code ON events(code);" in ddl

    def test_multi_column_index(self):
        cfg = _minimal_config(tables=[
            {"name": "events", "primary_key": "id", "schema": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "a", "type": "string", "nullable": True},
                {"name": "b", "type": "string", "nullable": True},
            ], "indexes": [
                {"columns": ["a", "b"]},
            ]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "idx_events_a_b ON events(a, b)" in ddl

    def test_all_type_mappings(self):
        """Verify every type in the spec maps correctly."""
        assert _map_sql_type("integer") == "INTEGER"
        assert _map_sql_type("int") == "INTEGER"
        assert _map_sql_type("string") == "VARCHAR(255)"
        assert _map_sql_type("varchar") == "VARCHAR(255)"
        assert _map_sql_type("text") == "TEXT"
        assert _map_sql_type("double") == "DOUBLE PRECISION"
        assert _map_sql_type("float") == "DOUBLE PRECISION"
        assert _map_sql_type("real") == "REAL"
        assert _map_sql_type("long") == "BIGINT"
        assert _map_sql_type("bigint") == "BIGINT"
        assert _map_sql_type("boolean") == "BOOLEAN"
        assert _map_sql_type("timestamp") == "TIMESTAMP"
        assert _map_sql_type("date") == "DATE"

    def test_decimal_type_mapping(self):
        assert _map_sql_type("decimal(5,2)") == "DECIMAL(5,2)"
        assert _map_sql_type("decimal(10, 4)") == "DECIMAL(10,4)"

    def test_unknown_type_defaults_to_varchar(self):
        assert _map_sql_type("unknown_type") == "VARCHAR(255)"

    def test_n_tables_produce_n_create_statements(self):
        tables = [
            {"name": "t{}".format(i), "primary_key": "id",
             "schema": [{"name": "id", "type": "integer"}]}
            for i in range(4)
        ]
        cfg = _minimal_config(tables=tables)
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert ddl.count("CREATE TABLE IF NOT EXISTS") == 4

    def test_internal_tables_excluded_from_ddl(self):
        cfg = _minimal_config(tables=[
            {"name": "users", "primary_key": "id",
             "schema": [{"name": "id", "type": "integer"}]},
            {"name": "dq_metrics", "internal": True, "primary_key": None,
             "schema": [{"name": "table_name", "type": "string"}]},
        ])
        compiler = ConfigCompiler(cfg)
        ddl = compiler.compile_rds_ddl()
        assert "dq_metrics" not in ddl
        assert ddl.count("CREATE TABLE IF NOT EXISTS") == 1


# ===========================================================================
# Task 4.4 — CFn parameters and CLI
# ===========================================================================

class TestCfnParameters:
    """Verify CloudFormation parameter generation."""

    def test_environment_name_default(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        params = compiler.compile_cfn_parameters()
        assert params["EnvironmentName"] == "dq-etl"

    def test_environment_name_from_stack_name(self):
        cfg = _minimal_config()
        cfg["_stack_name"] = "fleet-demo"
        compiler = ConfigCompiler(cfg)
        params = compiler.compile_cfn_parameters()
        assert params["EnvironmentName"] == "fleet-demo"


class TestKafkaTopicList:
    """Verify topic list extraction."""

    def test_auto_default_topics(self):
        cfg = _minimal_config(tables=[
            {"name": "a", "primary_key": "id", "schema": [{"name": "id", "type": "integer"}]},
            {"name": "b", "primary_key": "id", "schema": [{"name": "id", "type": "integer"}]},
        ])
        compiler = ConfigCompiler(cfg)
        topics = compiler.compile_kafka_topic_list()
        assert topics == ["cdc-a", "cdc-b"]

    def test_explicit_topics(self):
        cfg = _minimal_config(tables=[
            {"name": "a", "primary_key": "id", "topic": "custom-a",
             "schema": [{"name": "id", "type": "integer"}]},
        ])
        compiler = ConfigCompiler(cfg)
        topics = compiler.compile_kafka_topic_list()
        assert topics == ["custom-a"]

    def test_internal_tables_excluded_from_topics(self):
        cfg = _minimal_config(tables=[
            {"name": "users", "primary_key": "id",
             "schema": [{"name": "id", "type": "integer"}]},
            {"name": "dq_metrics", "internal": True, "primary_key": None,
             "schema": [{"name": "table_name", "type": "string"}]},
        ])
        compiler = ConfigCompiler(cfg)
        topics = compiler.compile_kafka_topic_list()
        assert topics == ["cdc-users"]


class TestCompileAll:
    """Verify compile_all writes correct files and returns CompileResult."""

    def test_writes_three_files(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = compiler.compile_all(tmpdir)
            assert os.path.basename(result.dms_mappings_path) == "dms_table_mappings.json"
            assert os.path.basename(result.ddl_path) == "create_tables.sql"
            assert os.path.basename(result.cfn_params_path) == "cfn_parameters.json"

    def test_dms_json_is_valid(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = compiler.compile_all(tmpdir)
            with open(result.dms_mappings_path) as f:
                data = json.load(f)
            assert "rules" in data

    def test_cfn_json_is_valid(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = compiler.compile_all(tmpdir)
            with open(result.cfn_params_path) as f:
                data = json.load(f)
            assert "EnvironmentName" in data

    def test_ddl_file_content(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = compiler.compile_all(tmpdir)
            with open(result.ddl_path) as f:
                ddl = f.read()
            assert "CREATE TABLE IF NOT EXISTS orders" in ddl

    def test_topic_list_in_result(self):
        cfg = _minimal_config()
        compiler = ConfigCompiler(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            result = compiler.compile_all(tmpdir)
            assert result.topic_list == ["cdc-orders"]

    def test_deterministic_output(self):
        """Same config compiled twice produces identical files."""
        cfg = _minimal_config(tables=[
            {"name": "a", "primary_key": "id",
             "schema": [{"name": "id", "type": "integer"}, {"name": "x", "type": "string"}]},
            {"name": "b", "primary_key": "id", "topic": "my-b",
             "schema": [{"name": "id", "type": "integer"}, {"name": "y", "type": "double"}],
             "indexes": [{"columns": ["y"]}]},
        ])
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            ConfigCompiler(cfg).compile_all(d1)
            ConfigCompiler(cfg).compile_all(d2)
            for fname in ["dms_table_mappings.json", "create_tables.sql", "cfn_parameters.json"]:
                with open(os.path.join(d1, fname)) as f1, open(os.path.join(d2, fname)) as f2:
                    assert f1.read() == f2.read(), "Non-deterministic output for {}".format(fname)


class TestCli:
    """Verify CLI entry point behavior."""

    def _write_config(self, tmpdir, config):
        path = os.path.join(tmpdir, "tables.yaml")
        with open(path, "w") as f:
            yaml.dump(config, f)
        return path

    def test_cli_success(self):
        cfg = _minimal_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_config(tmpdir, cfg)
            out_dir = os.path.join(tmpdir, "build")
            result = subprocess.run(
                [sys.executable, "-m", "src.config_compiler",
                 "--config", config_path, "--output", out_dir],
                capture_output=True, text=True,
            )
            assert result.returncode == 0, result.stderr
            assert os.path.isfile(os.path.join(out_dir, "dms_table_mappings.json"))
            assert os.path.isfile(os.path.join(out_dir, "create_tables.sql"))
            assert os.path.isfile(os.path.join(out_dir, "cfn_parameters.json"))

    def test_cli_missing_config_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                [sys.executable, "-m", "src.config_compiler",
                 "--config", "/nonexistent/tables.yaml", "--output", tmpdir],
                capture_output=True, text=True,
            )
            assert result.returncode != 0
            assert "not found" in result.stdout.lower() or "not found" in result.stderr.lower()

    def test_cli_invalid_config(self):
        """Config missing required fields should fail validation."""
        bad_cfg = {"tables": [{"name": "x"}]}  # missing settings, schema, pk
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_config(tmpdir, bad_cfg)
            out_dir = os.path.join(tmpdir, "build")
            result = subprocess.run(
                [sys.executable, "-m", "src.config_compiler",
                 "--config", config_path, "--output", out_dir],
                capture_output=True, text=True,
            )
            assert result.returncode != 0
            assert "error" in result.stdout.lower() or "fail" in result.stdout.lower()

    def test_cli_prints_summary(self):
        cfg = _minimal_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = self._write_config(tmpdir, cfg)
            out_dir = os.path.join(tmpdir, "build")
            result = subprocess.run(
                [sys.executable, "-m", "src.config_compiler",
                 "--config", config_path, "--output", out_dir],
                capture_output=True, text=True,
            )
            assert result.returncode == 0
            assert "Validating config" in result.stdout
            assert "DMS table mappings" in result.stdout
            assert "RDS DDL" in result.stdout
            assert "CFn parameters" in result.stdout


class TestFormatDefault:
    """Verify _format_default helper."""

    def test_bool_true(self):
        assert _format_default(True, "boolean") == "TRUE"

    def test_bool_false(self):
        assert _format_default(False, "boolean") == "FALSE"

    def test_integer(self):
        assert _format_default(42, "integer") == "42"

    def test_float(self):
        assert _format_default(3.14, "double") == "3.14"

    def test_string(self):
        assert _format_default("active", "string") == "'active'"

    def test_now_function(self):
        assert _format_default("NOW()", "timestamp") == "NOW()"
