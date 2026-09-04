"""Unit tests for the YAML -> DQDL compiler (native Glue Data Quality)."""

import pytest

from src.dqdl_compiler import SUPPORTED_METRICS, build_dqdl_rules, build_dqdl_ruleset


class TestBuildDqdlRules:
    def test_completeness(self):
        rules = build_dqdl_rules(
            [{"metric": "completeness", "column": "vehicle_id", "threshold": 0.95}]
        )
        assert len(rules) == 1
        assert rules[0][0] == 'Completeness "vehicle_id" >= 0.95'

    def test_uniqueness(self):
        rules = build_dqdl_rules(
            [{"metric": "uniqueness", "column": "id", "threshold": 1.0}]
        )
        assert rules[0][0] == 'Uniqueness "id" >= 1.0'

    def test_size_maps_to_rowcount(self):
        rules = build_dqdl_rules([{"metric": "size", "threshold": 1}])
        assert rules[0][0] == "RowCount >= 1"

    def test_size_threshold_coerced_to_int(self):
        rules = build_dqdl_rules([{"metric": "size", "threshold": 5.0}])
        assert rules[0][0] == "RowCount >= 5"

    def test_compliance_with_pattern(self):
        rules = build_dqdl_rules(
            [{"metric": "compliance", "column": "employee_id",
              "pattern": "^DRV\\d{5}$"}]
        )
        assert rules[0][0] == 'ColumnValues "employee_id" matches "^DRV\\d{5}$"'

    def test_compliance_with_pattern_and_threshold(self):
        rules = build_dqdl_rules(
            [{"metric": "compliance", "column": "employee_id",
              "pattern": "^DRV", "threshold": 0.9}]
        )
        assert rules[0][0] == 'ColumnValues "employee_id" matches "^DRV" with threshold >= 0.9'

    def test_compliance_without_pattern_skipped(self):
        rules = build_dqdl_rules(
            [{"metric": "compliance", "column": "employee_id"}]
        )
        assert rules == []

    def test_unknown_metric_skipped(self):
        rules = build_dqdl_rules([{"metric": "entropy", "column": "x", "threshold": 1}])
        assert rules == []

    def test_missing_threshold_skipped(self):
        rules = build_dqdl_rules([{"metric": "completeness", "column": "x"}])
        assert rules == []

    def test_missing_column_skipped(self):
        rules = build_dqdl_rules([{"metric": "completeness", "threshold": 0.9}])
        assert rules == []

    def test_source_check_returned_for_severity_mapping(self):
        check = {"metric": "uniqueness", "column": "id", "threshold": 1.0,
                 "severity": "error"}
        rules = build_dqdl_rules([check])
        assert rules[0][1] is check

    def test_empty_and_none_input(self):
        assert build_dqdl_rules([]) == []
        assert build_dqdl_rules(None) == []


class TestBuildDqdlRuleset:
    def test_full_ruleset(self):
        ruleset = build_dqdl_ruleset([
            {"metric": "completeness", "column": "vehicle_id", "threshold": 0.95},
            {"metric": "uniqueness", "column": "id", "threshold": 1.0},
            {"metric": "size", "threshold": 1},
        ])
        assert ruleset == (
            'Rules = [Completeness "vehicle_id" >= 0.95, '
            'Uniqueness "id" >= 1.0, RowCount >= 1]'
        )

    def test_no_valid_rules_returns_none(self):
        assert build_dqdl_ruleset([]) is None
        assert build_dqdl_ruleset([{"metric": "bogus"}]) is None

    def test_supported_metrics_frozen(self):
        assert SUPPORTED_METRICS == {"completeness", "uniqueness", "compliance", "size"}


class TestExampleConfigsCompile:
    """The shipped example configs must produce valid rulesets."""

    @pytest.mark.parametrize("use_case", ["vehicle-telemetry", "healthcare-iot"])
    def test_example_quality_checks_compile(self, use_case):
        yaml = pytest.importorskip("yaml")
        import pathlib
        cfg_path = (pathlib.Path(__file__).parent.parent / "examples" / use_case
                    / "config" / "tables.yaml")
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        found_any = False
        for table in cfg.get("tables", []):
            checks = table.get("quality_checks")
            if not checks:
                continue
            found_any = True
            rules = build_dqdl_rules(checks)
            # Every configured check in the shipped examples must compile —
            # no silent drops.
            assert len(rules) == len(checks), (
                f"{use_case}/{table['name']}: {len(checks)} checks -> "
                f"{len(rules)} rules"
            )
        assert found_any, f"{use_case}: no quality_checks found in any table"
