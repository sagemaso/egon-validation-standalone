from typing import List, Dict, Any, Optional
from datetime import datetime

from src.core.database_manager import DatabaseManager
from src.core.validation_result import ValidationResult
from src.core.validation_logger import ValidationLogger
from src.config.validation_config import VALIDATION_CONFIGURATIONS, get_configuration_summary
from src.core.validation_monitor import ValidationMonitor


class ValidationOrchestrator:
    """Central orchestrator for running multiple validation rules"""

    def __init__(self, db_manager: DatabaseManager = None):
        self.db_manager = db_manager or DatabaseManager()
        self.logger = ValidationLogger("orchestrator")
        self.validation_rules = {}
        self.results = []

    def load_configuration(self, config_name: str):
        """
        Load a predefined validation configuration

        Parameters:
        -----------
        config_name : str
            Name of configuration to load (e.g., "comprehensive", "critical_only")
        """

        if config_name not in VALIDATION_CONFIGURATIONS:
            available = list(VALIDATION_CONFIGURATIONS.keys())
            raise ValueError(f"Configuration '{config_name}' not found. Available: {available}")

        config = VALIDATION_CONFIGURATIONS[config_name]

        # Clear existing rules
        self.validation_rules = {}

        # Load rules from configuration
        for rule_def in config["rules"]:
            self.register_rule(
                rule_name=rule_def["name"],
                rule_class=rule_def["rule_class"],
                rule_config=rule_def["config"]
            )

        print(f"✅ Loaded configuration '{config_name}': {config.get('description', '')}")
        print(f"   📊 Registered {len(self.validation_rules)} validation rules")

    def list_available_configurations(self):
        """List all available predefined configurations"""

        print("🔧 Available Validation Configurations:")
        print("=" * 50)

        for config_name in VALIDATION_CONFIGURATIONS.keys():
            summary = get_configuration_summary(config_name)
            print(f"📋 {config_name}")
            print(f"   Description: {summary['description']}")
            print(f"   Rules: {summary['total_rules']}")
            for rule in summary['rules']:
                print(f"      • {rule['name']} ({rule['type']}, {rule['table_count']} tables)")
            print()

    def quick_setup(self, config_name: str = "comprehensive"):
        """
        Quick setup method - load configuration and ready to run

        Parameters:
        -----------
        config_name : str
            Configuration to load (default: "comprehensive")

        Returns:
        --------
        ValidationOrchestrator
            Self, for method chaining
        """

        self.load_configuration(config_name)
        return self

    def register_rule(self, rule_name: str, rule_class, rule_config: Dict[str, Any]):
        """
        Register a validation rule with its configuration

        Parameters:
        -----------
        rule_name : str
            Unique name for this validation (e.g., "load_null_check")
        rule_class : class
            Validation rule class (e.g., NullCheckRule, TimeSeriesValidationRule)
        rule_config : dict
            Configuration for this validation
        """
        self.validation_rules[rule_name] = {
            "rule_class": rule_class,
            "config": rule_config
        }
        self.logger.info(f"Registered validation rule: {rule_name}")

    def run_all_validations(self) -> Dict[str, Any]:
        """
        Run all registered validation rules

        Returns:
        --------
        Dict with overall results and detailed breakdown
        """

        self.logger.log_validation_start("ValidationOrchestrator", len(self.validation_rules))
        self.results = []

        overall_start_time = datetime.now()
        total_rules = len(self.validation_rules)
        failed_rules = []
        passed_rules = []

        for i, (rule_name, rule_info) in enumerate(self.validation_rules.items(), 1):
            print(f"\n🔧 [{i}/{total_rules}] Running validation: {rule_name}")

            try:
                # Create rule instance with shared database manager
                rule_class = rule_info["rule_class"]
                rule_instance = rule_class(self.db_manager)

                # Run validation with config
                rule_result = rule_instance.validate(rule_info["config"])

                # Store result
                enhanced_result = {
                    "rule_name": rule_name,
                    "validation_type": rule_instance.rule_name,
                    "result": rule_result,
                    "timestamp": datetime.now()
                }
                self.results.append(enhanced_result)

                # Track success/failure
                if rule_result.status == "SUCCESS":
                    passed_rules.append(rule_name)
                    print(f"   ✅ {rule_name}: PASSED")
                else:
                    failed_rules.append(rule_name)
                    print(f"   ❌ {rule_name}: FAILED - {rule_result.error_details}")

            except Exception as e:
                print(f"   💥 {rule_name}: EXECUTION ERROR - {str(e)}")

                # Create error result
                error_result = ValidationResult(
                    rule_name=rule_name,
                    status="CRITICAL_FAILURE",
                    table="unknown",
                    function_name="run_all_validations",
                    module_name=self.__class__.__module__,
                    error_details=f"Rule execution failed: {str(e)}"
                )

                enhanced_result = {
                    "rule_name": rule_name,
                    "validation_type": "unknown",
                    "result": error_result,
                    "timestamp": datetime.now(),
                    "execution_error": str(e)
                }
                self.results.append(enhanced_result)
                failed_rules.append(rule_name)

        # Calculate overall results
        overall_end_time = datetime.now()
        duration = (overall_end_time - overall_start_time).total_seconds()

        overall_status = "SUCCESS" if len(failed_rules) == 0 else "CRITICAL_FAILURE"

        # Create comprehensive report
        report = {
            "timestamp": overall_start_time.isoformat(),
            "duration_seconds": duration,
            "overall_status": overall_status,
            "total_rules": total_rules,
            "passed_rules": len(passed_rules),
            "failed_rules": len(failed_rules),
            "passed_rule_names": passed_rules,
            "failed_rule_names": failed_rules,
            "detailed_results": self.results
        }

        # Summary logging
        self._log_final_summary(report)

        return report

    def run_specific_validations(self, rule_names: List[str]) -> Dict[str, Any]:
        """
        Run only specific validation rules

        Parameters:
        -----------
        rule_names : List[str]
            List of rule names to execute
        """

        # Temporarily store original rules
        original_rules = self.validation_rules.copy()

        # Filter to only requested rules
        self.validation_rules = {
            name: rule_info for name, rule_info in original_rules.items()
            if name in rule_names
        }

        # Run filtered validations
        report = self.run_all_validations()

        # Restore original rules
        self.validation_rules = original_rules

        return report

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of registered validations without running them"""

        summary = {
            "total_registered_rules": len(self.validation_rules),
            "rules": {}
        }

        for rule_name, rule_info in self.validation_rules.items():
            rule_class = rule_info["rule_class"]
            config = rule_info["config"]

            summary["rules"][rule_name] = {
                "validation_type": rule_class.__name__,
                "table_count": len(config) if isinstance(config, list) else 1,
                "tables": self._extract_table_names(config)
            }

        return summary

    def _extract_table_names(self, config) -> List[str]:
        """Extract table names from validation config"""
        if isinstance(config, list):
            return [item.get("table", "unknown") for item in config]
        elif isinstance(config, dict):
            return [config.get("table", "unknown")]
        else:
            return ["unknown"]

    def _log_final_summary(self, report: Dict[str, Any]):
        """Log comprehensive final summary"""

        print(f"\n" + "=" * 80)
        print(f"🎯 VALIDATION ORCHESTRATOR SUMMARY")
        print(f"=" * 80)
        print(f"⏱️  Duration: {report['duration_seconds']:.2f} seconds")
        print(f"📊 Overall Status: {report['overall_status']}")
        print(f"📈 Rules Summary: {report['passed_rules']}/{report['total_rules']} passed")

        if report['failed_rule_names']:
            print(f"\n❌ Failed Rules:")
            for rule_name in report['failed_rule_names']:
                print(f"   • {rule_name}")

        if report['passed_rule_names']:
            print(f"\n✅ Passed Rules:")
            for rule_name in report['passed_rule_names']:
                print(f"   • {rule_name}")

        print(f"\n🔍 Detailed Results: {len(report['detailed_results'])} validation rule results available")
        print(f"=" * 80)

        # Log to standard logger for persistence
        if report['overall_status'] == "SUCCESS":
            self.logger.info(f"All {report['total_rules']} validations passed in {report['duration_seconds']:.2f}s")
        else:
            self.logger.critical(
                f"Validation failed: {len(report['failed_rule_names'])} of {report['total_rules']} rules failed")

    def generate_monitoring_report(self, output_dir: str = "./monitoring_reports") -> Dict[str, str]:
        """
        Generate monitoring report for currently configured validations

        Parameters:
        -----------
        output_dir : str
            Directory for generated reports

        Returns:
        --------
        Dict[str, str]
            Paths to generated files
        """

        print("📊 Generating validation monitoring report...")

        # Initialize monitor with same DB connection
        monitor = ValidationMonitor(self.db_manager)

        # Generate complete report
        report_files = monitor.generate_full_report(output_dir)

        print(f"\n✅ Monitoring report generated!")
        print(f"📄 HTML Report: {report_files['html_report']}")

        return report_files

    def check_validation_coverage(self) -> Dict[str, Any]:
        """
        Quick coverage analysis without HTML generation

        Returns:
        --------
        Dict with coverage statistics
        """

        print("🔍 Checking validation coverage...")

        monitor = ValidationMonitor(self.db_manager)

        # Discover structure and analyze coverage
        discovery_data = monitor.discover_database_structure()
        coverage_data = monitor.analyze_validation_coverage()

        # Summary
        summary = {
            "total_tables": discovery_data['total_tables'],
            "total_schemas": discovery_data['total_schemas'],
            "covered_tables": coverage_data['covered_tables'],
            "coverage_percentage": coverage_data['coverage_percentage'],
            "uncovered_tables": coverage_data['uncovered_table_list']
        }

        print(f"📈 Coverage Summary:")
        print(f"   Tables: {summary['covered_tables']}/{summary['total_tables']}")
        print(f"   Coverage: {summary['coverage_percentage']:.1f}%")

        if summary['uncovered_tables']:
            print(f"   ⚠️  Uncovered: {len(summary['uncovered_tables'])} tables")

        return summary

    def run_with_monitoring(self, config_name: str = "comprehensive",
                            generate_report: bool = True) -> Dict[str, Any]:
        """
        Run validations and additionally generate monitoring report

        Parameters:
        -----------
        config_name : str
            Validation configuration
        generate_report : bool
            Whether to generate monitoring report

        Returns:
        --------
        Dict with validation results and report paths
        """

        print(f"🚀 Running validation with monitoring: {config_name}")

        # 1. Generate monitoring report (if requested)
        report_files = {}
        if generate_report:
            report_files = self.generate_monitoring_report()

        # 2. Run normal validations
        self.load_configuration(config_name)
        validation_results = self.run_all_validations()

        # 3. Return combined results
        combined_results = {
            "validation_results": validation_results,
            "monitoring_files": report_files,
            "config_used": config_name
        }

        return combined_results