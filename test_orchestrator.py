import sys
from typing import Dict, Any

sys.path.append('src')

from src.core.validation_orchestrator import ValidationOrchestrator
from src.core.database_manager import DatabaseManager
from src.rules.formal.null_check_rule import NullCheckRule
from src.rules.formal.time_series_rule import TimeSeriesValidationRule
from src.core.validation_monitor import ValidationMonitor


def test_orchestrator_manual_setup():
    """Test orchestrator with manual rule registration"""
    print("🧪 Testing ValidationOrchestrator - Manual Setup")
    print("=" * 60)

    # Create orchestrator with shared database manager
    db_manager = DatabaseManager()
    orchestrator = ValidationOrchestrator(db_manager)

    # Manually register validation rules
    orchestrator.register_rule(
        "test_null_checks",
        NullCheckRule,
        [
            {"table": "demand.egon_demandregio_hh", "column": "demand"},
            {"table": "demand.egon_demandregio_hh", "column": "nuts3"}
        ]
    )

    orchestrator.register_rule(
        "test_time_series",
        TimeSeriesValidationRule,
        [
            {"table": "grid.egon_etrago_load_timeseries", "column": "p_set", "expected_length": 8760}
        ]
    )

    # Run all validations
    report = orchestrator.run_all_validations()

    # Check results
    success = report['overall_status'] == 'SUCCESS'
    print(f"\n🎯 Manual Setup Test: {'PASSED' if success else 'FAILED'}")

    return success


def test_orchestrator_configuration_loading():
    """Test orchestrator with predefined configurations"""
    print("\n🧪 Testing ValidationOrchestrator - Configuration Loading")
    print("=" * 60)

    orchestrator = ValidationOrchestrator()

    # Test listing configurations
    orchestrator.list_available_configurations()

    # Test loading different configurations
    configurations_to_test = ["critical_only", "time_series_only", "quick_check"]

    results = []

    for config_name in configurations_to_test:
        print(f"\n🔧 Testing configuration: {config_name}")

        try:
            # Load configuration
            orchestrator.load_configuration(config_name)

            # Get summary without running
            summary = orchestrator.get_validation_summary()
            print(f"   📊 Summary: {summary['total_registered_rules']} rules loaded")

            results.append(True)

        except Exception as e:
            print(f"   ❌ Failed to load {config_name}: {e}")
            results.append(False)

    success = all(results)
    print(f"\n🎯 Configuration Loading Test: {'PASSED' if success else 'FAILED'}")

    return success


def test_orchestrator_quick_setup():
    """Test orchestrator quick setup and execution"""
    print("\n🧪 Testing ValidationOrchestrator - Quick Setup & Execution")
    print("=" * 60)

    try:
        # Quick setup with method chaining
        orchestrator = ValidationOrchestrator().quick_setup("quick_check")

        # Run validations
        report = orchestrator.run_all_validations()

        # Verify execution
        success = (
                "overall_status" in report and
                "total_rules" in report and
                "detailed_results" in report and
                len(report["detailed_results"]) > 0
        )

        print(f"\n🎯 Quick Setup Test: {'PASSED' if success else 'FAILED'}")
        return success

    except Exception as e:
        print(f"❌ Quick setup failed: {e}")
        return False


def test_orchestrator_comprehensive():
    """Test orchestrator with comprehensive configuration"""
    print("\n🧪 Testing ValidationOrchestrator - Comprehensive Configuration")
    print("=" * 60)

    try:
        # Load comprehensive configuration
        orchestrator = ValidationOrchestrator().quick_setup("comprehensive")

        # Run all validations
        report = orchestrator.run_all_validations()

        # Detailed analysis
        print(f"\n📊 Comprehensive Test Results:")
        print(f"   Duration: {report['duration_seconds']:.2f} seconds")
        print(f"   Total rules: {report['total_rules']}")
        print(f"   Passed: {report['passed_rules']}")
        print(f"   Failed: {report['failed_rules']}")

        # Test specific validations (subset)
        print(f"\n🔍 Testing specific validation subset...")
        subset_report = orchestrator.run_specific_validations(["critical_null_checks"])

        success = (
                report['total_rules'] > 1 and
                subset_report['total_rules'] >= 1 and
                subset_report['total_rules'] < report['total_rules']
        )

        print(f"\n🎯 Comprehensive Test: {'PASSED' if success else 'FAILED'}")
        return success

    except Exception as e:
        print(f"❌ Comprehensive test failed: {e}")
        return False

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


def main():
    """Run all orchestrator tests"""
    print("🎯 Testing ValidationOrchestrator Framework")
    print("=" * 80)

    tests = [
        test_orchestrator_manual_setup,
        test_orchestrator_configuration_loading,
        test_orchestrator_quick_setup,
        test_orchestrator_comprehensive
    ]

    results = []
    for test in tests:
        try:
            success = test()
            results.append(success)
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            results.append(False)

    # Final summary
    passed = sum(results)
    total = len(results)

    print(f"\n🎯 Orchestrator Test Summary: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 ValidationOrchestrator framework is production ready!")
    else:
        print("⚠️  Some tests failed. Check the output above.")

    return passed == total


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)