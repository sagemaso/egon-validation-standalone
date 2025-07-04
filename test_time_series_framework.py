import sys

sys.path.append('src')

from src.rules.time_series_rule import TimeSeriesValidationRule


def test_time_series_validation():
    """Test the framework-based time series validation"""

    print("🧪 Testing Time Series Validation Framework")
    print("=" * 50)

    # Create rule instance
    rule = TimeSeriesValidationRule()
    print(f"✓ Rule created: {rule.rule_name}")

    # Run validation
    result = rule.validate(
        table="grid.egon_etrago_load_timeseries",
        column="p_set",
        expected_length=8760,
        scenario="eGon2035"
    )

    # Print results
    print(f"\n📊 Validation Results:")
    print(f"   Rule: {result.rule_name}")
    print(f"   Status: {result.status}")
    print(f"   Table: {result.table}")
    print(f"   Module: {result.module_name}")
    print(f"   Function: {result.function_name}")
    print(f"   Timestamp: {result.timestamp}")

    if result.status == "SUCCESS":
        print(f"   ✅ Message: {result.message}")
    else:
        print(f"   ❌ Error: {result.error_details}")

    # Test the to_dict() method
    print(f"\n📋 Result as Dictionary:")
    result_dict = result.to_dict()
    for key, value in result_dict.items():
        print(f"   {key}: {value}")

    print(f"\n🎯 Overall Test: {'PASSED' if result.status == 'SUCCESS' else 'FAILED'}")
    return result.status == "SUCCESS"


if __name__ == "__main__":
    success = test_time_series_validation()
    exit(0 if success else 1)