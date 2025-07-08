import sys

sys.path.append('src')

from src.rules.formal.null_check_rule import NullCheckRule


def test_null_check_rule():
    """Test the NullCheckRule with multiple tables/columns"""

    print("🧪 Testing NullCheckRule Framework")
    print("=" * 50)

    # Create rule instance
    rule = NullCheckRule()
    print(f"✓ Rule created: {rule.rule_name}")

    # Configure multiple tables/columns to check
    configs = [
        {
            "table": "demand.egon_demandregio_hh",
            "column": "demand",
            "scenario": "eGon2035"
        },
        {
            "table": "demand.egon_demandregio_hh",
            "column": "nuts3",
            "scenario": "eGon2035"
        },
        {
            "table": "supply.egon_power_plants",
            "column": "el_capacity"
            # No scenario filter for this one
        }
    ]

    print(f"\n🔍 Checking {len(configs)} table/column combinations:")
    for i, config in enumerate(configs, 1):
        scenario_info = f" (scenario: {config.get('scenario', 'all')})" if config.get('scenario') else ""
        print(f"   {i}. {config['table']}.{config['column']}{scenario_info}")

    # Run validation
    result = rule.validate(configs)

    # Print summary results
    print(f"\n📊 Validation Summary:")
    print(f"   Rule: {result.rule_name}")
    print(f"   Status: {result.status}")
    print(f"   Table: {result.table}")
    print(f"   Timestamp: {result.timestamp}")

    if result.status == "SUCCESS":
        print(f"   ✅ Message: {result.message}")
    else:
        print(f"   ❌ Error: {result.error_details}")

    # Print detailed context
    if result.detailed_context:
        context = result.detailed_context
        print(f"\n📋 Detailed Results:")
        print(f"   Total validations: {context.get('total_validations', 0)}")
        print(f"   Passed: {context.get('passed', 0)}")
        print(f"   Failed: {context.get('failed', 0)}")

        if context.get('failed_tables'):
            print(f"   Failed tables: {context['failed_tables']}")

        print(f"\n📝 Individual Results:")
        for detail in context.get('detailed_results', []):
            status_icon = "✅" if detail['status'] == "SUCCESS" else "❌"
            print(f"   {status_icon} {detail['table']}.{detail['column']}: {detail['details']}")

    print(f"\n🎯 Overall Test: {'PASSED' if result.status == 'SUCCESS' else 'FAILED'}")
    return result.status == "SUCCESS"


if __name__ == "__main__":
    success = test_null_check_rule()
    exit(0 if success else 1)