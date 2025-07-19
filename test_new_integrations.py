#!/usr/bin/env python3
"""
Test script for newly integrated sanity check functions
"""

from src.config.validation_config import get_configuration_summary, VALIDATION_CONFIGURATIONS

def test_new_integrations():
    """Test that all new sanity check rules are properly integrated"""
    
    print("=== Testing New Sanity Check Integrations ===\n")
    
    # Test configuration loading
    print("1. Testing configuration loading...")
    try:
        configs = VALIDATION_CONFIGURATIONS
        print(f"   ✓ Loaded {len(configs)} configurations successfully")
    except Exception as e:
        print(f"   ✗ Failed to load configurations: {e}")
        return False
    
    # Test sanity_checks configuration
    print("\n2. Testing sanity_checks configuration...")
    try:
        summary = get_configuration_summary('sanity_checks')
        if summary:
            print(f"   ✓ Sanity checks configuration has {summary['total_rules']} rules")
            
            # List all rules
            expected_new_rules = [
                'emobility_sanity',
                'pv_rooftop_buildings_sanity', 
                'home_batteries_sanity',
                'gas_de_sanity',
                'gas_abroad_sanity',
                'dsm_sanity'
            ]
            
            rule_names = [rule['name'] for rule in summary['rules']]
            
            for rule in expected_new_rules:
                if rule in rule_names:
                    print(f"   ✓ {rule} found in configuration")
                else:
                    print(f"   ✗ {rule} NOT found in configuration")
                    return False
        else:
            print("   ✗ Could not get sanity_checks configuration summary")
            return False
    except Exception as e:
        print(f"   ✗ Failed to test sanity_checks configuration: {e}")
        return False
    
    # Test rule instantiation
    print("\n3. Testing rule instantiation...")
    
    # Mock database manager for testing
    class MockDatabaseManager:
        def execute_query(self, query, params=None):
            return []
    
    db_manager = MockDatabaseManager()
    
    test_rules = [
        ('EmobilitySanityRule', 'src.rules.sanity.emobility_sanity_rule'),
        ('PvRooftopBuildingsSanityRule', 'src.rules.sanity.pv_rooftop_buildings_sanity_rule'),
        ('HomeBatteriesSanityRule', 'src.rules.sanity.home_batteries_sanity_rule'),
        ('GasDeSanityRule', 'src.rules.sanity.gas_de_sanity_rule'),
        ('GasAbroadSanityRule', 'src.rules.sanity.gas_abroad_sanity_rule'),
        ('DsmSanityRule', 'src.rules.sanity.dsm_sanity_rule')
    ]
    
    for rule_class, module_path in test_rules:
        try:
            module = __import__(module_path, fromlist=[rule_class])
            rule_class_obj = getattr(module, rule_class)
            rule_instance = rule_class_obj(db_manager)
            print(f"   ✓ {rule_class} instantiated successfully")
        except Exception as e:
            print(f"   ✗ Failed to instantiate {rule_class}: {e}")
            return False
    
    print("\n=== All Tests Passed! ===")
    print("\nSummary of Integration:")
    print("- 6 new sanity check functions successfully integrated")
    print("- All rules added to validation configurations")
    print("- All rules can be instantiated without errors")
    print("- Framework is ready to use the new sanity checks")
    
    return True

if __name__ == "__main__":
    success = test_new_integrations()
    exit(0 if success else 1)