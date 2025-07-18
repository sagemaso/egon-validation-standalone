"""
Test for EtragoHeatSanityRule
"""

import unittest
from unittest.mock import Mock, patch
from src.rules.sanity.etrago_heat_sanity_rule import EtragoHeatSanityRule
from src.core.database_manager import DatabaseManager


class TestEtragoHeatSanityRule(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.rule = EtragoHeatSanityRule(self.mock_db_manager)
        
    def test_calculate_deviation_success(self):
        """Test deviation calculation for successful cases"""
        result = self.rule._calculate_deviation("central_heat_pump", 1000, 1050, 10.0)
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["component"], "central_heat_pump")
        self.assertEqual(result["deviation_percent"], 5.0)
        self.assertIn("within tolerance", result["message"])
    
    def test_calculate_deviation_warning(self):
        """Test deviation calculation for warning cases"""
        result = self.rule._calculate_deviation("solar_thermal", 1000, 1200, 10.0)
        
        self.assertEqual(result["status"], "WARNING")
        self.assertEqual(result["component"], "solar_thermal")
        self.assertEqual(result["deviation_percent"], 20.0)
        self.assertIn("exceeds tolerance", result["message"])
    
    def test_calculate_deviation_zero_both(self):
        """Test deviation when both input and output are zero"""
        result = self.rule._calculate_deviation("geothermal", 0, 0, 10.0)
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertIn("No capacity", result["message"])
        self.assertEqual(result["deviation_percent"], 0.0)
    
    def test_calculate_deviation_input_zero(self):
        """Test deviation when input is zero but output exists"""
        result = self.rule._calculate_deviation("resistive_heater", 0, 500, 10.0)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertIn("no input capacity", result["error"])
        self.assertEqual(result["deviation_percent"], float('inf'))
    
    def test_calculate_deviation_output_zero(self):
        """Test deviation when output is zero but input exists"""
        result = self.rule._calculate_deviation("residential_heat_pump", 500, 0, 10.0)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertIn("was not distributed", result["error"])
        self.assertEqual(result["deviation_percent"], -100.0)
    
    def test_validate_heat_demand_success(self):
        """Test heat demand validation with mock database responses"""
        # Mock database responses
        self.mock_db_manager.execute_query.side_effect = [
            # Output demand query
            [{"load_twh": 150.0}],
            # Input demand query
            [{"demand_mw_peta_heat": 150.0}]
        ]
        
        results = self.rule._validate_heat_demand("eGon2035", 5.0)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["component"], "heat_demand")
        self.assertEqual(results[0]["status"], "SUCCESS")
        self.assertEqual(results[0]["deviation_percent"], 0.0)
    
    def test_validate_heat_supply_success(self):
        """Test heat supply validation with mock database responses"""
        # Mock database responses for heat supply (output, input) for each component
        mock_responses = [
            # central_heat_pump
            [{"output_capacity_mw": 1000.0}], [{"input_capacity_mw": 1000.0}],
            # residential_heat_pump
            [{"output_capacity_mw": 800.0}], [{"input_capacity_mw": 800.0}],
            # resistive_heater
            [{"output_capacity_mw": 200.0}], [{"input_capacity_mw": 200.0}],
            # solar_thermal
            [{"output_capacity_mw": 300.0}], [{"input_capacity_mw": 300.0}],
            # geothermal
            [{"output_capacity_mw": 150.0}], [{"input_capacity_mw": 150.0}]
        ]
        
        self.mock_db_manager.execute_query.side_effect = mock_responses
        
        results = self.rule._validate_heat_supply("eGon2035", 5.0)
        
        self.assertEqual(len(results), 5)
        for result in results:
            self.assertEqual(result["status"], "SUCCESS")
            self.assertEqual(result["deviation_percent"], 0.0)
    
    def test_validate_full_success(self):
        """Test full validation with all components"""
        # Mock database responses for all queries
        mock_responses = [
            # Heat demand queries (output, input)
            [{"load_twh": 150.0}], [{"demand_mw_peta_heat": 150.0}],
            
            # Heat supply queries (output, input) for each component
            [{"output_capacity_mw": 1000.0}], [{"input_capacity_mw": 1000.0}],  # central_heat_pump
            [{"output_capacity_mw": 800.0}], [{"input_capacity_mw": 800.0}],   # residential_heat_pump
            [{"output_capacity_mw": 200.0}], [{"input_capacity_mw": 200.0}],   # resistive_heater
            [{"output_capacity_mw": 300.0}], [{"input_capacity_mw": 300.0}],   # solar_thermal
            [{"output_capacity_mw": 150.0}], [{"input_capacity_mw": 150.0}]    # geothermal
        ]
        
        self.mock_db_manager.execute_query.side_effect = mock_responses
        
        config = {"scenario": "eGon2035", "tolerance": 5.0}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertIn("eGon2035", result.message)
        self.assertEqual(result.detailed_context["summary"]["total_validations"], 6)  # 1 demand + 5 supply
        self.assertEqual(result.detailed_context["summary"]["passed"], 6)
    
    def test_validate_with_failures(self):
        """Test validation with some failures"""
        # Mock database responses with some failures
        mock_responses = [
            # Heat demand queries (output, input) - success
            [{"load_twh": 150.0}], [{"demand_mw_peta_heat": 150.0}],
            
            # Heat supply queries with one failure
            [{"output_capacity_mw": 0}], [{"input_capacity_mw": 1000.0}],  # central_heat_pump - failure
            [{"output_capacity_mw": 800.0}], [{"input_capacity_mw": 800.0}],   # residential_heat_pump - success
            [{"output_capacity_mw": 200.0}], [{"input_capacity_mw": 200.0}],   # resistive_heater - success
            [{"output_capacity_mw": 300.0}], [{"input_capacity_mw": 300.0}],   # solar_thermal - success
            [{"output_capacity_mw": 150.0}], [{"input_capacity_mw": 150.0}]    # geothermal - success
        ]
        
        self.mock_db_manager.execute_query.side_effect = mock_responses
        
        config = {"scenario": "eGon2035", "tolerance": 5.0}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("critical failures", result.error_details)
        self.assertEqual(result.detailed_context["summary"]["critical_failures"], 1)
        self.assertEqual(result.detailed_context["summary"]["passed"], 5)
    
    def test_validate_heat_demand_failure(self):
        """Test heat demand validation with database error"""
        # Mock database to raise exception
        self.mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        results = self.rule._validate_heat_demand("eGon2035", 5.0)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "CRITICAL_FAILURE")
        self.assertIn("Failed to validate heat demand", results[0]["error"])
    
    def test_validate_heat_supply_failure(self):
        """Test heat supply validation with database error"""
        # Mock database to raise exception
        self.mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        results = self.rule._validate_heat_supply("eGon2035", 5.0)
        
        self.assertEqual(len(results), 5)  # One for each component
        for result in results:
            self.assertEqual(result["status"], "CRITICAL_FAILURE")
            self.assertIn("Failed to validate", result["error"])
    
    def test_heat_supply_components_configuration(self):
        """Test that heat supply components are properly configured"""
        components = self.rule.heat_supply_components
        
        self.assertEqual(len(components), 5)
        
        # Check that all expected components are present
        component_names = [comp["name"] for comp in components]
        expected_names = [
            "central_heat_pump", "residential_heat_pump", "resistive_heater",
            "solar_thermal", "geothermal"
        ]
        
        for name in expected_names:
            self.assertIn(name, component_names)
        
        # Check that each component has required fields
        for comp in components:
            self.assertIn("name", comp)
            self.assertIn("input_carrier", comp)
            self.assertIn("output_carrier", comp)
            self.assertIn("table", comp)


if __name__ == "__main__":
    unittest.main()