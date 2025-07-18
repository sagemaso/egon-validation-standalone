"""
Test for EtragoElectricitySanityRule
"""

import unittest
from unittest.mock import Mock, patch
from src.rules.sanity.etrago_electricity_sanity_rule import EtragoElectricitySanityRule
from src.core.database_manager import DatabaseManager


class TestEtragoElectricitySanityRule(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.rule = EtragoElectricitySanityRule(self.mock_db_manager)
        
    def test_calculate_deviation_success(self):
        """Test deviation calculation for successful cases"""
        result = self.rule._calculate_deviation("wind_onshore", 1000, 1050, 10.0)
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["carrier"], "wind_onshore")
        self.assertEqual(result["deviation_percent"], 5.0)
        self.assertIn("within tolerance", result["message"])
    
    def test_calculate_deviation_warning(self):
        """Test deviation calculation for warning cases"""
        result = self.rule._calculate_deviation("solar", 1000, 1200, 10.0)
        
        self.assertEqual(result["status"], "WARNING")
        self.assertEqual(result["carrier"], "solar")
        self.assertEqual(result["deviation_percent"], 20.0)
        self.assertIn("exceeds tolerance", result["message"])
    
    def test_calculate_deviation_zero_both(self):
        """Test deviation when both input and output are zero"""
        result = self.rule._calculate_deviation("oil", 0, 0, 10.0)
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertIn("No capacity", result["message"])
        self.assertEqual(result["deviation_percent"], 0.0)
    
    def test_calculate_deviation_input_zero(self):
        """Test deviation when input is zero but output exists"""
        result = self.rule._calculate_deviation("wind_offshore", 0, 500, 10.0)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertIn("no input capacity", result["error"])
        self.assertEqual(result["deviation_percent"], float('inf'))
    
    def test_calculate_deviation_output_zero(self):
        """Test deviation when output is zero but input exists"""
        result = self.rule._calculate_deviation("biomass", 500, 0, 10.0)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertIn("was not distributed", result["error"])
        self.assertEqual(result["deviation_percent"], -100.0)
    
    def test_validate_generators_success(self):
        """Test generator validation with mock database responses"""
        # Mock database responses
        self.mock_db_manager.execute_query.side_effect = [
            # Output capacity query
            [{"output_capacity_mw": 1050.0}],
            # Input capacity query
            [{"input_capacity_mw": 1000.0}]
        ]
        
        config = {"scenario": "eGon2035", "tolerance": 10.0}
        
        # Test only first carrier to keep it simple
        original_carriers = self.rule.electricity_carriers
        self.rule.electricity_carriers = ["wind_onshore"]
        
        try:
            results = self.rule._validate_generators("eGon2035", 10.0)
            
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["carrier"], "wind_onshore")
            self.assertEqual(results[0]["status"], "SUCCESS")
            self.assertEqual(results[0]["deviation_percent"], 5.0)
            
        finally:
            self.rule.electricity_carriers = original_carriers
    
    def test_validate_loads_success(self):
        """Test load validation with mock database responses"""
        # Mock database responses for loads
        self.mock_db_manager.execute_query.side_effect = [
            # Output demand query
            [{"load_twh": 500.0}],
            # Input CTS+IND query
            [{"demand_mw_regio_cts_ind": 200.0}],
            # Input HH query
            [{"demand_mw_regio_hh": 300.0}]
        ]
        
        results = self.rule._validate_loads("eGon2035", 5.0)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["carrier"], "electricity_demand")
        self.assertEqual(results[0]["status"], "SUCCESS")
        self.assertEqual(results[0]["deviation_percent"], 0.0)
    
    def test_validate_full_success(self):
        """Test full validation with all components"""
        # Mock database responses for all queries
        mock_responses = [
            # Generator queries (output, input) for each carrier
            [{"output_capacity_mw": 100.0}], [{"input_capacity_mw": 100.0}],  # others
            [{"output_capacity_mw": 200.0}], [{"input_capacity_mw": 200.0}],  # reservoir
            [{"output_capacity_mw": 300.0}], [{"input_capacity_mw": 300.0}],  # run_of_river
            [{"output_capacity_mw": 50.0}], [{"input_capacity_mw": 50.0}],    # oil
            [{"output_capacity_mw": 1000.0}], [{"input_capacity_mw": 1000.0}], # wind_onshore
            [{"output_capacity_mw": 800.0}], [{"input_capacity_mw": 800.0}],  # wind_offshore
            [{"output_capacity_mw": 1200.0}], [{"input_capacity_mw": 1200.0}], # solar
            [{"output_capacity_mw": 600.0}], [{"input_capacity_mw": 600.0}],  # solar_rooftop
            [{"output_capacity_mw": 400.0}], [{"input_capacity_mw": 400.0}],  # biomass
            
            # Storage queries (output, input)
            [{"output_capacity_mw": 150.0}], [{"input_capacity_mw": 150.0}],  # pumped_hydro
            
            # Load queries (output, input_cts_ind, input_hh)
            [{"load_twh": 500.0}],
            [{"demand_mw_regio_cts_ind": 200.0}],
            [{"demand_mw_regio_hh": 300.0}]
        ]
        
        self.mock_db_manager.execute_query.side_effect = mock_responses
        
        config = {"scenario": "eGon2035", "tolerance": 5.0}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertIn("eGon2035", result.message)
        self.assertEqual(result.detailed_context["summary"]["total_validations"], 11)  # 9 generators + 1 storage + 1 load
        self.assertEqual(result.detailed_context["summary"]["passed"], 11)
    
    def test_validate_with_failures(self):
        """Test validation with some failures"""
        # Mock database responses with some failures
        mock_responses = [
            # Generator with missing output
            [{"output_capacity_mw": 0}], [{"input_capacity_mw": 100.0}],  # others - failure
            [{"output_capacity_mw": 200.0}], [{"input_capacity_mw": 200.0}],  # reservoir - success
            
            # Storage queries (output, input)
            [{"output_capacity_mw": 150.0}], [{"input_capacity_mw": 150.0}],  # pumped_hydro - success
            
            # Load queries (output, input_cts_ind, input_hh)
            [{"load_twh": 500.0}],
            [{"demand_mw_regio_cts_ind": 200.0}],
            [{"demand_mw_regio_hh": 300.0}]  # load - success
        ]
        
        self.mock_db_manager.execute_query.side_effect = mock_responses
        
        # Limit to just 2 carriers for this test
        original_carriers = self.rule.electricity_carriers
        self.rule.electricity_carriers = ["others", "reservoir"]
        
        try:
            config = {"scenario": "eGon2035", "tolerance": 5.0}
            result = self.rule.validate(config)
            
            self.assertEqual(result.status, "CRITICAL_FAILURE")
            self.assertIn("critical failures", result.error_details)
            self.assertEqual(result.detailed_context["summary"]["critical_failures"], 1)
            
        finally:
            self.rule.electricity_carriers = original_carriers


if __name__ == "__main__":
    unittest.main()