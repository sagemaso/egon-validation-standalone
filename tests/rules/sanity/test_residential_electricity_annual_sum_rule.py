"""
Test for ResidentialElectricityAnnualSumRule
"""

import unittest
from unittest.mock import Mock, patch
import numpy as np
from src.rules.sanity.residential_electricity_annual_sum_rule import ResidentialElectricityAnnualSumRule
from src.core.database_manager import DatabaseManager


class TestResidentialElectricityAnnualSumRule(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.rule = ResidentialElectricityAnnualSumRule(self.mock_db_manager)
        
    def test_validate_scenario_success(self):
        """Test scenario validation with matching data"""
        # Mock database response with matching profile and demand sums
        mock_data = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 1000.0, "demand_regio_sum": 1000.0},
            {"nuts3": "DE112", "scenario": "eGon2035", "profile_sum": 1500.0, "demand_regio_sum": 1500.0},
            {"nuts3": "DE113", "scenario": "eGon2035", "profile_sum": 2000.0, "demand_regio_sum": 2000.0}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._validate_scenario("eGon2035", 1e-5)
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["scenario"], "eGon2035")
        self.assertEqual(result["nuts3_mismatches"], 0)
        self.assertEqual(result["total_nuts3"], 3)
        self.assertEqual(result["total_profile_sum"], 4500.0)
        self.assertEqual(result["total_demand_regio_sum"], 4500.0)
        self.assertIn("matches with DemandRegio", result["message"])
    
    def test_validate_scenario_with_mismatches(self):
        """Test scenario validation with mismatching data"""
        # Mock database response with some mismatches
        mock_data = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 1000.0, "demand_regio_sum": 1000.0},  # Match
            {"nuts3": "DE112", "scenario": "eGon2035", "profile_sum": 1500.0, "demand_regio_sum": 1600.0},  # Mismatch
            {"nuts3": "DE113", "scenario": "eGon2035", "profile_sum": 2000.0, "demand_regio_sum": 1900.0}   # Mismatch
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._validate_scenario("eGon2035", 1e-5)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["scenario"], "eGon2035")
        self.assertEqual(result["nuts3_mismatches"], 2)
        self.assertEqual(result["total_nuts3"], 3)
        self.assertIn("does not match DemandRegio", result["error"])
        self.assertIn("mismatch_details", result)
        self.assertEqual(len(result["mismatch_details"]), 2)
    
    def test_validate_scenario_no_data(self):
        """Test scenario validation with no data"""
        self.mock_db_manager.execute_query.return_value = []
        
        result = self.rule._validate_scenario("eGon2035", 1e-5)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["scenario"], "eGon2035")
        self.assertIn("No data found", result["error"])
        self.assertEqual(result["nuts3_mismatches"], 0)
        self.assertEqual(result["total_nuts3"], 0)
    
    def test_validate_scenario_database_error(self):
        """Test scenario validation with database error"""
        self.mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        result = self.rule._validate_scenario("eGon2035", 1e-5)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["scenario"], "eGon2035")
        self.assertIn("Failed to validate scenario", result["error"])
        self.assertIn("Database connection failed", result["error"])
    
    def test_validate_scenario_with_tolerance(self):
        """Test scenario validation with tolerance"""
        # Mock database response with small differences within tolerance
        mock_data = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 1000.0, "demand_regio_sum": 1000.001},  # Within tolerance
            {"nuts3": "DE112", "scenario": "eGon2035", "profile_sum": 1500.0, "demand_regio_sum": 1500.0015}  # Within tolerance
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._validate_scenario("eGon2035", 1e-3)  # 0.1% tolerance
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["nuts3_mismatches"], 0)
        self.assertEqual(result["total_nuts3"], 2)
    
    def test_validate_full_success(self):
        """Test full validation with multiple scenarios"""
        # Mock database responses for multiple scenarios
        mock_data_2035 = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 1000.0, "demand_regio_sum": 1000.0},
            {"nuts3": "DE112", "scenario": "eGon2035", "profile_sum": 1500.0, "demand_regio_sum": 1500.0}
        ]
        
        mock_data_100re = [
            {"nuts3": "DE111", "scenario": "eGon100RE", "profile_sum": 1200.0, "demand_regio_sum": 1200.0},
            {"nuts3": "DE112", "scenario": "eGon100RE", "profile_sum": 1800.0, "demand_regio_sum": 1800.0}
        ]
        
        self.mock_db_manager.execute_query.side_effect = [mock_data_2035, mock_data_100re]
        
        config = {
            "scenarios": ["eGon2035", "eGon100RE"],
            "tolerance": 1e-5
        }
        
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertIn("2/2 scenarios passed", result.message)
        self.assertEqual(result.detailed_context["summary"]["total_scenarios"], 2)
        self.assertEqual(result.detailed_context["summary"]["passed"], 2)
    
    def test_validate_with_failures(self):
        """Test full validation with some failures"""
        # Mock database responses with one success and one failure
        mock_data_2035 = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 1000.0, "demand_regio_sum": 1000.0}
        ]
        
        mock_data_100re = [
            {"nuts3": "DE111", "scenario": "eGon100RE", "profile_sum": 1200.0, "demand_regio_sum": 1500.0}  # Mismatch
        ]
        
        self.mock_db_manager.execute_query.side_effect = [mock_data_2035, mock_data_100re]
        
        config = {
            "scenarios": ["eGon2035", "eGon100RE"],
            "tolerance": 1e-5
        }
        
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("critical failures", result.error_details)
        self.assertEqual(result.detailed_context["summary"]["critical_failures"], 1)
        self.assertEqual(result.detailed_context["summary"]["passed"], 1)
    
    def test_validate_with_default_config(self):
        """Test validation with default configuration"""
        # Mock database responses for default scenarios
        mock_data_2035 = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 1000.0, "demand_regio_sum": 1000.0}
        ]
        
        mock_data_100re = [
            {"nuts3": "DE111", "scenario": "eGon100RE", "profile_sum": 1200.0, "demand_regio_sum": 1200.0}
        ]
        
        self.mock_db_manager.execute_query.side_effect = [mock_data_2035, mock_data_100re]
        
        config = {}  # Use defaults
        
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.detailed_context["tolerance"], 1e-5)
        self.assertEqual(result.detailed_context["scenarios"], ["eGon2035", "eGon100RE"])
    
    def test_validate_exception_handling(self):
        """Test validation with exception during execution"""
        self.mock_db_manager.execute_query.side_effect = Exception("Unexpected error")
        
        config = {"scenarios": ["eGon2035"], "tolerance": 1e-5}
        
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("critical failures", result.error_details)
    
    def test_mismatch_details_truncation(self):
        """Test that mismatch details are truncated to 10 items"""
        # Create mock data with more than 10 mismatches
        mock_data = []
        for i in range(15):
            mock_data.append({
                "nuts3": f"DE{i:03d}", 
                "scenario": "eGon2035", 
                "profile_sum": 1000.0, 
                "demand_regio_sum": 1100.0  # All mismatches
            })
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._validate_scenario("eGon2035", 1e-5)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["nuts3_mismatches"], 15)
        self.assertEqual(len(result["mismatch_details"]), 10)  # Truncated to 10
        self.assertEqual(result["total_nuts3"], 15)
    
    def test_zero_demand_handling(self):
        """Test handling of zero demand values"""
        mock_data = [
            {"nuts3": "DE111", "scenario": "eGon2035", "profile_sum": 0.0, "demand_regio_sum": 0.0},  # Both zero
            {"nuts3": "DE112", "scenario": "eGon2035", "profile_sum": 100.0, "demand_regio_sum": 0.0}  # Zero demand
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._validate_scenario("eGon2035", 1e-5)
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["nuts3_mismatches"], 1)  # DE112 should be a mismatch
        # Check that the mismatch details handle the zero demand case
        self.assertEqual(result["mismatch_details"][0]["relative_error"], float('inf'))


if __name__ == "__main__":
    unittest.main()