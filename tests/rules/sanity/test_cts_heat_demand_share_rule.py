"""
Test for CtsHeatDemandShareRule
"""

import unittest
from unittest.mock import Mock, patch
import numpy as np
from src.rules.sanity.cts_heat_demand_share_rule import CtsHeatDemandShareRule
from src.core.database_manager import DatabaseManager


class TestCtsHeatDemandShareRule(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.rule = CtsHeatDemandShareRule(self.mock_db_manager)
        
    def test_get_cts_heat_demand_share_data_success(self):
        """Test successful retrieval of CTS heat demand share data"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.5},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.5}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._get_cts_heat_demand_share_data()
        
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]["bus_id"], 1001)
        self.assertEqual(result[0]["scenario"], "eGon2035")
        self.assertEqual(result[0]["profile_share"], 0.3)
    
    def test_get_cts_heat_demand_share_data_database_error(self):
        """Test demand share data retrieval with database error"""
        self.mock_db_manager.execute_query.side_effect = Exception("Database error")
        
        result = self.rule._get_cts_heat_demand_share_data()
        
        self.assertEqual(result, [])
    
    def test_validate_demand_share_consistency_success(self):
        """Test demand share consistency validation with shares summing to 1.0"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.4},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.6},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.2},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.8}
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035", "eGon100RE"])
        
        self.assertEqual(len(results), 2)  # 2 scenarios
        for result in results:
            self.assertEqual(result["status"], "SUCCESS")
            self.assertEqual(result["mismatches"], 0)
            self.assertIn("equal 1.0", result["message"])
    
    def test_validate_demand_share_consistency_with_mismatches(self):
        """Test demand share consistency validation with shares not summing to 1.0"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.6},  # Sum = 0.9, not 1.0
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.4},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.6}   # Sum = 1.0, correct
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035"])
        
        self.assertEqual(len(results), 1)
        result = results[0]
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["mismatches"], 1)  # Only bus_id 1001 has mismatch
        self.assertEqual(result["total_bus_ids"], 2)
        self.assertIn("do not sum to 1.0", result["error"])
        self.assertIn("mismatch_details", result)
        self.assertEqual(len(result["mismatch_details"]), 1)
        self.assertEqual(result["mismatch_details"][0]["bus_id"], 1001)
        self.assertAlmostEqual(result["mismatch_details"][0]["share_sum"], 0.9, places=5)
    
    def test_validate_demand_share_consistency_with_tolerance(self):
        """Test demand share consistency validation with tolerance"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7001},  # Sum = 1.0001, within tolerance
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.4999},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.5001}   # Sum = 1.0, correct
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-3, ["eGon2035"])  # 0.1% tolerance
        
        self.assertEqual(len(results), 1)
        result = results[0]
        
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["mismatches"], 0)
        self.assertEqual(result["total_bus_ids"], 2)
    
    def test_validate_demand_share_consistency_no_data_for_scenario(self):
        """Test demand share consistency validation with no data for scenario"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7}
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035", "eGon100RE"])
        
        self.assertEqual(len(results), 2)
        
        # Check eGon2035 (has data)
        egon2035_result = next(r for r in results if r["scenario"] == "eGon2035")
        self.assertEqual(egon2035_result["status"], "SUCCESS")
        
        # Check eGon100RE (no data)
        egon100re_result = next(r for r in results if r["scenario"] == "eGon100RE")
        self.assertEqual(egon100re_result["status"], "WARNING")
        self.assertIn("No data found", egon100re_result["message"])
        self.assertEqual(egon100re_result["total_bus_ids"], 0)
    
    def test_validate_demand_share_consistency_exception_handling(self):
        """Test demand share consistency validation with exception"""
        # This test simulates an exception during validation by passing invalid data
        # that will cause an exception when trying to convert to float
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": "invalid"}
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035"])
        
        self.assertEqual(len(results), 1)
        result = results[0]
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertIn("Failed to validate scenario", result["error"])
    
    def test_validate_full_success(self):
        """Test full validation with successful demand share consistency"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.4},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.6},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.2},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.8}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        config = {
            "tolerance": 1e-5,
            "scenarios": ["eGon2035", "eGon100RE"]
        }
        
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertIn("2/2 scenarios passed", result.message)
        self.assertEqual(result.detailed_context["summary"]["total_scenarios"], 2)
        self.assertEqual(result.detailed_context["summary"]["passed"], 2)
        self.assertEqual(result.detailed_context["data_summary"]["total_records"], 6)
        self.assertEqual(result.detailed_context["data_summary"]["unique_bus_ids"], 2)
        self.assertEqual(result.detailed_context["data_summary"]["unique_scenarios"], 2)
    
    def test_validate_with_failures(self):
        """Test full validation with some failures"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7},  # Sum = 1.0, success
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.2},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.7}   # Sum = 0.9, failure
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        config = {
            "tolerance": 1e-5,
            "scenarios": ["eGon2035", "eGon100RE"]
        }
        
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("critical failures", result.error_details)
        self.assertEqual(result.detailed_context["summary"]["critical_failures"], 1)
        self.assertEqual(result.detailed_context["summary"]["passed"], 1)
    
    def test_validate_no_data(self):
        """Test validation with no data"""
        self.mock_db_manager.execute_query.return_value = []
        
        config = {"tolerance": 1e-5}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("No CTS heat demand share data found", result.error_details)
    
    def test_validate_with_default_config(self):
        """Test validation with default configuration"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.4},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.6}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        config = {}  # Use defaults
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.detailed_context["tolerance"], 1e-5)
        self.assertEqual(result.detailed_context["scenarios"], ["eGon2035", "eGon100RE"])
    
    def test_validate_exception_handling(self):
        """Test validation with exception during execution"""
        self.mock_db_manager.execute_query.side_effect = Exception("Unexpected error")
        
        config = {"tolerance": 1e-5}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("No CTS heat demand share data found", result.error_details)
    
    def test_mismatch_details_truncation(self):
        """Test that mismatch details are truncated to 10 items"""
        # Create mock data with more than 10 mismatches
        mock_data = []
        for i in range(15):
            mock_data.extend([
                {"bus_id": 1000 + i, "scenario": "eGon2035", "profile_share": 0.3},
                {"bus_id": 1000 + i, "scenario": "eGon2035", "profile_share": 0.6}  # Sum = 0.9, all mismatches
            ])
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035"])
        
        self.assertEqual(len(results), 1)
        result = results[0]
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["mismatches"], 15)
        self.assertEqual(len(result["mismatch_details"]), 10)  # Truncated to 10
        self.assertEqual(result["total_bus_ids"], 15)
    
    def test_multiple_scenarios_grouping(self):
        """Test that data is properly grouped by bus_id and scenario"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.7},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.2},
            {"bus_id": 1001, "scenario": "eGon100RE", "profile_share": 0.8},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.4},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.6}
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035", "eGon100RE"])
        
        self.assertEqual(len(results), 2)  # 2 scenarios
        
        # Check eGon2035 (2 bus_ids)
        egon2035_result = next(r for r in results if r["scenario"] == "eGon2035")
        self.assertEqual(egon2035_result["total_bus_ids"], 2)
        self.assertEqual(egon2035_result["status"], "SUCCESS")
        
        # Check eGon100RE (1 bus_id)
        egon100re_result = next(r for r in results if r["scenario"] == "eGon100RE")
        self.assertEqual(egon100re_result["total_bus_ids"], 1)
        self.assertEqual(egon100re_result["status"], "SUCCESS")
    
    def test_single_share_per_bus(self):
        """Test handling of bus_ids with only one share (should not sum to 1.0)"""
        mock_data = [
            {"bus_id": 1001, "scenario": "eGon2035", "profile_share": 0.5},  # Only one share, should fail
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.3},
            {"bus_id": 1002, "scenario": "eGon2035", "profile_share": 0.7}   # Two shares, should pass
        ]
        
        results = self.rule._validate_demand_share_consistency(mock_data, 1e-5, ["eGon2035"])
        
        self.assertEqual(len(results), 1)
        result = results[0]
        
        self.assertEqual(result["status"], "CRITICAL_FAILURE")
        self.assertEqual(result["mismatches"], 1)  # Only bus_id 1001 has mismatch
        self.assertEqual(result["total_bus_ids"], 2)
        self.assertEqual(result["mismatch_details"][0]["bus_id"], 1001)
        self.assertEqual(result["mismatch_details"][0]["share_sum"], 0.5)
        self.assertEqual(result["mismatch_details"][0]["num_shares"], 1)


if __name__ == "__main__":
    unittest.main()