"""
Test for ResidentialElectricityHhRefinementRule
"""

import unittest
from unittest.mock import Mock, patch
import numpy as np
from src.rules.sanity.residential_electricity_hh_refinement_rule import ResidentialElectricityHhRefinementRule
from src.core.database_manager import DatabaseManager


class TestResidentialElectricityHhRefinementRule(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.rule = ResidentialElectricityHhRefinementRule(self.mock_db_manager)
        
    def test_get_refinement_data_success(self):
        """Test successful retrieval of refinement data"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000},
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1500},
            {"nuts3": "DE111", "characteristics_code": "HHTYP_2", "sum_refined": 800, "sum_census": 800}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        result = self.rule._get_refinement_data()
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0]["nuts3"], "DE111")
        self.assertEqual(result[0]["characteristics_code"], "HHTYP_1")
    
    def test_get_refinement_data_database_error(self):
        """Test refinement data retrieval with database error"""
        self.mock_db_manager.execute_query.side_effect = Exception("Database error")
        
        result = self.rule._get_refinement_data()
        
        self.assertEqual(result, [])
    
    def test_validate_refinement_consistency_success(self):
        """Test refinement consistency validation with matching data"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000},
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1500},
            {"nuts3": "DE111", "characteristics_code": "HHTYP_2", "sum_refined": 800, "sum_census": 800}
        ]
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-5)
        
        self.assertEqual(len(results), 2)  # 2 unique characteristics codes
        for result in results:
            self.assertEqual(result["status"], "SUCCESS")
            self.assertEqual(result["mismatches"], 0)
            self.assertIn("match at NUTS-3", result["message"])
    
    def test_validate_refinement_consistency_with_mismatches(self):
        """Test refinement consistency validation with mismatching data"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000},  # Match
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1600},  # Mismatch
            {"nuts3": "DE111", "characteristics_code": "HHTYP_2", "sum_refined": 800, "sum_census": 800}     # Match
        ]
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-5)
        
        self.assertEqual(len(results), 2)
        
        # Check HHTYP_1 (has mismatch)
        hhtyp1_result = next(r for r in results if r["characteristics_code"] == "HHTYP_1")
        self.assertEqual(hhtyp1_result["status"], "CRITICAL_FAILURE")
        self.assertEqual(hhtyp1_result["mismatches"], 1)
        self.assertIn("do not match", hhtyp1_result["error"])
        
        # Check HHTYP_2 (matches)
        hhtyp2_result = next(r for r in results if r["characteristics_code"] == "HHTYP_2")
        self.assertEqual(hhtyp2_result["status"], "SUCCESS")
        self.assertEqual(hhtyp2_result["mismatches"], 0)
    
    def test_validate_refinement_consistency_with_tolerance(self):
        """Test refinement consistency validation with tolerance"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1001},  # Within tolerance
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1501}   # Within tolerance
        ]
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-3)  # 0.1% tolerance
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "SUCCESS")
        self.assertEqual(results[0]["mismatches"], 0)
    
    def test_validate_refinement_consistency_exception_handling(self):
        """Test refinement consistency validation with exception"""
        # Create mock data that will cause an exception in numpy operations
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": "invalid", "sum_census": 1000}
        ]
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-5)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "CRITICAL_FAILURE")
        self.assertIn("Failed to validate characteristics", results[0]["error"])
    
    def test_validate_full_success(self):
        """Test full validation with successful refinement"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000},
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1500},
            {"nuts3": "DE111", "characteristics_code": "HHTYP_2", "sum_refined": 800, "sum_census": 800}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        config = {"tolerance": 1e-5}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertIn("2/2 characteristics passed", result.message)
        self.assertEqual(result.detailed_context["summary"]["total_characteristics"], 2)
        self.assertEqual(result.detailed_context["summary"]["passed"], 2)
        self.assertEqual(result.detailed_context["data_summary"]["total_refined_records"], 3)
        self.assertEqual(result.detailed_context["data_summary"]["unique_nuts3_regions"], 2)
        self.assertEqual(result.detailed_context["data_summary"]["unique_characteristics"], 2)
    
    def test_validate_with_failures(self):
        """Test full validation with some failures"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000},  # Success
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1500},  # Success
            {"nuts3": "DE111", "characteristics_code": "HHTYP_2", "sum_refined": 800, "sum_census": 900}    # Failure
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        config = {"tolerance": 1e-5}
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
        self.assertIn("No refinement data found", result.error_details)
    
    def test_validate_with_default_config(self):
        """Test validation with default configuration"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000}
        ]
        
        self.mock_db_manager.execute_query.return_value = mock_data
        
        config = {}  # Use defaults
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.detailed_context["tolerance"], 1e-5)
    
    def test_validate_exception_handling(self):
        """Test validation with exception during execution"""
        self.mock_db_manager.execute_query.side_effect = Exception("Unexpected error")
        
        config = {"tolerance": 1e-5}
        result = self.rule.validate(config)
        
        self.assertEqual(result.status, "CRITICAL_FAILURE")
        self.assertIn("No refinement data found", result.error_details)
    
    def test_mismatch_details_truncation(self):
        """Test that mismatch details are truncated to 5 items"""
        # Create mock data with more than 5 mismatches for one characteristic
        mock_data = []
        for i in range(8):
            mock_data.append({
                "nuts3": f"DE{i:03d}",
                "characteristics_code": "HHTYP_1",
                "sum_refined": 1000,
                "sum_census": 1100  # All mismatches
            })
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-5)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "CRITICAL_FAILURE")
        self.assertEqual(results[0]["mismatches"], 8)
        self.assertEqual(len(results[0]["mismatch_details"]), 5)  # Truncated to 5
    
    def test_zero_census_handling(self):
        """Test handling of zero census values"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 0, "sum_census": 0},     # Both zero
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 100, "sum_census": 0}   # Zero census
        ]
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-5)
        
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "CRITICAL_FAILURE")
        self.assertEqual(results[0]["mismatches"], 1)  # DE112 should be a mismatch
        # Check that the mismatch details handle the zero census case
        self.assertEqual(results[0]["mismatch_details"][0]["relative_error"], float('inf'))
    
    def test_multiple_characteristics_grouping(self):
        """Test that data is properly grouped by characteristics code"""
        mock_data = [
            {"nuts3": "DE111", "characteristics_code": "HHTYP_1", "sum_refined": 1000, "sum_census": 1000},
            {"nuts3": "DE112", "characteristics_code": "HHTYP_1", "sum_refined": 1500, "sum_census": 1500},
            {"nuts3": "DE111", "characteristics_code": "HHTYP_2", "sum_refined": 800, "sum_census": 800},
            {"nuts3": "DE112", "characteristics_code": "HHTYP_2", "sum_refined": 900, "sum_census": 900},
            {"nuts3": "DE111", "characteristics_code": "HHTYP_3", "sum_refined": 600, "sum_census": 600}
        ]
        
        results = self.rule._validate_refinement_consistency(mock_data, 1e-5)
        
        self.assertEqual(len(results), 3)  # 3 unique characteristics codes
        
        # Check that each characteristic has correct totals
        hhtyp1_result = next(r for r in results if r["characteristics_code"] == "HHTYP_1")
        self.assertEqual(hhtyp1_result["total_nuts3"], 2)
        self.assertEqual(hhtyp1_result["total_refined_sum"], 2500)
        self.assertEqual(hhtyp1_result["total_census_sum"], 2500)
        
        hhtyp2_result = next(r for r in results if r["characteristics_code"] == "HHTYP_2")
        self.assertEqual(hhtyp2_result["total_nuts3"], 2)
        self.assertEqual(hhtyp2_result["total_refined_sum"], 1700)
        self.assertEqual(hhtyp2_result["total_census_sum"], 1700)
        
        hhtyp3_result = next(r for r in results if r["characteristics_code"] == "HHTYP_3")
        self.assertEqual(hhtyp3_result["total_nuts3"], 1)
        self.assertEqual(hhtyp3_result["total_refined_sum"], 600)
        self.assertEqual(hhtyp3_result["total_census_sum"], 600)


if __name__ == "__main__":
    unittest.main()