"""
Sanity check rule for residential electricity household refinement validation
Based on the residential_electricity_hh_refinement function from sanity_checks.py
"""

from typing import Dict, Any, List
import numpy as np
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class ResidentialElectricityHhRefinementRule(BaseValidationRule):
    """
    Sanity check for residential electricity household refinement consistency.
    
    Validates that the sum of aggregated household types after refinement method
    matches the original census values by comparing:
    - Refined household types from society.egon_destatis_zensus_household_per_ha_refined
    - Original census household types (before refinement)
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("ResidentialElectricityHhRefinementCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
    
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the household refinement validation
        
        Parameters:
        -----------
        config : Dict[str, Any]
            Configuration containing validation parameters
            
        Returns:
        --------
        ValidationResult
            Validation result with detailed findings
        """
        tolerance = config.get("tolerance", 1e-5)  # Default relative tolerance
        
        self.logger.info(f"Starting residential electricity household refinement validation")
        
        try:
            # Get refinement comparison data
            refinement_data = self._get_refinement_data()
            
            if not refinement_data:
                return self._create_failure_result(
                    table="society.egon_destatis_zensus_household_per_ha_refined",
                    error_details="No refinement data found"
                )
            
            # Validate refinement consistency
            validation_results = self._validate_refinement_consistency(refinement_data, tolerance)
            
            # Determine overall status
            critical_failures = [r for r in validation_results if r["status"] == "CRITICAL_FAILURE"]
            warnings = [r for r in validation_results if r["status"] == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in household refinement validation"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in household refinement validation"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "tolerance": tolerance,
                "validation_results": validation_results,
                "summary": {
                    "total_characteristics": len(validation_results),
                    "passed": len([r for r in validation_results if r["status"] == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                },
                "data_summary": {
                    "total_refined_records": len(refinement_data),
                    "unique_nuts3_regions": len(set(row["nuts3"] for row in refinement_data)),
                    "unique_characteristics": len(set(row["characteristics_code"] for row in refinement_data))
                }
            }
            
            message = f"Household refinement validation completed: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_characteristics']} characteristics passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="society.egon_destatis_zensus_household_per_ha_refined",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.logger.error(f"Error in household refinement validation: {str(e)}")
            return self._create_failure_result(
                table="society.egon_destatis_zensus_household_per_ha_refined",
                error_details=f"Household refinement validation failed: {str(e)}"
            )
    
    def _get_refinement_data(self) -> List[Dict[str, Any]]:
        """Get refinement comparison data from database"""
        
        query = """
            SELECT refined.nuts3, refined.characteristics_code,
                   refined.sum_refined::int, census.sum_census::int
            FROM (
                SELECT nuts3, characteristics_code, SUM(hh_10types) as sum_refined
                FROM society.egon_destatis_zensus_household_per_ha_refined
                GROUP BY nuts3, characteristics_code
            ) AS refined
            JOIN (
                SELECT t.nuts3, t.characteristics_code, sum(orig) as sum_census
                FROM (
                    SELECT nuts3, cell_id, characteristics_code,
                           sum(DISTINCT(hh_5types)) as orig
                    FROM society.egon_destatis_zensus_household_per_ha_refined
                    GROUP BY cell_id, characteristics_code, nuts3
                ) AS t
                GROUP BY t.nuts3, t.characteristics_code
            ) AS census
            ON refined.nuts3 = census.nuts3
            AND refined.characteristics_code = census.characteristics_code
        """
        
        try:
            result = self.db_manager.execute_query(query)
            return result
        except Exception as e:
            self.logger.logger.error(f"Failed to get refinement data: {str(e)}")
            return []
    
    def _validate_refinement_consistency(self, refinement_data: List[Dict[str, Any]], tolerance: float) -> List[Dict[str, Any]]:
        """Validate that refined household types match original census data"""
        
        # Group data by characteristics_code
        characteristics_groups = {}
        for row in refinement_data:
            char_code = row["characteristics_code"]
            if char_code not in characteristics_groups:
                characteristics_groups[char_code] = {
                    "refined_sums": [],
                    "census_sums": [],
                    "nuts3_codes": []
                }
            
            characteristics_groups[char_code]["refined_sums"].append(row["sum_refined"])
            characteristics_groups[char_code]["census_sums"].append(row["sum_census"])
            characteristics_groups[char_code]["nuts3_codes"].append(row["nuts3"])
        
        results = []
        
        for char_code, data in characteristics_groups.items():
            try:
                # Convert to numpy arrays for comparison
                refined_sums = np.array(data["refined_sums"])
                census_sums = np.array(data["census_sums"])
                
                # Check if arrays are close within tolerance
                try:
                    np.testing.assert_allclose(
                        actual=refined_sums,
                        desired=census_sums,
                        rtol=tolerance,
                        verbose=False
                    )
                    
                    # If we get here, validation passed
                    results.append({
                        "characteristics_code": char_code,
                        "status": "SUCCESS",
                        "message": f"Aggregated household types match at NUTS-3 for characteristics {char_code}",
                        "mismatches": 0,
                        "total_nuts3": len(data["nuts3_codes"]),
                        "tolerance": tolerance,
                        "total_refined_sum": int(np.sum(refined_sums)),
                        "total_census_sum": int(np.sum(census_sums))
                    })
                    
                except AssertionError:
                    # Find which NUTS3 regions don't match
                    mismatches = []
                    for i, (nuts3, refined, census) in enumerate(zip(data["nuts3_codes"], refined_sums, census_sums)):
                        if not np.allclose(refined, census, rtol=tolerance):
                            relative_error = abs(refined - census) / census if census != 0 else float('inf')
                            mismatches.append({
                                "nuts3": nuts3,
                                "refined_sum": int(refined),
                                "census_sum": int(census),
                                "relative_error": float(relative_error)
                            })
                    
                    results.append({
                        "characteristics_code": char_code,
                        "status": "CRITICAL_FAILURE",
                        "error": f"Aggregated household types do not match at NUTS-3 for characteristics {char_code}",
                        "mismatches": len(mismatches),
                        "total_nuts3": len(data["nuts3_codes"]),
                        "tolerance": tolerance,
                        "mismatch_details": mismatches[:5],  # Limit to first 5 mismatches
                        "total_refined_sum": int(np.sum(refined_sums)),
                        "total_census_sum": int(np.sum(census_sums))
                    })
                    
            except Exception as e:
                results.append({
                    "characteristics_code": char_code,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Failed to validate characteristics {char_code}: {str(e)}",
                    "mismatches": None,
                    "total_nuts3": len(data["nuts3_codes"]) if data["nuts3_codes"] else 0
                })
        
        return results