"""
Sanity check rule for residential electricity annual sum validation
Based on the residential_electricity_annual_sum function from sanity_checks.py
"""

from typing import Dict, Any, List
import numpy as np
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class ResidentialElectricityAnnualSumRule(BaseValidationRule):
    """
    Sanity check for residential electricity demand aggregation consistency.
    
    Validates that the annual demand of all census cells at NUTS3 level matches
    the initial scaling parameters from DemandRegio by comparing:
    - Aggregated demand from demand.egon_demandregio_zensus_electricity
    - Original demand from demand.egon_demandregio_hh
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("ResidentialElectricityAnnualSumCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
    
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the residential electricity annual sum validation
        
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
        scenarios = config.get("scenarios", ["eGon2035", "eGon100RE"])
        
        self.logger.info(f"Starting residential electricity annual sum validation")
        
        try:
            # Validate for each scenario
            validation_results = []
            
            for scenario in scenarios:
                self.logger.info(f"Validating scenario: {scenario}")
                result = self._validate_scenario(scenario, tolerance)
                validation_results.append(result)
            
            # Determine overall status
            critical_failures = [r for r in validation_results if r["status"] == "CRITICAL_FAILURE"]
            warnings = [r for r in validation_results if r["status"] == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in residential electricity validation"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in residential electricity validation"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "tolerance": tolerance,
                "scenarios": scenarios,
                "validation_results": validation_results,
                "summary": {
                    "total_scenarios": len(validation_results),
                    "passed": len([r for r in validation_results if r["status"] == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                }
            }
            
            message = f"Residential electricity annual sum validation completed: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_scenarios']} scenarios passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="demand.egon_demandregio_zensus_electricity,demand.egon_demandregio_hh",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in residential electricity annual sum validation: {str(e)}")
            return self._create_failure_result(
                table="demand.egon_demandregio_*",
                error_details=f"Residential electricity validation failed: {str(e)}"
            )
    
    def _validate_scenario(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate residential electricity annual sum for a specific scenario"""
        
        try:
            # Get aggregated data from census electricity table
            census_query = """
                SELECT dr.nuts3, dr.scenario, dr.demand_regio_sum, profiles.profile_sum
                FROM (
                    SELECT scenario, SUM(demand) AS profile_sum, vg250_nuts3
                    FROM demand.egon_demandregio_zensus_electricity AS egon,
                         boundaries.egon_map_zensus_vg250 AS boundaries
                    WHERE egon.zensus_population_id = boundaries.zensus_population_id
                    AND sector = 'residential'
                    AND scenario = %s
                    GROUP BY vg250_nuts3, scenario
                ) AS profiles
                JOIN (
                    SELECT nuts3, scenario, sum(demand) AS demand_regio_sum
                    FROM demand.egon_demandregio_hh
                    WHERE scenario = %s
                    GROUP BY year, scenario, nuts3
                ) AS dr
                ON profiles.vg250_nuts3 = dr.nuts3 
                AND profiles.scenario = dr.scenario
            """
            
            result = self.db_manager.execute_query(census_query, (scenario, scenario))
            
            if not result:
                return {
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"No data found for scenario {scenario}",
                    "nuts3_mismatches": 0,
                    "total_nuts3": 0
                }
            
            # Extract profile sums and demand regio sums
            profile_sums = []
            demand_regio_sums = []
            nuts3_codes = []
            
            for row in result:
                profile_sums.append(float(row['profile_sum']))
                demand_regio_sums.append(float(row['demand_regio_sum']))
                nuts3_codes.append(row['nuts3'])
            
            # Convert to numpy arrays for comparison
            profile_sums = np.array(profile_sums)
            demand_regio_sums = np.array(demand_regio_sums)
            
            # Check if arrays are close within tolerance
            try:
                np.testing.assert_allclose(
                    actual=profile_sums,
                    desired=demand_regio_sums,
                    rtol=tolerance,
                    verbose=False
                )
                
                # If we get here, validation passed
                return {
                    "scenario": scenario,
                    "status": "SUCCESS",
                    "message": f"Aggregated annual residential electricity demand matches with DemandRegio at NUTS-3 for scenario {scenario}",
                    "nuts3_mismatches": 0,
                    "total_nuts3": len(nuts3_codes),
                    "tolerance": tolerance,
                    "total_profile_sum": float(np.sum(profile_sums)),
                    "total_demand_regio_sum": float(np.sum(demand_regio_sums))
                }
                
            except AssertionError as e:
                # Find which NUTS3 regions don't match
                mismatches = []
                for i, (nuts3, profile, demand) in enumerate(zip(nuts3_codes, profile_sums, demand_regio_sums)):
                    if not np.allclose(profile, demand, rtol=tolerance):
                        relative_error = abs(profile - demand) / demand if demand != 0 else float('inf')
                        mismatches.append({
                            "nuts3": nuts3,
                            "profile_sum": float(profile),
                            "demand_regio_sum": float(demand),
                            "relative_error": float(relative_error)
                        })
                
                return {
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Aggregated residential electricity demand does not match DemandRegio at NUTS-3 for scenario {scenario}",
                    "nuts3_mismatches": len(mismatches),
                    "total_nuts3": len(nuts3_codes),
                    "tolerance": tolerance,
                    "mismatch_details": mismatches[:10],  # Limit to first 10 mismatches
                    "total_profile_sum": float(np.sum(profile_sums)),
                    "total_demand_regio_sum": float(np.sum(demand_regio_sums))
                }
            
        except Exception as e:
            return {
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate scenario {scenario}: {str(e)}",
                "nuts3_mismatches": None,
                "total_nuts3": None
            }