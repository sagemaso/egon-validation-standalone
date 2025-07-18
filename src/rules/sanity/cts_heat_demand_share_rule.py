"""
Sanity check rule for CTS heat demand share validation
Based on the cts_heat_demand_share function from sanity_checks.py
"""

from typing import Dict, Any, List
import numpy as np
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class CtsHeatDemandShareRule(BaseValidationRule):
    """
    Sanity check for CTS heat demand share consistency.
    
    Validates that the sum of aggregated CTS heat demand shares equals 1.0
    for every substation, as the substation profile is linearly disaggregated
    to all buildings.
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("CtsHeatDemandShareCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
    
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the CTS heat demand share validation
        
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
        
        self.logger.info(f"Starting CTS heat demand share validation")
        
        try:
            # Get CTS heat demand share data
            demand_share_data = self._get_cts_heat_demand_share_data()
            
            if not demand_share_data:
                return self._create_failure_result(
                    table="demand.egon_cts_heat_demand_building_share",
                    error_details="No CTS heat demand share data found"
                )
            
            # Validate demand share consistency
            validation_results = self._validate_demand_share_consistency(demand_share_data, tolerance, scenarios)
            
            # Determine overall status
            critical_failures = [r for r in validation_results if r["status"] == "CRITICAL_FAILURE"]
            warnings = [r for r in validation_results if r["status"] == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in CTS heat demand share validation"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in CTS heat demand share validation"
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
                },
                "data_summary": {
                    "total_records": len(demand_share_data),
                    "unique_bus_ids": len(set(row["bus_id"] for row in demand_share_data)),
                    "unique_scenarios": len(set(row["scenario"] for row in demand_share_data))
                }
            }
            
            message = f"CTS heat demand share validation completed: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_scenarios']} scenarios passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="demand.egon_cts_heat_demand_building_share",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.logger.error(f"Error in CTS heat demand share validation: {str(e)}")
            return self._create_failure_result(
                table="demand.egon_cts_heat_demand_building_share",
                error_details=f"CTS heat demand share validation failed: {str(e)}"
            )
    
    def _get_cts_heat_demand_share_data(self) -> List[Dict[str, Any]]:
        """Get CTS heat demand share data from database"""
        
        # Note: The original function references EgonCtsHeatDemandBuildingShare
        # We'll use a direct SQL query to get the data
        query = """
            SELECT bus_id, scenario, profile_share
            FROM demand.egon_cts_heat_demand_building_share
            ORDER BY bus_id, scenario
        """
        
        try:
            result = self.db_manager.execute_query(query)
            return result
        except Exception as e:
            self.logger.logger.error(f"Failed to get CTS heat demand share data: {str(e)}")
            return []
    
    def _validate_demand_share_consistency(self, demand_share_data: List[Dict[str, Any]], tolerance: float, scenarios: List[str]) -> List[Dict[str, Any]]:
        """Validate that demand shares sum to 1.0 for each bus_id and scenario"""
        
        # Group data by bus_id and scenario
        grouped_data = {}
        try:
            for row in demand_share_data:
                bus_id = row["bus_id"]
                scenario = row["scenario"]
                key = (bus_id, scenario)
                
                if key not in grouped_data:
                    grouped_data[key] = []
                
                grouped_data[key].append(float(row["profile_share"]))
        except (ValueError, TypeError) as e:
            # Handle data conversion errors
            return [{"scenario": scenario, "status": "CRITICAL_FAILURE", "error": f"Failed to validate scenario {scenario}: {str(e)}", "mismatches": None, "total_bus_ids": 0} for scenario in scenarios]
        
        # Validate for each requested scenario
        results = []
        
        for scenario in scenarios:
            # Filter grouped data for this scenario
            scenario_data = {k: v for k, v in grouped_data.items() if k[1] == scenario}
            
            if not scenario_data:
                results.append({
                    "scenario": scenario,
                    "status": "WARNING",
                    "message": f"No data found for scenario {scenario}",
                    "mismatches": 0,
                    "total_bus_ids": 0,
                    "tolerance": tolerance
                })
                continue
            
            try:
                # Check if sums equal 1.0 for each bus_id
                mismatches = []
                total_bus_ids = len(scenario_data)
                
                for (bus_id, scen), shares in scenario_data.items():
                    share_sum = sum(shares)
                    
                    if not np.allclose(share_sum, 1.0, rtol=tolerance):
                        relative_error = abs(share_sum - 1.0)
                        mismatches.append({
                            "bus_id": bus_id,
                            "share_sum": share_sum,
                            "expected_sum": 1.0,
                            "relative_error": relative_error,
                            "num_shares": len(shares)
                        })
                
                if mismatches:
                    results.append({
                        "scenario": scenario,
                        "status": "CRITICAL_FAILURE",
                        "error": f"Heat demand shares do not sum to 1.0 for scenario {scenario}",
                        "mismatches": len(mismatches),
                        "total_bus_ids": total_bus_ids,
                        "tolerance": tolerance,
                        "mismatch_details": mismatches[:10]  # Limit to first 10 mismatches
                    })
                else:
                    results.append({
                        "scenario": scenario,
                        "status": "SUCCESS",
                        "message": f"All aggregated heat demand shares equal 1.0 for scenario {scenario}",
                        "mismatches": 0,
                        "total_bus_ids": total_bus_ids,
                        "tolerance": tolerance
                    })
                    
            except Exception as e:
                results.append({
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Failed to validate scenario {scenario}: {str(e)}",
                    "mismatches": None,
                    "total_bus_ids": len(scenario_data) if scenario_data else 0
                })
        
        return results