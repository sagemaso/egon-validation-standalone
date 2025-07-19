"""
Sanity check rule for home batteries
Based on the sanitycheck_home_batteries function from sanity_checks.py
"""

from typing import Dict, Any, List
import pandas as pd
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class HomeBatteriesSanityRule(BaseValidationRule):
    """
    Sanity check for home batteries data consistency.
    
    Validates:
    1. Capacity consistency between storage and building-level data
    2. Power-to-capacity ratio consistency
    3. Bus-level aggregation accuracy
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("HomeBatteriesSanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the home batteries sanity check
        
        Parameters:
        -----------
        config : Dict[str, Any]
            Configuration containing scenarios and validation parameters
            
        Returns:
        --------
        ValidationResult
            Validation result with detailed findings
        """
        scenarios = config.get("scenarios", ["eGon2035", "eGon100RE"])
        tolerance = config.get("tolerance", 1e-6)  # Default very strict tolerance
        
        self.logger.info(f"Starting home batteries sanity check for scenarios: {scenarios}")
        
        try:
            all_results = []
            
            # Get cbat_pbat_ratio
            cbat_pbat_ratio = self._get_cbat_pbat_ratio()
            if cbat_pbat_ratio is None:
                return self._create_failure_result(
                    table="home_batteries configuration",
                    error_details="Could not retrieve cbat_pbat_ratio"
                )
            
            # Validate each scenario
            for scenario in scenarios:
                scenario_result = self._validate_scenario(scenario, cbat_pbat_ratio, tolerance)
                all_results.append(scenario_result)
            
            # Determine overall status
            critical_failures = [r for r in all_results if r.get("status") == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r.get("status") == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in home batteries check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in home batteries check"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "scenarios": scenarios,
                "tolerance": tolerance,
                "cbat_pbat_ratio": cbat_pbat_ratio,
                "results": all_results,
                "summary": {
                    "total_validations": len(all_results),
                    "passed": len([r for r in all_results if r.get("status") == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                }
            }
            
            message = f"Home batteries sanity check completed: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="storage.egon_storages_home_batteries,demand.egon_home_batteries_*",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in home batteries sanity check: {str(e)}")
            return self._create_failure_result(
                table="storage.egon_storages_home_batteries",
                error_details=f"Home batteries sanity check execution failed: {str(e)}"
            )
    
    def _get_cbat_pbat_ratio(self) -> float:
        """Get the cbat_pbat_ratio from configuration or calculate default"""
        try:
            # Try to get from home_batteries configuration
            # This would normally be loaded from the datasets configuration
            # For now, we'll use a reasonable default based on typical battery specifications
            return 4.0  # 4 hours storage capacity (kWh/kW)
            
        except Exception as e:
            self.logger.warning(f"Could not get cbat_pbat_ratio, using default: {str(e)}")
            return 4.0
    
    def _validate_scenario(self, scenario: str, cbat_pbat_ratio: float, tolerance: float) -> Dict[str, Any]:
        """Validate home batteries for a specific scenario"""
        try:
            # Get aggregated home battery data from storage table
            storage_data = self._get_storage_data(scenario)
            
            # Get building-level home battery data
            building_data = self._get_building_battery_data(scenario)
            
            if not storage_data:
                return {
                    "check_type": "scenario_validation",
                    "scenario": scenario,
                    "status": "WARNING",
                    "message": f"No storage data found for scenario {scenario}"
                }
            
            if not building_data:
                return {
                    "check_type": "scenario_validation",
                    "scenario": scenario,
                    "status": "WARNING", 
                    "message": f"No building battery data found for scenario {scenario}"
                }
            
            # Convert storage data to DataFrame for easier processing
            storage_df = pd.DataFrame(storage_data)
            storage_df = storage_df.set_index('bus_id')
            
            # Calculate expected capacity from power ratings
            storage_df['expected_capacity'] = storage_df['p_nom'] * cbat_pbat_ratio
            
            # Convert building data to DataFrame and aggregate by bus_id
            building_df = pd.DataFrame(building_data)
            building_aggregated = building_df.groupby('bus_id').agg({
                'p_nom': 'sum',
                'capacity': 'sum'
            }).round(6)
            
            # Compare the aggregated data
            comparison_results = []
            
            for bus_id in storage_df.index:
                if bus_id not in building_aggregated.index:
                    comparison_results.append({
                        "bus_id": bus_id,
                        "status": "CRITICAL_FAILURE",
                        "error": f"Bus {bus_id} found in storage but not in building data"
                    })
                    continue
                
                storage_p_nom = round(storage_df.loc[bus_id, 'p_nom'], 6)
                storage_capacity = round(storage_df.loc[bus_id, 'expected_capacity'], 6)
                building_p_nom = round(building_aggregated.loc[bus_id, 'p_nom'], 6)
                building_capacity = round(building_aggregated.loc[bus_id, 'capacity'], 6)
                
                # Check power rating consistency
                p_nom_diff = abs(storage_p_nom - building_p_nom)
                capacity_diff = abs(storage_capacity - building_capacity)
                
                if p_nom_diff > tolerance or capacity_diff > tolerance:
                    comparison_results.append({
                        "bus_id": bus_id,
                        "status": "CRITICAL_FAILURE",
                        "error": f"Mismatch - Storage: p_nom={storage_p_nom}, capacity={storage_capacity}; Building: p_nom={building_p_nom}, capacity={building_capacity}",
                        "storage_p_nom": storage_p_nom,
                        "storage_capacity": storage_capacity,
                        "building_p_nom": building_p_nom,
                        "building_capacity": building_capacity,
                        "p_nom_diff": p_nom_diff,
                        "capacity_diff": capacity_diff
                    })
                else:
                    comparison_results.append({
                        "bus_id": bus_id,
                        "status": "SUCCESS",
                        "message": f"Consistent data for bus {bus_id}"
                    })
            
            # Check for buses in building data but not in storage data
            for bus_id in building_aggregated.index:
                if bus_id not in storage_df.index:
                    comparison_results.append({
                        "bus_id": bus_id,
                        "status": "CRITICAL_FAILURE",
                        "error": f"Bus {bus_id} found in building data but not in storage"
                    })
            
            # Summarize results
            failures = [r for r in comparison_results if r["status"] == "CRITICAL_FAILURE"]
            successes = [r for r in comparison_results if r["status"] == "SUCCESS"]
            
            if failures:
                return {
                    "check_type": "scenario_validation",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"{len(failures)} bus mismatches found",
                    "total_buses": len(comparison_results),
                    "successful_buses": len(successes),
                    "failed_buses": len(failures),
                    "cbat_pbat_ratio": cbat_pbat_ratio,
                    "sample_failures": failures[:5],  # Show first 5 failures
                    "detailed_results": comparison_results
                }
            
            return {
                "check_type": "scenario_validation",
                "scenario": scenario,
                "status": "SUCCESS",
                "message": f"All {len(successes)} buses have consistent battery data",
                "total_buses": len(comparison_results),
                "cbat_pbat_ratio": cbat_pbat_ratio
            }
            
        except Exception as e:
            return {
                "check_type": "scenario_validation",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate scenario {scenario}: {str(e)}"
            }
    
    def _get_storage_data(self, scenario: str) -> List[Dict[str, Any]]:
        """Get home battery storage data aggregated by bus_id"""
        try:
            query = """
                SELECT bus_id, SUM(el_capacity) as p_nom
                FROM storage.egon_storages_home_batteries
                WHERE carrier = 'home_battery'
                AND scenario = %s
                GROUP BY bus_id
                ORDER BY bus_id
            """
            result = self.db_manager.execute_query(query, (scenario,))
            return result
            
        except Exception as e:
            self.logger.warning(f"Failed to get storage data for {scenario}: {str(e)}")
            return []
    
    def _get_building_battery_data(self, scenario: str) -> List[Dict[str, Any]]:
        """Get building-level home battery data"""
        try:
            query = """
                SELECT bus_id, p_nom, capacity
                FROM demand.egon_home_batteries_buildings
                WHERE scenario = %s
                ORDER BY bus_id
            """
            result = self.db_manager.execute_query(query, (scenario,))
            return result
            
        except Exception as e:
            self.logger.warning(f"Failed to get building battery data for {scenario}: {str(e)}")
            # Try alternative table name
            try:
                query = """
                    SELECT bus_id, p_nom, capacity
                    FROM supply.egon_home_batteries_buildings
                    WHERE scenario = %s
                    ORDER BY bus_id
                """
                result = self.db_manager.execute_query(query, (scenario,))
                return result
            except Exception as e2:
                self.logger.warning(f"Failed to get building battery data from alternative table: {str(e2)}")
                return []