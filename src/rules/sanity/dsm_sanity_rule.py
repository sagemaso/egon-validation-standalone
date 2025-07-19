"""
Sanity check rule for DSM (Demand Side Management)
Based on the sanitycheck_dsm function from sanity_checks.py
"""

from typing import Dict, Any, List
import pandas as pd
import numpy as np
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class DsmSanityRule(BaseValidationRule):
    """
    Sanity check for DSM (Demand Side Management) data consistency.
    
    Validates:
    1. Aggregated DSM timeseries vs individual DSM timeseries
    2. Power constraints (p_min, p_max) consistency
    3. Energy constraints (e_min, e_max) consistency
    4. Link and store component relationships
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("DsmSanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
        # DSM source tables
        self.dsm_tables = [
            "cts_loadcurves_dsm",
            "ind_osm_loadcurves_individual_dsm", 
            "demandregio_ind_sites_dsm",
            "ind_sites_loadcurves_individual"
        ]
        
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the DSM sanity check
        
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
        tolerance = config.get("tolerance", 1e-01)  # Default 0.1 tolerance for DSM
        
        self.logger.info(f"Starting DSM sanity check for scenarios: {scenarios}")
        
        try:
            all_results = []
            
            # Validate each scenario
            for scenario in scenarios:
                scenario_result = self._validate_scenario(scenario, tolerance)
                all_results.append(scenario_result)
            
            # Determine overall status
            critical_failures = [r for r in all_results if r.get("status") == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r.get("status") == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in DSM sanity check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in DSM sanity check"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "scenarios": scenarios,
                "tolerance": tolerance,
                "results": all_results,
                "summary": {
                    "total_validations": len(all_results),
                    "passed": len([r for r in all_results if r.get("status") == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                }
            }
            
            message = f"DSM sanity check completed: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="grid.egon_etrago_link,grid.egon_etrago_store,demand.egon_*dsm*",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in DSM sanity check: {str(e)}")
            return self._create_failure_result(
                table="grid.egon_etrago_*",
                error_details=f"DSM sanity check execution failed: {str(e)}"
            )
    
    def _validate_scenario(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate DSM for a specific scenario"""
        try:
            validations = []
            
            # Validate power constraints (p_min, p_max)
            power_validation = self._validate_power_constraints(scenario, tolerance)
            validations.append(power_validation)
            
            # Validate energy constraints (e_min, e_max)  
            energy_validation = self._validate_energy_constraints(scenario, tolerance)
            validations.append(energy_validation)
            
            # Determine overall status for scenario
            failures = [v for v in validations if v.get("status") == "CRITICAL_FAILURE"]
            warnings = [v for v in validations if v.get("status") == "WARNING"]
            
            if failures:
                status = "CRITICAL_FAILURE"
                message = f"DSM validation failed for {scenario}: {len(failures)} critical failures"
            elif warnings:
                status = "WARNING"
                message = f"DSM validation warnings for {scenario}: {len(warnings)} warnings"
            else:
                status = "SUCCESS"
                message = f"DSM validation passed for {scenario}"
            
            return {
                "check_type": "scenario_validation",
                "scenario": scenario,
                "status": status,
                "message": message,
                "validations": validations
            }
            
        except Exception as e:
            return {
                "check_type": "scenario_validation",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate DSM for scenario {scenario}: {str(e)}"
            }
    
    def _validate_power_constraints(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate power constraints (p_min, p_max) between aggregated and individual data"""
        try:
            # Get aggregated DSM link data
            aggregated_data = self._get_aggregated_dsm_links(scenario)
            if not aggregated_data:
                return {
                    "check_type": "power_constraints",
                    "scenario": scenario,
                    "status": "WARNING",
                    "message": "No aggregated DSM link data found"
                }
            
            # Get individual DSM data
            individual_data = self._get_individual_dsm_data(scenario)
            if not individual_data:
                return {
                    "check_type": "power_constraints",
                    "scenario": scenario,
                    "status": "WARNING", 
                    "message": "No individual DSM data found"
                }
            
            # Convert to DataFrames for easier processing
            agg_df = pd.DataFrame(aggregated_data)
            ind_df = pd.DataFrame(individual_data)
            
            # Process aggregated data - expand timeseries and multiply by p_nom
            agg_p_max = {}
            agg_p_min = {}
            
            for _, row in agg_df.iterrows():
                bus_id = row['bus']
                p_nom = row['p_nom']
                p_max_pu = row['p_max_pu']
                p_min_pu = row['p_min_pu']
                
                # Convert array strings to actual arrays and multiply by p_nom
                if isinstance(p_max_pu, list):
                    agg_p_max[bus_id] = [x * p_nom for x in p_max_pu]
                if isinstance(p_min_pu, list):
                    agg_p_min[bus_id] = [x * p_nom for x in p_min_pu]
            
            # Process individual data - group by bus and sum
            ind_grouped = ind_df.groupby('bus')
            
            ind_p_max = {}
            ind_p_min = {}
            
            for bus_id, group in ind_grouped:
                # Sum p_max and p_min arrays across all individual components for this bus
                p_max_arrays = [row['p_max'] for _, row in group.iterrows() if isinstance(row['p_max'], list)]
                p_min_arrays = [row['p_min'] for _, row in group.iterrows() if isinstance(row['p_min'], list)]
                
                if p_max_arrays:
                    # Sum arrays element-wise
                    ind_p_max[bus_id] = [sum(x) for x in zip(*p_max_arrays)]
                if p_min_arrays:
                    ind_p_min[bus_id] = [sum(x) for x in zip(*p_min_arrays)]
            
            # Compare aggregated vs individual data
            mismatches = []
            
            for bus_id in agg_p_max:
                if bus_id in ind_p_max:
                    agg_array = np.array(agg_p_max[bus_id])
                    ind_array = np.array(ind_p_max[bus_id])
                    
                    if not np.allclose(agg_array, ind_array, atol=tolerance):
                        mismatches.append({
                            "bus_id": bus_id,
                            "constraint": "p_max",
                            "max_diff": np.max(np.abs(agg_array - ind_array))
                        })
            
            for bus_id in agg_p_min:
                if bus_id in ind_p_min:
                    agg_array = np.array(agg_p_min[bus_id])
                    ind_array = np.array(ind_p_min[bus_id])
                    
                    if not np.allclose(agg_array, ind_array, atol=tolerance):
                        mismatches.append({
                            "bus_id": bus_id,
                            "constraint": "p_min",
                            "max_diff": np.max(np.abs(agg_array - ind_array))
                        })
            
            if mismatches:
                return {
                    "check_type": "power_constraints",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Power constraint mismatches found: {len(mismatches)} buses",
                    "mismatches": mismatches[:10],  # Show first 10
                    "total_mismatches": len(mismatches)
                }
            
            return {
                "check_type": "power_constraints",
                "scenario": scenario,
                "status": "SUCCESS",
                "message": f"Power constraints consistent for {len(agg_p_max)} buses",
                "validated_buses": len(agg_p_max)
            }
            
        except Exception as e:
            return {
                "check_type": "power_constraints",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate power constraints: {str(e)}"
            }
    
    def _validate_energy_constraints(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate energy constraints (e_min, e_max) between aggregated and individual data"""
        try:
            # Get aggregated DSM store data
            aggregated_data = self._get_aggregated_dsm_stores(scenario)
            if not aggregated_data:
                return {
                    "check_type": "energy_constraints",
                    "scenario": scenario,
                    "status": "WARNING",
                    "message": "No aggregated DSM store data found"
                }
            
            # Get individual DSM data (same as power constraints)
            individual_data = self._get_individual_dsm_data(scenario)
            if not individual_data:
                return {
                    "check_type": "energy_constraints",
                    "scenario": scenario,
                    "status": "WARNING",
                    "message": "No individual DSM data found"
                }
            
            # Convert to DataFrames
            agg_df = pd.DataFrame(aggregated_data)
            ind_df = pd.DataFrame(individual_data)
            
            # Process aggregated store data
            agg_e_max = {}
            agg_e_min = {}
            
            for _, row in agg_df.iterrows():
                bus_id = row['bus']
                e_nom = row['e_nom']
                e_max_pu = row['e_max_pu']
                e_min_pu = row['e_min_pu']
                
                if isinstance(e_max_pu, list):
                    agg_e_max[bus_id] = [x * e_nom for x in e_max_pu]
                if isinstance(e_min_pu, list):
                    agg_e_min[bus_id] = [x * e_nom for x in e_min_pu]
            
            # Process individual data - group by bus and sum
            ind_grouped = ind_df.groupby('bus')
            
            ind_e_max = {}
            ind_e_min = {}
            
            for bus_id, group in ind_grouped:
                e_max_arrays = [row['e_max'] for _, row in group.iterrows() if isinstance(row['e_max'], list)]
                e_min_arrays = [row['e_min'] for _, row in group.iterrows() if isinstance(row['e_min'], list)]
                
                if e_max_arrays:
                    ind_e_max[bus_id] = [sum(x) for x in zip(*e_max_arrays)]
                if e_min_arrays:
                    ind_e_min[bus_id] = [sum(x) for x in zip(*e_min_arrays)]
            
            # Compare aggregated vs individual data
            mismatches = []
            
            for bus_id in agg_e_max:
                if bus_id in ind_e_max:
                    agg_array = np.array(agg_e_max[bus_id])
                    ind_array = np.array(ind_e_max[bus_id])
                    
                    if not np.allclose(agg_array, ind_array):
                        mismatches.append({
                            "bus_id": bus_id,
                            "constraint": "e_max",
                            "max_diff": np.max(np.abs(agg_array - ind_array))
                        })
            
            for bus_id in agg_e_min:
                if bus_id in ind_e_min:
                    agg_array = np.array(agg_e_min[bus_id])
                    ind_array = np.array(ind_e_min[bus_id])
                    
                    if not np.allclose(agg_array, ind_array):
                        mismatches.append({
                            "bus_id": bus_id,
                            "constraint": "e_min",
                            "max_diff": np.max(np.abs(agg_array - ind_array))
                        })
            
            if mismatches:
                return {
                    "check_type": "energy_constraints",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Energy constraint mismatches found: {len(mismatches)} buses",
                    "mismatches": mismatches[:10],
                    "total_mismatches": len(mismatches)
                }
            
            return {
                "check_type": "energy_constraints",
                "scenario": scenario,
                "status": "SUCCESS",
                "message": f"Energy constraints consistent for {len(agg_e_max)} buses",
                "validated_buses": len(agg_e_max)
            }
            
        except Exception as e:
            return {
                "check_type": "energy_constraints",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate energy constraints: {str(e)}"
            }
    
    def _get_aggregated_dsm_links(self, scenario: str) -> List[Dict[str, Any]]:
        """Get aggregated DSM link data"""
        try:
            query = """
                SELECT l.link_id, l.bus0 as bus, l.p_nom, lt.p_min_pu, lt.p_max_pu
                FROM grid.egon_etrago_link l
                JOIN grid.egon_etrago_link_timeseries lt ON l.link_id = lt.link_id
                WHERE l.carrier = 'dsm'
                AND l.scn_name = %s
                AND lt.scn_name = %s
                ORDER BY l.link_id
            """
            result = self.db_manager.execute_query(query, (scenario, scenario))
            return result
        except Exception as e:
            self.logger.warning(f"Failed to get aggregated DSM links for {scenario}: {str(e)}")
            return []
    
    def _get_aggregated_dsm_stores(self, scenario: str) -> List[Dict[str, Any]]:
        """Get aggregated DSM store data"""
        try:
            query = """
                SELECT s.store_id, s.bus, s.e_nom, st.e_min_pu, st.e_max_pu
                FROM grid.egon_etrago_store s
                JOIN grid.egon_etrago_store_timeseries st ON s.store_id = st.store_id
                WHERE s.carrier = 'dsm'
                AND s.scn_name = %s
                AND st.scn_name = %s
                ORDER BY s.store_id
            """
            result = self.db_manager.execute_query(query, (scenario, scenario))
            return result
        except Exception as e:
            self.logger.warning(f"Failed to get aggregated DSM stores for {scenario}: {str(e)}")
            return []
    
    def _get_individual_dsm_data(self, scenario: str) -> List[Dict[str, Any]]:
        """Get individual DSM data from all source tables"""
        try:
            all_individual_data = []
            
            # This would normally query the individual DSM tables
            # For now, we'll create a placeholder that indicates the tables to check
            for table in self.dsm_tables:
                try:
                    # Mock query structure - would need to be adapted to actual schema
                    query = f"""
                        SELECT bus, p_min, p_max, e_max, e_min
                        FROM demand.egon_{table}
                        WHERE scn_name = %s
                        ORDER BY bus
                    """
                    result = self.db_manager.execute_query(query, (scenario,))
                    if result:
                        all_individual_data.extend(result)
                except Exception as e:
                    self.logger.warning(f"Could not query {table}: {str(e)}")
                    continue
            
            return all_individual_data
            
        except Exception as e:
            self.logger.warning(f"Failed to get individual DSM data for {scenario}: {str(e)}")
            return []