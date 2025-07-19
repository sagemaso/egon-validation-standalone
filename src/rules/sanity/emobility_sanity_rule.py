"""
Sanity check rule for eMobility: motorized individual travel
Based on the sanitycheck_emobility_mit function from sanity_checks.py
"""

from typing import Dict, Any, List
import numpy as np
import pandas as pd
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class EmobilitySanityRule(BaseValidationRule):
    """
    Sanity check for eMobility data integrity for eGon2035, eGon2035_lowflex and eGon100RE scenarios.
    
    Validates:
    1. Allocated EV numbers and EVs allocated to grid districts
    2. Trip data (original input data from simBEV)
    3. Model data in eTraGo PF tables (grid.egon_etrago_*)
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("EmobilitySanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the eMobility sanity check
        
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
        tolerance = config.get("tolerance", 0.0001)  # Default 0.01% tolerance
        
        self.logger.info(f"Starting eMobility sanity check for scenarios: {scenarios}")
        
        try:
            all_results = []
            
            # Process each scenario
            for scenario in scenarios:
                self.logger.info(f"Processing scenario: {scenario}")
                scenario_results = self._validate_scenario(scenario, tolerance)
                all_results.extend(scenario_results)
            
            # Special check for eGon2035_lowflex
            if "eGon2035" in scenarios:
                lowflex_results = self._validate_lowflex_scenario(tolerance)
                all_results.extend(lowflex_results)
            
            # Determine overall status
            critical_failures = [r for r in all_results if r.get("status") == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r.get("status") == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in eMobility sanity check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in eMobility sanity check"
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
            
            message = f"eMobility sanity check completed: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="grid.egon_etrago_*,emobility.*",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in eMobility sanity check: {str(e)}")
            return self._create_failure_result(
                table="emobility.*",
                error_details=f"eMobility sanity check execution failed: {str(e)}"
            )
    
    def _validate_scenario(self, scenario: str, tolerance: float) -> List[Dict[str, Any]]:
        """Validate a specific scenario"""
        results = []
        
        try:
            # Check EV allocation
            ev_allocation_result = self._check_ev_allocation(scenario, tolerance)
            results.append(ev_allocation_result)
            
            # Check trip data
            trip_data_result = self._check_trip_data(scenario)
            results.append(trip_data_result)
            
            # Check model data
            model_data_result = self._check_model_data(scenario, tolerance)
            results.append(model_data_result)
            
        except Exception as e:
            results.append({
                "check_type": "scenario_validation",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate scenario {scenario}: {str(e)}"
            })
        
        return results
    
    def _check_ev_allocation(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Check EV allocation numbers"""
        try:
            # Get target EV count (this would need to be configured or retrieved from scenario parameters)
            # For now, we'll validate consistency between different allocation tables
            
            # Check counts across different levels
            mv_grid_query = """
                SELECT SUM(bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury) as ev_count
                FROM emobility.egon_ev_count_mv_grid_district
                WHERE scenario = %s AND scenario_variation = 'normal'
            """
            
            municipality_query = """
                SELECT SUM(bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury) as ev_count
                FROM emobility.egon_ev_count_municipality
                WHERE scenario = %s AND scenario_variation = 'normal'
            """
            
            registration_query = """
                SELECT SUM(bev_mini + bev_medium + bev_luxury + phev_mini + phev_medium + phev_luxury) as ev_count
                FROM emobility.egon_ev_count_registration_district
                WHERE scenario = %s AND scenario_variation = 'normal'
            """
            
            mv_result = self.db_manager.execute_query(mv_grid_query, (scenario,))
            municipality_result = self.db_manager.execute_query(municipality_query, (scenario,))
            registration_result = self.db_manager.execute_query(registration_query, (scenario,))
            
            mv_count = mv_result[0]["ev_count"] if mv_result and mv_result[0]["ev_count"] else 0
            municipality_count = municipality_result[0]["ev_count"] if municipality_result and municipality_result[0]["ev_count"] else 0
            registration_count = registration_result[0]["ev_count"] if registration_result and registration_result[0]["ev_count"] else 0
            
            # Check consistency
            if abs(mv_count - municipality_count) / max(mv_count, 1) > tolerance or \
               abs(mv_count - registration_count) / max(mv_count, 1) > tolerance:
                return {
                    "check_type": "ev_allocation",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"EV count inconsistency: MV Grid={mv_count}, Municipality={municipality_count}, Registration={registration_count}",
                    "mv_grid_count": mv_count,
                    "municipality_count": municipality_count,
                    "registration_count": registration_count
                }
            
            return {
                "check_type": "ev_allocation",
                "scenario": scenario,
                "status": "SUCCESS",
                "message": f"EV allocation consistent across levels: {mv_count} EVs",
                "ev_count": mv_count
            }
            
        except Exception as e:
            return {
                "check_type": "ev_allocation",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to check EV allocation: {str(e)}"
            }
    
    def _check_trip_data(self, scenario: str) -> Dict[str, Any]:
        """Check trip data timeranges and charging demand"""
        try:
            # Check if trips have valid timeranges
            invalid_trips_query = """
                SELECT COUNT(*) as invalid_count
                FROM emobility.egon_ev_trip
                WHERE scenario = %s
                AND (
                    (park_start > 0 AND simbev_event_id = 0)
                    OR park_end > 35040
                )
            """
            
            invalid_result = self.db_manager.execute_query(invalid_trips_query, (scenario,))
            invalid_count = invalid_result[0]["invalid_count"] if invalid_result else 0
            
            if invalid_count > 0:
                return {
                    "check_type": "trip_data",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"{invalid_count} trips have invalid timesteps",
                    "invalid_trips": invalid_count
                }
            
            # Check charging demand vs available energy
            charging_demand_query = """
                SELECT COUNT(*) as invalid_charging_count
                FROM emobility.egon_ev_trip
                WHERE scenario = %s
                AND ROUND(
                    (park_end - park_start + 1) * charging_capacity_nominal * (15.0 / 60.0), 3
                ) < charging_demand
            """
            
            charging_result = self.db_manager.execute_query(charging_demand_query, (scenario,))
            invalid_charging = charging_result[0]["invalid_charging_count"] if charging_result else 0
            
            if invalid_charging > 0:
                return {
                    "check_type": "trip_data",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"{invalid_charging} trips have charging demand exceeding available power",
                    "invalid_charging_trips": invalid_charging
                }
            
            return {
                "check_type": "trip_data",
                "scenario": scenario,
                "status": "SUCCESS",
                "message": "Trip data validation passed"
            }
            
        except Exception as e:
            return {
                "check_type": "trip_data",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to check trip data: {str(e)}"
            }
    
    def _check_model_data(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Check model data in eTraGo tables"""
        try:
            # Check if all model components exist
            load_count_query = """
                SELECT COUNT(*) as load_count
                FROM grid.egon_etrago_load
                WHERE scn_name = %s AND carrier = 'land_transport_EV'
            """
            
            store_count_query = """
                SELECT COUNT(*) as store_count
                FROM grid.egon_etrago_store
                WHERE scn_name = %s AND carrier = 'battery_storage'
            """
            
            link_count_query = """
                SELECT COUNT(*) as link_count
                FROM grid.egon_etrago_link
                WHERE scn_name = %s AND carrier = 'BEV_charger'
            """
            
            load_result = self.db_manager.execute_query(load_count_query, (scenario,))
            store_result = self.db_manager.execute_query(store_count_query, (scenario,))
            link_result = self.db_manager.execute_query(link_count_query, (scenario,))
            
            load_count = load_result[0]["load_count"] if load_result else 0
            store_count = store_result[0]["store_count"] if store_result else 0
            link_count = link_result[0]["link_count"] if link_result else 0
            
            if load_count == 0 or store_count == 0 or link_count == 0:
                return {
                    "check_type": "model_data",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Missing model components: loads={load_count}, stores={store_count}, links={link_count}",
                    "component_counts": {
                        "loads": load_count,
                        "stores": store_count,
                        "links": link_count
                    }
                }
            
            # Check timeseries completeness
            timeseries_check_query = """
                SELECT COUNT(*) as incomplete_ts
                FROM grid.egon_etrago_load_timeseries lt
                JOIN grid.egon_etrago_load l ON lt.load_id = l.load_id
                WHERE l.scn_name = %s AND l.carrier = 'land_transport_EV'
                AND lt.scn_name = %s
                AND array_length(lt.p_set, 1) != 8760
            """
            
            ts_result = self.db_manager.execute_query(timeseries_check_query, (scenario, scenario))
            incomplete_ts = ts_result[0]["incomplete_ts"] if ts_result else 0
            
            if incomplete_ts > 0:
                return {
                    "check_type": "model_data",
                    "scenario": scenario,
                    "status": "CRITICAL_FAILURE",
                    "error": f"{incomplete_ts} timeseries do not have 8760 timesteps",
                    "incomplete_timeseries": incomplete_ts
                }
            
            return {
                "check_type": "model_data",
                "scenario": scenario,
                "status": "SUCCESS",
                "message": f"Model data validation passed: {load_count} loads, {store_count} stores, {link_count} links",
                "component_counts": {
                    "loads": load_count,
                    "stores": store_count,
                    "links": link_count
                }
            }
            
        except Exception as e:
            return {
                "check_type": "model_data",
                "scenario": scenario,
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to check model data: {str(e)}"
            }
    
    def _validate_lowflex_scenario(self, tolerance: float) -> List[Dict[str, Any]]:
        """Compare eGon2035 driving load with eGon2035_lowflex charging load"""
        results = []
        
        try:
            # Get driving load from eGon2035
            driving_load_query = """
                SELECT SUM((SELECT SUM(p) FROM UNNEST(lt.p_set) p)) as total_driving_load
                FROM grid.egon_etrago_load l
                JOIN grid.egon_etrago_load_timeseries lt ON l.load_id = lt.load_id
                WHERE l.scn_name = 'eGon2035' AND l.carrier = 'land_transport_EV'
                AND lt.scn_name = 'eGon2035'
            """
            
            # Get charging load from eGon2035_lowflex
            charging_load_query = """
                SELECT SUM((SELECT SUM(p) FROM UNNEST(lt.p_set) p)) as total_charging_load
                FROM grid.egon_etrago_load l
                JOIN grid.egon_etrago_load_timeseries lt ON l.load_id = lt.load_id
                WHERE l.scn_name = 'eGon2035_lowflex' AND l.carrier = 'land_transport_EV'
                AND lt.scn_name = 'eGon2035_lowflex'
            """
            
            driving_result = self.db_manager.execute_query(driving_load_query)
            charging_result = self.db_manager.execute_query(charging_load_query)
            
            driving_load = driving_result[0]["total_driving_load"] if driving_result and driving_result[0]["total_driving_load"] else 0
            charging_load = charging_result[0]["total_charging_load"] if charging_result and charging_result[0]["total_charging_load"] else 0
            
            # Calculate theoretical driving load (charging load * efficiency 0.9)
            eta_cp = 0.9  # Charging efficiency
            theoretical_driving_load = charging_load * eta_cp
            
            # Check if ratio is within tolerance
            if charging_load > 0:
                deviation = abs(driving_load - theoretical_driving_load) / theoretical_driving_load
                if deviation > tolerance:
                    results.append({
                        "check_type": "lowflex_comparison",
                        "status": "CRITICAL_FAILURE",
                        "error": f"Driving/charging load ratio deviates by {deviation*100:.2f}% from expected efficiency",
                        "driving_load_twh": driving_load / 1e6,
                        "charging_load_twh": charging_load / 1e6,
                        "theoretical_driving_load_twh": theoretical_driving_load / 1e6,
                        "deviation_percent": deviation * 100
                    })
                else:
                    results.append({
                        "check_type": "lowflex_comparison",
                        "status": "SUCCESS",
                        "message": f"Driving/charging load ratio within tolerance: {deviation*100:.2f}%",
                        "driving_load_twh": driving_load / 1e6,
                        "charging_load_twh": charging_load / 1e6
                    })
            else:
                results.append({
                    "check_type": "lowflex_comparison",
                    "status": "WARNING",
                    "message": "No charging load data found for eGon2035_lowflex scenario"
                })
            
        except Exception as e:
            results.append({
                "check_type": "lowflex_comparison",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to compare lowflex scenario: {str(e)}"
            })
        
        return results