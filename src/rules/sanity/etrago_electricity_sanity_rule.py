"""
Sanity check rule for eGon2035 electricity sector
Based on the etrago_eGon2035_electricity function from sanity_checks.py
"""

from typing import Dict, Any, List
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class EtragoElectricitySanityRule(BaseValidationRule):
    """
    Sanity check for electricity sector data consistency in eGon2035 scenario.
    
    Compares input capacities from supply.egon_scenario_capacities with 
    output capacities from grid.egon_etrago_* tables for:
    - Generator capacities (various carriers)
    - Storage capacities (pumped_hydro)
    - Load demand (electricity)
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("EtragoElectricitySanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
        # Define electricity carriers to validate
        self.electricity_carriers = [
            "others", "reservoir", "run_of_river", "oil", "wind_onshore",
            "wind_offshore", "solar", "solar_rooftop", "biomass"
        ]
        
        self.storage_carriers = ["pumped_hydro"]
    
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the electricity sanity check for eGon2035 scenario
        
        Parameters:
        -----------
        config : Dict[str, Any]
            Configuration containing scenario name and validation parameters
            
        Returns:
        --------
        ValidationResult
            Validation result with detailed findings
        """
        scenario = config.get("scenario", "eGon2035")
        tolerance = config.get("tolerance", 5.0)  # Default 5% tolerance
        
        self.logger.info(f"Starting electricity sanity check for scenario: {scenario}")
        
        try:
            # Validate generators
            generator_results = self._validate_generators(scenario, tolerance)
            
            # Validate storage
            storage_results = self._validate_storage(scenario, tolerance)
            
            # Validate loads
            load_results = self._validate_loads(scenario, tolerance)
            
            # Combine results
            all_results = generator_results + storage_results + load_results
            
            # Determine overall status
            critical_failures = [r for r in all_results if r["status"] == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r["status"] == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in electricity sanity check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in electricity sanity check"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "scenario": scenario,
                "tolerance_percent": tolerance,
                "generator_results": generator_results,
                "storage_results": storage_results,
                "load_results": load_results,
                "summary": {
                    "total_validations": len(all_results),
                    "passed": len([r for r in all_results if r["status"] == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                }
            }
            
            message = f"Electricity sanity check completed for {scenario}: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="grid.egon_etrago_generator,grid.egon_etrago_storage,grid.egon_etrago_load",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in electricity sanity check: {str(e)}")
            return self._create_failure_result(
                table="grid.egon_etrago_*",
                error_details=f"Sanity check execution failed: {str(e)}"
            )
    
    def _validate_generators(self, scenario: str, tolerance: float) -> List[Dict[str, Any]]:
        """Validate generator capacities for all electricity carriers"""
        results = []
        
        for carrier in self.electricity_carriers:
            try:
                # Get output capacity from etrago_generator
                if carrier == "biomass":
                    output_query = """
                        SELECT SUM(p_nom::numeric) as output_capacity_mw
                        FROM grid.egon_etrago_generator
                        WHERE bus IN (
                            SELECT bus_id FROM grid.egon_etrago_bus
                            WHERE scn_name = %s
                            AND country = 'DE')
                        AND carrier IN ('biomass', 'industrial_biomass_CHP', 'central_biomass_CHP')
                        AND scn_name = %s
                    """
                    output_params = (scenario, scenario)
                else:
                    output_query = """
                        SELECT SUM(p_nom::numeric) as output_capacity_mw
                        FROM grid.egon_etrago_generator
                        WHERE scn_name = %s
                        AND carrier = %s
                        AND bus IN (
                            SELECT bus_id FROM grid.egon_etrago_bus
                            WHERE scn_name = %s
                            AND country = 'DE')
                    """
                    output_params = (scenario, carrier, scenario)
                
                output_result = self.db_manager.execute_query(output_query, output_params)
                output_capacity = output_result[0]["output_capacity_mw"] if output_result[0]["output_capacity_mw"] else 0
                
                # Get input capacity from scenario_capacities
                input_query = """
                    SELECT SUM(capacity::numeric) as input_capacity_mw
                    FROM supply.egon_scenario_capacities
                    WHERE carrier = %s
                    AND scenario_name = %s
                """
                input_result = self.db_manager.execute_query(input_query, (carrier, scenario))
                input_capacity = input_result[0]["input_capacity_mw"] if input_result[0]["input_capacity_mw"] else 0
                
                # Calculate deviation
                result = self._calculate_deviation(carrier, input_capacity, output_capacity, tolerance)
                results.append(result)
                
            except Exception as e:
                results.append({
                    "carrier": carrier,
                    "status": "CRITICAL_FAILURE",
                    "error": f"Failed to validate generator {carrier}: {str(e)}",
                    "input_capacity": None,
                    "output_capacity": None,
                    "deviation_percent": None
                })
        
        return results
    
    def _validate_storage(self, scenario: str, tolerance: float) -> List[Dict[str, Any]]:
        """Validate storage unit capacities"""
        results = []
        
        for carrier in self.storage_carriers:
            try:
                # Get output capacity from etrago_storage
                output_query = """
                    SELECT SUM(p_nom::numeric) as output_capacity_mw
                    FROM grid.egon_etrago_storage
                    WHERE scn_name = %s
                    AND carrier = %s
                    AND bus IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = %s
                        AND country = 'DE')
                """
                output_result = self.db_manager.execute_query(output_query, (scenario, carrier, scenario))
                output_capacity = output_result[0]["output_capacity_mw"] if output_result[0]["output_capacity_mw"] else 0
                
                # Get input capacity from scenario_capacities
                input_query = """
                    SELECT SUM(capacity::numeric) as input_capacity_mw
                    FROM supply.egon_scenario_capacities
                    WHERE carrier = %s
                    AND scenario_name = %s
                """
                input_result = self.db_manager.execute_query(input_query, (carrier, scenario))
                input_capacity = input_result[0]["input_capacity_mw"] if input_result[0]["input_capacity_mw"] else 0
                
                # Calculate deviation
                result = self._calculate_deviation(f"storage_{carrier}", input_capacity, output_capacity, tolerance)
                results.append(result)
                
            except Exception as e:
                results.append({
                    "carrier": f"storage_{carrier}",
                    "status": "CRITICAL_FAILURE",
                    "error": f"Failed to validate storage {carrier}: {str(e)}",
                    "input_capacity": None,
                    "output_capacity": None,
                    "deviation_percent": None
                })
        
        return results
    
    def _validate_loads(self, scenario: str, tolerance: float) -> List[Dict[str, Any]]:
        """Validate electricity load demand"""
        results = []
        
        try:
            # Get output demand from etrago_load
            output_query = """
                SELECT SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000::numeric as load_twh
                FROM grid.egon_etrago_load a
                JOIN grid.egon_etrago_load_timeseries b ON (a.load_id = b.load_id)
                JOIN grid.egon_etrago_bus c ON (a.bus=c.bus_id)
                WHERE b.scn_name = %s
                AND a.scn_name = %s
                AND a.carrier = 'AC'
                AND c.scn_name = %s
                AND c.country = 'DE'
            """
            output_result = self.db_manager.execute_query(output_query, (scenario, scenario, scenario))
            output_demand = output_result[0]["load_twh"] if output_result[0]["load_twh"] else 0
            
            # Get input demand from demandregio tables
            input_cts_ind_query = """
                SELECT SUM(demand::numeric/1000000) as demand_mw_regio_cts_ind
                FROM demand.egon_demandregio_cts_ind
                WHERE scenario = %s
                AND year = '2035'
            """
            input_cts_ind_result = self.db_manager.execute_query(input_cts_ind_query, (scenario,))
            input_cts_ind = input_cts_ind_result[0]["demand_mw_regio_cts_ind"] if input_cts_ind_result[0]["demand_mw_regio_cts_ind"] else 0
            
            input_hh_query = """
                SELECT SUM(demand::numeric/1000000) as demand_mw_regio_hh
                FROM demand.egon_demandregio_hh
                WHERE scenario = %s
                AND year = '2035'
            """
            input_hh_result = self.db_manager.execute_query(input_hh_query, (scenario,))
            input_hh = input_hh_result[0]["demand_mw_regio_hh"] if input_hh_result[0]["demand_mw_regio_hh"] else 0
            
            input_demand = input_hh + input_cts_ind
            
            # Calculate deviation
            result = self._calculate_deviation("electricity_demand", input_demand, output_demand, tolerance)
            results.append(result)
            
        except Exception as e:
            results.append({
                "carrier": "electricity_demand",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate electricity demand: {str(e)}",
                "input_capacity": None,
                "output_capacity": None,
                "deviation_percent": None
            })
        
        return results
    
    def _calculate_deviation(self, carrier: str, input_value: float, output_value: float, tolerance: float) -> Dict[str, Any]:
        """Calculate deviation between input and output values"""
        
        # Handle edge cases
        if input_value == 0 and output_value == 0:
            return {
                "carrier": carrier,
                "status": "SUCCESS",
                "message": f"No capacity for carrier '{carrier}' needed to be distributed. Everything is fine",
                "input_capacity": input_value,
                "output_capacity": output_value,
                "deviation_percent": 0.0
            }
        
        if input_value > 0 and output_value == 0:
            return {
                "carrier": carrier,
                "status": "CRITICAL_FAILURE",
                "error": f"Capacity for carrier '{carrier}' was not distributed at all!",
                "input_capacity": input_value,
                "output_capacity": output_value,
                "deviation_percent": -100.0
            }
        
        if output_value > 0 and input_value == 0:
            return {
                "carrier": carrier,
                "status": "CRITICAL_FAILURE",
                "error": f"Even though no input capacity was provided for carrier '{carrier}', capacity got distributed!",
                "input_capacity": input_value,
                "output_capacity": output_value,
                "deviation_percent": float('inf')
            }
        
        # Calculate percentage deviation
        deviation_percent = ((output_value - input_value) / input_value) * 100
        
        # Determine status based on tolerance
        if abs(deviation_percent) <= tolerance:
            status = "SUCCESS"
            message = f"{carrier}: {deviation_percent:.2f}% deviation (within tolerance)"
        else:
            status = "WARNING"
            message = f"{carrier}: {deviation_percent:.2f}% deviation (exceeds tolerance of {tolerance}%)"
        
        return {
            "carrier": carrier,
            "status": status,
            "message": message,
            "input_capacity": input_value,
            "output_capacity": output_value,
            "deviation_percent": deviation_percent
        }