"""
Sanity check rule for eGon2035 heat sector
Based on the etrago_eGon2035_heat function from sanity_checks.py
"""

from typing import Dict, Any, List
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class EtragoHeatSanityRule(BaseValidationRule):
    """
    Sanity check for heat sector data consistency in eGon2035 scenario.
    
    Compares input capacities from supply.egon_scenario_capacities with 
    output capacities from grid.egon_etrago_* tables for:
    - Heat demand (rural_heat, central_heat)
    - Heat supply components (heat pumps, resistive heaters, solar thermal, geothermal)
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("EtragoHeatSanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
        # Define heat supply components to validate
        self.heat_supply_components = [
            {
                "name": "central_heat_pump",
                "input_carrier": "urban_central_heat_pump",
                "output_carrier": "central_heat_pump",
                "table": "grid.egon_etrago_link"
            },
            {
                "name": "residential_heat_pump",
                "input_carrier": "residential_rural_heat_pump",
                "output_carrier": "rural_heat_pump",
                "table": "grid.egon_etrago_link"
            },
            {
                "name": "resistive_heater",
                "input_carrier": "urban_central_resistive_heater",
                "output_carrier": "central_resistive_heater",
                "table": "grid.egon_etrago_link"
            },
            {
                "name": "solar_thermal",
                "input_carrier": "urban_central_solar_thermal_collector",
                "output_carrier": "solar_thermal_collector",
                "table": "grid.egon_etrago_generator"
            },
            {
                "name": "geothermal",
                "input_carrier": "urban_central_geo_thermal",
                "output_carrier": "geo_thermal",
                "table": "grid.egon_etrago_generator"
            }
        ]
    
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the heat sanity check for eGon2035 scenario
        
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
        
        self.logger.info(f"Starting heat sanity check for scenario: {scenario}")
        
        try:
            # Validate heat demand
            demand_results = self._validate_heat_demand(scenario, tolerance)
            
            # Validate heat supply components
            supply_results = self._validate_heat_supply(scenario, tolerance)
            
            # Combine results
            all_results = demand_results + supply_results
            
            # Determine overall status
            critical_failures = [r for r in all_results if r["status"] == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r["status"] == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in heat sanity check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in heat sanity check"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "scenario": scenario,
                "tolerance_percent": tolerance,
                "demand_results": demand_results,
                "supply_results": supply_results,
                "summary": {
                    "total_validations": len(all_results),
                    "passed": len([r for r in all_results if r["status"] == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                }
            }
            
            message = f"Heat sanity check completed for {scenario}: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="grid.egon_etrago_load,grid.egon_etrago_link,grid.egon_etrago_generator",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in heat sanity check: {str(e)}")
            return self._create_failure_result(
                table="grid.egon_etrago_*",
                error_details=f"Heat sanity check execution failed: {str(e)}"
            )
    
    def _validate_heat_demand(self, scenario: str, tolerance: float) -> List[Dict[str, Any]]:
        """Validate heat demand consistency"""
        results = []
        
        try:
            # Get output heat demand from etrago_load
            output_query = """
                SELECT (SUM(
                    (SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000)::numeric as load_twh
                FROM grid.egon_etrago_load a
                JOIN grid.egon_etrago_load_timeseries b ON (a.load_id = b.load_id)
                JOIN grid.egon_etrago_bus c ON (a.bus=c.bus_id)
                WHERE b.scn_name = %s
                AND a.scn_name = %s
                AND c.scn_name = %s
                AND c.country = 'DE'
                AND a.carrier IN ('rural_heat', 'central_heat')
            """
            output_result = self.db_manager.execute_query(output_query, (scenario, scenario, scenario))
            output_demand = output_result[0]["load_twh"] if output_result[0]["load_twh"] else 0
            
            # Get input heat demand from peta_heat
            input_query = """
                SELECT SUM(demand::numeric/1000000) as demand_mw_peta_heat
                FROM demand.egon_peta_heat
                WHERE scenario = %s
            """
            input_result = self.db_manager.execute_query(input_query, (scenario,))
            input_demand = input_result[0]["demand_mw_peta_heat"] if input_result[0]["demand_mw_peta_heat"] else 0
            
            # Calculate deviation
            result = self._calculate_deviation("heat_demand", input_demand, output_demand, tolerance)
            results.append(result)
            
        except Exception as e:
            results.append({
                "component": "heat_demand",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate heat demand: {str(e)}",
                "input_capacity": None,
                "output_capacity": None,
                "deviation_percent": None
            })
        
        return results
    
    def _validate_heat_supply(self, scenario: str, tolerance: float) -> List[Dict[str, Any]]:
        """Validate heat supply component capacities"""
        results = []
        
        for component in self.heat_supply_components:
            try:
                # Get output capacity from appropriate etrago table
                if component["table"] == "grid.egon_etrago_link":
                    output_query = """
                        SELECT SUM(p_nom::numeric) as output_capacity_mw
                        FROM grid.egon_etrago_link
                        WHERE carrier = %s
                        AND scn_name = %s
                    """
                else:  # grid.egon_etrago_generator
                    output_query = """
                        SELECT SUM(p_nom::numeric) as output_capacity_mw
                        FROM grid.egon_etrago_generator
                        WHERE carrier = %s
                        AND scn_name = %s
                    """
                
                output_result = self.db_manager.execute_query(
                    output_query, 
                    (component["output_carrier"], scenario)
                )
                output_capacity = output_result[0]["output_capacity_mw"] if output_result[0]["output_capacity_mw"] else 0
                
                # Get input capacity from scenario_capacities
                input_query = """
                    SELECT SUM(capacity::numeric) as input_capacity_mw
                    FROM supply.egon_scenario_capacities
                    WHERE carrier = %s
                    AND scenario_name = %s
                """
                input_result = self.db_manager.execute_query(
                    input_query, 
                    (component["input_carrier"], scenario)
                )
                input_capacity = input_result[0]["input_capacity_mw"] if input_result[0]["input_capacity_mw"] else 0
                
                # Calculate deviation
                result = self._calculate_deviation(
                    component["name"], 
                    input_capacity, 
                    output_capacity, 
                    tolerance
                )
                results.append(result)
                
            except Exception as e:
                results.append({
                    "component": component["name"],
                    "status": "CRITICAL_FAILURE",
                    "error": f"Failed to validate {component['name']}: {str(e)}",
                    "input_capacity": None,
                    "output_capacity": None,
                    "deviation_percent": None
                })
        
        return results
    
    def _calculate_deviation(self, component: str, input_value: float, output_value: float, tolerance: float) -> Dict[str, Any]:
        """Calculate deviation between input and output values"""
        
        # Handle edge cases
        if input_value == 0 and output_value == 0:
            return {
                "component": component,
                "status": "SUCCESS",
                "message": f"No capacity for component '{component}' needed to be distributed. Everything is fine",
                "input_capacity": input_value,
                "output_capacity": output_value,
                "deviation_percent": 0.0
            }
        
        if input_value > 0 and output_value == 0:
            return {
                "component": component,
                "status": "CRITICAL_FAILURE",
                "error": f"Capacity for component '{component}' was not distributed at all!",
                "input_capacity": input_value,
                "output_capacity": output_value,
                "deviation_percent": -100.0
            }
        
        if output_value > 0 and input_value == 0:
            return {
                "component": component,
                "status": "CRITICAL_FAILURE",
                "error": f"Even though no input capacity was provided for component '{component}', capacity got distributed!",
                "input_capacity": input_value,
                "output_capacity": output_value,
                "deviation_percent": float('inf')
            }
        
        # Calculate percentage deviation
        deviation_percent = ((output_value - input_value) / input_value) * 100
        
        # Determine status based on tolerance
        if abs(deviation_percent) <= tolerance:
            status = "SUCCESS"
            message = f"{component}: {deviation_percent:.2f}% deviation (within tolerance)"
        else:
            status = "WARNING"
            message = f"{component}: {deviation_percent:.2f}% deviation (exceeds tolerance of {tolerance}%)"
        
        return {
            "component": component,
            "status": status,
            "message": message,
            "input_capacity": input_value,
            "output_capacity": output_value,
            "deviation_percent": deviation_percent
        }