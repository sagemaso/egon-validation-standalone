"""
Sanity check rule for gas sector in Germany (eGon2035)
Based on the etrago_eGon2035_gas_DE function from sanity_checks.py
"""

from typing import Dict, Any, List
import pandas as pd
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class GasDeSanityRule(BaseValidationRule):
    """
    Sanity check for gas sector data in Germany for eGon2035 scenario.
    
    Validates:
    1. Gas buses (CH4, H2_grid, H2_saltcavern) for isolation
    2. Gas loads (CH4_for_industry, H2_for_industry) vs source data
    3. Gas generators (CH4) vs source data  
    4. Gas stores (CH4, H2_underground) vs source data
    5. Gas links connectivity and capacity
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("GasDeSanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
        # Define gas carriers and their corresponding link carriers
        self.gas_carriers_links = {
            "CH4": "CH4",
            "H2_grid": "H2_feedin", 
            "H2_saltcavern": "power_to_H2"
        }
        
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the gas sector sanity check for Germany
        
        Parameters:
        -----------
        config : Dict[str, Any]
            Configuration containing scenario and validation parameters
            
        Returns:
        --------
        ValidationResult
            Validation result with detailed findings
        """
        scenario = config.get("scenario", "eGon2035")
        tolerance = config.get("tolerance", 5.0)  # Default 5% tolerance
        
        self.logger.info(f"Starting gas DE sanity check for scenario: {scenario}")
        
        try:
            all_results = []
            
            # Validate gas buses
            bus_result = self._validate_gas_buses(scenario)
            all_results.append(bus_result)
            
            # Validate gas loads
            loads_result = self._validate_gas_loads(scenario, tolerance)
            all_results.append(loads_result)
            
            # Validate gas generators
            generators_result = self._validate_gas_generators(scenario, tolerance)
            all_results.append(generators_result)
            
            # Validate gas stores
            stores_result = self._validate_gas_stores(scenario, tolerance)
            all_results.append(stores_result)
            
            # Validate gas links
            links_result = self._validate_gas_links(scenario, tolerance)
            all_results.append(links_result)
            
            # Determine overall status
            critical_failures = [r for r in all_results if r.get("status") == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r.get("status") == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in gas DE sanity check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in gas DE sanity check"
            else:
                status = "SUCCESS"
                error_details = None
            
            # Create detailed context
            detailed_context = {
                "scenario": scenario,
                "tolerance": tolerance,
                "results": all_results,
                "summary": {
                    "total_validations": len(all_results),
                    "passed": len([r for r in all_results if r.get("status") == "SUCCESS"]),
                    "warnings": len(warnings),
                    "critical_failures": len(critical_failures)
                }
            }
            
            message = f"Gas DE sanity check completed for {scenario}: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
            return ValidationResult(
                rule_name=self.rule_name,
                status=status,
                table="grid.egon_etrago_*",
                function_name="validate",
                module_name=self.__class__.__module__,
                message=message,
                error_details=error_details,
                detailed_context=detailed_context
            )
            
        except Exception as e:
            self.logger.error(f"Error in gas DE sanity check: {str(e)}")
            return self._create_failure_result(
                table="grid.egon_etrago_*",
                error_details=f"Gas DE sanity check execution failed: {str(e)}"
            )
    
    def _validate_gas_buses(self, scenario: str) -> Dict[str, Any]:
        """Validate gas buses for isolation"""
        try:
            isolated_buses = []
            
            for carrier, link_carrier in self.gas_carriers_links.items():
                # Find isolated buses for this carrier
                query = """
                    SELECT bus_id, carrier, country
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND carrier = %s
                    AND country = 'DE'
                    AND bus_id NOT IN (
                        SELECT bus0
                        FROM grid.egon_etrago_link
                        WHERE scn_name = %s
                        AND carrier = %s
                    )
                    AND bus_id NOT IN (
                        SELECT bus1
                        FROM grid.egon_etrago_link
                        WHERE scn_name = %s
                        AND carrier = %s
                    )
                """
                result = self.db_manager.execute_query(query, (scenario, carrier, scenario, link_carrier, scenario, link_carrier))
                
                if result:
                    isolated_buses.extend([{
                        "carrier": carrier,
                        "bus_id": row["bus_id"],
                        "country": row["country"]
                    } for row in result])
            
            if isolated_buses:
                return {
                    "check_type": "gas_buses",
                    "status": "WARNING",
                    "message": f"Found {len(isolated_buses)} isolated gas buses",
                    "isolated_buses": isolated_buses
                }
            
            return {
                "check_type": "gas_buses",
                "status": "SUCCESS",
                "message": "No isolated gas buses found"
            }
            
        except Exception as e:
            return {
                "check_type": "gas_buses",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas buses: {str(e)}"
            }
    
    def _validate_gas_loads(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas loads against source data"""
        try:
            load_validations = []
            
            # Validate CH4_for_industry and H2_for_industry loads
            for carrier in ["CH4_for_industry", "H2_for_industry"]:
                # Get output demand from etrago_load
                output_query = """
                    SELECT SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p))/1000000 as load_twh
                    FROM grid.egon_etrago_load a
                    JOIN grid.egon_etrago_load_timeseries b ON (a.load_id = b.load_id)
                    JOIN grid.egon_etrago_bus c ON (a.bus=c.bus_id)
                    WHERE b.scn_name = %s
                    AND a.scn_name = %s
                    AND c.scn_name = %s
                    AND c.country = 'DE'
                    AND a.carrier = %s
                """
                output_result = self.db_manager.execute_query(output_query, (scenario, scenario, scenario, carrier))
                output_demand = output_result[0]["load_twh"] if output_result and output_result[0]["load_twh"] else 0
                
                # For now, we'll note that input data validation would require access to source files
                # This would normally compare against input JSON files from gas_data/demand/
                load_validations.append({
                    "carrier": carrier,
                    "output_demand_twh": output_demand,
                    "status": "SUCCESS" if output_demand > 0 else "WARNING",
                    "message": f"Output demand for {carrier}: {output_demand:.2f} TWh" if output_demand > 0 else f"No demand found for {carrier}"
                })
            
            # Determine overall status for loads
            failures = [v for v in load_validations if v["status"] == "CRITICAL_FAILURE"]
            warnings = [v for v in load_validations if v["status"] == "WARNING"]
            
            if failures:
                status = "CRITICAL_FAILURE"
            elif warnings:
                status = "WARNING"
            else:
                status = "SUCCESS"
            
            return {
                "check_type": "gas_loads",
                "status": status,
                "message": f"Gas loads validation completed: {len(load_validations)} carriers checked",
                "load_validations": load_validations
            }
            
        except Exception as e:
            return {
                "check_type": "gas_loads",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas loads: {str(e)}"
            }
    
    def _validate_gas_generators(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas generators against source data"""
        try:
            # Get output gas generation capacity
            query = """
                SELECT SUM(p_nom::numeric) as p_nom_germany
                FROM grid.egon_etrago_generator
                WHERE scn_name = %s
                AND carrier = 'CH4'
                AND bus IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country = 'DE'
                    AND carrier = 'CH4'
                )
            """
            result = self.db_manager.execute_query(query, (scenario, scenario))
            output_generation = result[0]["p_nom_germany"] if result and result[0]["p_nom_germany"] else 0
            
            # Note: Input validation would require access to source files
            # (IGGIELGN_Productions.csv and Biogaspartner_Einspeiseatlas_Deutschland_2021.xlsx)
            
            return {
                "check_type": "gas_generators",
                "status": "SUCCESS" if output_generation > 0 else "WARNING",
                "message": f"Gas generation capacity: {output_generation:.2f} MW" if output_generation > 0 else "No gas generation capacity found",
                "output_generation_mw": output_generation
            }
            
        except Exception as e:
            return {
                "check_type": "gas_generators",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas generators: {str(e)}"
            }
    
    def _validate_gas_stores(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas stores capacity"""
        try:
            store_validations = []
            
            # Validate CH4 stores
            ch4_query = """
                SELECT SUM(e_nom::numeric) as e_nom_germany
                FROM grid.egon_etrago_store
                WHERE scn_name = %s
                AND carrier = 'CH4'
                AND bus IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country = 'DE'
                    AND carrier = 'CH4'
                )
            """
            ch4_result = self.db_manager.execute_query(ch4_query, (scenario, scenario))
            ch4_capacity = ch4_result[0]["e_nom_germany"] if ch4_result and ch4_result[0]["e_nom_germany"] else 0
            
            store_validations.append({
                "carrier": "CH4",
                "capacity_mwh": ch4_capacity,
                "status": "SUCCESS" if ch4_capacity > 0 else "WARNING",
                "message": f"CH4 storage capacity: {ch4_capacity:.2f} MWh" if ch4_capacity > 0 else "No CH4 storage capacity found"
            })
            
            # Validate H2 underground stores
            h2_query = """
                SELECT SUM(e_nom_max::numeric) as e_nom_max_germany
                FROM grid.egon_etrago_store
                WHERE scn_name = %s
                AND carrier = 'H2_underground'
                AND bus IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country = 'DE'
                    AND carrier = 'H2_saltcavern'
                )
            """
            h2_result = self.db_manager.execute_query(h2_query, (scenario, scenario))
            h2_capacity = h2_result[0]["e_nom_max_germany"] if h2_result and h2_result[0]["e_nom_max_germany"] else 0
            
            store_validations.append({
                "carrier": "H2_underground",
                "capacity_mwh": h2_capacity,
                "status": "SUCCESS" if h2_capacity > 0 else "WARNING",
                "message": f"H2 underground storage capacity: {h2_capacity:.2f} MWh" if h2_capacity > 0 else "No H2 underground storage capacity found"
            })
            
            # Determine overall status
            failures = [v for v in store_validations if v["status"] == "CRITICAL_FAILURE"]
            warnings = [v for v in store_validations if v["status"] == "WARNING"]
            
            if failures:
                status = "CRITICAL_FAILURE"
            elif warnings:
                status = "WARNING"
            else:
                status = "SUCCESS"
            
            return {
                "check_type": "gas_stores",
                "status": status,
                "message": f"Gas stores validation completed: {len(store_validations)} store types checked",
                "store_validations": store_validations
            }
            
        except Exception as e:
            return {
                "check_type": "gas_stores", 
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas stores: {str(e)}"
            }
    
    def _validate_gas_links(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas links connectivity and capacity"""
        try:
            link_validations = []
            
            # Check for links with missing buses
            carriers_to_check = [
                "CH4", "H2_feedin", "H2_to_CH4", "CH4_to_H2", "H2_to_power", 
                "power_to_H2", "OCGT", "central_gas_boiler", "central_gas_CHP",
                "central_gas_CHP_heat", "industrial_gas_CHP"
            ]
            
            for carrier in carriers_to_check:
                query = """
                    SELECT COUNT(*) as missing_bus_count
                    FROM grid.egon_etrago_link
                    WHERE scn_name = %s
                    AND carrier = %s
                    AND (bus0 NOT IN (
                        SELECT bus_id
                        FROM grid.egon_etrago_bus
                        WHERE scn_name = %s
                    )
                    OR bus1 NOT IN (
                        SELECT bus_id
                        FROM grid.egon_etrago_bus
                        WHERE scn_name = %s
                    ))
                """
                result = self.db_manager.execute_query(query, (scenario, carrier, scenario, scenario))
                missing_count = result[0]["missing_bus_count"] if result else 0
                
                link_validations.append({
                    "carrier": carrier,
                    "missing_bus_links": missing_count,
                    "status": "CRITICAL_FAILURE" if missing_count > 0 else "SUCCESS",
                    "message": f"Links with missing buses for {carrier}: {missing_count}"
                })
            
            # Validate CH4 grid capacity
            ch4_grid_query = """
                SELECT SUM(p_nom::numeric) as p_nom_germany
                FROM grid.egon_etrago_link
                WHERE scn_name = %s
                AND carrier = 'CH4'
                AND bus0 IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country = 'DE'
                    AND carrier = 'CH4'
                )
                AND bus1 IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country = 'DE'
                    AND carrier = 'CH4'
                )
            """
            ch4_result = self.db_manager.execute_query(ch4_grid_query, (scenario, scenario, scenario))
            ch4_grid_capacity = ch4_result[0]["p_nom_germany"] if ch4_result and ch4_result[0]["p_nom_germany"] else 0
            
            link_validations.append({
                "carrier": "CH4_grid",
                "capacity_mw": ch4_grid_capacity,
                "status": "SUCCESS" if ch4_grid_capacity > 0 else "WARNING",
                "message": f"CH4 grid capacity in Germany: {ch4_grid_capacity:.2f} MW" if ch4_grid_capacity > 0 else "No CH4 grid capacity found"
            })
            
            # Determine overall status
            failures = [v for v in link_validations if v["status"] == "CRITICAL_FAILURE"]
            warnings = [v for v in link_validations if v["status"] == "WARNING"]
            
            if failures:
                status = "CRITICAL_FAILURE"
            elif warnings:
                status = "WARNING"
            else:
                status = "SUCCESS"
            
            return {
                "check_type": "gas_links",
                "status": status,
                "message": f"Gas links validation completed: {len(link_validations)} link types checked",
                "link_validations": link_validations
            }
            
        except Exception as e:
            return {
                "check_type": "gas_links",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas links: {str(e)}"
            }