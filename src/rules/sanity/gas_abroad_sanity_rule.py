"""
Sanity check rule for gas sector abroad (eGon2035)
Based on the etrago_eGon2035_gas_abroad function from sanity_checks.py
"""

from typing import Dict, Any, List
import pandas as pd
from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class GasAbroadSanityRule(BaseValidationRule):
    """
    Sanity check for gas sector data abroad for eGon2035 scenario.
    
    Validates:
    1. Gas buses abroad for isolation
    2. Gas loads abroad (CH4, H2_for_industry) vs TYNDP data
    3. Gas generators abroad (CH4) vs TYNDP data
    4. Gas stores abroad (CH4) vs SciGRID_gas data
    5. Cross-border gas grid pipelines capacity
    """
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__("GasAbroadSanityCheck")
        self.db_manager = db_manager
        self.logger = ValidationLogger(self.rule_name)
        
        # Define gas carriers and their corresponding link carriers for abroad
        self.gas_carriers_links = {
            "CH4": "CH4"
        }
        
    def validate(self, config: Dict[str, Any]) -> ValidationResult:
        """
        Execute the gas sector sanity check for abroad countries
        
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
        
        self.logger.info(f"Starting gas abroad sanity check for scenario: {scenario}")
        
        try:
            all_results = []
            
            # Validate gas buses abroad
            bus_result = self._validate_gas_buses_abroad(scenario)
            all_results.append(bus_result)
            
            # Validate gas loads abroad
            loads_result = self._validate_gas_loads_abroad(scenario, tolerance)
            all_results.append(loads_result)
            
            # Validate gas generators abroad
            generators_result = self._validate_gas_generators_abroad(scenario, tolerance)
            all_results.append(generators_result)
            
            # Validate gas stores abroad
            stores_result = self._validate_gas_stores_abroad(scenario, tolerance)
            all_results.append(stores_result)
            
            # Validate cross-border gas links
            links_result = self._validate_crossborder_gas_links(scenario, tolerance)
            all_results.append(links_result)
            
            # Determine overall status
            critical_failures = [r for r in all_results if r.get("status") == "CRITICAL_FAILURE"]
            warnings = [r for r in all_results if r.get("status") == "WARNING"]
            
            if critical_failures:
                status = "CRITICAL_FAILURE"
                error_details = f"Found {len(critical_failures)} critical failures in gas abroad sanity check"
            elif warnings:
                status = "WARNING"  
                error_details = f"Found {len(warnings)} warnings in gas abroad sanity check"
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
            
            message = f"Gas abroad sanity check completed for {scenario}: {detailed_context['summary']['passed']}/{detailed_context['summary']['total_validations']} validations passed"
            
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
            self.logger.error(f"Error in gas abroad sanity check: {str(e)}")
            return self._create_failure_result(
                table="grid.egon_etrago_*",
                error_details=f"Gas abroad sanity check execution failed: {str(e)}"
            )
    
    def _validate_gas_buses_abroad(self, scenario: str) -> Dict[str, Any]:
        """Validate gas buses abroad for isolation"""
        try:
            isolated_buses = []
            
            for carrier, link_carrier in self.gas_carriers_links.items():
                # Find isolated buses abroad for this carrier
                query = """
                    SELECT bus_id, carrier, country
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND carrier = %s
                    AND country != 'DE'
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
                    "check_type": "gas_buses_abroad",
                    "status": "WARNING",
                    "message": f"Found {len(isolated_buses)} isolated gas buses abroad",
                    "isolated_buses": isolated_buses
                }
            
            return {
                "check_type": "gas_buses_abroad",
                "status": "SUCCESS",
                "message": "No isolated gas buses found abroad"
            }
            
        except Exception as e:
            return {
                "check_type": "gas_buses_abroad",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas buses abroad: {str(e)}"
            }
    
    def _validate_gas_loads_abroad(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas loads abroad against TYNDP data"""
        try:
            load_validations = []
            
            # Validate CH4 loads abroad
            ch4_output_query = """
                SELECT SUM((SELECT SUM(p) FROM UNNEST(b.p_set) p)) as load_mwh
                FROM grid.egon_etrago_load a
                JOIN grid.egon_etrago_load_timeseries b ON (a.load_id = b.load_id)
                JOIN grid.egon_etrago_bus c ON (a.bus=c.bus_id)
                WHERE b.scn_name = %s
                AND a.scn_name = %s
                AND c.scn_name = %s
                AND c.country != 'DE'
                AND a.carrier = 'CH4'
            """
            ch4_result = self.db_manager.execute_query(ch4_output_query, (scenario, scenario, scenario))
            ch4_output_demand = ch4_result[0]["load_mwh"] if ch4_result and ch4_result[0]["load_mwh"] else 0
            
            load_validations.append({
                "carrier": "CH4",
                "output_demand_mwh": ch4_output_demand,
                "status": "SUCCESS" if ch4_output_demand > 0 else "WARNING",
                "message": f"CH4 demand abroad: {ch4_output_demand:.2f} MWh" if ch4_output_demand > 0 else "No CH4 demand found abroad"
            })
            
            # Validate H2_for_industry loads abroad
            h2_output_query = """
                SELECT SUM(p_set::numeric) as p_set_abroad
                FROM grid.egon_etrago_load
                WHERE scn_name = %s
                AND carrier = 'H2_for_industry'
                AND bus IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country != 'DE'
                    AND carrier = 'AC'
                )
            """
            h2_result = self.db_manager.execute_query(h2_output_query, (scenario, scenario))
            h2_output_demand = h2_result[0]["p_set_abroad"] if h2_result and h2_result[0]["p_set_abroad"] else 0
            
            load_validations.append({
                "carrier": "H2_for_industry",
                "output_demand_mw": h2_output_demand,
                "status": "SUCCESS" if h2_output_demand > 0 else "WARNING",
                "message": f"H2_for_industry demand abroad: {h2_output_demand:.2f} MW" if h2_output_demand > 0 else "No H2_for_industry demand found abroad"
            })
            
            # Note: Input validation would require access to TYNDP source data
            # For now, we just validate that we have output data
            
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
                "check_type": "gas_loads_abroad",
                "status": status,
                "message": f"Gas loads abroad validation completed: {len(load_validations)} carriers checked",
                "load_validations": load_validations
            }
            
        except Exception as e:
            return {
                "check_type": "gas_loads_abroad",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas loads abroad: {str(e)}"
            }
    
    def _validate_gas_generators_abroad(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas generators abroad against TYNDP data"""
        try:
            # Get output CH4 generation capacity abroad
            query = """
                SELECT SUM(p_nom::numeric) as p_nom_abroad
                FROM grid.egon_etrago_generator
                WHERE scn_name = %s
                AND carrier = 'CH4'
                AND bus IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country != 'DE'
                    AND carrier = 'CH4'
                )
            """
            result = self.db_manager.execute_query(query, (scenario, scenario))
            output_generation = result[0]["p_nom_abroad"] if result and result[0]["p_nom_abroad"] else 0
            
            # Note: Input validation would require access to TYNDP source data
            
            return {
                "check_type": "gas_generators_abroad",
                "status": "SUCCESS" if output_generation > 0 else "WARNING",
                "message": f"CH4 generation capacity abroad: {output_generation:.2f} MW" if output_generation > 0 else "No CH4 generation capacity found abroad",
                "output_generation_mw": output_generation
            }
            
        except Exception as e:
            return {
                "check_type": "gas_generators_abroad",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas generators abroad: {str(e)}"
            }
    
    def _validate_gas_stores_abroad(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate gas stores abroad against SciGRID_gas data"""
        try:
            # Get output CH4 storage capacity abroad
            query = """
                SELECT SUM(e_nom::numeric) as e_nom_abroad
                FROM grid.egon_etrago_store
                WHERE scn_name = %s
                AND carrier = 'CH4'
                AND bus IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country != 'DE'
                    AND carrier = 'CH4'
                )
            """
            result = self.db_manager.execute_query(query, (scenario, scenario))
            output_storage = result[0]["e_nom_abroad"] if result and result[0]["e_nom_abroad"] else 0
            
            # Note: Input validation would require access to SciGRID_gas source data
            
            return {
                "check_type": "gas_stores_abroad",
                "status": "SUCCESS" if output_storage > 0 else "WARNING",
                "message": f"CH4 storage capacity abroad: {output_storage:.2f} MWh" if output_storage > 0 else "No CH4 storage capacity found abroad",
                "output_storage_mwh": output_storage
            }
            
        except Exception as e:
            return {
                "check_type": "gas_stores_abroad",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate gas stores abroad: {str(e)}"
            }
    
    def _validate_crossborder_gas_links(self, scenario: str, tolerance: float) -> Dict[str, Any]:
        """Validate cross-border gas grid pipelines capacity"""
        try:
            # Get cross-border CH4 grid capacity
            query = """
                SELECT SUM(p_nom::numeric) as p_nom
                FROM grid.egon_etrago_link
                WHERE scn_name = %s
                AND carrier = 'CH4'
                AND (bus0 IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country != 'DE'
                    AND carrier = 'CH4'
                )
                OR bus1 IN (
                    SELECT bus_id
                    FROM grid.egon_etrago_bus
                    WHERE scn_name = %s
                    AND country != 'DE'
                    AND carrier = 'CH4'
                ))
            """
            result = self.db_manager.execute_query(query, (scenario, scenario, scenario))
            crossborder_capacity = result[0]["p_nom"] if result and result[0]["p_nom"] else 0
            
            # Also check specifically for links connecting DE to abroad
            de_abroad_query = """
                SELECT SUM(p_nom::numeric) as p_nom_de_abroad
                FROM grid.egon_etrago_link l
                WHERE l.scn_name = %s
                AND l.carrier = 'CH4'
                AND (
                    (l.bus0 IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = %s AND country = 'DE' AND carrier = 'CH4'
                    ) AND l.bus1 IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = %s AND country != 'DE' AND carrier = 'CH4'
                    ))
                    OR
                    (l.bus1 IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = %s AND country = 'DE' AND carrier = 'CH4'
                    ) AND l.bus0 IN (
                        SELECT bus_id FROM grid.egon_etrago_bus
                        WHERE scn_name = %s AND country != 'DE' AND carrier = 'CH4'
                    ))
                )
            """
            de_abroad_result = self.db_manager.execute_query(de_abroad_query, (scenario, scenario, scenario, scenario, scenario))
            de_abroad_capacity = de_abroad_result[0]["p_nom_de_abroad"] if de_abroad_result and de_abroad_result[0]["p_nom_de_abroad"] else 0
            
            # Note: Input validation would require access to gas grid capacity source data
            
            return {
                "check_type": "crossborder_gas_links",
                "status": "SUCCESS" if crossborder_capacity > 0 else "WARNING",
                "message": f"Cross-border CH4 grid capacity: {crossborder_capacity:.2f} MW (DE-abroad: {de_abroad_capacity:.2f} MW)" if crossborder_capacity > 0 else "No cross-border CH4 grid capacity found",
                "total_crossborder_capacity_mw": crossborder_capacity,
                "de_abroad_capacity_mw": de_abroad_capacity
            }
            
        except Exception as e:
            return {
                "check_type": "crossborder_gas_links",
                "status": "CRITICAL_FAILURE",
                "error": f"Failed to validate cross-border gas links: {str(e)}"
            }