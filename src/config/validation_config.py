from src.rules.formal.null_check_rule import NullCheckRule
from src.rules.formal.time_series_rule import TimeSeriesValidationRule

# ==========================
# VALIDATION CONFIGURATIONS
# ==========================

VALIDATION_CONFIGURATIONS = {

    "comprehensive": {
        "description": "All validations - full data quality check",
        "rules": [
            {
                "name": "critical_null_checks",
                "rule_class": NullCheckRule,
                "config": [
                    {"table": "demand.egon_demandregio_hh", "column": "demand"},
                    {"table": "demand.egon_demandregio_hh", "column": "nuts3"},
                    {"table": "supply.egon_power_plants", "column": "el_capacity"},
                    {"table": "supply.egon_power_plants", "column": "carrier"}
                ]
            },
            {
                "name": "time_series_completeness",
                "rule_class": TimeSeriesValidationRule,
                "config": [
                    {"table": "demand.egon_demandregio_sites_ind_electricity_dsm_timeseries", "column": "p_mset", "expected_length": 8760},
                    {"table": "demand.egon_demandregio_timeseries_cts_ind", "column": "load_curve", "expected_length": 8760},
                    {"table": "demand.egon_etrago_electricity_cts_dsm_timeseries", "column": "p_set", "expected_length": 8760},
                    {"table": "demand.egon_etrago_timeseries_individual_heating", "column": "dist_aggregated_mw", "expected_length": 8760},
                    {"table": "demand.egon_heat_timeseries_selected_profiles", "column": "selected_idp_profiles", "expected_length": 8760},
                    {"table": "demand.egon_osm_ind_load_curves_individual_dsm_timeseries", "column": "p_set", "expected_length": 8760},
                    {"table": "demand.egon_sites_ind_load_curves_individual_dsm_timeseries", "column": "p_set", "expected_length": 8760},
                    {"table": "demand.egon_timeseries_district_heating", "column": "dist_aggregated_mw", "expected_length": 8760},
                    {"table": "grid.egon_etrago_bus_timeseries", "column": "v_mag_pu_set", "expected_length": 8760},
                    {"table": "grid.egon_etrago_generator_timeseries", "column": "p_max_pu", "expected_length": 8760},
                    {"table": "grid.egon_etrago_line_timeseries", "column": "s_max_pu", "expected_length": 8760},
                    {"table": "grid.egon_etrago_link_timeseries", "column": "p_min_pu", "expected_length": 8760},
                    {"table": "grid.egon_etrago_load_timeseries", "column": "p_set", "expected_length": 8760},
                    {"table": "grid.egon_etrago_storage_timeseries", "column": "inflow", "expected_length": 8760},
                    {"table": "grid.egon_etrago_store_timeseries", "column": "e_min_pu", "expected_length": 8760},
                    {"table": "grid.egon_etrago_transformer_timeseries", "column": "s_max_pu", "expected_length": 8760},
                ]
            }
        ]
    },

    "critical_only": {
        "description": "Only critical validations - for intermediate checks",
        "rules": [
            {
                "name": "essential_null_checks",
                "rule_class": NullCheckRule,
                "config": [
                    {"table": "demand.egon_demandregio_hh", "column": "demand"},
                    {"table": "supply.egon_power_plants", "column": "el_capacity"}
                ]
            },
            {
                "name": "core_time_series",
                "rule_class": TimeSeriesValidationRule,
                "config": [
                    {"table": "grid.egon_etrago_load_timeseries", "column": "p_set", "expected_length": 8760}
                ]
            }
        ]
    },

    "time_series_only": {
        "description": "Only time series validations",
        "rules": [
            {
                "name": "all_time_series",
                "rule_class": TimeSeriesValidationRule,
                "config": [
                    {"table": "grid.egon_etrago_load_timeseries", "column": "p_set", "expected_length": 8760},
                    {"table": "grid.egon_etrago_generator_timeseries", "column": "p_max_pu", "expected_length": 8760},
                    {"table": "grid.egon_etrago_link_timeseries", "column": "p_min_pu", "expected_length": 8760},
                    {"table": "grid.egon_etrago_line_timeseries", "column": "s_max_pu", "expected_length": 8760}
                ]
            }
        ]
    },

    "null_checks_only": {
        "description": "Only NULL validations",
        "rules": [
            {
                "name": "comprehensive_null_checks",
                "rule_class": NullCheckRule,
                "config": [
                    {"table": "demand.egon_demandregio_hh", "column": "demand"},
                    {"table": "demand.egon_demandregio_hh", "column": "nuts3"},
                    {"table": "demand.egon_demandregio_hh", "column": "scenario"},
                    {"table": "supply.egon_power_plants", "column": "el_capacity"},
                    {"table": "supply.egon_power_plants", "column": "carrier"},
                    {"table": "grid.egon_etrago_bus", "column": "carrier"}
                ]
            }
        ]
    },

    "quick_check": {
        "description": "Fast validation for development - minimal checks",
        "rules": [
            {
                "name": "basic_null_check",
                "rule_class": NullCheckRule,
                "config": [
                    {"table": "demand.egon_demandregio_hh", "column": "demand"}
                ]
            }
        ]
    }
}


# ====================================================================
# HELPER FUNCTIONS
# ====================================================================

def get_available_configurations():
    """Get list of available validation configurations"""
    return list(VALIDATION_CONFIGURATIONS.keys())


def get_configuration_description(config_name: str):
    """Get description of a specific configuration"""
    config = VALIDATION_CONFIGURATIONS.get(config_name)
    if config:
        return config.get("description", "No description available")
    return f"Configuration '{config_name}' not found"


def get_configuration_summary(config_name: str):
    """Get summary of rules in a configuration"""
    config = VALIDATION_CONFIGURATIONS.get(config_name)
    if not config:
        return None

    summary = {
        "config_name": config_name,
        "description": config.get("description", ""),
        "total_rules": len(config["rules"]),
        "rules": []
    }

    for rule in config["rules"]:
        rule_summary = {
            "name": rule["name"],
            "type": rule["rule_class"].__name__,
            "table_count": len(rule["config"]) if isinstance(rule["config"], list) else 1
        }
        summary["rules"].append(rule_summary)

    return summary


def validate_configuration(config_name: str):
    """Validate that a configuration is properly formed"""
    if config_name not in VALIDATION_CONFIGURATIONS:
        return False, f"Configuration '{config_name}' does not exist"

    config = VALIDATION_CONFIGURATIONS[config_name]

    if "rules" not in config:
        return False, f"Configuration '{config_name}' missing 'rules' key"

    for i, rule in enumerate(config["rules"]):
        required_keys = ["name", "rule_class", "config"]
        for key in required_keys:
            if key not in rule:
                return False, f"Rule {i} in '{config_name}' missing required key: {key}"

    return True, "Configuration is valid"