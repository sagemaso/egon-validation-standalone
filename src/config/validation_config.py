from src.rules.formal.null_check_rule import NullCheckRule
from src.rules.formal.nan_check_rule import NanCheckRule
from src.rules.formal.time_series_rule import TimeSeriesValidationRule
from src.rules.sanity.etrago_electricity_sanity_rule import EtragoElectricitySanityRule
from src.rules.sanity.etrago_heat_sanity_rule import EtragoHeatSanityRule
from src.rules.sanity.residential_electricity_annual_sum_rule import ResidentialElectricityAnnualSumRule
from src.rules.sanity.residential_electricity_hh_refinement_rule import ResidentialElectricityHhRefinementRule
from src.rules.sanity.cts_electricity_demand_share_rule import CtsElectricityDemandShareRule
from src.rules.sanity.cts_heat_demand_share_rule import CtsHeatDemandShareRule

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
                    {"table": "demand.egon_demandregio_hh", "column": "scenario"},
                    {"table": "supply.egon_power_plants", "column": "el_capacity"},
                    {"table": "supply.egon_power_plants", "column": "carrier"},
                    {"table": "grid.egon_etrago_bus", "column": "carrier"}
                ]
            },
            {
                "name": "critical_nan_checks",
                "rule_class": NanCheckRule,
                "config": [
                    {"table": "demand.egon_demandregio_hh", "column": "demand"},
                    {"table": "supply.egon_power_plants", "column": "el_capacity"},
                    {"table": "grid.egon_etrago_load_timeseries", "column": "p_set"}
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
            },
            {
                "name": "etrago_electricity_sanity",
                "rule_class": EtragoElectricitySanityRule,
                "config": {
                    "scenario": "eGon2035",
                    "tolerance": 5.0,
                    "table": "grid.egon_etrago_generator",
                    "column": "p_nom"
                }
            },
            {
                "name": "etrago_heat_sanity",
                "rule_class": EtragoHeatSanityRule,
                "config": {
                    "scenario": "eGon2035",
                    "tolerance": 5.0,
                    "table": "grid.egon_etrago_load",
                    "column": "p_set"
                }
            },
            {
                "name": "residential_electricity_annual_sum",
                "rule_class": ResidentialElectricityAnnualSumRule,
                "config": {
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "tolerance": 1e-5,
                    "table": "demand.egon_demandregio_zensus_electricity",
                    "column": "demand"
                }
            },
            {
                "name": "residential_electricity_hh_refinement",
                "rule_class": ResidentialElectricityHhRefinementRule,
                "config": {
                    "tolerance": 1e-5,
                    "table": "society.egon_destatis_zensus_household_per_ha_refined",
                    "column": "hh_10types"
                }
            },
            {
                "name": "cts_electricity_demand_share",
                "rule_class": CtsElectricityDemandShareRule,
                "config": {
                    "tolerance": 1e-5,
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "table": "demand.egon_cts_electricity_demand_building_share",
                    "column": "profile_share"
                }
            },
            {
                "name": "cts_heat_demand_share",
                "rule_class": CtsHeatDemandShareRule,
                "config": {
                    "tolerance": 1e-5,
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "table": "demand.egon_cts_heat_demand_building_share",
                    "column": "profile_share"
                }
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
                "rule_class": NanCheckRule,
                "config": [
                    {"table": "demand.egon_demandregio_hh", "column": "demand"}
                ]
            }
        ]
    },

    "sanity_checks": {
        "description": "Sanity checks for data consistency and business logic",
        "rules": [
            {
                "name": "etrago_electricity_sanity",
                "rule_class": EtragoElectricitySanityRule,
                "config": {
                    "scenario": "eGon2035",
                    "tolerance": 5.0,
                    "table": "grid.egon_etrago_generator",
                    "column": "p_nom"
                }
            },
            {
                "name": "etrago_heat_sanity",
                "rule_class": EtragoHeatSanityRule,
                "config": {
                    "scenario": "eGon2035",
                    "tolerance": 5.0,
                    "table": "grid.egon_etrago_load",
                    "column": "p_set"
                }
            },
            {
                "name": "residential_electricity_annual_sum",
                "rule_class": ResidentialElectricityAnnualSumRule,
                "config": {
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "tolerance": 1e-5,
                    "table": "demand.egon_demandregio_zensus_electricity",
                    "column": "demand"
                }
            },
            {
                "name": "residential_electricity_hh_refinement",
                "rule_class": ResidentialElectricityHhRefinementRule,
                "config": {
                    "tolerance": 1e-5,
                    "table": "society.egon_destatis_zensus_household_per_ha_refined",
                    "column": "hh_10types"
                }
            },
            {
                "name": "cts_electricity_demand_share",
                "rule_class": CtsElectricityDemandShareRule,
                "config": {
                    "tolerance": 1e-5,
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "table": "demand.egon_cts_electricity_demand_building_share",
                    "column": "profile_share"
                }
            },
            {
                "name": "cts_heat_demand_share",
                "rule_class": CtsHeatDemandShareRule,
                "config": {
                    "tolerance": 1e-5,
                    "scenarios": ["eGon2035", "eGon100RE"],
                    "table": "demand.egon_cts_heat_demand_building_share",
                    "column": "profile_share"
                }
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