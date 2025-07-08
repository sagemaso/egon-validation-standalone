import os
from abc import abstractmethod
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult

load_dotenv()


class BatchValidationRule(BaseValidationRule):
    """Base class for validation rules that can check multiple tables/columns"""

    def __init__(self, rule_name: str):
        super().__init__(rule_name)

    def validate(self, table_column_configs: List[Dict[str, str]]) -> ValidationResult:
        """
        Validates multiple table/column combinations

        Parameters:
        -----------
        table_column_configs : List[Dict]
            List of configurations like:
            [
                {"table": "demand.egon_demandregio_hh", "column": "demand"},
                {"table": "supply.egon_power_plants", "column": "capacity"}
            ]

        Returns:
        --------
        ValidationResult
            Single result summarizing all validations
        """

        all_results = []
        failed_count = 0
        total_count = len(table_column_configs)
        failed_tables = []
        summary = {}

        try:
            with self._get_ssh_tunnel() as tunnel:
                engine = self._get_database_connection()

                for config in table_column_configs:
                    table = config["table"]
                    column = config["column"]
                    scenario = config.get("scenario")  # Optional

                    # Call the specific validation method (implemented by subclasses)
                    single_result = self._validate_single_column(
                        engine, table, column, scenario
                    )

                    all_results.append(single_result)

                    # Track results for summary
                    key = f"{table}.{column}"
                    summary[key] = single_result["status"]

                    if single_result["status"] == "FAILED":
                        failed_count += 1
                        failed_tables.append(key)

                # Create summary ValidationResult
                if failed_count > 0:
                    status = "CRITICAL_FAILURE"
                    error_details = f"{failed_count} of {total_count} validations failed"
                    message = None
                else:
                    status = "SUCCESS"
                    error_details = None
                    message = f"All {total_count} validations passed"

                return ValidationResult(
                    rule_name=self.rule_name,
                    status=status,
                    table="multiple_tables",
                    function_name="validate",
                    module_name=self.__class__.__module__,
                    message=message,
                    error_details=error_details,
                    detailed_context={
                        "total_validations": total_count,
                        "passed": total_count - failed_count,
                        "failed": failed_count,
                        "failed_tables": failed_tables,
                        "summary": summary,
                        "detailed_results": all_results
                    }
                )

        except Exception as e:
            return self._create_failure_result(
                table="multiple_tables",
                error_details=f"Batch validation execution failed: {str(e)}"
            )

    @abstractmethod
    def _validate_single_column(self, engine, table: str, column: str,
                                scenario: str = None) -> Dict[str, Any]:
        """
        Abstract method to validate a single table/column combination

        Must be implemented by subclasses (NullCheckRule, NaNCheckRule, etc.)

        Returns:
        --------
        Dict with keys: "status", "rows_checked", "invalid_count", "details"
        """
        pass

    def _get_ssh_tunnel(self):
        """Creates SSH tunnel context manager"""
        ssh_config = {
            'host': os.getenv("SSH_HOST"),
            'user': os.getenv("SSH_USER"),
            'key': os.path.expanduser(os.getenv("SSH_KEY_FILE")),
            'local_port': int(os.getenv("SSH_LOCAL_PORT")),
            'remote_port': int(os.getenv("SSH_REMOTE_PORT"))
        }

        return SSHTunnelForwarder(
            (ssh_config['host'], 22),
            ssh_username=ssh_config['user'],
            ssh_pkey=ssh_config['key'],
            remote_bind_address=('localhost', ssh_config['remote_port']),
            local_bind_address=('localhost', ssh_config['local_port'])
        )

    def _get_database_connection(self):
        """Creates database engine"""
        db_config = {
            'host': os.getenv("DB_HOST"),
            'port': int(os.getenv("DB_PORT")),
            'name': os.getenv("DB_NAME"),
            'user': os.getenv("DB_USER"),
            'password': os.getenv("DB_PASSWORD")
        }

        connection_string = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        return create_engine(connection_string)