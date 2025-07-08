import os
import pandas as pd
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
from typing import Optional

from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult

load_dotenv()


class TimeSeriesValidationRule(BaseValidationRule):
    """Validates time series completeness with specified length"""

    def __init__(self):
        super().__init__("time_series_completeness")

    def validate(self, table: str, column: str, expected_length: int,
                 scenario: Optional[str] = None) -> ValidationResult:
        """
        Validates that time series arrays have the expected length

        Parameters:
        -----------
        table : str
            Database table to validate (e.g., "grid.egon_etrago_load_timeseries")
        column : str
            Column containing time series arrays (e.g., "p_set")
        expected_length : int
            Expected array length (e.g., 8760 for hourly data)
        scenario : str
            Scenario name to filter by
        """

        try:
            with self._get_ssh_tunnel() as tunnel:
                engine = self._get_database_connection()

                where_clause = ""
                if scenario:
                    where_clause = f"WHERE scn_name = '{scenario}'"

                query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(CASE WHEN cardinality({column}) = {expected_length} THEN 1 END) as correct_length,
                    COUNT(CASE WHEN cardinality({column}) != {expected_length} THEN 1 END) as wrong_length
                FROM {table}
                {where_clause}
                LIMIT 1000
                """

                result = pd.read_sql(query, engine)

                total = result.iloc[0]['total_rows']
                correct = result.iloc[0]['correct_length']
                wrong = result.iloc[0]['wrong_length']

                if wrong > 0:
                    return self._create_failure_result(
                        table=table,
                        error_details=f"Found {wrong} time series with invalid length. Expected: {expected_length}, Total checked: {total}, Wrong: {wrong}"
                    )

                return self._create_success_result(
                    table=table,
                    message=f"All {total} time series have correct length of {expected_length}"
                )

        except Exception as e:
            return self._create_failure_result(
                table=table,
                error_details=f"Validation execution failed: {str(e)}"
            )

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