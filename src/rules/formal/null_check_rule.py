from typing import Dict, Any
import pandas as pd

from src.rules.formal.batch_validation_rule import BatchValidationRule


class NullCheckRule(BatchValidationRule):
    """Validates that specified columns contain no NULL values"""

    def __init__(self):
        super().__init__("null_check")

    def _validate_single_column(self, engine, table: str, column: str,
                                scenario: str = None) -> Dict[str, Any]:
        """
        Validates that a single column contains no NULL values

        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Database connection
        table : str
            Table name (e.g., "demand.egon_demandregio_hh")
        column : str
            Column name (e.g., "demand")
        scenario : str, optional
            Scenario filter (e.g., "eGon2035")

        Returns:
        --------
        Dict with validation results for this column
        """

        # Build scenario filter
        where_clause = ""
        if scenario:
            where_clause = f"WHERE scenario = '{scenario}'"

        # SQL query to check for NULL values
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN {column} IS NULL THEN 1 END) as null_count
        FROM {table}
        {where_clause}
        LIMIT 10000
        """

        try:
            result = pd.read_sql(query, engine)
            total_rows = result.iloc[0]['total_rows']
            null_count = result.iloc[0]['null_count']

            # Determine validation result
            if null_count > 0:
                status = "FAILED"
                details = f"Found {null_count} NULL values in {table}.{column} ({total_rows} rows checked)"
            else:
                status = "SUCCESS"
                details = f"No NULL values found in {table}.{column} ({total_rows} rows checked)"

            # Add scenario info if applicable
            if scenario:
                details += f" for scenario '{scenario}'"

            return {
                "table": table,
                "column": column,
                "scenario": scenario,
                "status": status,
                "total_rows": total_rows,
                "null_count": null_count,
                "invalid_count": null_count,  # For consistency with other rules
                "check_type": "null",
                "details": details
            }

        except Exception as e:
            return {
                "table": table,
                "column": column,
                "scenario": scenario,
                "status": "FAILED",
                "total_rows": 0,
                "null_count": -1,
                "invalid_count": -1,
                "check_type": "null",
                "details": f"SQL execution failed: {str(e)}"
            }