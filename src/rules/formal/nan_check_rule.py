from typing import Dict, Any
import pandas as pd

from src.rules.formal.batch_validation_rule import BatchValidationRule


class NanCheckRule(BatchValidationRule):
    """Validates that specified columns contain no NaN values"""

    def __init__(self, db_manager=None):
        super().__init__("nan_check", db_manager)

    def _validate_single_column(self, engine, table: str, column: str, **kwargs) -> Dict[str, Any]:
        """
        Validates that a single column contains no NaN values
        Central logger handles all output

        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Database connection (provided by DatabaseManager)
        table : str
            Table name (e.g., "demand.egon_demandregio_hh")
        column : str
            Column name (e.g., "demand")
        **kwargs : additional parameters (ignored for NaN check)

        Returns:
        --------
        Dict with validation results for this column
        """

        # SQL query to check for NaN values
        # Note: In PostgreSQL, NaN is represented as 'NaN' string for float types
        # We check for both 'NaN' and cases where the column cannot be cast to numeric
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE 
                WHEN {column}::text = 'NaN' THEN 1 
                WHEN {column} IS NOT NULL AND NOT ({column}::text ~ '^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$') THEN 1
                ELSE NULL 
            END) as nan_count
        FROM {table}
        """

        try:
            result = pd.read_sql(query, engine)
            total_rows = result.iloc[0]['total_rows']
            nan_count = result.iloc[0]['nan_count']

            # Determine validation result
            if nan_count > 0:
                status = "FAILED"
                details = f"Found {nan_count} NaN values in {table}.{column} ({total_rows} rows checked)"
            else:
                status = "SUCCESS"
                details = f"No NaN values found in {table}.{column} ({total_rows} rows checked)"

            return {
                "table": table,
                "column": column,
                "status": status,
                "total_rows": total_rows,
                "nan_count": nan_count,
                "invalid_count": nan_count,  # For consistency with other rules
                "check_type": "nan",
                "details": details
            }

        except Exception as e:
            return {
                "table": table,
                "column": column,
                "status": "FAILED",
                "total_rows": 0,
                "nan_count": -1,
                "invalid_count": -1,
                "check_type": "nan",
                "details": f"SQL execution failed: {str(e)}"
            }