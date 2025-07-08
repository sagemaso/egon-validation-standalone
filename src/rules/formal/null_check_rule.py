from typing import Dict, Any
import pandas as pd

from src.rules.formal.batch_validation_rule import BatchValidationRule


class NullCheckRule(BatchValidationRule):
    """Validates that specified columns contain no NULL values"""

    def __init__(self, db_manager=None):
        super().__init__("null_check", db_manager)

    def _validate_single_column(self, engine, table: str, column: str, **kwargs) -> Dict[str, Any]:
        """
        Validates that a single column contains no NULL values with detailed logging
        """

        print(f"         🔍 Checking for NULL values in {table}.{column}")

        # Simple SQL query without scenario filtering
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN {column} IS NULL THEN 1 END) as null_count
        FROM {table}
        LIMIT 10000
        """

        try:
            result = pd.read_sql(query, engine)
            total_rows = result.iloc[0]['total_rows']
            null_count = result.iloc[0]['null_count']

            # Detailed logging of results
            print(f"         📊 Results for {table}.{column}:")
            print(f"            Total rows: {total_rows}")
            print(f"            NULL values: {null_count}")
            print(f"            Valid values: {total_rows - null_count}")

            # Determine validation result
            if null_count > 0:
                status = "FAILED"
                details = f"Found {null_count} NULL values in {table}.{column} ({total_rows} rows checked)"

                print(f"         ❌ VALIDATION FAILED for {table}.{column}")
                print(f"            Problem: {null_count} NULL values found")
            else:
                status = "SUCCESS"
                details = f"No NULL values found in {table}.{column} ({total_rows} rows checked)"

                print(f"         ✅ VALIDATION PASSED for {table}.{column}")
                print(f"            No NULL values found")

            return {
                "table": table,
                "column": column,
                "status": status,
                "total_rows": total_rows,
                "null_count": null_count,
                "invalid_count": null_count,  # For consistency with other rules
                "check_type": "null",
                "details": details
            }

        except Exception as e:
            print(f"         ❌ SQL EXECUTION FAILED for {table}.{column}")
            print(f"            Error: {str(e)}")

            return {
                "table": table,
                "column": column,
                "status": "FAILED",
                "total_rows": 0,
                "null_count": -1,
                "invalid_count": -1,
                "check_type": "null",
                "details": f"SQL execution failed: {str(e)}"
            }