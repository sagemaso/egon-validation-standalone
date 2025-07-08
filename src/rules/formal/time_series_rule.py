from typing import Dict, Any
import pandas as pd

from src.rules.formal.batch_validation_rule import BatchValidationRule


class TimeSeriesValidationRule(BatchValidationRule):
    """Validates time series completeness with specified length for multiple tables/columns"""

    def __init__(self, db_manager=None):
        super().__init__("time_series_completeness", db_manager)

    def _validate_single_column(self, engine, table: str, column: str, **kwargs) -> Dict[str, Any]:
        """
        Validates that a single time series column has the expected length with detailed logging
        """

        # Get expected_length from kwargs
        expected_length = kwargs.get('expected_length', 8760)

        print(f"         🔍 Checking time series length in {table}.{column}")
        print(f"         📏 Expected length: {expected_length} values")

        # Simple SQL query without scenario filtering
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN cardinality({column}) = {expected_length} THEN 1 END) as correct_length,
            COUNT(CASE WHEN cardinality({column}) != {expected_length} THEN 1 END) as wrong_length,
            array_agg(DISTINCT cardinality({column})) as found_lengths
        FROM {table}
        LIMIT 1000
        """

        try:
            result = pd.read_sql(query, engine)
            total_rows = result.iloc[0]['total_rows']
            correct_length = result.iloc[0]['correct_length']
            wrong_length = result.iloc[0]['wrong_length']
            found_lengths = result.iloc[0]['found_lengths']

            # Detailed logging of results
            print(f"         📊 Results for {table}.{column}:")
            print(f"            Total rows: {total_rows}")
            print(f"            Correct length ({expected_length}): {correct_length}")
            print(f"            Wrong length: {wrong_length}")
            print(f"            Found lengths: {found_lengths}")

            # Determine validation result
            if wrong_length > 0:
                status = "FAILED"
                details = f"Found {wrong_length} time series with invalid length in {table}.{column}. "
                details += f"Expected: {expected_length}, Found lengths: {found_lengths}, "
                details += f"Total checked: {total_rows}"

                print(f"         ❌ VALIDATION FAILED for {table}.{column}")
                print(f"            Problem: {wrong_length} out of {total_rows} time series have wrong length")
                print(f"            Expected: {expected_length} values per time series")
                print(f"            Found: {found_lengths} values per time series")
            else:
                status = "SUCCESS"
                details = f"All {total_rows} time series in {table}.{column} have correct length of {expected_length}"

                print(f"         ✅ VALIDATION PASSED for {table}.{column}")
                print(f"            All {total_rows} time series have correct length")

            return {
                "table": table,
                "column": column,
                "status": status,
                "total_rows": total_rows,
                "correct_length": correct_length,
                "wrong_length": wrong_length,
                "invalid_count": wrong_length,  # For consistency with other rules
                "expected_length": expected_length,
                "found_lengths": found_lengths,
                "check_type": "time_series",
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
                "correct_length": 0,
                "wrong_length": -1,
                "invalid_count": -1,
                "expected_length": expected_length,
                "check_type": "time_series",
                "details": f"SQL execution failed: {str(e)}"
            }