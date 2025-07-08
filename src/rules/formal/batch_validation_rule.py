from abc import abstractmethod
from typing import List, Dict, Any

from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager


class BatchValidationRule(BaseValidationRule):
    """Base class for validation rules that can check multiple tables/columns"""

    def __init__(self, rule_name: str, db_manager: DatabaseManager = None):
        super().__init__(rule_name)
        self.db_manager = db_manager or DatabaseManager()

    def validate(self, table_column_configs: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validates multiple table/column combinations with detailed logging
        """

        all_results = []
        failed_count = 0
        total_count = len(table_column_configs)
        failed_tables = []
        summary = {}

        # Add detailed logging
        print(f"\n🔍 Starting {self.rule_name} validation for {total_count} table/column combinations:")

        try:
            with self.db_manager.connection_context() as engine:

                for i, config in enumerate(table_column_configs, 1):
                    table = config["table"]
                    column = config["column"]

                    print(f"\n   📋 [{i}/{total_count}] Validating: {table}.{column}")

                    # Show expected parameters if available
                    if "expected_length" in config:
                        print(f"      Expected length: {config['expected_length']}")

                    # Pass the entire config to _validate_single_column
                    single_result = self._validate_single_column(
                        engine, table, column, **{k: v for k, v in config.items()
                                                  if k not in ["table", "column"]}
                    )

                    all_results.append(single_result)

                    # Detailed logging for each result
                    if single_result["status"] == "SUCCESS":
                        print(f"      ✅ SUCCESS: {single_result.get('details', 'Validation passed')}")
                    else:
                        print(f"      ❌ FAILED: {single_result.get('details', 'Validation failed')}")
                        if single_result.get('expected_length'):
                            print(f"         Expected length: {single_result['expected_length']}")
                        if single_result.get('found_lengths'):
                            print(f"         Found lengths: {single_result['found_lengths']}")
                        if single_result.get('wrong_length'):
                            print(f"         Rows with wrong length: {single_result['wrong_length']}")
                        if single_result.get('total_rows'):
                            print(f"         Total rows checked: {single_result['total_rows']}")

                    # Track results for summary
                    key = f"{table}.{column}"
                    summary[key] = single_result["status"]

                    if single_result["status"] == "FAILED":
                        failed_count += 1
                        failed_tables.append(key)

                # Summary logging
                print(f"\n📊 {self.rule_name} Summary:")
                print(f"   Total validations: {total_count}")
                print(f"   Passed: {total_count - failed_count}")
                print(f"   Failed: {failed_count}")

                if failed_tables:
                    print(f"   Failed tables/columns:")
                    for failed_table in failed_tables:
                        print(f"      ❌ {failed_table}")

                # Create summary ValidationResult
                if failed_count > 0:
                    status = "CRITICAL_FAILURE"
                    error_details = f"{failed_count} of {total_count} validations failed: {', '.join(failed_tables)}"
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
            print(f"❌ Batch validation execution failed: {str(e)}")
            return self._create_failure_result(
                table="multiple_tables",
                error_details=f"Batch validation execution failed: {str(e)}"
            )

    @abstractmethod
    def _validate_single_column(self, engine, table: str, column: str, **kwargs) -> Dict[str, Any]:
        """
        Abstract method to validate a single table/column combination

        Must be implemented by subclasses (NullCheckRule, TimeSeriesValidationRule, etc.)

        Parameters:
        -----------
        engine : SQLAlchemy Engine
            Database connection
        table : str
            Table name
        column : str
            Column name
        **kwargs : additional parameters specific to validation type
            e.g., expected_length for time series, min_value/max_value for ranges

        Returns:
        --------
        Dict with keys: "status", "total_rows", "invalid_count", "details", etc.
        """
        pass