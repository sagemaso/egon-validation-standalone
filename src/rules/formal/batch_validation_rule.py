from abc import abstractmethod
from typing import List, Dict, Any

from src.rules.base_rule import BaseValidationRule
from src.core.validation_result import ValidationResult
from src.core.database_manager import DatabaseManager
from src.core.validation_logger import ValidationLogger


class BatchValidationRule(BaseValidationRule):
    """Base class for validation rules that can check multiple tables/columns"""

    def __init__(self, rule_name: str, db_manager: DatabaseManager = None):
        super().__init__(rule_name)
        self.db_manager = db_manager or DatabaseManager()
        self.logger = ValidationLogger(rule_name)  # Add centralized logger

    def validate(self, table_column_configs: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validates multiple table/column combinations with centralized logging
        """

        all_results = []
        failed_count = 0
        total_count = len(table_column_configs)
        failed_tables = []
        summary = {}

        # Central logging start
        self.logger.log_validation_start(self.rule_name, total_count)

        try:
            with self.db_manager.connection_context() as engine:

                for i, config in enumerate(table_column_configs, 1):
                    table = config["table"]
                    column = config["column"]

                    # Log validation item start
                    self.logger.log_validation_item_start(i, total_count, table, column, **{k: v for k, v in config.items() if k not in ["table", "column"]})

                    try:
                        # Pass the entire config to _validate_single_column
                        single_result = self._validate_single_column(
                            engine, table, column, **{k: v for k, v in config.items()
                                                      if k not in ["table", "column"]}
                        )

                        all_results.append(single_result)

                        # Central logging for results
                        if single_result["status"] == "SUCCESS":
                            self.logger.log_success_brief(single_result)
                        else:
                            self.logger.log_failure_detailed(single_result)
                            failed_count += 1
                            failed_tables.append(f"{table}.{column}")

                        # Track results for summary
                        key = f"{table}.{column}"
                        summary[key] = single_result["status"]

                    except Exception as e:
                        # Log execution errors
                        self.logger.log_execution_error(table, column, e)

                        # Create error result
                        error_result = {
                            "table": table,
                            "column": column,
                            "status": "FAILED",
                            "error": str(e),
                            "details": f"Execution failed: {str(e)}"
                        }
                        all_results.append(error_result)
                        failed_count += 1
                        failed_tables.append(f"{table}.{column}")
                        summary[f"{table}.{column}"] = "FAILED"

                # Central summary logging
                passed_count = total_count - failed_count
                self.logger.log_validation_summary(self.rule_name, total_count, passed_count, failed_count,
                                                   failed_tables)

                # Create summary ValidationResult
                if failed_count > 0:
                    status = "CRITICAL_FAILURE"
                    error_details = f"{failed_count} of {total_count} validations failed: {', '.join(failed_tables)}"
                    message = None

                    # Log critical failure
                    self.logger.critical(f"Validation batch failed: {error_details}")
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
                        "passed": passed_count,
                        "failed": failed_count,
                        "failed_tables": failed_tables,
                        "summary": summary,
                        "detailed_results": all_results
                    }
                )

        except Exception as e:
            self.logger.critical(f"Batch validation execution failed: {str(e)}")
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