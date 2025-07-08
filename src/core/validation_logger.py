import logging
from typing import Dict, Any, Optional
from datetime import datetime


class ValidationLogger:
    """Centralized logger for validation operations with focus on failures"""

    def __init__(self, name: str = "validation"):
        self.logger = logging.getLogger(f"egon.data.{name}")
        self.logger.setLevel(logging.INFO)

        # Create formatter focused on validation context
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s'
        )

        # Add console handler if not already present
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def log_validation_start(self, rule_name: str, total_count: int):
        """Log start of validation batch"""
        print(f"\n🔍 Starting {rule_name} validation for {total_count} table/column combinations")

    def log_validation_item_start(self, index: int, total: int, table: str, column: str, **params):
        """Log start of individual validation with minimal output"""
        param_info = ""
        if "expected_length" in params:
            param_info = f" (expected: {params['expected_length']})"

        print(f"   [{index}/{total}] {table}.{column}{param_info}")

    def log_success_brief(self, result: Dict[str, Any]):
        """Brief success logging"""
        table = result.get('table', 'unknown')
        column = result.get('column', 'unknown')
        total_rows = result.get('total_rows', 0)

        print(f"      ✅ OK ({total_rows} rows)")

    def log_failure_detailed(self, result: Dict[str, Any]):
        """Detailed failure logging with all relevant information"""
        table = result.get('table', 'unknown')
        column = result.get('column', 'unknown')
        check_type = result.get('check_type', 'unknown')

        print(f"      ❌ FAILED - {check_type.upper()} CHECK")

        # Common failure info
        if result.get('total_rows'):
            print(f"         Total rows checked: {result['total_rows']}")

        # Type-specific failure details
        if check_type == "time_series":
            self._log_time_series_failure(result)
        elif check_type == "null":
            self._log_null_failure(result)
        else:
            print(f"         Details: {result.get('details', 'No details available')}")

    def _log_time_series_failure(self, result: Dict[str, Any]):
        """Detailed logging for time series failures"""
        expected = result.get('expected_length', 'unknown')
        wrong_count = result.get('wrong_length', 0)
        found_lengths = result.get('found_lengths', [])

        print(f"         Expected length: {expected} values per time series")
        print(f"         Rows with wrong length: {wrong_count}")
        print(f"         Found lengths: {found_lengths}")

        # Calculate percentage of failures
        total = result.get('total_rows', 0)
        if total > 0:
            failure_rate = (wrong_count / total) * 100
            print(f"         Failure rate: {failure_rate:.2f}%")

    def _log_null_failure(self, result: Dict[str, Any]):
        """Detailed logging for NULL check failures"""
        null_count = result.get('null_count', 0)
        total = result.get('total_rows', 0)

        print(f"         NULL values found: {null_count}")

        if total > 0:
            null_rate = (null_count / total) * 100
            print(f"         NULL rate: {null_rate:.2f}%")

    def log_validation_summary(self, rule_name: str, total: int, passed: int, failed: int, failed_tables: list):
        """Log final validation summary"""
        print(f"\n📊 {rule_name} Summary:")
        print(f"   Total: {total} | Passed: {passed} | Failed: {failed}")

        if failed_tables:
            print(f"   ❌ Failed validations:")
            for table in failed_tables:
                print(f"      • {table}")
        else:
            print(f"   ✅ All validations passed!")

    def log_execution_error(self, table: str, column: str, error: Exception):
        """Log SQL execution or other technical errors"""
        print(f"      ❌ EXECUTION ERROR")
        print(f"         Table: {table}")
        print(f"         Column: {column}")
        print(f"         Error: {str(error)}")

        # Log to standard logger for debugging
        self.logger.error(f"Validation execution failed for {table}.{column}: {error}")

    def critical(self, message: str, **context):
        """Log critical validation failures"""
        self.logger.critical(message, extra=context)

    def warning(self, message: str, **context):
        """Log validation warnings"""
        self.logger.warning(message, extra=context)

    def info(self, message: str, **context):
        """Log general validation info"""
        self.logger.info(message, extra=context)