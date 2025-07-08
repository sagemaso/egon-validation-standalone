from abc import ABC, abstractmethod

from src.core.validation_result import ValidationResult

class BaseValidationRule(ABC):
    """Abstract base class for all validation rules"""

    def __init__(self, rule_name: str):
        self.rule_name = rule_name

    @abstractmethod
    def validate(self, **kwargs) -> ValidationResult:
        """Abstract method that must be implemented by subclasses"""
        pass

    def _create_success_result(self, table: str, message: str) -> ValidationResult:
        """Helper method to create success results"""
        return ValidationResult(
            rule_name=self.rule_name,
            status="SUCCESS",
            table=table,
            function_name="validate",
            module_name=self.__class__.__module__,
            message=message
        )

    def _create_failure_result(self, table: str, error_details: str,
                               status: str = "CRITICAL_FAILURE") -> ValidationResult:
        """Helper method to create failure results"""
        return ValidationResult(
            rule_name=self.rule_name,
            status=status,
            table=table,
            function_name="validate",
            module_name=self.__class__.__module__,
            error_details=error_details
        )