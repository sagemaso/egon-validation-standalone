from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, Optional

@dataclass
class ValidationResult:
    """Result of a validation rule execution"""
    rule_name: str
    status: str  # "SUCCESS", "WARNING", "CRITICAL_FAILURE"
    table: str
    function_name: str
    module_name: str
    message: Optional[str] = None
    error_details: Optional[str] = None
    detailed_context: Optional[Dict[str, Any]] = None
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for JSON serialization"""
        return {
            "rule_name": self.rule_name,
            "status": self.status,
            "table": self.table,
            "function_name": self.function_name,
            "module_name": self.module_name,
            "message": self.message,
            "error_details": self.error_details,
            "detailed_context": self.detailed_context,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None
        }
