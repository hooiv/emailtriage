"""
Request/Response Validation System for Email Triage Environment

Advanced validation providing:
- JSON Schema validation
- Custom validation rules
- Request sanitization
- Response validation
- Validation analytics
"""

from typing import Any, Dict, List, Optional, Callable, Set, Union
from datetime import datetime
from collections import deque
import re
import json
import threading


class ValidationRule:
    """Individual validation rule"""
    
    def __init__(
        self,
        name: str,
        validator: Callable[[Any], bool],
        error_message: str,
        severity: str = "error"  # error, warning, info
    ):
        self.name = name
        self.validator = validator
        self.error_message = error_message
        self.severity = severity
    
    def validate(self, value: Any) -> Dict[str, Any]:
        """Run the validation"""
        try:
            is_valid = self.validator(value)
            return {
                "rule": self.name,
                "valid": is_valid,
                "severity": self.severity,
                "message": None if is_valid else self.error_message
            }
        except Exception as e:
            return {
                "rule": self.name,
                "valid": False,
                "severity": "error",
                "message": f"Validation error: {str(e)}"
            }


class FieldSchema:
    """Schema for a field"""
    
    def __init__(
        self,
        name: str,
        field_type: str,  # string, integer, float, boolean, array, object
        required: bool = False,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        pattern: Optional[str] = None,
        enum: Optional[List[str]] = None,
        default: Any = None,
        description: str = ""
    ):
        self.name = name
        self.field_type = field_type
        self.required = required
        self.min_length = min_length
        self.max_length = max_length
        self.min_value = min_value
        self.max_value = max_value
        self.pattern = pattern
        self.enum = enum
        self.default = default
        self.description = description
    
    def validate(self, value: Any) -> List[Dict[str, Any]]:
        """Validate a value against the schema"""
        errors = []
        
        # Required check
        if value is None:
            if self.required:
                errors.append({
                    "field": self.name,
                    "error": "Field is required",
                    "severity": "error"
                })
            return errors
        
        # Type check
        type_valid = self._check_type(value)
        if not type_valid:
            errors.append({
                "field": self.name,
                "error": f"Expected type {self.field_type}, got {type(value).__name__}",
                "severity": "error"
            })
            return errors
        
        # String validations
        if self.field_type == "string" and isinstance(value, str):
            if self.min_length and len(value) < self.min_length:
                errors.append({
                    "field": self.name,
                    "error": f"Minimum length is {self.min_length}",
                    "severity": "error"
                })
            if self.max_length and len(value) > self.max_length:
                errors.append({
                    "field": self.name,
                    "error": f"Maximum length is {self.max_length}",
                    "severity": "error"
                })
            if self.pattern and not re.match(self.pattern, value):
                errors.append({
                    "field": self.name,
                    "error": f"Does not match pattern {self.pattern}",
                    "severity": "error"
                })
        
        # Numeric validations
        if self.field_type in ["integer", "float"] and isinstance(value, (int, float)):
            if self.min_value is not None and value < self.min_value:
                errors.append({
                    "field": self.name,
                    "error": f"Minimum value is {self.min_value}",
                    "severity": "error"
                })
            if self.max_value is not None and value > self.max_value:
                errors.append({
                    "field": self.name,
                    "error": f"Maximum value is {self.max_value}",
                    "severity": "error"
                })
        
        # Enum validation
        if self.enum and value not in self.enum:
            errors.append({
                "field": self.name,
                "error": f"Value must be one of: {', '.join(self.enum)}",
                "severity": "error"
            })
        
        return errors
    
    def _check_type(self, value: Any) -> bool:
        """Check if value matches expected type"""
        if self.field_type == "string":
            return isinstance(value, str)
        elif self.field_type == "integer":
            return isinstance(value, int) and not isinstance(value, bool)
        elif self.field_type == "float":
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif self.field_type == "boolean":
            return isinstance(value, bool)
        elif self.field_type == "array":
            return isinstance(value, list)
        elif self.field_type == "object":
            return isinstance(value, dict)
        return True


class RequestSchema:
    """Schema for a request"""
    
    def __init__(self, name: str, fields: List[FieldSchema]):
        self.name = name
        self.fields = {f.name: f for f in fields}
    
    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate request data against schema"""
        errors = []
        warnings = []
        
        # Validate each field
        for field_name, field_schema in self.fields.items():
            value = data.get(field_name)
            field_errors = field_schema.validate(value)
            for error in field_errors:
                if error["severity"] == "warning":
                    warnings.append(error)
                else:
                    errors.append(error)
        
        # Check for unknown fields
        for key in data.keys():
            if key not in self.fields:
                warnings.append({
                    "field": key,
                    "error": "Unknown field",
                    "severity": "warning"
                })
        
        return {
            "valid": len(errors) == 0,
            "schema": self.name,
            "errors": errors,
            "warnings": warnings,
            "fields_validated": len(self.fields),
            "fields_present": len(data)
        }


class RequestValidator:
    """Main request validation system"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.schemas: Dict[str, RequestSchema] = {}
        self.custom_rules: Dict[str, List[ValidationRule]] = {}
        self.validation_history = deque(maxlen=1000)
        self.stats = {
            "total_validations": 0,
            "passed": 0,
            "failed": 0,
            "warnings": 0
        }
        
        # Register default schemas
        self._register_default_schemas()
    
    def _register_default_schemas(self):
        """Register default request schemas"""
        
        # Action schema
        action_schema = RequestSchema("action", [
            FieldSchema("action", "string", required=True, 
                       enum=["categorize", "prioritize", "reply", "forward", "archive", "flag", "batch", "undo"]),
            FieldSchema("email_id", "string", required=True, min_length=1),
            FieldSchema("category", "string", required=False,
                       enum=["urgent", "support", "newsletter", "spam", "personal", "work"]),
            FieldSchema("priority", "string", required=False,
                       enum=["low", "medium", "high"]),
            FieldSchema("reply_content", "string", required=False, max_length=10000),
            FieldSchema("forward_to", "string", required=False, 
                       pattern=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
        ])
        
        # Reset schema
        reset_schema = RequestSchema("reset", [
            FieldSchema("task_id", "string", required=False,
                       enum=["easy", "medium", "hard"]),
            FieldSchema("seed", "integer", required=False, min_value=0)
        ])
        
        # Search schema
        search_schema = RequestSchema("search", [
            FieldSchema("query", "string", required=False, max_length=1000),
            FieldSchema("category", "string", required=False),
            FieldSchema("priority", "string", required=False),
            FieldSchema("processed", "boolean", required=False),
            FieldSchema("limit", "integer", required=False, min_value=1, max_value=100),
            FieldSchema("offset", "integer", required=False, min_value=0)
        ])
        
        # Batch schema
        batch_schema = RequestSchema("batch", [
            FieldSchema("actions", "array", required=True)
        ])
        
        self.schemas = {
            "action": action_schema,
            "reset": reset_schema,
            "search": search_schema,
            "batch": batch_schema
        }
    
    def register_schema(self, name: str, fields: List[Dict[str, Any]]):
        """Register a new request schema"""
        with self._lock:
            field_schemas = []
            for f in fields:
                field_schemas.append(FieldSchema(
                    name=f.get("name"),
                    field_type=f.get("type", "string"),
                    required=f.get("required", False),
                    min_length=f.get("min_length"),
                    max_length=f.get("max_length"),
                    min_value=f.get("min_value"),
                    max_value=f.get("max_value"),
                    pattern=f.get("pattern"),
                    enum=f.get("enum"),
                    default=f.get("default"),
                    description=f.get("description", "")
                ))
            self.schemas[name] = RequestSchema(name, field_schemas)
    
    def register_rule(self, schema_name: str, rule: ValidationRule):
        """Register a custom validation rule"""
        with self._lock:
            if schema_name not in self.custom_rules:
                self.custom_rules[schema_name] = []
            self.custom_rules[schema_name].append(rule)
    
    def validate(self, schema_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against a schema"""
        with self._lock:
            start = datetime.now()
            self.stats["total_validations"] += 1
            
            if schema_name not in self.schemas:
                result = {
                    "valid": False,
                    "schema": schema_name,
                    "errors": [{"error": f"Unknown schema: {schema_name}"}],
                    "warnings": []
                }
            else:
                # Run schema validation
                result = self.schemas[schema_name].validate(data)
                
                # Run custom rules
                if schema_name in self.custom_rules:
                    for rule in self.custom_rules[schema_name]:
                        rule_result = rule.validate(data)
                        if not rule_result["valid"]:
                            if rule_result["severity"] == "warning":
                                result["warnings"].append({
                                    "rule": rule_result["rule"],
                                    "error": rule_result["message"]
                                })
                            else:
                                result["errors"].append({
                                    "rule": rule_result["rule"],
                                    "error": rule_result["message"]
                                })
                                result["valid"] = False
            
            # Update stats
            if result["valid"]:
                self.stats["passed"] += 1
            else:
                self.stats["failed"] += 1
            if result.get("warnings"):
                self.stats["warnings"] += len(result["warnings"])
            
            # Record history
            duration = (datetime.now() - start).total_seconds() * 1000
            self.validation_history.append({
                "schema": schema_name,
                "valid": result["valid"],
                "errors": len(result.get("errors", [])),
                "warnings": len(result.get("warnings", [])),
                "duration_ms": round(duration, 2),
                "timestamp": start.isoformat()
            })
            
            result["duration_ms"] = round(duration, 2)
            return result
    
    def sanitize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Sanitize request data"""
        sanitized = {}
        
        for key, value in data.items():
            # Sanitize strings
            if isinstance(value, str):
                # Remove control characters
                value = ''.join(c for c in value if c.isprintable() or c in '\n\r\t')
                # Trim excessive whitespace
                value = ' '.join(value.split())
                # Limit length
                if len(value) > 10000:
                    value = value[:10000]
            
            # Recursively sanitize dicts
            elif isinstance(value, dict):
                value = self.sanitize(value)
            
            # Sanitize lists
            elif isinstance(value, list):
                value = [
                    self.sanitize(item) if isinstance(item, dict)
                    else item
                    for item in value[:1000]  # Limit array size
                ]
            
            sanitized[key] = value
        
        return sanitized
    
    def get_schema(self, name: str) -> Optional[Dict[str, Any]]:
        """Get schema definition"""
        with self._lock:
            if name not in self.schemas:
                return None
            
            schema = self.schemas[name]
            return {
                "name": schema.name,
                "fields": [
                    {
                        "name": f.name,
                        "type": f.field_type,
                        "required": f.required,
                        "enum": f.enum,
                        "description": f.description
                    }
                    for f in schema.fields.values()
                ]
            }
    
    def get_all_schemas(self) -> Dict[str, Any]:
        """Get all schema definitions"""
        with self._lock:
            return {
                name: self.get_schema(name)
                for name in self.schemas.keys()
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get validation statistics"""
        with self._lock:
            pass_rate = (
                self.stats["passed"] / self.stats["total_validations"]
                if self.stats["total_validations"] > 0 else 0
            )
            return {
                **self.stats,
                "pass_rate": round(pass_rate * 100, 2),
                "schemas_registered": len(self.schemas),
                "custom_rules": sum(len(rules) for rules in self.custom_rules.values())
            }
    
    def get_history(self, limit: int = 100) -> List[Dict]:
        """Get validation history"""
        with self._lock:
            return list(self.validation_history)[-limit:]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get validation analytics"""
        stats = self.get_stats()
        return {
            "status": "active",
            "total_validations": stats["total_validations"],
            "pass_rate": stats["pass_rate"],
            "schemas_registered": stats["schemas_registered"],
            "custom_rules": stats["custom_rules"],
            "features": [
                "json_schema_validation",
                "custom_rules",
                "request_sanitization",
                "type_checking",
                "pattern_matching",
                "enum_validation",
                "validation_history"
            ],
            "statistics": stats
        }


# Global instance
_request_validator: Optional[RequestValidator] = None
_validator_lock = threading.Lock()


def get_request_validator() -> RequestValidator:
    """Get or create request validator instance"""
    global _request_validator
    with _validator_lock:
        if _request_validator is None:
            _request_validator = RequestValidator()
        return _request_validator
