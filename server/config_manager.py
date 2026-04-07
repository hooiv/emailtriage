"""
Configuration Management System
Hot-reloadable configuration with validation and versioning
"""
import os
import json
import yaml
import threading
import logging
import hashlib
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable, Union
from pathlib import Path
from enum import Enum
from copy import deepcopy

logger = logging.getLogger("config")


class ConfigSource(Enum):
    """Configuration source types"""
    DEFAULT = "default"
    FILE = "file"
    ENVIRONMENT = "environment"
    OVERRIDE = "override"
    REMOTE = "remote"


class ConfigFormat(Enum):
    """Configuration file formats"""
    JSON = "json"
    YAML = "yaml"
    ENV = "env"


@dataclass
class ConfigValue:
    """Configuration value with metadata"""
    key: str
    value: Any
    source: ConfigSource
    type_hint: type = str
    description: str = ""
    required: bool = False
    default: Any = None
    validators: List[Callable] = field(default_factory=list)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def validate(self) -> bool:
        """Validate the value"""
        for validator in self.validators:
            try:
                if not validator(self.value):
                    return False
            except Exception:
                return False
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "source": self.source.value,
            "type": self.type_hint.__name__,
            "description": self.description,
            "required": self.required,
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class ConfigSchema:
    """Configuration schema definition"""
    key: str
    type_hint: type = str
    description: str = ""
    required: bool = False
    default: Any = None
    validators: List[Callable] = field(default_factory=list)
    env_var: Optional[str] = None  # Environment variable name
    sensitive: bool = False  # Don't log sensitive values


class ConfigVersion:
    """Configuration version tracking"""
    
    def __init__(self):
        self.versions: List[Dict] = []
        self._lock = threading.Lock()
    
    def snapshot(self, config: Dict[str, Any], source: str = "manual") -> str:
        """Create a config snapshot"""
        version_id = hashlib.sha256(
            json.dumps(config, sort_keys=True, default=str).encode()
        ).hexdigest()[:12]
        
        with self._lock:
            self.versions.append({
                "version_id": version_id,
                "config": deepcopy(config),
                "timestamp": datetime.now().isoformat(),
                "source": source
            })
            
            # Keep last 50 versions
            if len(self.versions) > 50:
                self.versions = self.versions[-50:]
        
        return version_id
    
    def rollback(self, version_id: str) -> Optional[Dict[str, Any]]:
        """Rollback to a specific version"""
        with self._lock:
            for v in self.versions:
                if v["version_id"] == version_id:
                    return deepcopy(v["config"])
        return None
    
    def list_versions(self, limit: int = 10) -> List[Dict]:
        """List recent versions"""
        with self._lock:
            return [
                {"version_id": v["version_id"], "timestamp": v["timestamp"], "source": v["source"]}
                for v in self.versions[-limit:]
            ]


class ConfigManager:
    """
    Production Configuration Management
    
    Features:
    - Multi-source configuration (defaults, files, env, overrides)
    - Hot-reload support
    - Schema validation
    - Version tracking with rollback
    - Change notifications
    """
    
    def __init__(self, app_name: str = "email-triage"):
        self.app_name = app_name
        self._config: Dict[str, ConfigValue] = {}
        self._schemas: Dict[str, ConfigSchema] = {}
        self._listeners: List[Callable] = []
        self._version = ConfigVersion()
        self._lock = threading.RLock()
        
        # Register default schemas
        self._register_default_schemas()
        
        # Load configuration
        self._load_defaults()
        self._load_environment()
        
        logger.info(f"Configuration Manager initialized for {app_name}")
    
    def _register_default_schemas(self):
        """Register default configuration schemas"""
        schemas = [
            # Server settings
            ConfigSchema("server.host", str, "Server host", default="0.0.0.0"),
            ConfigSchema("server.port", int, "Server port", default=8000),
            ConfigSchema("server.debug", bool, "Debug mode", default=False),
            ConfigSchema("server.workers", int, "Number of workers", default=4),
            
            # API settings
            ConfigSchema("api.rate_limit", int, "Rate limit per minute", default=60),
            ConfigSchema("api.timeout", float, "Request timeout", default=30.0),
            ConfigSchema("api.max_payload", int, "Max payload size bytes", default=10485760),
            
            # Email settings
            ConfigSchema("email.max_batch_size", int, "Max emails per batch", default=100),
            ConfigSchema("email.retention_days", int, "Email retention days", default=90),
            ConfigSchema("email.categories", list, "Valid categories", default=["work", "personal", "spam"]),
            
            # AI settings
            ConfigSchema("ai.model", str, "LLM model name", default="gpt-4", env_var="MODEL_NAME"),
            ConfigSchema("ai.temperature", float, "Model temperature", default=0.7),
            ConfigSchema("ai.max_tokens", int, "Max tokens", default=1000),
            ConfigSchema("ai.api_key", str, "API key", env_var="OPENAI_API_KEY", sensitive=True),
            ConfigSchema("ai.api_base", str, "API base URL", env_var="API_BASE_URL"),
            
            # Security settings
            ConfigSchema("security.enabled", bool, "Security enabled", default=True),
            ConfigSchema("security.jwt_secret", str, "JWT secret", sensitive=True),
            ConfigSchema("security.cors_origins", list, "CORS origins", default=["*"]),
            
            # Cache settings
            ConfigSchema("cache.enabled", bool, "Caching enabled", default=True),
            ConfigSchema("cache.ttl", int, "Cache TTL seconds", default=300),
            ConfigSchema("cache.max_size", int, "Max cache entries", default=10000),
            
            # Queue settings
            ConfigSchema("queue.workers", int, "Queue workers", default=3),
            ConfigSchema("queue.max_retries", int, "Max job retries", default=3),
            ConfigSchema("queue.timeout", float, "Job timeout", default=60.0),
            
            # Monitoring settings
            ConfigSchema("monitoring.enabled", bool, "Monitoring enabled", default=True),
            ConfigSchema("monitoring.interval", int, "Health check interval", default=30),
            ConfigSchema("monitoring.metrics_retention", int, "Metrics retention hours", default=24),
            
            # Feature flags
            ConfigSchema("features.ml_pipeline", bool, "ML pipeline enabled", default=True),
            ConfigSchema("features.blockchain_audit", bool, "Blockchain audit enabled", default=True),
            ConfigSchema("features.multi_agent", bool, "Multi-agent enabled", default=True),
        ]
        
        for schema in schemas:
            self._schemas[schema.key] = schema
    
    def _load_defaults(self):
        """Load default configuration values"""
        for key, schema in self._schemas.items():
            self._config[key] = ConfigValue(
                key=key,
                value=schema.default,
                source=ConfigSource.DEFAULT,
                type_hint=schema.type_hint,
                description=schema.description,
                required=schema.required,
                default=schema.default,
                validators=schema.validators
            )
    
    def _load_environment(self):
        """Load configuration from environment variables"""
        for key, schema in self._schemas.items():
            env_var = schema.env_var or f"{self.app_name.upper().replace('-', '_')}_{key.upper().replace('.', '_')}"
            
            env_value = os.environ.get(env_var)
            if env_value is not None:
                try:
                    # Type conversion
                    if schema.type_hint == bool:
                        value = env_value.lower() in ("true", "1", "yes")
                    elif schema.type_hint == int:
                        value = int(env_value)
                    elif schema.type_hint == float:
                        value = float(env_value)
                    elif schema.type_hint == list:
                        value = json.loads(env_value)
                    else:
                        value = env_value
                    
                    self._config[key] = ConfigValue(
                        key=key,
                        value=value,
                        source=ConfigSource.ENVIRONMENT,
                        type_hint=schema.type_hint,
                        description=schema.description,
                        required=schema.required,
                        default=schema.default,
                        validators=schema.validators
                    )
                except Exception as e:
                    logger.warning(f"Failed to parse env var {env_var}: {e}")
    
    def load_file(self, path: str, format: ConfigFormat = None) -> bool:
        """Load configuration from a file"""
        try:
            filepath = Path(path)
            
            if not filepath.exists():
                logger.warning(f"Config file not found: {path}")
                return False
            
            # Detect format
            if format is None:
                if filepath.suffix in (".yml", ".yaml"):
                    format = ConfigFormat.YAML
                elif filepath.suffix == ".json":
                    format = ConfigFormat.JSON
                else:
                    format = ConfigFormat.JSON
            
            with open(filepath, "r") as f:
                if format == ConfigFormat.YAML:
                    data = yaml.safe_load(f)
                else:
                    data = json.load(f)
            
            # Flatten nested config
            def flatten(d, prefix=""):
                items = {}
                for k, v in d.items():
                    key = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, dict):
                        items.update(flatten(v, key))
                    else:
                        items[key] = v
                return items
            
            flat_config = flatten(data)
            
            for key, value in flat_config.items():
                schema = self._schemas.get(key)
                self._config[key] = ConfigValue(
                    key=key,
                    value=value,
                    source=ConfigSource.FILE,
                    type_hint=schema.type_hint if schema else type(value),
                    description=schema.description if schema else "",
                    required=schema.required if schema else False,
                    default=schema.default if schema else None
                )
            
            self._version.snapshot(self.get_all(), f"file:{path}")
            self._notify_listeners("file_loaded", path)
            
            logger.info(f"Loaded configuration from {path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            return False
    
    def set(
        self,
        key: str,
        value: Any,
        source: ConfigSource = ConfigSource.OVERRIDE
    ) -> bool:
        """Set a configuration value"""
        with self._lock:
            schema = self._schemas.get(key)
            
            config_value = ConfigValue(
                key=key,
                value=value,
                source=source,
                type_hint=schema.type_hint if schema else type(value),
                description=schema.description if schema else "",
                required=schema.required if schema else False,
                default=schema.default if schema else None,
                validators=schema.validators if schema else []
            )
            
            # Validate
            if not config_value.validate():
                logger.warning(f"Validation failed for config: {key}")
                return False
            
            old_value = self._config.get(key)
            self._config[key] = config_value
            
            # Snapshot and notify
            self._version.snapshot(self.get_all(), "set")
            self._notify_listeners("value_changed", {
                "key": key,
                "old_value": old_value.value if old_value else None,
                "new_value": value
            })
            
            return True
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value"""
        config_value = self._config.get(key)
        if config_value:
            return config_value.value
        return default
    
    def get_typed(self, key: str, type_hint: type, default: Any = None) -> Any:
        """Get a typed configuration value"""
        value = self.get(key, default)
        try:
            if value is not None:
                return type_hint(value)
        except (ValueError, TypeError):
            pass
        return default
    
    def get_all(self, include_sensitive: bool = False) -> Dict[str, Any]:
        """Get all configuration values"""
        result = {}
        for key, config_value in self._config.items():
            schema = self._schemas.get(key)
            if schema and schema.sensitive and not include_sensitive:
                result[key] = "***REDACTED***"
            else:
                result[key] = config_value.value
        return result
    
    def get_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get configuration value with metadata"""
        config_value = self._config.get(key)
        if config_value:
            info = config_value.to_dict()
            schema = self._schemas.get(key)
            if schema and schema.sensitive:
                info["value"] = "***REDACTED***"
            return info
        return None
    
    def validate_all(self) -> Dict[str, List[str]]:
        """Validate all configuration"""
        errors = {}
        
        # Check required fields
        for key, schema in self._schemas.items():
            if schema.required:
                value = self.get(key)
                if value is None:
                    errors.setdefault(key, []).append("Required field is missing")
        
        # Validate values
        for key, config_value in self._config.items():
            if not config_value.validate():
                errors.setdefault(key, []).append("Validation failed")
        
        return errors
    
    def rollback(self, version_id: str) -> bool:
        """Rollback to a previous version"""
        config = self._version.rollback(version_id)
        if config:
            with self._lock:
                for key, value in config.items():
                    self.set(key, value, ConfigSource.OVERRIDE)
            self._notify_listeners("rollback", version_id)
            return True
        return False
    
    def add_listener(self, listener: Callable[[str, Any], None]):
        """Add a configuration change listener"""
        self._listeners.append(listener)
    
    def _notify_listeners(self, event: str, data: Any):
        """Notify listeners of changes"""
        for listener in self._listeners:
            try:
                listener(event, data)
            except Exception as e:
                logger.error(f"Listener error: {e}")
    
    def export(self, path: str, format: ConfigFormat = ConfigFormat.JSON) -> bool:
        """Export configuration to file"""
        try:
            config = self.get_all(include_sensitive=False)
            
            with open(path, "w") as f:
                if format == ConfigFormat.YAML:
                    yaml.safe_dump(config, f, default_flow_style=False)
                else:
                    json.dump(config, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Failed to export config: {e}")
            return False
    
    def get_schema(self, key: str = None) -> Union[Dict, List[Dict]]:
        """Get configuration schema"""
        if key:
            schema = self._schemas.get(key)
            if schema:
                return {
                    "key": schema.key,
                    "type": schema.type_hint.__name__,
                    "description": schema.description,
                    "required": schema.required,
                    "default": schema.default,
                    "sensitive": schema.sensitive,
                    "env_var": schema.env_var
                }
            return None
        
        return [
            {
                "key": s.key,
                "type": s.type_hint.__name__,
                "description": s.description,
                "required": s.required,
                "default": s.default,
                "sensitive": s.sensitive,
                "env_var": s.env_var
            }
            for s in self._schemas.values()
        ]
    
    def get_versions(self, limit: int = 10) -> List[Dict]:
        """Get configuration versions"""
        return self._version.list_versions(limit)
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get configuration analytics"""
        total = len(self._config)
        by_source = {}
        for cv in self._config.values():
            by_source[cv.source.value] = by_source.get(cv.source.value, 0) + 1
        
        errors = self.validate_all()
        
        return {
            "total_keys": total,
            "by_source": by_source,
            "schema_count": len(self._schemas),
            "validation_errors": len(errors),
            "version_count": len(self._version.versions),
            "listeners": len(self._listeners)
        }


# Global instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager() -> ConfigManager:
    """Get global configuration manager"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager


def get_config(key: str, default: Any = None) -> Any:
    """Shorthand to get config value"""
    return get_config_manager().get(key, default)
