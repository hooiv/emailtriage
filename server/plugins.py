"""
Plugin Architecture System
Extensible plugin framework for custom categorizers, graders, and integrations
"""
import importlib
import importlib.util
import inspect
import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Type, Callable
from enum import Enum
from pathlib import Path

logger = logging.getLogger("plugins")


class PluginType(Enum):
    """Types of plugins"""
    CATEGORIZER = "categorizer"
    GRADER = "grader"
    NOTIFIER = "notifier"
    TRANSFORMER = "transformer"
    ANALYZER = "analyzer"
    EXPORTER = "exporter"
    INTEGRATION = "integration"


class PluginStatus(Enum):
    """Plugin lifecycle status"""
    REGISTERED = "registered"
    LOADED = "loaded"
    ENABLED = "enabled"
    DISABLED = "disabled"
    ERROR = "error"


@dataclass
class PluginMetadata:
    """Plugin metadata"""
    name: str
    version: str
    author: str
    description: str
    plugin_type: PluginType
    dependencies: List[str] = field(default_factory=list)
    config_schema: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "author": self.author,
            "description": self.description,
            "plugin_type": self.plugin_type.value,
            "dependencies": self.dependencies,
            "config_schema": self.config_schema
        }


class PluginBase(ABC):
    """Base class for all plugins"""
    
    # Override in subclass
    metadata: PluginMetadata = None
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self._enabled = True
        self._initialized = False
    
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the plugin. Return True on success."""
        pass
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the plugin's main functionality."""
        pass
    
    def cleanup(self):
        """Cleanup resources. Override if needed."""
        pass
    
    def validate_config(self) -> bool:
        """Validate plugin configuration."""
        return True
    
    @property
    def is_enabled(self) -> bool:
        return self._enabled
    
    def enable(self):
        self._enabled = True
    
    def disable(self):
        self._enabled = False


class CategorizerPlugin(PluginBase):
    """Plugin for custom email categorization"""
    
    @abstractmethod
    def categorize(self, email: Dict[str, Any]) -> Dict[str, Any]:
        """
        Categorize an email.
        
        Returns:
            Dict with 'category', 'confidence', and optional 'reason'
        """
        pass
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        email = context.get("email", {})
        return self.categorize(email)


class GraderPlugin(PluginBase):
    """Plugin for custom task grading"""
    
    @abstractmethod
    def grade(self, actions: List[Dict], expected: Dict[str, Any]) -> Dict[str, Any]:
        """
        Grade a sequence of actions.
        
        Returns:
            Dict with 'score' (0.0-1.0), 'breakdown', and 'feedback'
        """
        pass
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        actions = context.get("actions", [])
        expected = context.get("expected", {})
        return self.grade(actions, expected)


class NotifierPlugin(PluginBase):
    """Plugin for custom notifications"""
    
    @abstractmethod
    def notify(self, event: str, data: Dict[str, Any]) -> bool:
        """
        Send a notification.
        
        Returns:
            True if notification sent successfully
        """
        pass
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        event = context.get("event", "")
        data = context.get("data", {})
        success = self.notify(event, data)
        return {"success": success}


class TransformerPlugin(PluginBase):
    """Plugin for data transformation"""
    
    @abstractmethod
    def transform(self, data: Any) -> Any:
        """Transform input data."""
        pass
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        data = context.get("data")
        result = self.transform(data)
        return {"result": result}


class AnalyzerPlugin(PluginBase):
    """Plugin for custom analysis"""
    
    @abstractmethod
    def analyze(self, data: Any) -> Dict[str, Any]:
        """Analyze data and return insights."""
        pass
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        data = context.get("data")
        return self.analyze(data)


@dataclass
class PluginInstance:
    """Registered plugin instance"""
    id: str
    plugin: PluginBase
    metadata: PluginMetadata
    status: PluginStatus
    config: Dict[str, Any]
    registered_at: datetime = field(default_factory=datetime.now)
    last_executed: Optional[datetime] = None
    execution_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "metadata": self.metadata.to_dict(),
            "status": self.status.value,
            "config": self.config,
            "registered_at": self.registered_at.isoformat(),
            "last_executed": self.last_executed.isoformat() if self.last_executed else None,
            "execution_count": self.execution_count,
            "error_count": self.error_count,
            "last_error": self.last_error
        }


class PluginManager:
    """
    Plugin Management System
    
    Features:
    - Dynamic plugin loading
    - Lifecycle management
    - Dependency resolution
    - Configuration validation
    - Execution hooks
    """
    
    def __init__(self):
        self._plugins: Dict[str, PluginInstance] = {}
        self._type_index: Dict[PluginType, List[str]] = {t: [] for t in PluginType}
        self._hooks: Dict[str, List[Callable]] = {}
        self._lock = threading.RLock()
        
        # Register built-in plugins
        self._register_builtin_plugins()
        
        logger.info("Plugin Manager initialized")
    
    def _register_builtin_plugins(self):
        """Register built-in plugins"""
        # Sentiment Analyzer Plugin
        class SentimentAnalyzerPlugin(AnalyzerPlugin):
            metadata = PluginMetadata(
                name="builtin_sentiment",
                version="1.0.0",
                author="System",
                description="Analyzes email sentiment",
                plugin_type=PluginType.ANALYZER
            )
            
            def initialize(self) -> bool:
                self._initialized = True
                return True
            
            def analyze(self, data: Any) -> Dict[str, Any]:
                text = str(data).lower()
                
                positive_words = ["thanks", "great", "excellent", "good", "happy", "pleased"]
                negative_words = ["urgent", "problem", "issue", "angry", "frustrated", "bad"]
                
                pos_count = sum(1 for w in positive_words if w in text)
                neg_count = sum(1 for w in negative_words if w in text)
                
                if pos_count > neg_count:
                    sentiment = "positive"
                    score = min(1.0, 0.5 + pos_count * 0.1)
                elif neg_count > pos_count:
                    sentiment = "negative"
                    score = max(-1.0, -0.5 - neg_count * 0.1)
                else:
                    sentiment = "neutral"
                    score = 0.0
                
                return {
                    "sentiment": sentiment,
                    "score": score,
                    "positive_signals": pos_count,
                    "negative_signals": neg_count
                }
        
        # Spam Detector Plugin
        class SpamDetectorPlugin(CategorizerPlugin):
            metadata = PluginMetadata(
                name="builtin_spam_detector",
                version="1.0.0",
                author="System",
                description="Detects spam emails",
                plugin_type=PluginType.CATEGORIZER
            )
            
            def initialize(self) -> bool:
                self._spam_keywords = [
                    "winner", "lottery", "prize", "click here", "free money",
                    "urgent action", "act now", "limited time", "congratulations",
                    "nigerian prince", "inheritance", "million dollars"
                ]
                self._initialized = True
                return True
            
            def categorize(self, email: Dict[str, Any]) -> Dict[str, Any]:
                text = f"{email.get('subject', '')} {email.get('body', '')}".lower()
                
                matches = sum(1 for kw in self._spam_keywords if kw in text)
                
                if matches >= 3:
                    return {
                        "category": "spam",
                        "confidence": min(0.99, 0.7 + matches * 0.05),
                        "reason": f"Matched {matches} spam indicators"
                    }
                elif matches >= 1:
                    return {
                        "category": "suspicious",
                        "confidence": 0.5 + matches * 0.1,
                        "reason": f"Matched {matches} spam indicators"
                    }
                else:
                    return {
                        "category": "legitimate",
                        "confidence": 0.8,
                        "reason": "No spam indicators found"
                    }
        
        # Priority Scorer Plugin
        class PriorityScorerPlugin(AnalyzerPlugin):
            metadata = PluginMetadata(
                name="builtin_priority_scorer",
                version="1.0.0",
                author="System",
                description="Scores email priority",
                plugin_type=PluginType.ANALYZER
            )
            
            def initialize(self) -> bool:
                self._initialized = True
                return True
            
            def analyze(self, data: Any) -> Dict[str, Any]:
                email = data if isinstance(data, dict) else {}
                
                score = 50  # Base score
                reasons = []
                
                subject = email.get("subject", "").lower()
                body = email.get("body", "").lower()
                sender = email.get("sender", "").lower()
                
                # Urgency indicators
                if "urgent" in subject or "asap" in subject:
                    score += 30
                    reasons.append("Urgent in subject")
                
                if "deadline" in body or "immediately" in body:
                    score += 20
                    reasons.append("Deadline mentioned")
                
                # VIP sender
                vip_domains = ["ceo", "cto", "cfo", "president", "director"]
                if any(vip in sender for vip in vip_domains):
                    score += 25
                    reasons.append("VIP sender")
                
                # Internal email
                if "@company.com" in sender:
                    score += 10
                    reasons.append("Internal sender")
                
                priority = "low"
                if score >= 80:
                    priority = "critical"
                elif score >= 60:
                    priority = "high"
                elif score >= 40:
                    priority = "medium"
                
                return {
                    "priority": priority,
                    "score": min(100, score),
                    "reasons": reasons
                }
        
        # Register built-in plugins
        self.register_plugin(SentimentAnalyzerPlugin())
        self.register_plugin(SpamDetectorPlugin())
        self.register_plugin(PriorityScorerPlugin())
    
    def register_plugin(
        self,
        plugin: PluginBase,
        config: Dict[str, Any] = None
    ) -> str:
        """Register a plugin"""
        with self._lock:
            if not plugin.metadata:
                raise ValueError("Plugin must have metadata")
            
            plugin_id = f"{plugin.metadata.name}_{plugin.metadata.version}"
            
            if plugin_id in self._plugins:
                logger.warning(f"Plugin {plugin_id} already registered")
                return plugin_id
            
            # Set config
            plugin.config = config or {}
            
            # Validate config
            if not plugin.validate_config():
                raise ValueError(f"Invalid configuration for plugin {plugin_id}")
            
            # Create instance
            instance = PluginInstance(
                id=plugin_id,
                plugin=plugin,
                metadata=plugin.metadata,
                status=PluginStatus.REGISTERED,
                config=plugin.config
            )
            
            self._plugins[plugin_id] = instance
            self._type_index[plugin.metadata.plugin_type].append(plugin_id)
            
            logger.info(f"Registered plugin: {plugin_id}")
            return plugin_id
    
    def load_plugin(self, plugin_id: str) -> bool:
        """Load and initialize a plugin"""
        with self._lock:
            instance = self._plugins.get(plugin_id)
            if not instance:
                return False
            
            try:
                success = instance.plugin.initialize()
                if success:
                    instance.status = PluginStatus.LOADED
                    logger.info(f"Loaded plugin: {plugin_id}")
                else:
                    instance.status = PluginStatus.ERROR
                    instance.last_error = "Initialization returned False"
                return success
            except Exception as e:
                instance.status = PluginStatus.ERROR
                instance.last_error = str(e)
                logger.error(f"Failed to load plugin {plugin_id}: {e}")
                return False
    
    def enable_plugin(self, plugin_id: str) -> bool:
        """Enable a plugin"""
        with self._lock:
            instance = self._plugins.get(plugin_id)
            if not instance:
                return False
            
            if instance.status == PluginStatus.REGISTERED:
                self.load_plugin(plugin_id)
            
            instance.plugin.enable()
            instance.status = PluginStatus.ENABLED
            return True
    
    def disable_plugin(self, plugin_id: str) -> bool:
        """Disable a plugin"""
        with self._lock:
            instance = self._plugins.get(plugin_id)
            if not instance:
                return False
            
            instance.plugin.disable()
            instance.status = PluginStatus.DISABLED
            return True
    
    def execute_plugin(
        self,
        plugin_id: str,
        context: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Execute a plugin"""
        instance = self._plugins.get(plugin_id)
        if not instance:
            return None
        
        if not instance.plugin.is_enabled:
            return {"error": "Plugin is disabled"}
        
        try:
            result = instance.plugin.execute(context)
            instance.last_executed = datetime.now()
            instance.execution_count += 1
            return result
        except Exception as e:
            instance.error_count += 1
            instance.last_error = str(e)
            logger.error(f"Plugin execution error {plugin_id}: {e}")
            return {"error": str(e)}
    
    def execute_all_of_type(
        self,
        plugin_type: PluginType,
        context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Execute all plugins of a type"""
        results = []
        for plugin_id in self._type_index.get(plugin_type, []):
            result = self.execute_plugin(plugin_id, context)
            if result:
                results.append({
                    "plugin_id": plugin_id,
                    "result": result
                })
        return results
    
    def get_plugin(self, plugin_id: str) -> Optional[Dict[str, Any]]:
        """Get plugin info"""
        instance = self._plugins.get(plugin_id)
        if instance:
            return instance.to_dict()
        return None
    
    def list_plugins(
        self,
        plugin_type: PluginType = None
    ) -> List[Dict[str, Any]]:
        """List all plugins"""
        with self._lock:
            if plugin_type:
                plugin_ids = self._type_index.get(plugin_type, [])
            else:
                plugin_ids = list(self._plugins.keys())
            
            return [self._plugins[pid].to_dict() for pid in plugin_ids]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get plugin analytics"""
        with self._lock:
            total = len(self._plugins)
            enabled = sum(1 for p in self._plugins.values() if p.status == PluginStatus.ENABLED)
            errors = sum(1 for p in self._plugins.values() if p.status == PluginStatus.ERROR)
            
            by_type = {t.value: len(self._type_index[t]) for t in PluginType}
            
            total_executions = sum(p.execution_count for p in self._plugins.values())
            total_errors = sum(p.error_count for p in self._plugins.values())
            
            return {
                "total_plugins": total,
                "enabled": enabled,
                "disabled": total - enabled - errors,
                "errors": errors,
                "by_type": by_type,
                "total_executions": total_executions,
                "total_execution_errors": total_errors,
                "error_rate": (total_errors / total_executions * 100) if total_executions > 0 else 0
            }


# Global instance
_plugin_manager = PluginManager()


def get_plugin_manager() -> PluginManager:
    """Get global plugin manager"""
    return _plugin_manager
