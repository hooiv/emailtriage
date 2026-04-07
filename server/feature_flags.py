"""
Feature Flags System
Dynamic feature management with A/B testing and gradual rollouts
"""
import json
import hashlib
import threading
import logging
from enum import Enum
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger("feature_flags")


class RolloutStrategy(Enum):
    """Rollout strategies for features"""
    ALL = "all"                    # Available to everyone
    NONE = "none"                  # Disabled for everyone
    PERCENTAGE = "percentage"       # Percentage-based rollout
    USER_LIST = "user_list"        # Specific users
    USER_ATTRIBUTE = "attribute"   # Based on user attributes
    GRADUAL = "gradual"            # Time-based gradual rollout
    AB_TEST = "ab_test"            # A/B testing


@dataclass
class FeatureFlag:
    """Feature flag configuration"""
    name: str
    description: str
    enabled: bool = False
    strategy: RolloutStrategy = RolloutStrategy.NONE
    percentage: float = 0.0
    allowed_users: Set[str] = field(default_factory=set)
    user_attribute: Optional[str] = None
    attribute_values: Set[str] = field(default_factory=set)
    gradual_start: Optional[datetime] = None
    gradual_end: Optional[datetime] = None
    ab_variants: Dict[str, float] = field(default_factory=dict)  # variant -> percentage
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "name": self.name,
            "description": self.description,
            "enabled": self.enabled,
            "strategy": self.strategy.value,
            "percentage": self.percentage,
            "allowed_users": list(self.allowed_users),
            "user_attribute": self.user_attribute,
            "attribute_values": list(self.attribute_values),
            "gradual_start": self.gradual_start.isoformat() if self.gradual_start else None,
            "gradual_end": self.gradual_end.isoformat() if self.gradual_end else None,
            "ab_variants": self.ab_variants,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class FeatureEvaluation:
    """Result of feature flag evaluation"""
    flag_name: str
    enabled: bool
    variant: Optional[str] = None
    reason: str = ""
    user_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class FeatureFlagManager:
    """
    Enterprise Feature Flag System
    
    Features:
    - Percentage-based rollouts
    - User targeting
    - A/B testing with variants
    - Gradual rollouts
    - Evaluation logging
    """
    
    def __init__(self):
        self._flags: Dict[str, FeatureFlag] = {}
        self._lock = threading.RLock()
        self._evaluation_log: deque = deque(maxlen=10000)
        self._initialize_default_flags()
        
        logger.info("Feature Flag Manager initialized")
    
    def _initialize_default_flags(self):
        """Initialize default feature flags"""
        defaults = [
            FeatureFlag(
                name="advanced_ml_pipeline",
                description="Use advanced ML pipeline for categorization",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="knowledge_graph_enrichment",
                description="Enrich emails with knowledge graph data",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="smart_reply_suggestions",
                description="Show AI-generated reply suggestions",
                enabled=True,
                strategy=RolloutStrategy.PERCENTAGE,
                percentage=100.0
            ),
            FeatureFlag(
                name="realtime_streaming",
                description="Enable real-time event streaming",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="blockchain_audit",
                description="Record actions to blockchain audit trail",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="predictive_priority",
                description="Use ML to predict email priority",
                enabled=True,
                strategy=RolloutStrategy.PERCENTAGE,
                percentage=100.0
            ),
            FeatureFlag(
                name="multi_agent_consensus",
                description="Use multi-agent AI for decisions",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="experimental_ui",
                description="New experimental UI features",
                enabled=False,
                strategy=RolloutStrategy.PERCENTAGE,
                percentage=10.0,
                metadata={"experiment_id": "ui_v2_2024"}
            ),
            FeatureFlag(
                name="response_tone_analysis",
                description="Analyze tone in response generation",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="semantic_search",
                description="Use semantic search for email queries",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="auto_categorization",
                description="Automatically categorize incoming emails",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="vip_detection",
                description="Detect and prioritize VIP senders",
                enabled=True,
                strategy=RolloutStrategy.ALL
            ),
            FeatureFlag(
                name="spam_ml_v2",
                description="Use ML v2 for spam detection",
                enabled=False,
                strategy=RolloutStrategy.AB_TEST,
                ab_variants={"control": 50.0, "ml_v2": 50.0}
            ),
            FeatureFlag(
                name="batch_optimization",
                description="Optimize batch email processing",
                enabled=True,
                strategy=RolloutStrategy.GRADUAL,
                gradual_start=datetime.now() - timedelta(days=7),
                gradual_end=datetime.now() + timedelta(days=7)
            )
        ]
        
        for flag in defaults:
            self._flags[flag.name] = flag
    
    def _hash_user(self, user_id: str, flag_name: str) -> float:
        """Generate consistent hash for user + flag"""
        combined = f"{user_id}:{flag_name}"
        hash_bytes = hashlib.md5(combined.encode()).digest()
        return int.from_bytes(hash_bytes[:4], 'big') / (2**32)
    
    def _evaluate_percentage(self, flag: FeatureFlag, user_id: str) -> bool:
        """Evaluate percentage-based rollout"""
        if not user_id:
            return False
        
        user_hash = self._hash_user(user_id, flag.name)
        return (user_hash * 100) < flag.percentage
    
    def _evaluate_gradual(self, flag: FeatureFlag) -> float:
        """Calculate current percentage for gradual rollout"""
        if not flag.gradual_start or not flag.gradual_end:
            return 0.0
        
        now = datetime.now()
        
        if now < flag.gradual_start:
            return 0.0
        if now > flag.gradual_end:
            return 100.0
        
        total_duration = (flag.gradual_end - flag.gradual_start).total_seconds()
        elapsed = (now - flag.gradual_start).total_seconds()
        
        return (elapsed / total_duration) * 100
    
    def _evaluate_ab_test(self, flag: FeatureFlag, user_id: str) -> Optional[str]:
        """Evaluate A/B test and return variant"""
        if not user_id or not flag.ab_variants:
            return None
        
        user_hash = self._hash_user(user_id, flag.name) * 100
        
        cumulative = 0.0
        for variant, percentage in flag.ab_variants.items():
            cumulative += percentage
            if user_hash < cumulative:
                return variant
        
        # Fallback to first variant
        return list(flag.ab_variants.keys())[0] if flag.ab_variants else None
    
    def evaluate(
        self,
        flag_name: str,
        user_id: Optional[str] = None,
        user_attributes: Optional[Dict[str, Any]] = None
    ) -> FeatureEvaluation:
        """
        Evaluate a feature flag
        
        Args:
            flag_name: Name of the feature flag
            user_id: Optional user identifier
            user_attributes: Optional user attributes for targeting
            
        Returns:
            FeatureEvaluation with enabled status and variant
        """
        with self._lock:
            flag = self._flags.get(flag_name)
            
            if not flag:
                result = FeatureEvaluation(
                    flag_name=flag_name,
                    enabled=False,
                    reason="flag_not_found",
                    user_id=user_id
                )
                self._evaluation_log.append(result)
                return result
            
            if not flag.enabled:
                result = FeatureEvaluation(
                    flag_name=flag_name,
                    enabled=False,
                    reason="flag_disabled",
                    user_id=user_id
                )
                self._evaluation_log.append(result)
                return result
            
            enabled = False
            variant = None
            reason = ""
            
            if flag.strategy == RolloutStrategy.ALL:
                enabled = True
                reason = "strategy_all"
            
            elif flag.strategy == RolloutStrategy.NONE:
                enabled = False
                reason = "strategy_none"
            
            elif flag.strategy == RolloutStrategy.PERCENTAGE:
                enabled = self._evaluate_percentage(flag, user_id or "anonymous")
                reason = f"percentage_{flag.percentage}"
            
            elif flag.strategy == RolloutStrategy.USER_LIST:
                enabled = user_id in flag.allowed_users if user_id else False
                reason = "user_list_match" if enabled else "user_list_no_match"
            
            elif flag.strategy == RolloutStrategy.USER_ATTRIBUTE:
                if user_attributes and flag.user_attribute:
                    attr_value = str(user_attributes.get(flag.user_attribute, ""))
                    enabled = attr_value in flag.attribute_values
                    reason = f"attribute_{flag.user_attribute}"
                else:
                    enabled = False
                    reason = "missing_attribute"
            
            elif flag.strategy == RolloutStrategy.GRADUAL:
                current_percentage = self._evaluate_gradual(flag)
                enabled = self._evaluate_percentage(
                    FeatureFlag(name=flag.name, description="", percentage=current_percentage),
                    user_id or "anonymous"
                )
                reason = f"gradual_{current_percentage:.1f}%"
            
            elif flag.strategy == RolloutStrategy.AB_TEST:
                variant = self._evaluate_ab_test(flag, user_id or "anonymous")
                enabled = variant is not None
                reason = f"ab_test_variant_{variant}"
            
            result = FeatureEvaluation(
                flag_name=flag_name,
                enabled=enabled,
                variant=variant,
                reason=reason,
                user_id=user_id
            )
            
            self._evaluation_log.append(result)
            return result
    
    def is_enabled(
        self,
        flag_name: str,
        user_id: Optional[str] = None,
        user_attributes: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Simple check if feature is enabled"""
        return self.evaluate(flag_name, user_id, user_attributes).enabled
    
    def get_variant(
        self,
        flag_name: str,
        user_id: Optional[str] = None
    ) -> Optional[str]:
        """Get A/B test variant for user"""
        return self.evaluate(flag_name, user_id).variant
    
    def create_flag(
        self,
        name: str,
        description: str,
        enabled: bool = False,
        strategy: str = "none",
        **kwargs
    ) -> FeatureFlag:
        """Create a new feature flag"""
        with self._lock:
            flag = FeatureFlag(
                name=name,
                description=description,
                enabled=enabled,
                strategy=RolloutStrategy(strategy),
                **kwargs
            )
            self._flags[name] = flag
            logger.info(f"Created feature flag: {name}")
            return flag
    
    def update_flag(
        self,
        name: str,
        **updates
    ) -> Optional[FeatureFlag]:
        """Update an existing feature flag"""
        with self._lock:
            flag = self._flags.get(name)
            if not flag:
                return None
            
            for key, value in updates.items():
                if key == "strategy":
                    value = RolloutStrategy(value)
                if key == "allowed_users":
                    value = set(value)
                if key == "attribute_values":
                    value = set(value)
                if hasattr(flag, key):
                    setattr(flag, key, value)
            
            flag.updated_at = datetime.now()
            logger.info(f"Updated feature flag: {name}")
            return flag
    
    def delete_flag(self, name: str) -> bool:
        """Delete a feature flag"""
        with self._lock:
            if name in self._flags:
                del self._flags[name]
                logger.info(f"Deleted feature flag: {name}")
                return True
            return False
    
    def get_flag(self, name: str) -> Optional[FeatureFlag]:
        """Get a feature flag by name"""
        return self._flags.get(name)
    
    def list_flags(self) -> List[Dict[str, Any]]:
        """List all feature flags"""
        return [flag.to_dict() for flag in self._flags.values()]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get feature flag analytics"""
        evaluations = list(self._evaluation_log)
        
        # Per-flag stats
        flag_stats = {}
        for flag_name in self._flags:
            flag_evals = [e for e in evaluations if e.flag_name == flag_name]
            enabled_count = sum(1 for e in flag_evals if e.enabled)
            
            flag_stats[flag_name] = {
                "total_evaluations": len(flag_evals),
                "enabled_count": enabled_count,
                "disabled_count": len(flag_evals) - enabled_count,
                "enable_rate": (enabled_count / len(flag_evals) * 100) if flag_evals else 0
            }
        
        # A/B test variant distribution
        ab_stats = {}
        for flag in self._flags.values():
            if flag.strategy == RolloutStrategy.AB_TEST:
                flag_evals = [e for e in evaluations if e.flag_name == flag.name and e.variant]
                variant_counts = {}
                for eval in flag_evals:
                    variant_counts[eval.variant] = variant_counts.get(eval.variant, 0) + 1
                ab_stats[flag.name] = variant_counts
        
        return {
            "summary": {
                "total_flags": len(self._flags),
                "enabled_flags": sum(1 for f in self._flags.values() if f.enabled),
                "total_evaluations": len(evaluations),
                "unique_users": len(set(e.user_id for e in evaluations if e.user_id))
            },
            "flag_stats": flag_stats,
            "ab_test_distribution": ab_stats,
            "strategies_in_use": list(set(f.strategy.value for f in self._flags.values())),
            "recent_evaluations": [
                {
                    "flag": e.flag_name,
                    "enabled": e.enabled,
                    "variant": e.variant,
                    "reason": e.reason,
                    "timestamp": e.timestamp.isoformat()
                }
                for e in list(evaluations)[-50:]
            ]
        }
    
    def bulk_enable(self, flag_names: List[str]):
        """Enable multiple flags at once"""
        with self._lock:
            for name in flag_names:
                if name in self._flags:
                    self._flags[name].enabled = True
                    self._flags[name].updated_at = datetime.now()
    
    def bulk_disable(self, flag_names: List[str]):
        """Disable multiple flags at once"""
        with self._lock:
            for name in flag_names:
                if name in self._flags:
                    self._flags[name].enabled = False
                    self._flags[name].updated_at = datetime.now()
    
    def export_config(self) -> str:
        """Export all flags as JSON"""
        return json.dumps({
            "flags": self.list_flags(),
            "exported_at": datetime.now().isoformat()
        }, indent=2)
    
    def import_config(self, config_json: str):
        """Import flags from JSON"""
        config = json.loads(config_json)
        flags_data = config.get("flags", [])
        
        with self._lock:
            for flag_data in flags_data:
                name = flag_data["name"]
                if name in self._flags:
                    self.update_flag(name, **flag_data)
                else:
                    self.create_flag(**flag_data)


# Global instance
_feature_flag_manager = FeatureFlagManager()


def get_feature_flag_manager() -> FeatureFlagManager:
    """Get global feature flag manager"""
    return _feature_flag_manager


def is_feature_enabled(
    flag_name: str,
    user_id: Optional[str] = None,
    user_attributes: Optional[Dict[str, Any]] = None
) -> bool:
    """Convenience function to check if feature is enabled"""
    return _feature_flag_manager.is_enabled(flag_name, user_id, user_attributes)
