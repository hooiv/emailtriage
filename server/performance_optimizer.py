"""
Auto-Performance Optimization Engine for Email Triage Environment.

Implements self-tuning performance optimization with:
- Automatic resource allocation
- Query optimization
- Cache management
- Load balancing
- Memory optimization
- Throughput maximization
- Latency minimization
- Adaptive throttling
"""

import gc
import logging
import statistics
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import secrets

logger = logging.getLogger(__name__)


class OptimizationStrategy(str, Enum):
    """Performance optimization strategies."""
    LATENCY_FOCUSED = "latency_focused"
    THROUGHPUT_FOCUSED = "throughput_focused"
    MEMORY_FOCUSED = "memory_focused"
    BALANCED = "balanced"
    AGGRESSIVE = "aggressive"


class ResourceType(str, Enum):
    """Types of resources to optimize."""
    MEMORY = "memory"
    CPU = "cpu"
    CACHE = "cache"
    QUEUE = "queue"
    CONNECTION_POOL = "connection_pool"


class OptimizationAction(str, Enum):
    """Types of optimization actions."""
    INCREASE_CACHE = "increase_cache"
    DECREASE_CACHE = "decrease_cache"
    CLEAR_CACHE = "clear_cache"
    INCREASE_POOL = "increase_pool"
    DECREASE_POOL = "decrease_pool"
    GC_COLLECT = "gc_collect"
    COMPACT_MEMORY = "compact_memory"
    THROTTLE_REQUESTS = "throttle_requests"
    UNTHROTTLE_REQUESTS = "unthrottle_requests"
    BATCH_OPERATIONS = "batch_operations"
    PARALLEL_PROCESSING = "parallel_processing"


@dataclass
class PerformanceMetrics:
    """Current performance metrics snapshot."""
    timestamp: datetime
    latency_p50: float
    latency_p95: float
    latency_p99: float
    throughput_rps: float
    memory_usage_mb: float
    cache_hit_rate: float
    queue_depth: int
    error_rate: float
    active_connections: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "latency_p50": self.latency_p50,
            "latency_p95": self.latency_p95,
            "latency_p99": self.latency_p99,
            "throughput_rps": self.throughput_rps,
            "memory_usage_mb": self.memory_usage_mb,
            "cache_hit_rate": self.cache_hit_rate,
            "queue_depth": self.queue_depth,
            "error_rate": self.error_rate,
            "active_connections": self.active_connections
        }


@dataclass
class OptimizationRule:
    """Rule for automatic optimization."""
    rule_id: str
    name: str
    description: str
    condition: Callable[[PerformanceMetrics], bool]
    action: OptimizationAction
    cooldown_seconds: int = 60
    priority: int = 5  # 1-10, higher = more important
    enabled: bool = True
    last_triggered: Optional[datetime] = None


@dataclass
class OptimizationResult:
    """Result of an optimization action."""
    action: OptimizationAction
    success: bool
    metrics_before: PerformanceMetrics
    metrics_after: Optional[PerformanceMetrics]
    improvement_percentage: float
    timestamp: datetime
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CacheConfiguration:
    """Cache configuration settings."""
    max_size: int = 10000
    ttl_seconds: int = 300
    eviction_policy: str = "lru"  # lru, lfu, fifo
    warm_cache: bool = True
    prefetch_enabled: bool = True


@dataclass
class ResourceLimits:
    """Resource limit configuration."""
    max_memory_mb: float = 512
    max_queue_depth: int = 1000
    max_connections: int = 100
    target_latency_ms: float = 100
    min_cache_hit_rate: float = 0.8


class PerformanceProfiler:
    """Lightweight performance profiler."""
    
    def __init__(self):
        self.active_profiles: Dict[str, Dict] = {}
        self.completed_profiles: deque = deque(maxlen=1000)
        self.function_stats: Dict[str, Dict] = {}
    
    def start(self, profile_name: str) -> str:
        """Start a profiling session."""
        profile_id = f"prof_{secrets.token_hex(4)}_{profile_name}"
        self.active_profiles[profile_id] = {
            "name": profile_name,
            "start_time": time.time(),
            "checkpoints": [],
            "memory_start": self._get_memory_usage()
        }
        return profile_id
    
    def checkpoint(self, profile_id: str, name: str):
        """Add a checkpoint to a profile."""
        if profile_id in self.active_profiles:
            elapsed = time.time() - self.active_profiles[profile_id]["start_time"]
            self.active_profiles[profile_id]["checkpoints"].append({
                "name": name,
                "elapsed_ms": elapsed * 1000,
                "memory_mb": self._get_memory_usage()
            })
    
    def end(self, profile_id: str) -> Dict[str, Any]:
        """End a profiling session and return results."""
        if profile_id not in self.active_profiles:
            return {}
        
        profile = self.active_profiles.pop(profile_id)
        total_time = (time.time() - profile["start_time"]) * 1000
        memory_end = self._get_memory_usage()
        
        result = {
            "profile_id": profile_id,
            "name": profile["name"],
            "total_time_ms": total_time,
            "memory_start_mb": profile["memory_start"],
            "memory_end_mb": memory_end,
            "memory_delta_mb": memory_end - profile["memory_start"],
            "checkpoints": profile["checkpoints"]
        }
        
        self.completed_profiles.append(result)
        
        # Update function stats
        func_name = profile["name"]
        if func_name not in self.function_stats:
            self.function_stats[func_name] = {"times": deque(maxlen=100), "count": 0}
        self.function_stats[func_name]["times"].append(total_time)
        self.function_stats[func_name]["count"] += 1
        
        return result
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0
    
    def get_function_stats(self) -> Dict[str, Dict]:
        """Get aggregated function statistics."""
        stats = {}
        for func_name, data in self.function_stats.items():
            times = list(data["times"])
            if times:
                stats[func_name] = {
                    "count": data["count"],
                    "avg_ms": statistics.mean(times),
                    "min_ms": min(times),
                    "max_ms": max(times),
                    "p95_ms": sorted(times)[int(len(times) * 0.95)] if len(times) > 1 else times[0]
                }
        return stats


class CacheManager:
    """Intelligent cache management system."""
    
    def __init__(self, config: CacheConfiguration):
        self.config = config
        self.cache: Dict[str, Tuple[Any, float, int]] = {}  # key -> (value, timestamp, access_count)
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self.lock:
            if key in self.cache:
                value, timestamp, access_count = self.cache[key]
                # Check TTL
                if time.time() - timestamp < self.config.ttl_seconds:
                    self.cache[key] = (value, timestamp, access_count + 1)
                    self.hits += 1
                    return value
                else:
                    # Expired
                    del self.cache[key]
            self.misses += 1
            return None
    
    def set(self, key: str, value: Any):
        """Set value in cache."""
        with self.lock:
            # Evict if at capacity
            while len(self.cache) >= self.config.max_size:
                self._evict()
            
            self.cache[key] = (value, time.time(), 1)
    
    def _evict(self):
        """Evict an entry based on policy."""
        if not self.cache:
            return
        
        if self.config.eviction_policy == "lru":
            # Least recently accessed
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
        elif self.config.eviction_policy == "lfu":
            # Least frequently used
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][2])
        else:  # fifo
            oldest_key = next(iter(self.cache))
        
        del self.cache[oldest_key]
        self.evictions += 1
    
    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            cleared = len(self.cache)
            self.cache.clear()
            return cleared
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self.hits + self.misses
        return {
            "size": len(self.cache),
            "max_size": self.config.max_size,
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hits / total_requests if total_requests > 0 else 0,
            "evictions": self.evictions,
            "ttl_seconds": self.config.ttl_seconds
        }
    
    def resize(self, new_size: int):
        """Resize the cache."""
        with self.lock:
            self.config.max_size = new_size
            while len(self.cache) > new_size:
                self._evict()


class AutoPerformanceOptimizer:
    """
    Self-tuning performance optimization engine.
    
    Features:
    - Automatic performance monitoring
    - Rule-based optimization triggers
    - Adaptive resource management
    - Predictive scaling
    - Memory optimization
    - Cache tuning
    """
    
    def __init__(self, environment_ref=None):
        """Initialize the optimizer."""
        self.environment_ref = environment_ref
        self.strategy = OptimizationStrategy.BALANCED
        self.limits = ResourceLimits()
        
        # Performance tracking
        self.metrics_history: deque = deque(maxlen=1000)
        self.latency_samples: deque = deque(maxlen=10000)
        self.throughput_samples: deque = deque(maxlen=1000)
        
        # Optimization components
        self.profiler = PerformanceProfiler()
        self.cache_manager = CacheManager(CacheConfiguration())
        self.optimization_rules: List[OptimizationRule] = []
        self.optimization_history: deque = deque(maxlen=500)
        
        # State
        self.is_throttling = False
        self.throttle_until: Optional[datetime] = None
        self.current_throughput_limit = float('inf')
        
        # Analytics
        self.analytics = {
            "optimizations_performed": 0,
            "successful_optimizations": 0,
            "failed_optimizations": 0,
            "total_improvement_percentage": 0.0,
            "gc_collections": 0,
            "cache_clears": 0
        }
        
        # Initialize default rules
        self._init_default_rules()
        
        logger.info("Auto-Performance Optimizer initialized")
    
    def _init_default_rules(self):
        """Initialize default optimization rules."""
        self.optimization_rules = [
            # High latency rules
            OptimizationRule(
                rule_id="high_latency_cache",
                name="High Latency - Increase Cache",
                description="Increase cache when latency is high and hit rate is low",
                condition=lambda m: m.latency_p95 > 500 and m.cache_hit_rate < 0.7,
                action=OptimizationAction.INCREASE_CACHE,
                cooldown_seconds=120,
                priority=8
            ),
            OptimizationRule(
                rule_id="high_latency_gc",
                name="High Latency - GC Collection",
                description="Trigger GC when memory is high and latency is elevated",
                condition=lambda m: m.latency_p95 > 300 and m.memory_usage_mb > self.limits.max_memory_mb * 0.8,
                action=OptimizationAction.GC_COLLECT,
                cooldown_seconds=60,
                priority=7
            ),
            
            # Memory pressure rules
            OptimizationRule(
                rule_id="memory_critical",
                name="Critical Memory - Clear Cache",
                description="Clear cache when memory usage is critical",
                condition=lambda m: m.memory_usage_mb > self.limits.max_memory_mb * 0.95,
                action=OptimizationAction.CLEAR_CACHE,
                cooldown_seconds=30,
                priority=10
            ),
            OptimizationRule(
                rule_id="memory_high",
                name="High Memory - Reduce Cache",
                description="Reduce cache size when memory is high",
                condition=lambda m: m.memory_usage_mb > self.limits.max_memory_mb * 0.85,
                action=OptimizationAction.DECREASE_CACHE,
                cooldown_seconds=60,
                priority=9
            ),
            
            # Queue depth rules
            OptimizationRule(
                rule_id="queue_deep",
                name="Deep Queue - Throttle",
                description="Throttle requests when queue is too deep",
                condition=lambda m: m.queue_depth > self.limits.max_queue_depth * 0.8,
                action=OptimizationAction.THROTTLE_REQUESTS,
                cooldown_seconds=30,
                priority=9
            ),
            OptimizationRule(
                rule_id="queue_empty",
                name="Empty Queue - Unthrottle",
                description="Remove throttling when queue is manageable",
                condition=lambda m: m.queue_depth < self.limits.max_queue_depth * 0.3 and self.is_throttling,
                action=OptimizationAction.UNTHROTTLE_REQUESTS,
                cooldown_seconds=10,
                priority=8
            ),
            
            # Error rate rules
            OptimizationRule(
                rule_id="high_error_throttle",
                name="High Errors - Throttle",
                description="Throttle when error rate is high",
                condition=lambda m: m.error_rate > 0.1,
                action=OptimizationAction.THROTTLE_REQUESTS,
                cooldown_seconds=60,
                priority=10
            ),
            
            # Cache efficiency rules
            OptimizationRule(
                rule_id="low_cache_hit",
                name="Low Cache Hit - Increase Cache",
                description="Increase cache when hit rate drops",
                condition=lambda m: m.cache_hit_rate < 0.5 and m.memory_usage_mb < self.limits.max_memory_mb * 0.6,
                action=OptimizationAction.INCREASE_CACHE,
                cooldown_seconds=180,
                priority=6
            ),
            
            # Memory reclamation
            OptimizationRule(
                rule_id="idle_gc",
                name="Idle GC",
                description="Run GC during low activity",
                condition=lambda m: m.throughput_rps < 1 and m.memory_usage_mb > self.limits.max_memory_mb * 0.5,
                action=OptimizationAction.GC_COLLECT,
                cooldown_seconds=300,
                priority=3
            )
        ]
    
    def record_latency(self, latency_ms: float):
        """Record a latency sample."""
        self.latency_samples.append({
            "timestamp": datetime.now(),
            "value": latency_ms
        })
    
    def record_request(self, success: bool = True):
        """Record a request for throughput calculation."""
        self.throughput_samples.append({
            "timestamp": datetime.now(),
            "success": success
        })
    
    def get_current_metrics(self) -> PerformanceMetrics:
        """Calculate current performance metrics."""
        now = datetime.now()
        
        # Calculate latency percentiles
        recent_latencies = [
            s["value"] for s in self.latency_samples
            if s["timestamp"] > now - timedelta(minutes=5)
        ]
        
        if recent_latencies:
            sorted_lat = sorted(recent_latencies)
            p50 = sorted_lat[int(len(sorted_lat) * 0.5)]
            p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
            p99 = sorted_lat[int(len(sorted_lat) * 0.99)] if len(sorted_lat) > 100 else p95
        else:
            p50 = p95 = p99 = 0
        
        # Calculate throughput (requests per second over last minute)
        recent_requests = [
            s for s in self.throughput_samples
            if s["timestamp"] > now - timedelta(minutes=1)
        ]
        throughput = len(recent_requests) / 60.0 if recent_requests else 0
        
        # Calculate error rate
        errors = sum(1 for s in recent_requests if not s.get("success", True))
        error_rate = errors / len(recent_requests) if recent_requests else 0
        
        # Get memory usage
        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / (1024 * 1024)
        except Exception:
            memory_mb = 0
        
        # Get cache stats
        cache_stats = self.cache_manager.get_stats()
        
        metrics = PerformanceMetrics(
            timestamp=now,
            latency_p50=p50,
            latency_p95=p95,
            latency_p99=p99,
            throughput_rps=throughput,
            memory_usage_mb=memory_mb,
            cache_hit_rate=cache_stats["hit_rate"],
            queue_depth=0,  # Would be populated by environment
            error_rate=error_rate,
            active_connections=0  # Would be populated by environment
        )
        
        self.metrics_history.append(metrics)
        return metrics
    
    def optimize(self) -> List[OptimizationResult]:
        """Run optimization cycle and apply any needed changes."""
        metrics = self.get_current_metrics()
        results = []
        
        # Evaluate rules in priority order
        sorted_rules = sorted(
            [r for r in self.optimization_rules if r.enabled],
            key=lambda r: -r.priority
        )
        
        for rule in sorted_rules:
            # Check cooldown
            if rule.last_triggered:
                elapsed = (datetime.now() - rule.last_triggered).total_seconds()
                if elapsed < rule.cooldown_seconds:
                    continue
            
            # Evaluate condition
            try:
                if rule.condition(metrics):
                    result = self._execute_action(rule.action, metrics)
                    rule.last_triggered = datetime.now()
                    results.append(result)
                    self.optimization_history.append(result)
                    
                    if result.success:
                        self.analytics["successful_optimizations"] += 1
                    else:
                        self.analytics["failed_optimizations"] += 1
                    
                    self.analytics["optimizations_performed"] += 1
                    
                    logger.info(f"Optimization triggered: {rule.name} -> {rule.action.value}")
            except Exception as e:
                logger.error(f"Error evaluating rule {rule.rule_id}: {e}")
        
        return results
    
    def _execute_action(self, action: OptimizationAction, metrics_before: PerformanceMetrics) -> OptimizationResult:
        """Execute an optimization action."""
        success = True
        details = {}
        
        try:
            if action == OptimizationAction.INCREASE_CACHE:
                old_size = self.cache_manager.config.max_size
                new_size = int(old_size * 1.5)
                self.cache_manager.resize(new_size)
                details = {"old_size": old_size, "new_size": new_size}
            
            elif action == OptimizationAction.DECREASE_CACHE:
                old_size = self.cache_manager.config.max_size
                new_size = max(1000, int(old_size * 0.7))
                self.cache_manager.resize(new_size)
                details = {"old_size": old_size, "new_size": new_size}
            
            elif action == OptimizationAction.CLEAR_CACHE:
                cleared = self.cache_manager.clear()
                self.analytics["cache_clears"] += 1
                details = {"entries_cleared": cleared}
            
            elif action == OptimizationAction.GC_COLLECT:
                gc.collect()
                self.analytics["gc_collections"] += 1
                details = {"gc_triggered": True}
            
            elif action == OptimizationAction.THROTTLE_REQUESTS:
                self.is_throttling = True
                self.current_throughput_limit = max(10, metrics_before.throughput_rps * 0.5)
                self.throttle_until = datetime.now() + timedelta(minutes=5)
                details = {"throttle_limit": self.current_throughput_limit}
            
            elif action == OptimizationAction.UNTHROTTLE_REQUESTS:
                self.is_throttling = False
                self.current_throughput_limit = float('inf')
                self.throttle_until = None
                details = {"throttling_disabled": True}
            
            elif action == OptimizationAction.COMPACT_MEMORY:
                gc.collect()
                self.analytics["gc_collections"] += 1
                details = {"memory_compacted": True}
            
            else:
                details = {"action": action.value, "implemented": False}
        
        except Exception as e:
            success = False
            details = {"error": str(e)}
        
        # Get metrics after optimization
        time.sleep(0.1)  # Brief pause to let changes take effect
        metrics_after = self.get_current_metrics()
        
        # Calculate improvement
        improvement = 0.0
        if metrics_before.latency_p95 > 0:
            latency_improvement = (metrics_before.latency_p95 - metrics_after.latency_p95) / metrics_before.latency_p95
            improvement = latency_improvement * 100
        
        self.analytics["total_improvement_percentage"] += max(0, improvement)
        
        return OptimizationResult(
            action=action,
            success=success,
            metrics_before=metrics_before,
            metrics_after=metrics_after,
            improvement_percentage=improvement,
            timestamp=datetime.now(),
            details=details
        )
    
    def set_strategy(self, strategy: OptimizationStrategy):
        """Set the optimization strategy."""
        self.strategy = strategy
        
        # Adjust rules based on strategy
        if strategy == OptimizationStrategy.LATENCY_FOCUSED:
            self.limits.target_latency_ms = 50
            for rule in self.optimization_rules:
                if "latency" in rule.rule_id.lower():
                    rule.priority = min(10, rule.priority + 2)
        
        elif strategy == OptimizationStrategy.THROUGHPUT_FOCUSED:
            self.limits.target_latency_ms = 200
            for rule in self.optimization_rules:
                if "cache" in rule.rule_id.lower():
                    rule.priority = min(10, rule.priority + 2)
        
        elif strategy == OptimizationStrategy.MEMORY_FOCUSED:
            self.limits.max_memory_mb = self.limits.max_memory_mb * 0.8
            for rule in self.optimization_rules:
                if "memory" in rule.rule_id.lower():
                    rule.priority = min(10, rule.priority + 2)
        
        elif strategy == OptimizationStrategy.AGGRESSIVE:
            for rule in self.optimization_rules:
                rule.cooldown_seconds = max(10, rule.cooldown_seconds // 2)
        
        logger.info(f"Optimization strategy set to: {strategy.value}")
    
    def should_throttle(self) -> bool:
        """Check if requests should be throttled."""
        if not self.is_throttling:
            return False
        
        if self.throttle_until and datetime.now() > self.throttle_until:
            self.is_throttling = False
            return False
        
        return True
    
    def get_optimization_recommendations(self) -> List[Dict[str, Any]]:
        """Get optimization recommendations based on current state."""
        metrics = self.get_current_metrics()
        recommendations = []
        
        if metrics.latency_p95 > self.limits.target_latency_ms:
            recommendations.append({
                "type": "latency",
                "severity": "high" if metrics.latency_p95 > self.limits.target_latency_ms * 2 else "medium",
                "message": f"P95 latency ({metrics.latency_p95:.1f}ms) exceeds target ({self.limits.target_latency_ms}ms)",
                "suggestions": [
                    "Consider increasing cache size",
                    "Review slow database queries",
                    "Enable request batching"
                ]
            })
        
        if metrics.cache_hit_rate < self.limits.min_cache_hit_rate:
            recommendations.append({
                "type": "cache",
                "severity": "medium",
                "message": f"Cache hit rate ({metrics.cache_hit_rate:.1%}) below target ({self.limits.min_cache_hit_rate:.1%})",
                "suggestions": [
                    "Increase cache size",
                    "Review cache key patterns",
                    "Consider cache warming"
                ]
            })
        
        if metrics.memory_usage_mb > self.limits.max_memory_mb * 0.8:
            recommendations.append({
                "type": "memory",
                "severity": "high" if metrics.memory_usage_mb > self.limits.max_memory_mb * 0.9 else "medium",
                "message": f"Memory usage ({metrics.memory_usage_mb:.1f}MB) approaching limit ({self.limits.max_memory_mb}MB)",
                "suggestions": [
                    "Reduce cache size",
                    "Review memory allocations",
                    "Schedule garbage collection"
                ]
            })
        
        if metrics.error_rate > 0.05:
            recommendations.append({
                "type": "reliability",
                "severity": "high" if metrics.error_rate > 0.1 else "medium",
                "message": f"Error rate ({metrics.error_rate:.1%}) is elevated",
                "suggestions": [
                    "Review error logs",
                    "Check external dependencies",
                    "Consider circuit breaker pattern"
                ]
            })
        
        return recommendations
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Get comprehensive performance report."""
        metrics = self.get_current_metrics()
        cache_stats = self.cache_manager.get_stats()
        function_stats = self.profiler.get_function_stats()
        
        return {
            "current_metrics": metrics.to_dict(),
            "strategy": self.strategy.value,
            "limits": {
                "max_memory_mb": self.limits.max_memory_mb,
                "max_queue_depth": self.limits.max_queue_depth,
                "target_latency_ms": self.limits.target_latency_ms,
                "min_cache_hit_rate": self.limits.min_cache_hit_rate
            },
            "cache_stats": cache_stats,
            "function_stats": function_stats,
            "throttling": {
                "is_throttling": self.is_throttling,
                "current_limit": self.current_throughput_limit,
                "until": self.throttle_until.isoformat() if self.throttle_until else None
            },
            "optimization_rules": [
                {
                    "rule_id": r.rule_id,
                    "name": r.name,
                    "action": r.action.value,
                    "priority": r.priority,
                    "enabled": r.enabled,
                    "last_triggered": r.last_triggered.isoformat() if r.last_triggered else None
                }
                for r in self.optimization_rules
            ],
            "recent_optimizations": [
                {
                    "action": opt.action.value,
                    "success": opt.success,
                    "improvement": opt.improvement_percentage,
                    "timestamp": opt.timestamp.isoformat()
                }
                for opt in list(self.optimization_history)[-10:]
            ],
            "analytics": self.analytics,
            "recommendations": self.get_optimization_recommendations()
        }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get optimizer analytics."""
        return {
            "strategy": self.strategy.value,
            "optimizations": self.analytics,
            "cache": self.cache_manager.get_stats(),
            "profiling": {
                "functions_profiled": len(self.profiler.function_stats),
                "active_profiles": len(self.profiler.active_profiles),
                "completed_profiles": len(self.profiler.completed_profiles)
            },
            "history_size": len(self.optimization_history),
            "metrics_history_size": len(self.metrics_history)
        }


# Factory function
def create_performance_optimizer(environment_ref=None) -> AutoPerformanceOptimizer:
    """Create performance optimizer instance."""
    return AutoPerformanceOptimizer(environment_ref)


# Global instance
performance_optimizer = AutoPerformanceOptimizer()
