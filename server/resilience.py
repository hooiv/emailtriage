"""
Enterprise Resilience Patterns
Circuit Breaker, Rate Limiter Dashboard, and Fault Tolerance
"""
import time
import threading
import logging
from enum import Enum
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque
import functools
import asyncio

logger = logging.getLogger("resilience")


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5          # Failures before opening
    success_threshold: int = 3          # Successes to close from half-open
    timeout_seconds: float = 30.0       # Time before half-open
    half_open_max_calls: int = 3        # Max calls in half-open state
    excluded_exceptions: tuple = ()     # Exceptions that don't count as failures


@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker"""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    last_state_change: Optional[datetime] = None
    time_in_open: float = 0.0
    time_in_closed: float = 0.0
    current_state: str = "closed"
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreaker:
    """
    Circuit Breaker Pattern Implementation
    
    Prevents cascading failures by failing fast when a service is down.
    Three states: CLOSED (normal), OPEN (failing), HALF_OPEN (testing)
    """
    
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.last_state_change = datetime.now()
        self.half_open_calls = 0
        self.metrics = CircuitBreakerMetrics()
        self._lock = threading.RLock()
        self._call_history: deque = deque(maxlen=1000)
        
        logger.info(f"Circuit breaker '{name}' initialized")
    
    def _record_call(self, success: bool, duration_ms: float, error: str = None):
        """Record a call in history"""
        self._call_history.append({
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "duration_ms": duration_ms,
            "state": self.state.value,
            "error": error
        })
    
    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state"""
        old_state = self.state
        if old_state == new_state:
            return
        
        now = datetime.now()
        time_in_state = (now - self.last_state_change).total_seconds()
        
        if old_state == CircuitState.OPEN:
            self.metrics.time_in_open += time_in_state
        elif old_state == CircuitState.CLOSED:
            self.metrics.time_in_closed += time_in_state
        
        self.state = new_state
        self.last_state_change = now
        self.metrics.state_changes += 1
        self.metrics.last_state_change = now
        self.metrics.current_state = new_state.value
        
        if new_state == CircuitState.HALF_OPEN:
            self.half_open_calls = 0
            self.success_count = 0
        elif new_state == CircuitState.CLOSED:
            self.failure_count = 0
            self.success_count = 0
        
        logger.info(f"Circuit '{self.name}': {old_state.value} -> {new_state.value}")
    
    def _should_allow_request(self) -> bool:
        """Check if request should be allowed"""
        if self.state == CircuitState.CLOSED:
            return True
        
        if self.state == CircuitState.OPEN:
            # Check if timeout has passed
            if self.last_failure_time:
                elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                if elapsed >= self.config.timeout_seconds:
                    self._transition_to(CircuitState.HALF_OPEN)
                    return True
            return False
        
        if self.state == CircuitState.HALF_OPEN:
            # Allow limited calls in half-open
            return self.half_open_calls < self.config.half_open_max_calls
        
        return False
    
    def _record_success(self):
        """Record a successful call"""
        with self._lock:
            self.metrics.total_calls += 1
            self.metrics.successful_calls += 1
            self.metrics.last_success_time = datetime.now()
            self.metrics.consecutive_successes += 1
            self.metrics.consecutive_failures = 0
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            
            self.failure_count = 0
    
    def _record_failure(self, error: Exception):
        """Record a failed call"""
        with self._lock:
            # Check if exception should be excluded
            if isinstance(error, self.config.excluded_exceptions):
                return
            
            self.metrics.total_calls += 1
            self.metrics.failed_calls += 1
            self.metrics.last_failure_time = datetime.now()
            self.metrics.consecutive_failures += 1
            self.metrics.consecutive_successes = 0
            
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.state == CircuitState.HALF_OPEN:
                # Immediate transition back to OPEN on failure
                self._transition_to(CircuitState.OPEN)
            elif self.state == CircuitState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self._transition_to(CircuitState.OPEN)
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function through the circuit breaker"""
        with self._lock:
            if not self._should_allow_request():
                self.metrics.rejected_calls += 1
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN. "
                    f"Retry after {self.config.timeout_seconds}s"
                )
            
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_calls += 1
        
        start = time.time()
        try:
            result = func(*args, **kwargs)
            duration = (time.time() - start) * 1000
            self._record_call(True, duration)
            self._record_success()
            return result
        except Exception as e:
            duration = (time.time() - start) * 1000
            self._record_call(False, duration, str(e))
            self._record_failure(e)
            raise
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute an async function through the circuit breaker"""
        with self._lock:
            if not self._should_allow_request():
                self.metrics.rejected_calls += 1
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN"
                )
            
            if self.state == CircuitState.HALF_OPEN:
                self.half_open_calls += 1
        
        start = time.time()
        try:
            result = await func(*args, **kwargs)
            duration = (time.time() - start) * 1000
            self._record_call(True, duration)
            self._record_success()
            return result
        except Exception as e:
            duration = (time.time() - start) * 1000
            self._record_call(False, duration, str(e))
            self._record_failure(e)
            raise
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status"""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "config": {
                "failure_threshold": self.config.failure_threshold,
                "success_threshold": self.config.success_threshold,
                "timeout_seconds": self.config.timeout_seconds,
                "half_open_max_calls": self.config.half_open_max_calls
            },
            "metrics": {
                "total_calls": self.metrics.total_calls,
                "successful_calls": self.metrics.successful_calls,
                "failed_calls": self.metrics.failed_calls,
                "rejected_calls": self.metrics.rejected_calls,
                "state_changes": self.metrics.state_changes,
                "consecutive_failures": self.metrics.consecutive_failures,
                "consecutive_successes": self.metrics.consecutive_successes,
                "time_in_open_seconds": self.metrics.time_in_open,
                "last_failure": self.metrics.last_failure_time.isoformat() if self.metrics.last_failure_time else None,
                "last_success": self.metrics.last_success_time.isoformat() if self.metrics.last_success_time else None
            },
            "recent_calls": list(self._call_history)[-20:]
        }
    
    def reset(self):
        """Manually reset the circuit breaker"""
        with self._lock:
            self._transition_to(CircuitState.CLOSED)
            self.failure_count = 0
            self.success_count = 0
            logger.info(f"Circuit '{self.name}' manually reset")


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open"""
    pass


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers"""
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
        self._initialize_default_breakers()
    
    def _initialize_default_breakers(self):
        """Initialize default circuit breakers"""
        defaults = [
            ("llm_api", CircuitBreakerConfig(
                failure_threshold=3,
                timeout_seconds=60.0,
                success_threshold=2
            )),
            ("email_service", CircuitBreakerConfig(
                failure_threshold=5,
                timeout_seconds=30.0
            )),
            ("external_api", CircuitBreakerConfig(
                failure_threshold=5,
                timeout_seconds=45.0
            )),
            ("database", CircuitBreakerConfig(
                failure_threshold=3,
                timeout_seconds=10.0,
                success_threshold=1
            )),
            ("cache", CircuitBreakerConfig(
                failure_threshold=10,
                timeout_seconds=5.0
            ))
        ]
        
        for name, config in defaults:
            self.register(name, config)
    
    def register(self, name: str, config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Register a new circuit breaker"""
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, config)
            return self._breakers[name]
    
    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name"""
        return self._breakers.get(name)
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get status of all circuit breakers"""
        statuses = {}
        for name, breaker in self._breakers.items():
            statuses[name] = breaker.get_status()
        
        # Calculate summary
        total = len(self._breakers)
        open_count = sum(1 for b in self._breakers.values() if b.state == CircuitState.OPEN)
        half_open_count = sum(1 for b in self._breakers.values() if b.state == CircuitState.HALF_OPEN)
        closed_count = total - open_count - half_open_count
        
        return {
            "summary": {
                "total_circuits": total,
                "closed": closed_count,
                "open": open_count,
                "half_open": half_open_count,
                "health_percentage": (closed_count / total * 100) if total > 0 else 100
            },
            "circuits": statuses
        }
    
    def reset_all(self):
        """Reset all circuit breakers"""
        for breaker in self._breakers.values():
            breaker.reset()


# Decorator for circuit breaker
def circuit_breaker(name: str, registry: CircuitBreakerRegistry = None):
    """Decorator to wrap function with circuit breaker"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            reg = registry or _global_registry
            breaker = reg.get(name) or reg.register(name)
            return breaker.call(func, *args, **kwargs)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            reg = registry or _global_registry
            breaker = reg.get(name) or reg.register(name)
            return await breaker.call_async(func, *args, **kwargs)
        
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper
    return decorator


# Global registry instance
_global_registry = CircuitBreakerRegistry()


def get_circuit_breaker_registry() -> CircuitBreakerRegistry:
    """Get global circuit breaker registry"""
    return _global_registry


# ============================================================================
# Advanced Rate Limiter with Dashboard
# ============================================================================

@dataclass
class RateLimitRule:
    """Rate limit rule configuration"""
    name: str
    requests_per_window: int
    window_seconds: int
    burst_allowance: int = 0
    penalty_seconds: int = 0  # Additional cooldown after limit hit


@dataclass
class RateLimitBucket:
    """Token bucket for rate limiting"""
    tokens: float
    last_update: datetime
    violations: int = 0
    last_violation: Optional[datetime] = None
    penalty_until: Optional[datetime] = None


class AdvancedRateLimiter:
    """
    Advanced Rate Limiter with Token Bucket Algorithm
    
    Features:
    - Multiple rate limit tiers
    - Burst allowance
    - Violation penalties
    - Per-client tracking
    - Analytics dashboard
    """
    
    def __init__(self):
        self._buckets: Dict[str, Dict[str, RateLimitBucket]] = {}
        self._rules: Dict[str, RateLimitRule] = {}
        self._lock = threading.RLock()
        self._request_log: deque = deque(maxlen=10000)
        self._initialize_default_rules()
        
        logger.info("Advanced Rate Limiter initialized")
    
    def _initialize_default_rules(self):
        """Initialize default rate limit rules"""
        default_rules = [
            RateLimitRule("default", 100, 60, burst_allowance=20),
            RateLimitRule("api_heavy", 30, 60, burst_allowance=10, penalty_seconds=30),
            RateLimitRule("api_light", 200, 60, burst_allowance=50),
            RateLimitRule("auth", 10, 60, penalty_seconds=300),
            RateLimitRule("search", 50, 60, burst_allowance=10),
            RateLimitRule("export", 5, 300, penalty_seconds=60),
            RateLimitRule("webhook", 1000, 60, burst_allowance=100),
            RateLimitRule("admin", 500, 60, burst_allowance=100)
        ]
        
        for rule in default_rules:
            self._rules[rule.name] = rule
            self._buckets[rule.name] = {}
    
    def add_rule(self, rule: RateLimitRule):
        """Add a new rate limit rule"""
        with self._lock:
            self._rules[rule.name] = rule
            self._buckets[rule.name] = {}
    
    def _get_bucket(self, rule_name: str, client_id: str) -> RateLimitBucket:
        """Get or create bucket for client"""
        if rule_name not in self._buckets:
            self._buckets[rule_name] = {}
        
        if client_id not in self._buckets[rule_name]:
            rule = self._rules[rule_name]
            self._buckets[rule_name][client_id] = RateLimitBucket(
                tokens=rule.requests_per_window + rule.burst_allowance,
                last_update=datetime.now()
            )
        
        return self._buckets[rule_name][client_id]
    
    def _refill_tokens(self, bucket: RateLimitBucket, rule: RateLimitRule):
        """Refill tokens based on time passed"""
        now = datetime.now()
        elapsed = (now - bucket.last_update).total_seconds()
        
        # Calculate tokens to add
        tokens_per_second = rule.requests_per_window / rule.window_seconds
        new_tokens = elapsed * tokens_per_second
        
        max_tokens = rule.requests_per_window + rule.burst_allowance
        bucket.tokens = min(max_tokens, bucket.tokens + new_tokens)
        bucket.last_update = now
    
    def check_rate_limit(
        self,
        client_id: str,
        rule_name: str = "default",
        cost: int = 1
    ) -> Dict[str, Any]:
        """
        Check if request is allowed under rate limit
        
        Returns:
            Dict with 'allowed', 'remaining', 'reset_time', etc.
        """
        with self._lock:
            if rule_name not in self._rules:
                rule_name = "default"
            
            rule = self._rules[rule_name]
            bucket = self._get_bucket(rule_name, client_id)
            
            # Check penalty
            if bucket.penalty_until and datetime.now() < bucket.penalty_until:
                remaining_penalty = (bucket.penalty_until - datetime.now()).total_seconds()
                self._log_request(client_id, rule_name, False, "penalty")
                return {
                    "allowed": False,
                    "reason": "rate_limit_penalty",
                    "remaining": 0,
                    "retry_after": remaining_penalty,
                    "violations": bucket.violations
                }
            
            # Refill tokens
            self._refill_tokens(bucket, rule)
            
            # Check if allowed
            if bucket.tokens >= cost:
                bucket.tokens -= cost
                self._log_request(client_id, rule_name, True)
                
                return {
                    "allowed": True,
                    "remaining": int(bucket.tokens),
                    "limit": rule.requests_per_window,
                    "reset_seconds": rule.window_seconds
                }
            else:
                # Rate limit exceeded
                bucket.violations += 1
                bucket.last_violation = datetime.now()
                
                if rule.penalty_seconds > 0:
                    bucket.penalty_until = datetime.now() + timedelta(seconds=rule.penalty_seconds)
                
                retry_after = rule.window_seconds * (1 - bucket.tokens / rule.requests_per_window)
                self._log_request(client_id, rule_name, False, "exceeded")
                
                return {
                    "allowed": False,
                    "reason": "rate_limit_exceeded",
                    "remaining": 0,
                    "retry_after": retry_after,
                    "violations": bucket.violations,
                    "penalty_applied": rule.penalty_seconds > 0
                }
    
    def _log_request(self, client_id: str, rule_name: str, allowed: bool, reason: str = None):
        """Log request for analytics"""
        self._request_log.append({
            "timestamp": datetime.now().isoformat(),
            "client_id": client_id[:20] if client_id else "unknown",  # Truncate for privacy
            "rule": rule_name,
            "allowed": allowed,
            "reason": reason
        })
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get rate limiter analytics"""
        now = datetime.now()
        last_minute = now - timedelta(minutes=1)
        last_hour = now - timedelta(hours=1)
        
        recent_requests = list(self._request_log)
        
        # Last minute stats
        minute_requests = [r for r in recent_requests 
                          if datetime.fromisoformat(r["timestamp"]) > last_minute]
        minute_allowed = sum(1 for r in minute_requests if r["allowed"])
        minute_denied = len(minute_requests) - minute_allowed
        
        # Per-rule stats
        rule_stats = {}
        for rule_name in self._rules:
            rule_reqs = [r for r in recent_requests if r["rule"] == rule_name]
            rule_stats[rule_name] = {
                "total_requests": len(rule_reqs),
                "allowed": sum(1 for r in rule_reqs if r["allowed"]),
                "denied": sum(1 for r in rule_reqs if not r["allowed"]),
                "active_clients": len(self._buckets.get(rule_name, {}))
            }
        
        # Top violators
        violations_by_client: Dict[str, int] = {}
        for rule_name, buckets in self._buckets.items():
            for client_id, bucket in buckets.items():
                if bucket.violations > 0:
                    key = f"{client_id[:10]}..."
                    violations_by_client[key] = violations_by_client.get(key, 0) + bucket.violations
        
        top_violators = sorted(violations_by_client.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            "summary": {
                "total_rules": len(self._rules),
                "total_tracked_clients": sum(len(b) for b in self._buckets.values()),
                "requests_last_minute": len(minute_requests),
                "allowed_last_minute": minute_allowed,
                "denied_last_minute": minute_denied,
                "denial_rate": (minute_denied / len(minute_requests) * 100) if minute_requests else 0
            },
            "rules": rule_stats,
            "top_violators": [{"client": c, "violations": v} for c, v in top_violators],
            "recent_denials": [r for r in recent_requests if not r["allowed"]][-20:]
        }
    
    def reset_client(self, client_id: str, rule_name: str = None):
        """Reset rate limit for a client"""
        with self._lock:
            if rule_name:
                if rule_name in self._buckets and client_id in self._buckets[rule_name]:
                    del self._buckets[rule_name][client_id]
            else:
                for buckets in self._buckets.values():
                    if client_id in buckets:
                        del buckets[client_id]


# ============================================================================
# Retry Pattern with Exponential Backoff
# ============================================================================

@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_retries: int = 3
    initial_delay_ms: int = 100
    max_delay_ms: int = 10000
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: tuple = (Exception,)


class RetryHandler:
    """Handles retries with exponential backoff"""
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
        self._metrics = {
            "total_attempts": 0,
            "successful_retries": 0,
            "failed_after_retries": 0,
            "total_retry_time_ms": 0
        }
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for attempt"""
        import random
        
        delay = self.config.initial_delay_ms * (self.config.exponential_base ** attempt)
        delay = min(delay, self.config.max_delay_ms)
        
        if self.config.jitter:
            delay = delay * (0.5 + random.random())
        
        return delay / 1000.0  # Convert to seconds
    
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        last_exception = None
        start_time = time.time()
        
        for attempt in range(self.config.max_retries + 1):
            self._metrics["total_attempts"] += 1
            
            try:
                result = func(*args, **kwargs)
                if attempt > 0:
                    self._metrics["successful_retries"] += 1
                return result
            
            except self.config.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Retry {attempt + 1}/{self.config.max_retries} after {delay:.2f}s: {e}")
                    time.sleep(delay)
                    self._metrics["total_retry_time_ms"] += delay * 1000
        
        self._metrics["failed_after_retries"] += 1
        raise last_exception
    
    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            self._metrics["total_attempts"] += 1
            
            try:
                result = await func(*args, **kwargs)
                if attempt > 0:
                    self._metrics["successful_retries"] += 1
                return result
            
            except self.config.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    delay = self._calculate_delay(attempt)
                    logger.warning(f"Async retry {attempt + 1}/{self.config.max_retries}: {e}")
                    await asyncio.sleep(delay)
                    self._metrics["total_retry_time_ms"] += delay * 1000
        
        self._metrics["failed_after_retries"] += 1
        raise last_exception
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get retry metrics"""
        return {
            **self._metrics,
            "retry_success_rate": (
                self._metrics["successful_retries"] / 
                (self._metrics["successful_retries"] + self._metrics["failed_after_retries"])
                * 100
            ) if (self._metrics["successful_retries"] + self._metrics["failed_after_retries"]) > 0 else 0
        }


# Global instances
_rate_limiter = AdvancedRateLimiter()
_retry_handler = RetryHandler()


def get_rate_limiter() -> AdvancedRateLimiter:
    """Get global rate limiter"""
    return _rate_limiter


def get_retry_handler() -> RetryHandler:
    """Get global retry handler"""
    return _retry_handler
