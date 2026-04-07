"""
Health Check System for Email Triage Environment

Kubernetes-ready health probes providing:
- Liveness checks (is the service alive?)
- Readiness checks (can it serve traffic?)
- Deep health checks (all dependencies)
- Startup probes (for slow-starting containers)
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import deque
from enum import Enum
import threading
import asyncio
import time


class HealthStatus(str, Enum):
    """Health check status values"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class HealthCheck:
    """Individual health check definition"""
    
    def __init__(
        self,
        name: str,
        check_fn: Callable[[], Dict[str, Any]],
        critical: bool = True,
        timeout: float = 5.0,
        interval: float = 30.0
    ):
        self.name = name
        self.check_fn = check_fn
        self.critical = critical
        self.timeout = timeout
        self.interval = interval
        self.last_check: Optional[datetime] = None
        self.last_result: Optional[Dict[str, Any]] = None
        self.consecutive_failures = 0
        self.total_checks = 0
        self.total_failures = 0
    
    def execute(self) -> Dict[str, Any]:
        """Execute the health check"""
        start = time.time()
        self.total_checks += 1
        self.last_check = datetime.now()
        
        try:
            result = self.check_fn()
            duration = time.time() - start
            
            self.last_result = {
                "name": self.name,
                "status": result.get("status", HealthStatus.HEALTHY),
                "duration_ms": round(duration * 1000, 2),
                "details": result.get("details", {}),
                "critical": self.critical,
                "timestamp": self.last_check.isoformat()
            }
            
            if result.get("status") == HealthStatus.UNHEALTHY:
                self.consecutive_failures += 1
                self.total_failures += 1
            else:
                self.consecutive_failures = 0
            
            return self.last_result
            
        except Exception as e:
            duration = time.time() - start
            self.consecutive_failures += 1
            self.total_failures += 1
            
            self.last_result = {
                "name": self.name,
                "status": HealthStatus.UNHEALTHY,
                "duration_ms": round(duration * 1000, 2),
                "error": str(e),
                "critical": self.critical,
                "timestamp": self.last_check.isoformat()
            }
            return self.last_result


class HealthCheckManager:
    """Manage health checks for the system"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.checks: Dict[str, HealthCheck] = {}
        self.history = deque(maxlen=1000)
        self.startup_complete = False
        self.startup_time: Optional[datetime] = None
        self.degraded_threshold = 1  # Failures before degraded
        self.unhealthy_threshold = 3  # Failures before unhealthy
        
        # Register default checks
        self._register_default_checks()
    
    def _register_default_checks(self):
        """Register default health checks"""
        
        # Memory check
        def memory_check():
            try:
                import psutil
                memory = psutil.virtual_memory()
                percent_used = memory.percent
                
                if percent_used > 95:
                    return {"status": HealthStatus.UNHEALTHY, "details": {"memory_percent": percent_used}}
                elif percent_used > 85:
                    return {"status": HealthStatus.DEGRADED, "details": {"memory_percent": percent_used}}
                return {"status": HealthStatus.HEALTHY, "details": {"memory_percent": percent_used}}
            except ImportError:
                return {"status": HealthStatus.HEALTHY, "details": {"message": "psutil not available"}}
        
        # CPU check
        def cpu_check():
            try:
                import psutil
                cpu_percent = psutil.cpu_percent(interval=0.1)
                
                if cpu_percent > 95:
                    return {"status": HealthStatus.UNHEALTHY, "details": {"cpu_percent": cpu_percent}}
                elif cpu_percent > 80:
                    return {"status": HealthStatus.DEGRADED, "details": {"cpu_percent": cpu_percent}}
                return {"status": HealthStatus.HEALTHY, "details": {"cpu_percent": cpu_percent}}
            except ImportError:
                return {"status": HealthStatus.HEALTHY, "details": {"message": "psutil not available"}}
        
        # Disk check
        def disk_check():
            try:
                import psutil
                disk = psutil.disk_usage('/')
                percent_used = disk.percent
                
                if percent_used > 95:
                    return {"status": HealthStatus.UNHEALTHY, "details": {"disk_percent": percent_used}}
                elif percent_used > 85:
                    return {"status": HealthStatus.DEGRADED, "details": {"disk_percent": percent_used}}
                return {"status": HealthStatus.HEALTHY, "details": {"disk_percent": percent_used}}
            except ImportError:
                return {"status": HealthStatus.HEALTHY, "details": {"message": "psutil not available"}}
        
        # Thread pool check
        def thread_check():
            active = threading.active_count()
            if active > 100:
                return {"status": HealthStatus.DEGRADED, "details": {"active_threads": active}}
            return {"status": HealthStatus.HEALTHY, "details": {"active_threads": active}}
        
        # Event loop check (for async)
        def event_loop_check():
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    return {"status": HealthStatus.UNHEALTHY, "details": {"loop_closed": True}}
                return {"status": HealthStatus.HEALTHY, "details": {"loop_running": loop.is_running()}}
            except RuntimeError:
                return {"status": HealthStatus.HEALTHY, "details": {"no_loop": True}}
        
        self.register_check("memory", memory_check, critical=True)
        self.register_check("cpu", cpu_check, critical=False)
        self.register_check("disk", disk_check, critical=True)
        self.register_check("threads", thread_check, critical=False)
        self.register_check("event_loop", event_loop_check, critical=False)
    
    def register_check(
        self, 
        name: str, 
        check_fn: Callable, 
        critical: bool = True,
        timeout: float = 5.0,
        interval: float = 30.0
    ):
        """Register a health check"""
        with self._lock:
            self.checks[name] = HealthCheck(name, check_fn, critical, timeout, interval)
    
    def unregister_check(self, name: str) -> bool:
        """Unregister a health check"""
        with self._lock:
            if name in self.checks:
                del self.checks[name]
                return True
            return False
    
    def mark_startup_complete(self):
        """Mark startup as complete"""
        with self._lock:
            self.startup_complete = True
            self.startup_time = datetime.now()
    
    def liveness(self) -> Dict[str, Any]:
        """Kubernetes liveness probe - is the process alive?"""
        with self._lock:
            return {
                "status": HealthStatus.HEALTHY,
                "timestamp": datetime.now().isoformat(),
                "type": "liveness",
                "details": {
                    "process_alive": True,
                    "uptime_seconds": self._get_uptime_seconds()
                }
            }
    
    def readiness(self) -> Dict[str, Any]:
        """Kubernetes readiness probe - can we serve traffic?"""
        with self._lock:
            if not self.startup_complete:
                return {
                    "status": HealthStatus.UNHEALTHY,
                    "timestamp": datetime.now().isoformat(),
                    "type": "readiness",
                    "details": {"startup_complete": False}
                }
            
            # Run critical checks
            critical_checks = {k: v for k, v in self.checks.items() if v.critical}
            results = {}
            all_healthy = True
            
            for name, check in critical_checks.items():
                result = check.execute()
                results[name] = result
                if result["status"] != HealthStatus.HEALTHY:
                    all_healthy = False
            
            return {
                "status": HealthStatus.HEALTHY if all_healthy else HealthStatus.UNHEALTHY,
                "timestamp": datetime.now().isoformat(),
                "type": "readiness",
                "checks": results
            }
    
    def startup(self) -> Dict[str, Any]:
        """Kubernetes startup probe - has the container started?"""
        with self._lock:
            return {
                "status": HealthStatus.HEALTHY if self.startup_complete else HealthStatus.UNHEALTHY,
                "timestamp": datetime.now().isoformat(),
                "type": "startup",
                "details": {
                    "startup_complete": self.startup_complete,
                    "startup_time": self.startup_time.isoformat() if self.startup_time else None
                }
            }
    
    def deep_health(self) -> Dict[str, Any]:
        """Deep health check - check all dependencies"""
        with self._lock:
            results = {}
            status_counts = {
                HealthStatus.HEALTHY: 0,
                HealthStatus.DEGRADED: 0,
                HealthStatus.UNHEALTHY: 0
            }
            
            for name, check in self.checks.items():
                result = check.execute()
                results[name] = result
                status_counts[result["status"]] += 1
                
                # Record history
                self.history.append({
                    "check": name,
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                })
            
            # Determine overall status
            if status_counts[HealthStatus.UNHEALTHY] > 0:
                # Check if any critical checks failed
                critical_failed = any(
                    results[name]["status"] == HealthStatus.UNHEALTHY 
                    for name, check in self.checks.items() 
                    if check.critical
                )
                overall = HealthStatus.UNHEALTHY if critical_failed else HealthStatus.DEGRADED
            elif status_counts[HealthStatus.DEGRADED] > 0:
                overall = HealthStatus.DEGRADED
            else:
                overall = HealthStatus.HEALTHY
            
            return {
                "status": overall,
                "timestamp": datetime.now().isoformat(),
                "type": "deep",
                "checks": results,
                "summary": {
                    "total": len(self.checks),
                    "healthy": status_counts[HealthStatus.HEALTHY],
                    "degraded": status_counts[HealthStatus.DEGRADED],
                    "unhealthy": status_counts[HealthStatus.UNHEALTHY]
                },
                "uptime_seconds": self._get_uptime_seconds()
            }
    
    def get_check_history(self, check_name: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get health check history"""
        with self._lock:
            history = list(self.history)
            if check_name:
                history = [h for h in history if h["check"] == check_name]
            return history[-limit:]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get health check statistics"""
        with self._lock:
            check_stats = {}
            for name, check in self.checks.items():
                check_stats[name] = {
                    "total_checks": check.total_checks,
                    "total_failures": check.total_failures,
                    "consecutive_failures": check.consecutive_failures,
                    "failure_rate": (
                        check.total_failures / check.total_checks 
                        if check.total_checks > 0 else 0
                    ),
                    "last_check": check.last_check.isoformat() if check.last_check else None,
                    "critical": check.critical
                }
            
            return {
                "total_checks_registered": len(self.checks),
                "startup_complete": self.startup_complete,
                "startup_time": self.startup_time.isoformat() if self.startup_time else None,
                "uptime_seconds": self._get_uptime_seconds(),
                "checks": check_stats,
                "history_size": len(self.history)
            }
    
    def _get_uptime_seconds(self) -> float:
        """Calculate uptime in seconds"""
        if not self.startup_time:
            return 0.0
        return (datetime.now() - self.startup_time).total_seconds()
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get health system analytics"""
        stats = self.get_stats()
        return {
            "status": "active",
            "checks_registered": len(self.checks),
            "startup_complete": self.startup_complete,
            "uptime_seconds": self._get_uptime_seconds(),
            "features": [
                "liveness_probe",
                "readiness_probe", 
                "startup_probe",
                "deep_health",
                "check_history",
                "statistics",
                "kubernetes_ready"
            ],
            "probe_endpoints": {
                "liveness": "/health/live",
                "readiness": "/health/ready",
                "startup": "/health/startup",
                "deep": "/health/deep"
            },
            "check_statistics": stats["checks"]
        }


# Global instance
_health_manager: Optional[HealthCheckManager] = None
_health_lock = threading.Lock()


def get_health_manager() -> HealthCheckManager:
    """Get or create health manager instance"""
    global _health_manager
    with _health_lock:
        if _health_manager is None:
            _health_manager = HealthCheckManager()
        return _health_manager
