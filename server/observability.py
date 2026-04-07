"""
Real-time Metrics & Observability Dashboard
Production-grade metrics collection, aggregation, and visualization
"""
import time
import threading
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import deque
from enum import Enum
import statistics

logger = logging.getLogger("metrics")


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"        # Always increasing
    GAUGE = "gauge"           # Point-in-time value
    HISTOGRAM = "histogram"   # Distribution of values
    SUMMARY = "summary"       # Statistical summary


class AggregationMethod(Enum):
    """How to aggregate metric values"""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    P50 = "p50"
    P95 = "p95"
    P99 = "p99"


@dataclass
class MetricValue:
    """Single metric data point"""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "value": self.value,
            "labels": self.labels
        }


@dataclass
class Metric:
    """Metric definition with time-series data"""
    name: str
    type: MetricType
    description: str
    unit: str = ""
    values: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def record(self, value: float, labels: Dict[str, str] = None):
        """Record a metric value"""
        self.values.append(MetricValue(
            timestamp=time.time(),
            value=value,
            labels=labels or {}
        ))
    
    def increment(self, amount: float = 1.0, labels: Dict[str, str] = None):
        """Increment counter metric"""
        if self.type == MetricType.COUNTER:
            current = self.values[-1].value if self.values else 0
            self.record(current + amount, labels)
    
    def set(self, value: float, labels: Dict[str, str] = None):
        """Set gauge metric"""
        if self.type == MetricType.GAUGE:
            self.record(value, labels)
    
    def get_latest(self) -> Optional[float]:
        """Get latest value"""
        if self.values:
            return self.values[-1].value
        return None
    
    def get_stats(self, window_seconds: int = 60) -> Dict[str, float]:
        """Get statistics for recent values"""
        cutoff = time.time() - window_seconds
        recent = [v.value for v in self.values if v.timestamp >= cutoff]
        
        if not recent:
            return {"count": 0}
        
        return {
            "count": len(recent),
            "sum": sum(recent),
            "avg": statistics.mean(recent),
            "min": min(recent),
            "max": max(recent),
            "p50": statistics.median(recent) if len(recent) > 0 else 0,
            "p95": statistics.quantiles(recent, n=20)[18] if len(recent) >= 20 else max(recent),
            "p99": statistics.quantiles(recent, n=100)[98] if len(recent) >= 100 else max(recent)
        }
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type.value,
            "description": self.description,
            "unit": self.unit,
            "latest_value": self.get_latest(),
            "data_points": len(self.values),
            "stats_60s": self.get_stats(60)
        }


@dataclass
class Alert:
    """Metric alert definition"""
    id: str
    metric_name: str
    condition: str  # gt, lt, eq
    threshold: float
    duration_seconds: int
    severity: str = "warning"
    message: str = ""
    enabled: bool = True
    triggered_at: Optional[float] = None
    resolved_at: Optional[float] = None
    trigger_count: int = 0
    
    def check(self, metric: Metric) -> bool:
        """Check if alert should trigger"""
        if not self.enabled:
            return False
        
        latest = metric.get_latest()
        if latest is None:
            return False
        
        # Check condition
        triggered = False
        if self.condition == "gt" and latest > self.threshold:
            triggered = True
        elif self.condition == "lt" and latest < self.threshold:
            triggered = True
        elif self.condition == "eq" and latest == self.threshold:
            triggered = True
        
        # Update state
        now = time.time()
        if triggered:
            if self.triggered_at is None:
                self.triggered_at = now
                self.trigger_count += 1
            self.resolved_at = None
        else:
            if self.triggered_at is not None:
                self.resolved_at = now
            self.triggered_at = None
        
        # Check duration
        if self.triggered_at and (now - self.triggered_at) >= self.duration_seconds:
            return True
        
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "metric_name": self.metric_name,
            "condition": f"{self.condition} {self.threshold}",
            "duration_seconds": self.duration_seconds,
            "severity": self.severity,
            "message": self.message,
            "enabled": self.enabled,
            "is_triggered": self.triggered_at is not None,
            "trigger_count": self.trigger_count
        }


class MetricsCollector:
    """
    Production Metrics & Observability System
    
    Features:
    - Real-time metric collection
    - Multiple metric types (counter, gauge, histogram)
    - Statistical aggregations
    - Alerting on thresholds
    - Time-series data retention
    - Custom dashboards
    """
    
    def __init__(self):
        self._metrics: Dict[str, Metric] = {}
        self._alerts: Dict[str, Alert] = {}
        self._lock = threading.RLock()
        self._start_time = time.time()
        
        # Register default metrics
        self._register_default_metrics()
        
        logger.info("Metrics Collector initialized")
    
    def _register_default_metrics(self):
        """Register default system metrics"""
        
        # Request metrics
        self.register_metric("http_requests_total", MetricType.COUNTER, "Total HTTP requests", "requests")
        self.register_metric("http_request_duration_ms", MetricType.HISTOGRAM, "Request duration", "ms")
        self.register_metric("http_errors_total", MetricType.COUNTER, "Total HTTP errors", "errors")
        
        # Email metrics
        self.register_metric("emails_processed", MetricType.COUNTER, "Emails processed", "emails")
        self.register_metric("emails_categorized", MetricType.COUNTER, "Emails categorized", "emails")
        self.register_metric("emails_prioritized", MetricType.COUNTER, "Emails prioritized", "emails")
        self.register_metric("emails_replied", MetricType.COUNTER, "Email replies sent", "replies")
        
        # Performance metrics
        self.register_metric("cpu_usage_percent", MetricType.GAUGE, "CPU usage", "%")
        self.register_metric("memory_usage_mb", MetricType.GAUGE, "Memory usage", "MB")
        self.register_metric("active_sessions", MetricType.GAUGE, "Active sessions", "sessions")
        
        # AI system metrics
        self.register_metric("ml_predictions", MetricType.COUNTER, "ML predictions made", "predictions")
        self.register_metric("ml_accuracy", MetricType.GAUGE, "ML accuracy", "%")
        self.register_metric("security_scans", MetricType.COUNTER, "Security scans", "scans")
        self.register_metric("threats_detected", MetricType.COUNTER, "Threats detected", "threats")
        
        # Queue metrics
        self.register_metric("queue_size", MetricType.GAUGE, "Job queue size", "jobs")
        self.register_metric("jobs_completed", MetricType.COUNTER, "Jobs completed", "jobs")
        self.register_metric("jobs_failed", MetricType.COUNTER, "Jobs failed", "jobs")
        
        # Cache metrics
        self.register_metric("cache_hits", MetricType.COUNTER, "Cache hits", "hits")
        self.register_metric("cache_misses", MetricType.COUNTER, "Cache misses", "misses")
        self.register_metric("cache_evictions", MetricType.COUNTER, "Cache evictions", "evictions")
        
        # Register default alerts
        self.register_alert("high_error_rate", "http_errors_total", "gt", 100, 60, "critical", 
                          "High error rate detected")
        self.register_alert("high_cpu", "cpu_usage_percent", "gt", 80, 300, "warning",
                          "High CPU usage")
        self.register_alert("high_memory", "memory_usage_mb", "gt", 7000, 300, "warning",
                          "High memory usage")
    
    def register_metric(
        self,
        name: str,
        type: MetricType,
        description: str,
        unit: str = ""
    ) -> Metric:
        """Register a new metric"""
        metric = Metric(
            name=name,
            type=type,
            description=description,
            unit=unit
        )
        
        with self._lock:
            self._metrics[name] = metric
        
        return metric
    
    def register_alert(
        self,
        id: str,
        metric_name: str,
        condition: str,
        threshold: float,
        duration_seconds: int,
        severity: str = "warning",
        message: str = ""
    ) -> Alert:
        """Register an alert"""
        alert = Alert(
            id=id,
            metric_name=metric_name,
            condition=condition,
            threshold=threshold,
            duration_seconds=duration_seconds,
            severity=severity,
            message=message
        )
        
        with self._lock:
            self._alerts[id] = alert
        
        return alert
    
    def record(self, metric_name: str, value: float, labels: Dict[str, str] = None):
        """Record a metric value"""
        metric = self._metrics.get(metric_name)
        if metric:
            metric.record(value, labels)
    
    def increment(self, metric_name: str, amount: float = 1.0, labels: Dict[str, str] = None):
        """Increment a counter metric"""
        metric = self._metrics.get(metric_name)
        if metric and metric.type == MetricType.COUNTER:
            metric.increment(amount, labels)
    
    def set_gauge(self, metric_name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric"""
        metric = self._metrics.get(metric_name)
        if metric and metric.type == MetricType.GAUGE:
            metric.set(value, labels)
    
    def get_metric(self, name: str) -> Optional[Metric]:
        """Get a metric by name"""
        return self._metrics.get(name)
    
    def list_metrics(self) -> List[Dict[str, Any]]:
        """List all metrics"""
        with self._lock:
            return [m.to_dict() for m in self._metrics.values()]
    
    def get_time_series(
        self,
        metric_name: str,
        window_seconds: int = 300,
        resolution_seconds: int = 10
    ) -> List[Dict[str, Any]]:
        """Get time-series data for a metric"""
        metric = self._metrics.get(metric_name)
        if not metric:
            return []
        
        cutoff = time.time() - window_seconds
        values = [v for v in metric.values if v.timestamp >= cutoff]
        
        # Bucket by resolution
        if resolution_seconds > 0:
            buckets = {}
            for v in values:
                bucket = int(v.timestamp / resolution_seconds) * resolution_seconds
                if bucket not in buckets:
                    buckets[bucket] = []
                buckets[bucket].append(v.value)
            
            return [
                {
                    "timestamp": bucket,
                    "datetime": datetime.fromtimestamp(bucket).isoformat(),
                    "value": statistics.mean(vals)
                }
                for bucket, vals in sorted(buckets.items())
            ]
        
        return [v.to_dict() for v in values]
    
    def check_alerts(self) -> List[Dict[str, Any]]:
        """Check all alerts and return triggered ones"""
        triggered = []
        
        with self._lock:
            for alert in self._alerts.values():
                metric = self._metrics.get(alert.metric_name)
                if metric and alert.check(metric):
                    triggered.append(alert.to_dict())
        
        return triggered
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive dashboard data"""
        uptime = time.time() - self._start_time
        
        # Key metrics
        key_metrics = {}
        for name in ["http_requests_total", "emails_processed", "active_sessions", "queue_size"]:
            metric = self._metrics.get(name)
            if metric:
                key_metrics[name] = {
                    "value": metric.get_latest() or 0,
                    "unit": metric.unit,
                    "description": metric.description
                }
        
        # Performance overview
        performance = {}
        for name in ["cpu_usage_percent", "memory_usage_mb", "http_request_duration_ms"]:
            metric = self._metrics.get(name)
            if metric:
                performance[name] = metric.get_stats(60)
        
        # Error rates
        error_metrics = {}
        for name in ["http_errors_total", "jobs_failed", "threats_detected"]:
            metric = self._metrics.get(name)
            if metric:
                error_metrics[name] = metric.get_latest() or 0
        
        # Cache stats
        hits = self._metrics.get("cache_hits")
        misses = self._metrics.get("cache_misses")
        hit_rate = 0
        if hits and misses:
            total = (hits.get_latest() or 0) + (misses.get_latest() or 0)
            if total > 0:
                hit_rate = ((hits.get_latest() or 0) / total) * 100
        
        # Active alerts
        triggered_alerts = self.check_alerts()
        
        return {
            "uptime_seconds": uptime,
            "uptime_human": str(timedelta(seconds=int(uptime))),
            "timestamp": datetime.now().isoformat(),
            "key_metrics": key_metrics,
            "performance": performance,
            "errors": error_metrics,
            "cache_hit_rate": hit_rate,
            "active_alerts": triggered_alerts,
            "total_metrics": len(self._metrics),
            "total_alerts": len(self._alerts)
        }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get metrics analytics"""
        with self._lock:
            total_data_points = sum(len(m.values) for m in self._metrics.values())
            
            # Metric types breakdown
            by_type = {}
            for m in self._metrics.values():
                by_type[m.type.value] = by_type.get(m.type.value, 0) + 1
            
            # Active alerts
            active_alerts = len([a for a in self._alerts.values() if a.triggered_at])
            
            return {
                "total_metrics": len(self._metrics),
                "total_data_points": total_data_points,
                "by_type": by_type,
                "total_alerts": len(self._alerts),
                "active_alerts": active_alerts,
                "uptime_seconds": time.time() - self._start_time
            }


# Global instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get global metrics collector"""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def record_metric(name: str, value: float, labels: Dict[str, str] = None):
    """Shorthand to record a metric"""
    get_metrics_collector().record(name, value, labels)


def increment_counter(name: str, amount: float = 1.0, labels: Dict[str, str] = None):
    """Shorthand to increment a counter"""
    get_metrics_collector().increment(name, amount, labels)


def set_gauge(name: str, value: float, labels: Dict[str, str] = None):
    """Shorthand to set a gauge"""
    get_metrics_collector().set_gauge(name, value, labels)
