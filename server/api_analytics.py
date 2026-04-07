"""
API Usage Analytics System for Email Triage Environment

Advanced request/response analytics providing:
- Per-endpoint usage tracking
- Response time percentiles (p50, p95, p99)
- Error rate analysis
- Traffic patterns and anomaly detection
- API consumer insights
"""

from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timedelta
from collections import deque, defaultdict
import statistics
import threading
import hashlib
import json


class EndpointStats:
    """Statistics for a single endpoint"""
    
    def __init__(self, endpoint: str, method: str):
        self.endpoint = endpoint
        self.method = method
        self.request_count = 0
        self.error_count = 0
        self.response_times: deque = deque(maxlen=1000)
        self.status_codes: Dict[int, int] = defaultdict(int)
        self.hourly_requests: Dict[int, int] = defaultdict(int)
        self.first_seen: datetime = datetime.now()
        self.last_seen: datetime = datetime.now()
    
    def record(self, duration_ms: float, status_code: int):
        """Record a request"""
        self.request_count += 1
        self.last_seen = datetime.now()
        self.response_times.append(duration_ms)
        self.status_codes[status_code] += 1
        self.hourly_requests[datetime.now().hour] += 1
        
        if status_code >= 400:
            self.error_count += 1
    
    def get_percentiles(self) -> Dict[str, float]:
        """Calculate response time percentiles"""
        if not self.response_times:
            return {"p50": 0, "p75": 0, "p95": 0, "p99": 0}
        
        times = sorted(self.response_times)
        n = len(times)
        
        return {
            "p50": times[int(n * 0.50)] if n > 0 else 0,
            "p75": times[int(n * 0.75)] if n > 0 else 0,
            "p95": times[int(n * 0.95)] if n > 0 else 0,
            "p99": times[int(n * 0.99)] if n > 0 else 0,
            "min": min(times) if times else 0,
            "max": max(times) if times else 0,
            "avg": statistics.mean(times) if times else 0
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        percentiles = self.get_percentiles()
        return {
            "endpoint": self.endpoint,
            "method": self.method,
            "request_count": self.request_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / self.request_count if self.request_count > 0 else 0,
            "response_times": percentiles,
            "status_codes": dict(self.status_codes),
            "peak_hour": max(self.hourly_requests.keys(), key=lambda h: self.hourly_requests[h]) if self.hourly_requests else None,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat()
        }


class ConsumerStats:
    """Statistics for an API consumer (by IP or API key)"""
    
    def __init__(self, consumer_id: str):
        self.consumer_id = consumer_id
        self.request_count = 0
        self.endpoints_used: Set[str] = set()
        self.first_seen: datetime = datetime.now()
        self.last_seen: datetime = datetime.now()
        self.error_count = 0
        self.daily_requests: Dict[str, int] = defaultdict(int)
    
    def record(self, endpoint: str, is_error: bool = False):
        """Record a request from this consumer"""
        self.request_count += 1
        self.last_seen = datetime.now()
        self.endpoints_used.add(endpoint)
        self.daily_requests[datetime.now().strftime("%Y-%m-%d")] += 1
        
        if is_error:
            self.error_count += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "consumer_id": self.consumer_id[:16] + "...",  # Truncate for privacy
            "request_count": self.request_count,
            "endpoints_used": len(self.endpoints_used),
            "error_count": self.error_count,
            "error_rate": self.error_count / self.request_count if self.request_count > 0 else 0,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "daily_average": self.request_count / max(len(self.daily_requests), 1)
        }


class TrafficAnalyzer:
    """Analyze traffic patterns and detect anomalies"""
    
    def __init__(self):
        self.hourly_traffic: Dict[int, int] = defaultdict(int)
        self.daily_traffic: Dict[str, int] = defaultdict(int)
        self.minute_traffic: deque = deque(maxlen=60)  # Last 60 minutes
        self.baseline_rpm = 100  # Requests per minute baseline
        self.anomaly_threshold = 2.0  # Multiplier for anomaly detection
    
    def record(self):
        """Record a request for traffic analysis"""
        now = datetime.now()
        self.hourly_traffic[now.hour] += 1
        self.daily_traffic[now.strftime("%Y-%m-%d")] += 1
        
        # Update minute traffic
        minute_key = now.strftime("%H:%M")
        if not self.minute_traffic or self.minute_traffic[-1]["minute"] != minute_key:
            self.minute_traffic.append({"minute": minute_key, "count": 1})
        else:
            self.minute_traffic[-1]["count"] += 1
    
    def detect_anomalies(self) -> List[Dict[str, Any]]:
        """Detect traffic anomalies"""
        anomalies = []
        
        if len(self.minute_traffic) < 5:
            return anomalies
        
        # Calculate recent average
        recent = list(self.minute_traffic)[-10:]
        avg = statistics.mean([m["count"] for m in recent])
        
        # Check for spikes
        current = recent[-1]["count"] if recent else 0
        if current > avg * self.anomaly_threshold:
            anomalies.append({
                "type": "spike",
                "current": current,
                "average": avg,
                "ratio": current / avg if avg > 0 else 0,
                "timestamp": datetime.now().isoformat()
            })
        
        # Check for drops
        if current < avg / self.anomaly_threshold and avg > 10:
            anomalies.append({
                "type": "drop",
                "current": current,
                "average": avg,
                "ratio": current / avg if avg > 0 else 0,
                "timestamp": datetime.now().isoformat()
            })
        
        return anomalies
    
    def get_patterns(self) -> Dict[str, Any]:
        """Get traffic patterns"""
        return {
            "hourly_distribution": dict(self.hourly_traffic),
            "daily_traffic": dict(list(self.daily_traffic.items())[-7:]),  # Last 7 days
            "peak_hour": max(self.hourly_traffic.keys(), key=lambda h: self.hourly_traffic[h]) if self.hourly_traffic else None,
            "current_rpm": self.minute_traffic[-1]["count"] if self.minute_traffic else 0,
            "anomalies": self.detect_anomalies()
        }


class APIUsageAnalytics:
    """Main API usage analytics system"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.endpoints: Dict[str, EndpointStats] = {}
        self.consumers: Dict[str, ConsumerStats] = {}
        self.traffic = TrafficAnalyzer()
        self.request_log = deque(maxlen=10000)
        self.error_log = deque(maxlen=1000)
        self.start_time = datetime.now()
        
        # Summary stats
        self.total_requests = 0
        self.total_errors = 0
        self.total_response_time = 0.0
    
    def record_request(
        self,
        endpoint: str,
        method: str,
        status_code: int,
        duration_ms: float,
        consumer_id: Optional[str] = None,
        request_size: int = 0,
        response_size: int = 0
    ):
        """Record an API request"""
        with self._lock:
            # Generate endpoint key
            endpoint_key = f"{method}:{endpoint}"
            
            # Update endpoint stats
            if endpoint_key not in self.endpoints:
                self.endpoints[endpoint_key] = EndpointStats(endpoint, method)
            self.endpoints[endpoint_key].record(duration_ms, status_code)
            
            # Update consumer stats
            if consumer_id:
                consumer_hash = hashlib.sha256(consumer_id.encode()).hexdigest()[:16]
                if consumer_hash not in self.consumers:
                    self.consumers[consumer_hash] = ConsumerStats(consumer_hash)
                self.consumers[consumer_hash].record(endpoint, status_code >= 400)
            
            # Update traffic
            self.traffic.record()
            
            # Update totals
            self.total_requests += 1
            self.total_response_time += duration_ms
            if status_code >= 400:
                self.total_errors += 1
            
            # Log request
            request_entry = {
                "endpoint": endpoint,
                "method": method,
                "status_code": status_code,
                "duration_ms": round(duration_ms, 2),
                "request_size": request_size,
                "response_size": response_size,
                "timestamp": datetime.now().isoformat()
            }
            self.request_log.append(request_entry)
            
            if status_code >= 400:
                self.error_log.append(request_entry)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get usage summary"""
        with self._lock:
            uptime = (datetime.now() - self.start_time).total_seconds()
            
            return {
                "total_requests": self.total_requests,
                "total_errors": self.total_errors,
                "error_rate": self.total_errors / self.total_requests if self.total_requests > 0 else 0,
                "average_response_time_ms": self.total_response_time / self.total_requests if self.total_requests > 0 else 0,
                "requests_per_second": self.total_requests / uptime if uptime > 0 else 0,
                "unique_endpoints": len(self.endpoints),
                "unique_consumers": len(self.consumers),
                "uptime_seconds": uptime,
                "start_time": self.start_time.isoformat()
            }
    
    def get_endpoint_stats(self, endpoint: Optional[str] = None) -> Dict[str, Any]:
        """Get endpoint statistics"""
        with self._lock:
            if endpoint:
                return self.endpoints.get(endpoint, EndpointStats(endpoint, "GET")).to_dict()
            
            return {
                "endpoints": [e.to_dict() for e in self.endpoints.values()],
                "total_endpoints": len(self.endpoints),
                "most_used": max(
                    self.endpoints.values(), 
                    key=lambda e: e.request_count
                ).to_dict() if self.endpoints else None,
                "slowest": max(
                    self.endpoints.values(),
                    key=lambda e: e.get_percentiles()["p95"]
                ).to_dict() if self.endpoints else None,
                "highest_error_rate": max(
                    self.endpoints.values(),
                    key=lambda e: e.error_count / e.request_count if e.request_count > 0 else 0
                ).to_dict() if self.endpoints else None
            }
    
    def get_consumer_stats(self, limit: int = 20) -> Dict[str, Any]:
        """Get consumer statistics"""
        with self._lock:
            sorted_consumers = sorted(
                self.consumers.values(),
                key=lambda c: c.request_count,
                reverse=True
            )[:limit]
            
            return {
                "consumers": [c.to_dict() for c in sorted_consumers],
                "total_consumers": len(self.consumers),
                "top_consumer_requests": sorted_consumers[0].request_count if sorted_consumers else 0
            }
    
    def get_traffic_patterns(self) -> Dict[str, Any]:
        """Get traffic patterns"""
        with self._lock:
            return self.traffic.get_patterns()
    
    def get_error_analysis(self) -> Dict[str, Any]:
        """Analyze errors"""
        with self._lock:
            error_by_endpoint: Dict[str, int] = defaultdict(int)
            error_by_status: Dict[int, int] = defaultdict(int)
            
            for error in self.error_log:
                error_by_endpoint[error["endpoint"]] += 1
                error_by_status[error["status_code"]] += 1
            
            return {
                "total_errors": self.total_errors,
                "error_rate": self.total_errors / self.total_requests if self.total_requests > 0 else 0,
                "by_endpoint": dict(error_by_endpoint),
                "by_status_code": dict(error_by_status),
                "recent_errors": list(self.error_log)[-10:]
            }
    
    def get_recent_requests(self, limit: int = 100) -> List[Dict]:
        """Get recent requests"""
        with self._lock:
            return list(self.request_log)[-limit:]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get comprehensive analytics"""
        summary = self.get_summary()
        return {
            "status": "active",
            "total_requests": summary["total_requests"],
            "error_rate": round(summary["error_rate"] * 100, 2),
            "avg_response_time_ms": round(summary["average_response_time_ms"], 2),
            "unique_endpoints": summary["unique_endpoints"],
            "unique_consumers": summary["unique_consumers"],
            "features": [
                "endpoint_tracking",
                "consumer_analytics",
                "traffic_patterns",
                "anomaly_detection",
                "error_analysis",
                "percentile_calculation",
                "request_logging"
            ],
            "summary": summary,
            "traffic": self.get_traffic_patterns()
        }
    
    def reset(self):
        """Reset all statistics"""
        with self._lock:
            self.endpoints.clear()
            self.consumers.clear()
            self.traffic = TrafficAnalyzer()
            self.request_log.clear()
            self.error_log.clear()
            self.total_requests = 0
            self.total_errors = 0
            self.total_response_time = 0.0
            self.start_time = datetime.now()


# Global instance
_api_analytics: Optional[APIUsageAnalytics] = None
_analytics_lock = threading.Lock()


def get_api_analytics() -> APIUsageAnalytics:
    """Get or create API analytics instance"""
    global _api_analytics
    with _analytics_lock:
        if _api_analytics is None:
            _api_analytics = APIUsageAnalytics()
        return _api_analytics
