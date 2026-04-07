"""
Request/Response Audit Logging System
Comprehensive audit trail with compliance and forensics support
"""
import json
import time
import hashlib
import threading
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from collections import deque
from enum import Enum
import gzip
import base64

logger = logging.getLogger("audit")


class AuditEventType(Enum):
    """Types of audit events"""
    REQUEST = "request"
    RESPONSE = "response"
    ACTION = "action"
    ERROR = "error"
    SECURITY = "security"
    CONFIG_CHANGE = "config_change"
    STATE_CHANGE = "state_change"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    SYSTEM = "system"


class AuditSeverity(Enum):
    """Audit event severity"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Single audit event"""
    id: str
    timestamp: float
    event_type: AuditEventType
    severity: AuditSeverity
    actor: str  # User or system identifier
    action: str
    resource: str
    details: Dict[str, Any]
    request_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    duration_ms: Optional[float] = None
    status_code: Optional[int] = None
    success: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "event_type": self.event_type.value,
            "severity": self.severity.value,
            "actor": self.actor,
            "action": self.action,
            "resource": self.resource,
            "details": self.details,
            "request_id": self.request_id,
            "session_id": self.session_id,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "duration_ms": self.duration_ms,
            "status_code": self.status_code,
            "success": self.success,
            "metadata": self.metadata
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)
    
    def hash(self) -> str:
        """Generate hash of audit event for integrity"""
        content = f"{self.id}{self.timestamp}{self.event_type.value}{self.action}{self.resource}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]


@dataclass
class AuditSession:
    """Audit session tracking"""
    session_id: str
    actor: str
    start_time: float
    end_time: Optional[float] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    event_count: int = 0
    actions: List[str] = field(default_factory=list)
    
    @property
    def duration_seconds(self) -> Optional[float]:
        if self.end_time:
            return self.end_time - self.start_time
        return time.time() - self.start_time
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "actor": self.actor,
            "start_time": datetime.fromtimestamp(self.start_time).isoformat(),
            "end_time": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
            "event_count": self.event_count,
            "actions": self.actions[-20:]  # Last 20 actions
        }


class AuditFilter:
    """Filter for querying audit events"""
    
    def __init__(
        self,
        event_types: List[AuditEventType] = None,
        severity: AuditSeverity = None,
        actor: str = None,
        resource: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        success: bool = None,
        request_id: str = None
    ):
        self.event_types = event_types
        self.severity = severity
        self.actor = actor
        self.resource = resource
        self.start_time = start_time.timestamp() if start_time else None
        self.end_time = end_time.timestamp() if end_time else None
        self.success = success
        self.request_id = request_id
    
    def matches(self, event: AuditEvent) -> bool:
        """Check if event matches filter"""
        if self.event_types and event.event_type not in self.event_types:
            return False
        
        if self.severity and event.severity != self.severity:
            return False
        
        if self.actor and event.actor != self.actor:
            return False
        
        if self.resource and self.resource not in event.resource:
            return False
        
        if self.start_time and event.timestamp < self.start_time:
            return False
        
        if self.end_time and event.timestamp > self.end_time:
            return False
        
        if self.success is not None and event.success != self.success:
            return False
        
        if self.request_id and event.request_id != self.request_id:
            return False
        
        return True


class AuditExporter:
    """Export audit logs to various formats"""
    
    @staticmethod
    def to_json(events: List[AuditEvent], pretty: bool = False) -> str:
        """Export to JSON"""
        data = [e.to_dict() for e in events]
        if pretty:
            return json.dumps(data, indent=2, default=str)
        return json.dumps(data, default=str)
    
    @staticmethod
    def to_csv(events: List[AuditEvent]) -> str:
        """Export to CSV"""
        if not events:
            return ""
        
        headers = ["id", "timestamp", "event_type", "severity", "actor", "action", "resource", "success"]
        lines = [",".join(headers)]
        
        for e in events:
            row = [
                e.id,
                datetime.fromtimestamp(e.timestamp).isoformat(),
                e.event_type.value,
                e.severity.value,
                e.actor,
                e.action,
                e.resource,
                str(e.success)
            ]
            lines.append(",".join(f'"{v}"' for v in row))
        
        return "\n".join(lines)
    
    @staticmethod
    def to_syslog(event: AuditEvent) -> str:
        """Format for syslog"""
        severity_map = {
            AuditSeverity.DEBUG: "debug",
            AuditSeverity.INFO: "info",
            AuditSeverity.WARNING: "warning",
            AuditSeverity.ERROR: "err",
            AuditSeverity.CRITICAL: "crit"
        }
        
        return f"<{severity_map.get(event.severity, 'info')}> {event.timestamp:.0f} " \
               f"{event.event_type.value} {event.actor} {event.action} {event.resource} " \
               f"success={event.success}"
    
    @staticmethod
    def compress(data: str) -> str:
        """Compress audit data"""
        compressed = gzip.compress(data.encode())
        return base64.b64encode(compressed).decode()
    
    @staticmethod
    def decompress(data: str) -> str:
        """Decompress audit data"""
        compressed = base64.b64decode(data.encode())
        return gzip.decompress(compressed).decode()


class AuditLogger:
    """
    Production Audit Logging System
    
    Features:
    - Request/response logging
    - Session tracking
    - Compliance support
    - Forensic analysis
    - Export capabilities
    - Query and filtering
    """
    
    def __init__(self, max_events: int = 10000, retention_hours: int = 168):
        self._events: deque = deque(maxlen=max_events)
        self._sessions: Dict[str, AuditSession] = {}
        self._retention_hours = retention_hours
        self._event_counter = 0
        self._lock = threading.RLock()
        self._handlers: List[Callable] = []
        
        # Statistics
        self._stats = {
            "total_events": 0,
            "by_type": {},
            "by_severity": {},
            "by_actor": {},
            "errors": 0,
            "security_events": 0
        }
        
        logger.info(f"Audit Logger initialized (max={max_events}, retention={retention_hours}h)")
    
    def _generate_id(self) -> str:
        """Generate unique event ID"""
        self._event_counter += 1
        return f"audit_{int(time.time())}_{self._event_counter:06d}"
    
    def log(
        self,
        event_type: AuditEventType,
        severity: AuditSeverity,
        actor: str,
        action: str,
        resource: str,
        details: Dict[str, Any] = None,
        request_id: str = None,
        session_id: str = None,
        ip_address: str = None,
        user_agent: str = None,
        duration_ms: float = None,
        status_code: int = None,
        success: bool = True,
        metadata: Dict[str, Any] = None
    ) -> AuditEvent:
        """Log an audit event"""
        event = AuditEvent(
            id=self._generate_id(),
            timestamp=time.time(),
            event_type=event_type,
            severity=severity,
            actor=actor,
            action=action,
            resource=resource,
            details=details or {},
            request_id=request_id,
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent,
            duration_ms=duration_ms,
            status_code=status_code,
            success=success,
            metadata=metadata or {}
        )
        
        with self._lock:
            self._events.append(event)
            
            # Update stats
            self._stats["total_events"] += 1
            self._stats["by_type"][event_type.value] = \
                self._stats["by_type"].get(event_type.value, 0) + 1
            self._stats["by_severity"][severity.value] = \
                self._stats["by_severity"].get(severity.value, 0) + 1
            self._stats["by_actor"][actor] = \
                self._stats["by_actor"].get(actor, 0) + 1
            
            if event_type == AuditEventType.ERROR:
                self._stats["errors"] += 1
            if event_type == AuditEventType.SECURITY:
                self._stats["security_events"] += 1
            
            # Update session
            if session_id and session_id in self._sessions:
                session = self._sessions[session_id]
                session.event_count += 1
                session.actions.append(action)
        
        # Notify handlers
        for handler in self._handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Audit handler error: {e}")
        
        return event
    
    def log_request(
        self,
        method: str,
        path: str,
        actor: str = "anonymous",
        request_id: str = None,
        ip_address: str = None,
        user_agent: str = None,
        body: Dict[str, Any] = None
    ) -> AuditEvent:
        """Log an incoming request"""
        return self.log(
            event_type=AuditEventType.REQUEST,
            severity=AuditSeverity.INFO,
            actor=actor,
            action=f"{method} {path}",
            resource=path,
            details={"method": method, "body": body or {}},
            request_id=request_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
    
    def log_response(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        actor: str = "anonymous",
        request_id: str = None,
        response_size: int = None
    ) -> AuditEvent:
        """Log an outgoing response"""
        success = 200 <= status_code < 400
        severity = AuditSeverity.INFO if success else AuditSeverity.WARNING
        
        if status_code >= 500:
            severity = AuditSeverity.ERROR
        
        return self.log(
            event_type=AuditEventType.RESPONSE,
            severity=severity,
            actor=actor,
            action=f"{method} {path} -> {status_code}",
            resource=path,
            details={"response_size": response_size} if response_size else {},
            request_id=request_id,
            duration_ms=duration_ms,
            status_code=status_code,
            success=success
        )
    
    def log_action(
        self,
        action: str,
        resource: str,
        actor: str,
        details: Dict[str, Any] = None,
        success: bool = True
    ) -> AuditEvent:
        """Log a business action"""
        return self.log(
            event_type=AuditEventType.ACTION,
            severity=AuditSeverity.INFO,
            actor=actor,
            action=action,
            resource=resource,
            details=details or {},
            success=success
        )
    
    def log_security_event(
        self,
        action: str,
        resource: str,
        actor: str,
        severity: AuditSeverity = AuditSeverity.WARNING,
        details: Dict[str, Any] = None,
        ip_address: str = None
    ) -> AuditEvent:
        """Log a security-related event"""
        return self.log(
            event_type=AuditEventType.SECURITY,
            severity=severity,
            actor=actor,
            action=action,
            resource=resource,
            details=details or {},
            ip_address=ip_address
        )
    
    def start_session(
        self,
        session_id: str,
        actor: str,
        ip_address: str = None,
        user_agent: str = None
    ) -> AuditSession:
        """Start an audit session"""
        session = AuditSession(
            session_id=session_id,
            actor=actor,
            start_time=time.time(),
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        with self._lock:
            self._sessions[session_id] = session
        
        self.log(
            event_type=AuditEventType.AUTHENTICATION,
            severity=AuditSeverity.INFO,
            actor=actor,
            action="session_start",
            resource=f"session/{session_id}",
            session_id=session_id,
            ip_address=ip_address,
            user_agent=user_agent
        )
        
        return session
    
    def end_session(self, session_id: str):
        """End an audit session"""
        with self._lock:
            session = self._sessions.get(session_id)
            if session:
                session.end_time = time.time()
                
                self.log(
                    event_type=AuditEventType.AUTHENTICATION,
                    severity=AuditSeverity.INFO,
                    actor=session.actor,
                    action="session_end",
                    resource=f"session/{session_id}",
                    session_id=session_id,
                    details={"duration_seconds": session.duration_seconds}
                )
    
    def query(
        self,
        filter: AuditFilter = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[AuditEvent]:
        """Query audit events"""
        with self._lock:
            events = list(self._events)
        
        # Apply filter
        if filter:
            events = [e for e in events if filter.matches(e)]
        
        # Sort by timestamp descending
        events.sort(key=lambda e: e.timestamp, reverse=True)
        
        # Apply pagination
        return events[offset:offset + limit]
    
    def get_event(self, event_id: str) -> Optional[AuditEvent]:
        """Get event by ID"""
        with self._lock:
            for event in self._events:
                if event.id == event_id:
                    return event
        return None
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session info"""
        session = self._sessions.get(session_id)
        if session:
            return session.to_dict()
        return None
    
    def get_request_trail(self, request_id: str) -> List[Dict[str, Any]]:
        """Get all events for a request"""
        with self._lock:
            events = [e for e in self._events if e.request_id == request_id]
        
        events.sort(key=lambda e: e.timestamp)
        return [e.to_dict() for e in events]
    
    def export(
        self,
        format: str = "json",
        filter: AuditFilter = None,
        limit: int = 1000
    ) -> str:
        """Export audit logs"""
        events = self.query(filter, limit)
        
        if format == "csv":
            return AuditExporter.to_csv(events)
        elif format == "compressed":
            json_data = AuditExporter.to_json(events)
            return AuditExporter.compress(json_data)
        else:
            return AuditExporter.to_json(events, pretty=True)
    
    def add_handler(self, handler: Callable[[AuditEvent], None]):
        """Add event handler"""
        self._handlers.append(handler)
    
    def cleanup_old_events(self):
        """Remove events older than retention period"""
        cutoff = time.time() - (self._retention_hours * 3600)
        
        with self._lock:
            # Filter to keep recent events
            self._events = deque(
                [e for e in self._events if e.timestamp > cutoff],
                maxlen=self._events.maxlen
            )
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get audit analytics"""
        with self._lock:
            events = list(self._events)
            sessions = list(self._sessions.values())
        
        if not events:
            return {
                "total_events": 0,
                "active_sessions": len([s for s in sessions if not s.end_time]),
                "stats": self._stats
            }
        
        # Time range
        timestamps = [e.timestamp for e in events]
        
        # Response times
        response_events = [e for e in events if e.event_type == AuditEventType.RESPONSE and e.duration_ms]
        avg_response_time = sum(e.duration_ms for e in response_events) / len(response_events) if response_events else 0
        
        # Error rate
        error_count = sum(1 for e in events if not e.success)
        error_rate = (error_count / len(events) * 100) if events else 0
        
        # Top actors
        actor_counts = {}
        for e in events:
            actor_counts[e.actor] = actor_counts.get(e.actor, 0) + 1
        top_actors = sorted(actor_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Top resources
        resource_counts = {}
        for e in events:
            resource_counts[e.resource] = resource_counts.get(e.resource, 0) + 1
        top_resources = sorted(resource_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        # Recent security events
        security_events = [e for e in events if e.event_type == AuditEventType.SECURITY][-10:]
        
        return {
            "total_events": len(events),
            "time_range": {
                "start": datetime.fromtimestamp(min(timestamps)).isoformat(),
                "end": datetime.fromtimestamp(max(timestamps)).isoformat()
            },
            "active_sessions": len([s for s in sessions if not s.end_time]),
            "total_sessions": len(sessions),
            "avg_response_time_ms": avg_response_time,
            "error_rate": error_rate,
            "by_type": self._stats["by_type"],
            "by_severity": self._stats["by_severity"],
            "top_actors": dict(top_actors),
            "top_resources": dict(top_resources),
            "recent_security_events": [e.to_dict() for e in security_events],
            "security_event_count": len(security_events)
        }


# Global instance
_audit_logger: Optional[AuditLogger] = None


def get_audit_logger() -> AuditLogger:
    """Get global audit logger"""
    global _audit_logger
    if _audit_logger is None:
        _audit_logger = AuditLogger()
    return _audit_logger
