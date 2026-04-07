"""
Smart Real-time Event Processing Engine for Email Triage Environment

Advanced event streaming providing:
- Complex event processing (CEP)
- Event sourcing and replay
- Real-time stream analytics
- Event correlation and pattern detection
"""

from typing import Any, Dict, List, Optional, Callable, Set
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import json
import time
import uuid
import asyncio


class EventType(str, Enum):
    """Event types in the system"""
    EMAIL_RECEIVED = "email_received"
    EMAIL_CATEGORIZED = "email_categorized"
    EMAIL_PRIORITIZED = "email_prioritized"
    EMAIL_REPLIED = "email_replied"
    EMAIL_FORWARDED = "email_forwarded"
    EMAIL_ARCHIVED = "email_archived"
    EMAIL_FLAGGED = "email_flagged"
    SPAM_DETECTED = "spam_detected"
    VIP_EMAIL = "vip_email"
    SLA_VIOLATION = "sla_violation"
    SYSTEM_ALERT = "system_alert"
    USER_ACTION = "user_action"
    MODEL_PREDICTION = "model_prediction"
    PERFORMANCE_METRIC = "performance_metric"


class EventSeverity(str, Enum):
    """Event severity levels"""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class Event:
    """Individual event in the system"""
    
    def __init__(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        severity: EventSeverity = EventSeverity.INFO,
        source: str = "system",
        correlation_id: Optional[str] = None
    ):
        self.id = str(uuid.uuid4())
        self.event_type = event_type
        self.payload = payload
        self.severity = severity
        self.source = source
        self.correlation_id = correlation_id or self.id
        self.timestamp = datetime.now()
        self.processed = False
        self.retry_count = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary"""
        return {
            "id": self.id,
            "type": self.event_type,
            "payload": self.payload,
            "severity": self.severity,
            "source": self.source,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp.isoformat(),
            "processed": self.processed,
            "retry_count": self.retry_count
        }


class EventPattern:
    """Complex event pattern definition"""
    
    def __init__(
        self,
        name: str,
        pattern_fn: Callable[[List[Event]], bool],
        window_seconds: int = 300,
        min_events: int = 2,
        max_events: int = 100
    ):
        self.name = name
        self.pattern_fn = pattern_fn
        self.window_seconds = window_seconds
        self.min_events = min_events
        self.max_events = max_events
        self.matches = 0
        self.last_match = None


class EventProcessor:
    """Real-time event processing engine"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.event_store = deque(maxlen=50000)
        self.event_handlers: Dict[EventType, List[Callable]] = defaultdict(list)
        self.event_patterns: List[EventPattern] = []
        self.subscribers: Dict[str, Callable] = {}
        self.metrics = {
            "events_processed": 0,
            "events_per_second": 0.0,
            "pattern_matches": 0,
            "processing_errors": 0
        }
        self.recent_events = deque(maxlen=1000)
        self.event_counts: Dict[EventType, int] = defaultdict(int)
        self.processing_times = deque(maxlen=1000)
        
        # Register default patterns
        self._register_default_patterns()
    
    def _register_default_patterns(self):
        """Register default event patterns"""
        
        # Spam wave detection
        def spam_wave_pattern(events: List[Event]) -> bool:
            spam_events = [e for e in events if e.event_type == EventType.SPAM_DETECTED]
            return len(spam_events) >= 5
        
        self.register_pattern(
            "spam_wave",
            spam_wave_pattern,
            window_seconds=60,
            min_events=5
        )
        
        # VIP email rush
        def vip_rush_pattern(events: List[Event]) -> bool:
            vip_events = [e for e in events if e.event_type == EventType.VIP_EMAIL]
            return len(vip_events) >= 3
        
        self.register_pattern(
            "vip_rush",
            vip_rush_pattern,
            window_seconds=300,
            min_events=3
        )
        
        # SLA violation cascade
        def sla_cascade_pattern(events: List[Event]) -> bool:
            sla_events = [e for e in events if e.event_type == EventType.SLA_VIOLATION]
            return len(sla_events) >= 2
        
        self.register_pattern(
            "sla_cascade",
            sla_cascade_pattern,
            window_seconds=180,
            min_events=2
        )
        
        # System overload
        def system_overload_pattern(events: List[Event]) -> bool:
            alert_events = [e for e in events if e.event_type == EventType.SYSTEM_ALERT]
            error_events = [e for e in events if e.severity == EventSeverity.ERROR]
            return len(alert_events) >= 3 or len(error_events) >= 10
        
        self.register_pattern(
            "system_overload", 
            system_overload_pattern,
            window_seconds=120,
            min_events=3
        )
    
    def register_handler(self, event_type: EventType, handler: Callable[[Event], None]):
        """Register an event handler"""
        with self._lock:
            self.event_handlers[event_type].append(handler)
    
    def register_pattern(
        self,
        name: str,
        pattern_fn: Callable[[List[Event]], bool],
        window_seconds: int = 300,
        min_events: int = 2
    ):
        """Register an event pattern"""
        with self._lock:
            pattern = EventPattern(name, pattern_fn, window_seconds, min_events)
            self.event_patterns.append(pattern)
    
    def subscribe(self, subscriber_id: str, callback: Callable[[Event], None]):
        """Subscribe to all events"""
        with self._lock:
            self.subscribers[subscriber_id] = callback
    
    def unsubscribe(self, subscriber_id: str):
        """Unsubscribe from events"""
        with self._lock:
            if subscriber_id in self.subscribers:
                del self.subscribers[subscriber_id]
    
    def emit(
        self,
        event_type: EventType,
        payload: Dict[str, Any],
        severity: EventSeverity = EventSeverity.INFO,
        source: str = "system",
        correlation_id: Optional[str] = None
    ) -> Event:
        """Emit a new event"""
        start_time = time.time()
        
        try:
            event = Event(event_type, payload, severity, source, correlation_id)
            
            with self._lock:
                # Store event
                self.event_store.append(event)
                self.recent_events.append(event)
                self.event_counts[event_type] += 1
                self.metrics["events_processed"] += 1
                
                # Process handlers
                for handler in self.event_handlers[event_type]:
                    try:
                        handler(event)
                    except Exception as e:
                        self.metrics["processing_errors"] += 1
                        print(f"Event handler error: {e}")
                
                # Notify subscribers
                for callback in self.subscribers.values():
                    try:
                        callback(event)
                    except Exception as e:
                        self.metrics["processing_errors"] += 1
                        print(f"Subscriber error: {e}")
                
                # Check patterns
                self._check_patterns()
            
            # Record processing time
            processing_time = (time.time() - start_time) * 1000
            self.processing_times.append(processing_time)
            
            event.processed = True
            return event
            
        except Exception as e:
            self.metrics["processing_errors"] += 1
            raise
    
    def _check_patterns(self):
        """Check for event patterns"""
        current_time = datetime.now()
        
        for pattern in self.event_patterns:
            # Get events in time window
            window_start = current_time - timedelta(seconds=pattern.window_seconds)
            window_events = [
                e for e in self.recent_events 
                if e.timestamp >= window_start
            ]
            
            if len(window_events) >= pattern.min_events:
                try:
                    if pattern.pattern_fn(window_events):
                        pattern.matches += 1
                        pattern.last_match = current_time
                        self.metrics["pattern_matches"] += 1
                        
                        # Emit pattern match event
                        self.emit(
                            EventType.SYSTEM_ALERT,
                            {
                                "pattern": pattern.name,
                                "matches": pattern.matches,
                                "events_in_window": len(window_events)
                            },
                            EventSeverity.WARNING,
                            "event_processor"
                        )
                except Exception as e:
                    print(f"Pattern check error for {pattern.name}: {e}")
    
    def get_events(
        self,
        event_type: Optional[EventType] = None,
        severity: Optional[EventSeverity] = None,
        source: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Query events with filters"""
        with self._lock:
            events = list(self.recent_events)
            
            # Apply filters
            if event_type:
                events = [e for e in events if e.event_type == event_type]
            if severity:
                events = [e for e in events if e.severity == severity]
            if source:
                events = [e for e in events if e.source == source]
            if since:
                events = [e for e in events if e.timestamp >= since]
            
            # Sort by timestamp and limit
            events = sorted(events, key=lambda e: e.timestamp, reverse=True)[:limit]
            return [e.to_dict() for e in events]
    
    def get_event_stream(self, filters: Optional[Dict] = None) -> List[Dict[str, Any]]:
        """Get real-time event stream"""
        return self.get_events(limit=50)
    
    def replay_events(
        self,
        from_time: datetime,
        to_time: datetime,
        event_types: Optional[List[EventType]] = None
    ) -> List[Dict[str, Any]]:
        """Replay events from a time range"""
        with self._lock:
            events = [
                e for e in self.event_store
                if from_time <= e.timestamp <= to_time
            ]
            
            if event_types:
                events = [e for e in events if e.event_type in event_types]
            
            return [e.to_dict() for e in sorted(events, key=lambda e: e.timestamp)]
    
    def get_pattern_stats(self) -> List[Dict[str, Any]]:
        """Get pattern matching statistics"""
        with self._lock:
            return [
                {
                    "name": p.name,
                    "matches": p.matches,
                    "last_match": p.last_match.isoformat() if p.last_match else None,
                    "window_seconds": p.window_seconds,
                    "min_events": p.min_events
                }
                for p in self.event_patterns
            ]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        with self._lock:
            # Calculate events per second
            recent_events_count = len([
                e for e in self.recent_events
                if e.timestamp > datetime.now() - timedelta(minutes=1)
            ])
            events_per_second = recent_events_count / 60.0
            
            # Calculate average processing time
            avg_processing_time = (
                sum(self.processing_times) / len(self.processing_times)
                if self.processing_times else 0
            )
            
            return {
                **self.metrics,
                "events_per_second": round(events_per_second, 2),
                "avg_processing_time_ms": round(avg_processing_time, 2),
                "event_types": len(self.event_counts),
                "active_patterns": len(self.event_patterns),
                "subscribers": len(self.subscribers),
                "event_distribution": dict(self.event_counts)
            }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get comprehensive analytics"""
        stats = self.get_stats()
        patterns = self.get_pattern_stats()
        
        return {
            "status": "active",
            "events_processed": stats["events_processed"],
            "events_per_second": stats["events_per_second"],
            "pattern_matches": stats["pattern_matches"],
            "processing_errors": stats["processing_errors"],
            "features": [
                "complex_event_processing",
                "event_sourcing", 
                "stream_analytics",
                "pattern_detection",
                "real_time_processing",
                "event_replay",
                "subscriber_notifications"
            ],
            "patterns": patterns,
            "statistics": stats
        }


# Global instance
_event_processor: Optional[EventProcessor] = None
_processor_lock = threading.Lock()


def get_event_processor() -> EventProcessor:
    """Get or create event processor instance"""
    global _event_processor
    with _processor_lock:
        if _event_processor is None:
            _event_processor = EventProcessor()
        return _event_processor