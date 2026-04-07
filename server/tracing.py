"""
OpenTelemetry Distributed Tracing
Full observability with spans, context propagation, and metrics
"""
import time
import uuid
import threading
import logging
import functools
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from collections import deque
from enum import Enum
from contextlib import contextmanager

logger = logging.getLogger("tracing")


class SpanKind(Enum):
    """Types of spans"""
    INTERNAL = "internal"
    SERVER = "server"
    CLIENT = "client"
    PRODUCER = "producer"
    CONSUMER = "consumer"


class SpanStatus(Enum):
    """Span execution status"""
    UNSET = "unset"
    OK = "ok"
    ERROR = "error"


@dataclass
class SpanEvent:
    """Event within a span"""
    name: str
    timestamp: float
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Span:
    """Distributed tracing span"""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    name: str
    kind: SpanKind
    start_time: float
    end_time: Optional[float] = None
    status: SpanStatus = SpanStatus.UNSET
    attributes: Dict[str, Any] = field(default_factory=dict)
    events: List[SpanEvent] = field(default_factory=list)
    links: List[str] = field(default_factory=list)
    
    @property
    def duration_ms(self) -> Optional[float]:
        if self.end_time:
            return (self.end_time - self.start_time) * 1000
        return None
    
    def add_event(self, name: str, attributes: Dict[str, Any] = None):
        """Add event to span"""
        self.events.append(SpanEvent(
            name=name,
            timestamp=time.time(),
            attributes=attributes or {}
        ))
    
    def set_attribute(self, key: str, value: Any):
        """Set span attribute"""
        self.attributes[key] = value
    
    def set_status(self, status: SpanStatus, description: str = None):
        """Set span status"""
        self.status = status
        if description:
            self.attributes["status_description"] = description
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "kind": self.kind.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "status": self.status.value,
            "attributes": self.attributes,
            "events": [
                {"name": e.name, "timestamp": e.timestamp, "attributes": e.attributes}
                for e in self.events
            ],
            "links": self.links
        }


@dataclass
class Trace:
    """Complete distributed trace"""
    trace_id: str
    spans: List[Span] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    
    @property
    def duration_ms(self) -> float:
        if not self.spans:
            return 0
        end_times = [s.end_time for s in self.spans if s.end_time]
        if end_times:
            return (max(end_times) - self.start_time) * 1000
        return 0
    
    @property
    def root_span(self) -> Optional[Span]:
        for span in self.spans:
            if span.parent_span_id is None:
                return span
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "trace_id": self.trace_id,
            "start_time": self.start_time,
            "duration_ms": self.duration_ms,
            "span_count": len(self.spans),
            "spans": [s.to_dict() for s in self.spans]
        }


class TracingContext:
    """Thread-local tracing context"""
    _local = threading.local()
    
    @classmethod
    def get_current_span(cls) -> Optional[Span]:
        return getattr(cls._local, 'current_span', None)
    
    @classmethod
    def set_current_span(cls, span: Optional[Span]):
        cls._local.current_span = span
    
    @classmethod
    def get_current_trace_id(cls) -> Optional[str]:
        span = cls.get_current_span()
        return span.trace_id if span else None


class Tracer:
    """
    OpenTelemetry-compatible Distributed Tracer
    
    Features:
    - Automatic span creation and propagation
    - Context management
    - Sampling strategies
    - Export to various backends
    """
    
    def __init__(self, service_name: str = "email-triage"):
        self.service_name = service_name
        self._traces: Dict[str, Trace] = {}
        self._completed_traces: deque = deque(maxlen=1000)
        self._lock = threading.RLock()
        self._sample_rate = 1.0  # 100% sampling
        self._exporters: List[Callable] = []
        
        logger.info(f"Tracer initialized for service: {service_name}")
    
    def _generate_id(self, length: int = 16) -> str:
        """Generate random ID"""
        return uuid.uuid4().hex[:length]
    
    def _should_sample(self) -> bool:
        """Determine if trace should be sampled"""
        import random
        return random.random() < self._sample_rate
    
    def start_trace(self, name: str = "request") -> Trace:
        """Start a new trace"""
        trace_id = self._generate_id(32)
        trace = Trace(trace_id=trace_id)
        
        with self._lock:
            self._traces[trace_id] = trace
        
        return trace
    
    def start_span(
        self,
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
        trace_id: str = None,
        parent_span_id: str = None,
        attributes: Dict[str, Any] = None
    ) -> Span:
        """Start a new span"""
        # Get context from current span if not provided
        current = TracingContext.get_current_span()
        
        if trace_id is None:
            if current:
                trace_id = current.trace_id
            else:
                trace_id = self._generate_id(32)
        
        if parent_span_id is None and current:
            parent_span_id = current.span_id
        
        span = Span(
            trace_id=trace_id,
            span_id=self._generate_id(16),
            parent_span_id=parent_span_id,
            name=name,
            kind=kind,
            start_time=time.time(),
            attributes=attributes or {}
        )
        
        # Add service info
        span.set_attribute("service.name", self.service_name)
        
        # Add to trace
        with self._lock:
            if trace_id not in self._traces:
                self._traces[trace_id] = Trace(trace_id=trace_id)
            self._traces[trace_id].spans.append(span)
        
        # Set as current
        TracingContext.set_current_span(span)
        
        return span
    
    def end_span(self, span: Span, status: SpanStatus = SpanStatus.OK):
        """End a span"""
        span.end_time = time.time()
        span.status = status
        
        # Restore parent as current
        if span.parent_span_id:
            with self._lock:
                trace = self._traces.get(span.trace_id)
                if trace:
                    for s in trace.spans:
                        if s.span_id == span.parent_span_id:
                            TracingContext.set_current_span(s)
                            break
        else:
            TracingContext.set_current_span(None)
    
    def finish_trace(self, trace_id: str):
        """Mark trace as complete"""
        with self._lock:
            if trace_id in self._traces:
                trace = self._traces.pop(trace_id)
                self._completed_traces.append(trace)
                
                # Export
                for exporter in self._exporters:
                    try:
                        exporter(trace)
                    except Exception as e:
                        logger.error(f"Export failed: {e}")
    
    @contextmanager
    def span(
        self,
        name: str,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Dict[str, Any] = None
    ):
        """Context manager for spans"""
        span = self.start_span(name, kind, attributes=attributes)
        try:
            yield span
            self.end_span(span, SpanStatus.OK)
        except Exception as e:
            span.set_status(SpanStatus.ERROR, str(e))
            span.add_event("exception", {"message": str(e), "type": type(e).__name__})
            self.end_span(span, SpanStatus.ERROR)
            raise
    
    def trace_function(
        self,
        name: str = None,
        kind: SpanKind = SpanKind.INTERNAL,
        attributes: Dict[str, Any] = None
    ):
        """Decorator to trace a function"""
        def decorator(func: Callable):
            span_name = name or func.__name__
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                with self.span(span_name, kind, attributes):
                    return func(*args, **kwargs)
            
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                with self.span(span_name, kind, attributes):
                    return await func(*args, **kwargs)
            
            import asyncio
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return wrapper
        
        return decorator
    
    def get_trace(self, trace_id: str) -> Optional[Trace]:
        """Get trace by ID"""
        with self._lock:
            # Check active traces
            if trace_id in self._traces:
                return self._traces[trace_id]
            
            # Check completed traces
            for trace in self._completed_traces:
                if trace.trace_id == trace_id:
                    return trace
        
        return None
    
    def get_recent_traces(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent completed traces"""
        with self._lock:
            traces = list(self._completed_traces)[-limit:]
            return [t.to_dict() for t in reversed(traces)]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get tracing analytics"""
        with self._lock:
            completed = list(self._completed_traces)
        
        if not completed:
            return {
                "total_traces": 0,
                "active_traces": len(self._traces),
                "avg_duration_ms": 0,
                "avg_span_count": 0
            }
        
        durations = [t.duration_ms for t in completed]
        span_counts = [len(t.spans) for t in completed]
        
        # Error rate
        error_count = sum(
            1 for t in completed
            for s in t.spans
            if s.status == SpanStatus.ERROR
        )
        total_spans = sum(len(t.spans) for t in completed)
        
        # Slowest operations
        all_spans = [s for t in completed for s in t.spans if s.duration_ms]
        slowest = sorted(all_spans, key=lambda s: s.duration_ms or 0, reverse=True)[:10]
        
        return {
            "total_traces": len(completed),
            "active_traces": len(self._traces),
            "avg_duration_ms": sum(durations) / len(durations) if durations else 0,
            "max_duration_ms": max(durations) if durations else 0,
            "min_duration_ms": min(durations) if durations else 0,
            "avg_span_count": sum(span_counts) / len(span_counts) if span_counts else 0,
            "total_spans": total_spans,
            "error_count": error_count,
            "error_rate": (error_count / total_spans * 100) if total_spans > 0 else 0,
            "slowest_operations": [
                {"name": s.name, "duration_ms": s.duration_ms}
                for s in slowest
            ],
            "sample_rate": self._sample_rate
        }
    
    def add_exporter(self, exporter: Callable[[Trace], None]):
        """Add trace exporter"""
        self._exporters.append(exporter)
    
    def set_sample_rate(self, rate: float):
        """Set sampling rate (0.0 to 1.0)"""
        self._sample_rate = max(0.0, min(1.0, rate))


# Global tracer instance
_tracer = Tracer()


def get_tracer() -> Tracer:
    """Get global tracer"""
    return _tracer


def trace(name: str = None, kind: SpanKind = SpanKind.INTERNAL):
    """Decorator shorthand for tracing"""
    return _tracer.trace_function(name, kind)
