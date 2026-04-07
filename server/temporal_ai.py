"""
TEMPORAL AI SYSTEM  
Time-aware artificial intelligence with causal reasoning, temporal patterns, and future prediction
"""

import asyncio
import numpy as np
import time
import random
from typing import Dict, List, Optional, Any, Tuple, Union, Set
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import threading
from datetime import datetime, timedelta
import json
import math

class TemporalRelation(Enum):
    """Types of temporal relationships"""
    BEFORE = "before"
    AFTER = "after"
    DURING = "during"
    OVERLAPS = "overlaps"
    MEETS = "meets"
    STARTS = "starts"
    FINISHES = "finishes"
    EQUALS = "equals"

class CausalType(Enum):
    """Types of causal relationships"""
    DIRECT_CAUSE = "direct_cause"
    INDIRECT_CAUSE = "indirect_cause"
    NECESSARY_CONDITION = "necessary_condition"
    SUFFICIENT_CONDITION = "sufficient_condition"
    PROBABILISTIC_CAUSE = "probabilistic_cause"
    COUNTERFACTUAL = "counterfactual"

class TemporalScale(Enum):
    """Different temporal scales"""
    MICROSECOND = "microsecond"
    MILLISECOND = "millisecond" 
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"

@dataclass
class TemporalEvent:
    """Represents an event in time"""
    event_id: str
    event_type: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[timedelta] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    causal_links: List[str] = field(default_factory=list)  # Links to other event IDs
    confidence: float = 1.0
    
    def __post_init__(self):
        """Calculate duration if not provided"""
        if self.duration is None and self.end_time is not None:
            self.duration = self.end_time - self.start_time
    
    def overlaps_with(self, other: 'TemporalEvent') -> bool:
        """Check if this event overlaps with another"""
        if self.end_time is None or other.end_time is None:
            return False
        
        return (self.start_time < other.end_time and other.start_time < self.end_time)
    
    def get_temporal_relation(self, other: 'TemporalEvent') -> TemporalRelation:
        """Determine temporal relationship with another event"""
        if self.end_time is None or other.end_time is None:
            if self.start_time < other.start_time:
                return TemporalRelation.BEFORE
            elif self.start_time > other.start_time:
                return TemporalRelation.AFTER
            else:
                return TemporalRelation.EQUALS
        
        if self.end_time <= other.start_time:
            return TemporalRelation.BEFORE
        elif other.end_time <= self.start_time:
            return TemporalRelation.AFTER
        elif self.start_time == other.start_time and self.end_time == other.end_time:
            return TemporalRelation.EQUALS
        elif self.overlaps_with(other):
            return TemporalRelation.OVERLAPS
        else:
            return TemporalRelation.DURING

@dataclass
class CausalRelation:
    """Represents a causal relationship between events"""
    cause_event_id: str
    effect_event_id: str
    causal_type: CausalType
    strength: float  # 0.0 to 1.0
    time_lag: timedelta
    conditions: List[str] = field(default_factory=list)
    evidence: Dict[str, Any] = field(default_factory=dict)
    
    def is_strong_causal_link(self) -> bool:
        """Check if this is a strong causal relationship"""
        return self.strength > 0.7 and self.causal_type in [
            CausalType.DIRECT_CAUSE, 
            CausalType.NECESSARY_CONDITION,
            CausalType.SUFFICIENT_CONDITION
        ]

@dataclass
class TemporalPattern:
    """Represents a recurring temporal pattern"""
    pattern_id: str
    pattern_type: str
    frequency: timedelta
    phase: timedelta = field(default_factory=lambda: timedelta(0))
    amplitude: float = 1.0
    decay_rate: float = 0.0
    confidence: float = 1.0
    instances: List[datetime] = field(default_factory=list)
    
    def predict_next_occurrence(self, current_time: datetime) -> datetime:
        """Predict the next occurrence of this pattern"""
        if not self.instances:
            return current_time + self.frequency
        
        last_occurrence = max(self.instances)
        time_since_last = current_time - last_occurrence
        
        # Calculate how many periods have passed
        periods_passed = time_since_last / self.frequency
        next_period = math.ceil(periods_passed)
        
        next_occurrence = last_occurrence + (self.frequency * next_period)
        
        # Apply phase shift
        next_occurrence += self.phase
        
        return next_occurrence
    
    def calculate_pattern_strength(self, time_window: timedelta) -> float:
        """Calculate how strong the pattern is in a time window"""
        if len(self.instances) < 2:
            return 0.0
        
        # Calculate regularity score
        intervals = []
        for i in range(1, len(self.instances)):
            interval = self.instances[i] - self.instances[i-1]
            intervals.append(interval.total_seconds())
        
        if not intervals:
            return 0.0
        
        # Measure consistency (lower variance = stronger pattern)
        mean_interval = sum(intervals) / len(intervals)
        variance = sum((x - mean_interval) ** 2 for x in intervals) / len(intervals)
        coefficient_of_variation = math.sqrt(variance) / mean_interval if mean_interval > 0 else 1.0
        
        # Convert to strength score (lower CV = higher strength)
        strength = max(0.0, 1.0 - coefficient_of_variation)
        
        return strength

class TemporalMemory:
    """Long-term temporal memory system"""
    
    def __init__(self, capacity: int = 100000):
        self.events: Dict[str, TemporalEvent] = {}
        self.causal_relations: Dict[str, CausalRelation] = {}
        self.temporal_patterns: Dict[str, TemporalPattern] = {}
        self.capacity = capacity
        self.access_times: Dict[str, datetime] = {}
        self.consolidation_threshold = 0.8
        
        # Temporal indices for fast retrieval
        self.time_index = defaultdict(list)  # Maps time periods to event IDs
        self.type_index = defaultdict(list)  # Maps event types to event IDs
        
        # Memory consolidation parameters
        self.short_term_window = timedelta(hours=1)
        self.medium_term_window = timedelta(days=7)
        self.long_term_window = timedelta(days=365)
    
    def store_event(self, event: TemporalEvent) -> bool:
        """Store an event in temporal memory"""
        if len(self.events) >= self.capacity:
            self._cleanup_old_events()
        
        self.events[event.event_id] = event
        self.access_times[event.event_id] = datetime.now()
        
        # Update indices
        time_key = event.start_time.strftime("%Y-%m-%d-%H")
        self.time_index[time_key].append(event.event_id)
        self.type_index[event.event_type].append(event.event_id)
        
        # Trigger pattern detection
        self._detect_patterns_for_event(event)
        
        return True
    
    def retrieve_events_by_time(self, start_time: datetime, 
                              end_time: datetime) -> List[TemporalEvent]:
        """Retrieve events within a time range"""
        events = []
        
        current_time = start_time
        while current_time <= end_time:
            time_key = current_time.strftime("%Y-%m-%d-%H")
            for event_id in self.time_index.get(time_key, []):
                if event_id in self.events:
                    event = self.events[event_id]
                    if start_time <= event.start_time <= end_time:
                        events.append(event)
                        self.access_times[event_id] = datetime.now()
            
            current_time += timedelta(hours=1)
        
        return events
    
    def retrieve_events_by_type(self, event_type: str) -> List[TemporalEvent]:
        """Retrieve events by type"""
        events = []
        for event_id in self.type_index.get(event_type, []):
            if event_id in self.events:
                events.append(self.events[event_id])
                self.access_times[event_id] = datetime.now()
        
        return events
    
    def _cleanup_old_events(self):
        """Remove old, unused events when capacity is exceeded"""
        # Sort by last access time
        events_by_access = sorted(
            self.access_times.items(),
            key=lambda x: x[1]
        )
        
        # Remove oldest 10% of events
        num_to_remove = max(1, len(events_by_access) // 10)
        
        for i in range(num_to_remove):
            event_id, _ = events_by_access[i]
            if event_id in self.events:
                event = self.events[event_id]
                
                # Remove from indices
                time_key = event.start_time.strftime("%Y-%m-%d-%H")
                if event_id in self.time_index[time_key]:
                    self.time_index[time_key].remove(event_id)
                
                if event_id in self.type_index[event.event_type]:
                    self.type_index[event.event_type].remove(event_id)
                
                # Remove event
                del self.events[event_id]
                del self.access_times[event_id]
    
    def _detect_patterns_for_event(self, event: TemporalEvent):
        """Detect temporal patterns involving a new event"""
        similar_events = self.retrieve_events_by_type(event.event_type)
        
        if len(similar_events) < 3:  # Need at least 3 events to detect patterns
            return
        
        # Sort events by time
        similar_events.sort(key=lambda e: e.start_time)
        
        # Look for regular intervals
        intervals = []
        for i in range(1, len(similar_events)):
            interval = similar_events[i].start_time - similar_events[i-1].start_time
            intervals.append(interval)
        
        if len(intervals) >= 2:
            # Check for consistent intervals
            avg_interval = sum(intervals, timedelta()) / len(intervals)
            
            # Calculate variance in intervals
            variance = sum((interval - avg_interval).total_seconds() ** 2 for interval in intervals) / len(intervals)
            std_dev = math.sqrt(variance)
            
            # If intervals are consistent, create/update pattern
            if std_dev < avg_interval.total_seconds() * 0.3:  # 30% tolerance
                pattern_id = f"pattern_{event.event_type}_{len(self.temporal_patterns)}"
                
                pattern = TemporalPattern(
                    pattern_id=pattern_id,
                    pattern_type=f"recurring_{event.event_type}",
                    frequency=avg_interval,
                    confidence=max(0.5, 1.0 - (std_dev / avg_interval.total_seconds())),
                    instances=[e.start_time for e in similar_events]
                )
                
                self.temporal_patterns[pattern_id] = pattern

class CausalReasoner:
    """Causal reasoning engine for temporal events"""
    
    def __init__(self):
        self.causal_rules = {}
        self.causal_strength_threshold = 0.3
        self.temporal_window = timedelta(hours=24)  # Look for causes within 24 hours
        self.statistical_tests = {}
        
        # Initialize causal inference methods
        self._initialize_causal_methods()
    
    def _initialize_causal_methods(self):
        """Initialize causal inference methods"""
        self.causal_rules = {
            "email_response": {
                "potential_causes": ["email_received", "urgent_flag", "manager_request"],
                "typical_lag": timedelta(minutes=30),
                "strength_factors": {"urgency": 0.8, "sender_authority": 0.6}
            },
            "system_overload": {
                "potential_causes": ["high_email_volume", "memory_shortage", "cpu_spike"],
                "typical_lag": timedelta(minutes=5),
                "strength_factors": {"volume_rate": 0.9, "resource_usage": 0.7}
            }
        }
    
    def infer_causal_relations(self, events: List[TemporalEvent]) -> List[CausalRelation]:
        """Infer causal relationships between events"""
        causal_relations = []
        
        # Sort events by time
        events.sort(key=lambda e: e.start_time)
        
        for i, effect_event in enumerate(events):
            # Look for potential causes before this event
            potential_causes = []
            
            for j in range(i):
                cause_event = events[j]
                time_diff = effect_event.start_time - cause_event.start_time
                
                # Check if timing is plausible for causation
                if timedelta(0) < time_diff <= self.temporal_window:
                    potential_causes.append((cause_event, time_diff))
            
            # Evaluate each potential cause
            for cause_event, time_lag in potential_causes:
                causal_relation = self._evaluate_causal_relation(cause_event, effect_event, time_lag)
                
                if causal_relation and causal_relation.strength >= self.causal_strength_threshold:
                    causal_relations.append(causal_relation)
        
        return causal_relations
    
    def _evaluate_causal_relation(self, cause: TemporalEvent, effect: TemporalEvent,
                                 time_lag: timedelta) -> Optional[CausalRelation]:
        """Evaluate potential causal relationship between two events"""
        
        # Check if there are domain-specific causal rules
        causal_rule = self.causal_rules.get(effect.event_type, {})
        potential_causes = causal_rule.get("potential_causes", [])
        
        if cause.event_type not in potential_causes and potential_causes:
            return None  # Not a plausible cause according to domain knowledge
        
        # Calculate causal strength based on multiple factors
        strength = 0.0
        
        # Temporal proximity (closer in time = stronger)
        typical_lag = causal_rule.get("typical_lag", timedelta(hours=1))
        proximity_score = max(0, 1 - abs((time_lag - typical_lag).total_seconds()) / typical_lag.total_seconds())
        strength += proximity_score * 0.4
        
        # Domain-specific strength factors
        strength_factors = causal_rule.get("strength_factors", {})
        for factor, weight in strength_factors.items():
            if factor in cause.properties:
                factor_value = cause.properties[factor]
                if isinstance(factor_value, (int, float)):
                    normalized_value = min(1.0, factor_value)
                    strength += normalized_value * weight * 0.3
        
        # Property correlation
        correlation_score = self._calculate_property_correlation(cause, effect)
        strength += correlation_score * 0.3
        
        # Determine causal type
        if strength > 0.8:
            causal_type = CausalType.DIRECT_CAUSE
        elif strength > 0.6:
            causal_type = CausalType.PROBABILISTIC_CAUSE
        elif strength > 0.4:
            causal_type = CausalType.INDIRECT_CAUSE
        else:
            causal_type = CausalType.PROBABILISTIC_CAUSE
        
        return CausalRelation(
            cause_event_id=cause.event_id,
            effect_event_id=effect.event_id,
            causal_type=causal_type,
            strength=min(1.0, strength),
            time_lag=time_lag,
            evidence={
                "proximity_score": proximity_score,
                "correlation_score": correlation_score,
                "domain_knowledge": len(potential_causes) > 0
            }
        )
    
    def _calculate_property_correlation(self, event1: TemporalEvent, 
                                      event2: TemporalEvent) -> float:
        """Calculate correlation between event properties"""
        if not event1.properties or not event2.properties:
            return 0.0
        
        common_props = set(event1.properties.keys()) & set(event2.properties.keys())
        if not common_props:
            return 0.0
        
        correlations = []
        for prop in common_props:
            val1 = event1.properties[prop]
            val2 = event2.properties[prop]
            
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                # Numerical correlation (simplified)
                if val1 != 0:
                    correlation = min(1.0, abs(val2) / abs(val1))
                    correlations.append(correlation)
            elif isinstance(val1, str) and isinstance(val2, str):
                # String similarity
                similarity = len(set(val1.lower()) & set(val2.lower())) / len(set(val1.lower()) | set(val2.lower()))
                correlations.append(similarity)
        
        return sum(correlations) / len(correlations) if correlations else 0.0

class FuturePredictionEngine:
    """Engine for predicting future events and trends"""
    
    def __init__(self):
        self.prediction_models = {}
        self.confidence_threshold = 0.5
        self.max_prediction_horizon = timedelta(days=90)
        self.learning_rate = 0.01
        
        # Initialize prediction models
        self._initialize_prediction_models()
    
    def _initialize_prediction_models(self):
        """Initialize different prediction models"""
        self.prediction_models = {
            "linear_trend": {
                "description": "Linear extrapolation of trends",
                "accuracy": 0.7,
                "suitable_for": ["volume_trends", "response_times"]
            },
            "seasonal_model": {
                "description": "Seasonal pattern prediction",
                "accuracy": 0.8,
                "suitable_for": ["daily_patterns", "weekly_cycles"]
            },
            "causal_model": {
                "description": "Causal chain prediction",
                "accuracy": 0.6,
                "suitable_for": ["event_sequences", "system_behaviors"]
            }
        }
    
    def predict_future_events(self, historical_events: List[TemporalEvent],
                            patterns: List[TemporalPattern],
                            prediction_horizon: timedelta) -> List[Dict[str, Any]]:
        """Predict future events based on historical data and patterns"""
        
        if prediction_horizon > self.max_prediction_horizon:
            prediction_horizon = self.max_prediction_horizon
        
        predictions = []
        current_time = datetime.now()
        end_time = current_time + prediction_horizon
        
        # Pattern-based predictions
        for pattern in patterns:
            if pattern.confidence >= self.confidence_threshold:
                next_occurrence = pattern.predict_next_occurrence(current_time)
                
                if current_time < next_occurrence <= end_time:
                    prediction = {
                        "prediction_type": "pattern_based",
                        "predicted_time": next_occurrence,
                        "event_type": pattern.pattern_type,
                        "confidence": pattern.confidence,
                        "basis": f"Recurring pattern with frequency {pattern.frequency}",
                        "pattern_id": pattern.pattern_id
                    }
                    predictions.append(prediction)
        
        # Trend-based predictions
        trend_predictions = self._predict_trends(historical_events, current_time, end_time)
        predictions.extend(trend_predictions)
        
        # Causal chain predictions
        causal_predictions = self._predict_causal_chains(historical_events, current_time, end_time)
        predictions.extend(causal_predictions)
        
        # Sort predictions by time
        predictions.sort(key=lambda p: p["predicted_time"])
        
        return predictions
    
    def _predict_trends(self, events: List[TemporalEvent], 
                       start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Predict future events based on trends"""
        predictions = []
        
        # Group events by type
        events_by_type = defaultdict(list)
        for event in events:
            events_by_type[event.event_type].append(event)
        
        for event_type, type_events in events_by_type.items():
            if len(type_events) < 3:
                continue
            
            # Sort by time
            type_events.sort(key=lambda e: e.start_time)
            
            # Calculate trend in frequency
            recent_events = [e for e in type_events if (start_time - e.start_time).days <= 30]
            
            if len(recent_events) >= 2:
                # Simple linear trend
                time_diffs = [(start_time - e.start_time).total_seconds() for e in recent_events]
                avg_interval = sum(time_diffs[:-1]) / (len(time_diffs) - 1) if len(time_diffs) > 1 else 86400
                
                # Predict next occurrence
                last_event_time = recent_events[-1].start_time
                predicted_time = last_event_time + timedelta(seconds=avg_interval)
                
                if start_time < predicted_time <= end_time:
                    confidence = min(0.8, len(recent_events) / 10)  # Higher confidence with more data
                    
                    prediction = {
                        "prediction_type": "trend_based",
                        "predicted_time": predicted_time,
                        "event_type": event_type,
                        "confidence": confidence,
                        "basis": f"Linear trend from {len(recent_events)} recent events",
                        "expected_interval_seconds": avg_interval
                    }
                    predictions.append(prediction)
        
        return predictions
    
    def _predict_causal_chains(self, events: List[TemporalEvent],
                              start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """Predict events based on causal relationships"""
        predictions = []
        
        # This would use causal relations to predict chains of events
        # Simplified implementation for demonstration
        
        recent_events = [e for e in events if (start_time - e.start_time).hours <= 24]
        
        for event in recent_events:
            # Look for typical consequences of this event type
            if event.event_type == "urgent_email":
                predicted_time = event.start_time + timedelta(minutes=30)
                if start_time < predicted_time <= end_time:
                    prediction = {
                        "prediction_type": "causal_chain",
                        "predicted_time": predicted_time,
                        "event_type": "email_response",
                        "confidence": 0.6,
                        "basis": f"Causal chain from {event.event_type}",
                        "trigger_event": event.event_id
                    }
                    predictions.append(prediction)
        
        return predictions

class TemporalAI:
    """Main temporal AI system"""
    
    def __init__(self):
        self.temporal_memory = TemporalMemory()
        self.causal_reasoner = CausalReasoner()
        self.prediction_engine = FuturePredictionEngine()
        
        self.performance_metrics = {
            "events_processed": 0,
            "patterns_detected": 0,
            "causal_relations_found": 0,
            "predictions_made": 0,
            "prediction_accuracy": deque(maxlen=100),
            "processing_times": deque(maxlen=100)
        }
        
        self.lock = threading.RLock()
        
        # Email-specific temporal patterns
        self._initialize_email_temporal_patterns()
    
    def _initialize_email_temporal_patterns(self):
        """Initialize email-specific temporal patterns"""
        
        # Daily email patterns
        daily_pattern = TemporalPattern(
            pattern_id="daily_email_cycle",
            pattern_type="daily_email_volume",
            frequency=timedelta(days=1),
            phase=timedelta(hours=9),  # Peak at 9 AM
            amplitude=0.8,
            confidence=0.9
        )
        
        self.temporal_memory.temporal_patterns[daily_pattern.pattern_id] = daily_pattern
        
        # Weekly patterns
        weekly_pattern = TemporalPattern(
            pattern_id="weekly_email_cycle", 
            pattern_type="weekly_email_volume",
            frequency=timedelta(days=7),
            phase=timedelta(days=1, hours=10),  # Peak Tuesday 10 AM
            amplitude=0.6,
            confidence=0.8
        )
        
        self.temporal_memory.temporal_patterns[weekly_pattern.pattern_id] = weekly_pattern
    
    def process_email_temporally(self, email_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process email with temporal awareness"""
        start_time = time.time()
        
        current_time = datetime.now()
        
        # Create temporal event for this email
        event = TemporalEvent(
            event_id=f"email_{int(current_time.timestamp())}_{random.randint(1000, 9999)}",
            event_type="email_received",
            start_time=current_time,
            properties={
                "sender": email_data.get("sender", ""),
                "subject_length": len(email_data.get("subject", "")),
                "content_length": len(email_data.get("content", "")),
                "priority": email_data.get("priority", "normal"),
                "urgency_score": self._calculate_urgency_score(email_data)
            }
        )
        
        # Store event in temporal memory
        self.temporal_memory.store_event(event)
        
        # Analyze temporal context
        temporal_context = self._analyze_temporal_context(event)
        
        # Predict future events
        future_predictions = self._predict_email_consequences(event)
        
        # Find causal relationships
        recent_events = self.temporal_memory.retrieve_events_by_time(
            current_time - timedelta(hours=24), current_time
        )
        causal_relations = self.causal_reasoner.infer_causal_relations(recent_events + [event])
        
        processing_time = (time.time() - start_time) * 1000
        
        with self.lock:
            self.performance_metrics["events_processed"] += 1
            self.performance_metrics["causal_relations_found"] += len(causal_relations)
            self.performance_metrics["predictions_made"] += len(future_predictions)
            self.performance_metrics["processing_times"].append(processing_time)
        
        return {
            "temporal_analysis": {
                "event_context": {
                    "event_id": event.event_id,
                    "timestamp": event.start_time.isoformat(),
                    "temporal_position": self._describe_temporal_position(event),
                    "time_since_last_email": temporal_context.get("time_since_last", "unknown")
                },
                "pattern_analysis": {
                    "detected_patterns": temporal_context.get("active_patterns", []),
                    "pattern_deviation": temporal_context.get("pattern_deviation", 0.0),
                    "expected_vs_actual": temporal_context.get("expected_vs_actual", {})
                },
                "causal_analysis": {
                    "potential_causes": [
                        {
                            "cause_event": rel.cause_event_id,
                            "causal_type": rel.causal_type.value,
                            "strength": rel.strength,
                            "time_lag_seconds": rel.time_lag.total_seconds()
                        }
                        for rel in causal_relations if rel.effect_event_id == event.event_id
                    ],
                    "potential_effects": [
                        {
                            "effect_event": rel.effect_event_id,
                            "causal_type": rel.causal_type.value,
                            "strength": rel.strength
                        }
                        for rel in causal_relations if rel.cause_event_id == event.event_id
                    ]
                }
            },
            "temporal_predictions": {
                "future_events": [
                    {
                        "predicted_event": pred["event_type"],
                        "predicted_time": pred["predicted_time"].isoformat(),
                        "confidence": pred["confidence"],
                        "prediction_basis": pred["basis"]
                    }
                    for pred in future_predictions
                ],
                "temporal_recommendations": self._generate_temporal_recommendations(event, future_predictions)
            },
            "time_intelligence": {
                "optimal_response_time": self._calculate_optimal_response_time(event),
                "urgency_assessment": {
                    "time_criticality": self._assess_time_criticality(event),
                    "deadline_pressure": self._calculate_deadline_pressure(event),
                    "temporal_priority": self._calculate_temporal_priority(event)
                },
                "contextual_timing": {
                    "business_hours_alignment": self._check_business_hours_alignment(event),
                    "recipient_timezone_consideration": self._analyze_timezone_factors(event),
                    "optimal_follow_up_timing": self._suggest_follow_up_timing(event)
                }
            },
            "processing_metadata": {
                "temporal_processing_time_ms": round(processing_time, 2),
                "events_in_memory": len(self.temporal_memory.events),
                "patterns_tracked": len(self.temporal_memory.temporal_patterns),
                "causal_relations_found": len(causal_relations)
            }
        }
    
    def _calculate_urgency_score(self, email_data: Dict[str, Any]) -> float:
        """Calculate urgency score based on email content and timing"""
        score = 0.0
        
        subject = email_data.get("subject", "").lower()
        content = email_data.get("content", "").lower()
        
        # Urgency keywords
        urgent_keywords = ["urgent", "asap", "emergency", "critical", "deadline", "immediately"]
        for keyword in urgent_keywords:
            if keyword in subject:
                score += 0.3
            if keyword in content:
                score += 0.2
        
        # Time-based urgency (emails outside business hours might be more urgent)
        current_hour = datetime.now().hour
        if current_hour < 8 or current_hour > 18:  # Outside business hours
            score += 0.2
        
        return min(1.0, score)
    
    def _analyze_temporal_context(self, event: TemporalEvent) -> Dict[str, Any]:
        """Analyze the temporal context of an event"""
        context = {}
        
        # Find recent similar events
        similar_events = self.temporal_memory.retrieve_events_by_type(event.event_type)
        similar_events.sort(key=lambda e: e.start_time)
        
        if similar_events:
            # Time since last similar event
            last_similar = similar_events[-1]
            time_diff = event.start_time - last_similar.start_time
            context["time_since_last"] = str(time_diff)
        
        # Check active patterns
        active_patterns = []
        for pattern in self.temporal_memory.temporal_patterns.values():
            next_occurrence = pattern.predict_next_occurrence(event.start_time)
            time_to_next = abs((next_occurrence - event.start_time).total_seconds())
            
            # If event occurs close to predicted time, pattern is active
            if time_to_next < pattern.frequency.total_seconds() * 0.1:  # Within 10% of frequency
                active_patterns.append({
                    "pattern_id": pattern.pattern_id,
                    "pattern_type": pattern.pattern_type,
                    "confidence": pattern.confidence,
                    "deviation": time_to_next
                })
        
        context["active_patterns"] = active_patterns
        
        return context
    
    def _predict_email_consequences(self, event: TemporalEvent) -> List[Dict[str, Any]]:
        """Predict consequences of email event"""
        
        # Get historical events for prediction
        historical_events = self.temporal_memory.retrieve_events_by_time(
            event.start_time - timedelta(days=30),
            event.start_time
        )
        
        # Get active patterns
        patterns = list(self.temporal_memory.temporal_patterns.values())
        
        # Predict future events
        predictions = self.prediction_engine.predict_future_events(
            historical_events, patterns, timedelta(days=7)
        )
        
        return predictions
    
    def _describe_temporal_position(self, event: TemporalEvent) -> str:
        """Describe the temporal position of an event"""
        current_hour = event.start_time.hour
        current_weekday = event.start_time.weekday()
        
        if current_hour < 9:
            time_desc = "early_morning"
        elif current_hour < 12:
            time_desc = "morning"
        elif current_hour < 17:
            time_desc = "afternoon"
        else:
            time_desc = "evening"
        
        weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        day_desc = weekdays[current_weekday]
        
        return f"{time_desc}_{day_desc}"
    
    def _generate_temporal_recommendations(self, event: TemporalEvent, 
                                         predictions: List[Dict[str, Any]]) -> List[str]:
        """Generate temporal-based recommendations"""
        recommendations = []
        
        urgency = event.properties.get("urgency_score", 0.0)
        
        if urgency > 0.7:
            recommendations.append("Respond within 30 minutes due to high urgency")
        elif urgency > 0.4:
            recommendations.append("Respond within 2 hours for optimal timing")
        else:
            recommendations.append("Response can be scheduled for next business day")
        
        # Check for predicted follow-ups
        response_predictions = [p for p in predictions if "response" in p["event_type"]]
        if response_predictions:
            recommendations.append("Expect response within predicted timeframe")
        
        return recommendations
    
    def _calculate_optimal_response_time(self, event: TemporalEvent) -> str:
        """Calculate optimal response time for email"""
        urgency = event.properties.get("urgency_score", 0.0)
        
        if urgency > 0.8:
            return "immediate (< 15 minutes)"
        elif urgency > 0.6:
            return "urgent (< 1 hour)"
        elif urgency > 0.4:
            return "prompt (< 4 hours)"
        else:
            return "standard (< 24 hours)"
    
    def _assess_time_criticality(self, event: TemporalEvent) -> float:
        """Assess time criticality of email"""
        # Simplified time criticality based on urgency and timing
        urgency = event.properties.get("urgency_score", 0.0)
        
        # Increase criticality for off-hours emails
        current_hour = event.start_time.hour
        time_factor = 1.2 if (current_hour < 8 or current_hour > 18) else 1.0
        
        return min(1.0, urgency * time_factor)
    
    def _calculate_deadline_pressure(self, event: TemporalEvent) -> float:
        """Calculate deadline pressure from email content"""
        # Simplified deadline detection
        content = event.properties.get("content", "").lower() if "content" in event.properties else ""
        subject = event.properties.get("subject", "").lower() if "subject" in event.properties else ""
        
        deadline_indicators = ["deadline", "due", "expires", "ends", "closes"]
        pressure = 0.0
        
        for indicator in deadline_indicators:
            if indicator in subject:
                pressure += 0.4
            if indicator in str(content):
                pressure += 0.2
        
        return min(1.0, pressure)
    
    def _calculate_temporal_priority(self, event: TemporalEvent) -> float:
        """Calculate overall temporal priority"""
        criticality = self._assess_time_criticality(event)
        deadline_pressure = self._calculate_deadline_pressure(event)
        urgency = event.properties.get("urgency_score", 0.0)
        
        priority = (criticality * 0.4 + deadline_pressure * 0.3 + urgency * 0.3)
        return priority
    
    def _check_business_hours_alignment(self, event: TemporalEvent) -> Dict[str, Any]:
        """Check alignment with business hours"""
        hour = event.start_time.hour
        weekday = event.start_time.weekday()
        
        in_business_hours = (9 <= hour <= 17) and (weekday < 5)
        
        return {
            "in_business_hours": in_business_hours,
            "hour": hour,
            "weekday": weekday,
            "recommendation": "normal_processing" if in_business_hours else "urgent_review"
        }
    
    def _analyze_timezone_factors(self, event: TemporalEvent) -> Dict[str, Any]:
        """Analyze timezone considerations"""
        # Simplified timezone analysis
        return {
            "sender_timezone_consideration": "assumed_same_timezone",
            "optimal_response_timezone": "business_hours_local",
            "cross_timezone_factor": 1.0
        }
    
    def _suggest_follow_up_timing(self, event: TemporalEvent) -> Dict[str, Any]:
        """Suggest optimal follow-up timing"""
        urgency = event.properties.get("urgency_score", 0.0)
        
        if urgency > 0.7:
            follow_up_hours = 1
        elif urgency > 0.4:
            follow_up_hours = 4
        else:
            follow_up_hours = 24
        
        suggested_time = event.start_time + timedelta(hours=follow_up_hours)
        
        return {
            "suggested_follow_up_time": suggested_time.isoformat(),
            "follow_up_reason": "based_on_urgency_assessment",
            "hours_after_initial": follow_up_hours
        }
    
    def get_temporal_ai_analytics(self) -> Dict[str, Any]:
        """Get comprehensive temporal AI analytics"""
        
        avg_processing_time = (sum(self.performance_metrics["processing_times"]) /
                             len(self.performance_metrics["processing_times"])
                             if self.performance_metrics["processing_times"] else 0)
        
        return {
            "temporal_intelligence_overview": {
                "paradigm": "Time-aware artificial intelligence",
                "capabilities": [
                    "Temporal pattern recognition",
                    "Causal relationship inference", 
                    "Future event prediction",
                    "Time-sensitive decision making"
                ],
                "temporal_scales": ["microsecond", "second", "minute", "hour", "day", "week", "month", "year"]
            },
            "memory_system": {
                "events_stored": len(self.temporal_memory.events),
                "patterns_detected": len(self.temporal_memory.temporal_patterns),
                "memory_capacity": self.temporal_memory.capacity,
                "temporal_index_size": len(self.temporal_memory.time_index),
                "consolidation_threshold": self.temporal_memory.consolidation_threshold
            },
            "causal_reasoning": {
                "causal_rules": len(self.causal_reasoner.causal_rules),
                "strength_threshold": self.causal_reasoner.causal_strength_threshold,
                "temporal_window_hours": self.causal_reasoner.temporal_window.total_seconds() / 3600,
                "inference_methods": ["domain_knowledge", "temporal_proximity", "property_correlation"]
            },
            "prediction_engine": {
                "prediction_models": list(self.prediction_engine.prediction_models.keys()),
                "max_horizon_days": self.prediction_engine.max_prediction_horizon.days,
                "confidence_threshold": self.prediction_engine.confidence_threshold,
                "prediction_types": ["pattern_based", "trend_based", "causal_chain"]
            },
            "performance_metrics": {
                "events_processed": self.performance_metrics["events_processed"],
                "patterns_detected": self.performance_metrics["patterns_detected"],
                "causal_relations_found": self.performance_metrics["causal_relations_found"],
                "predictions_made": self.performance_metrics["predictions_made"],
                "average_processing_time_ms": round(avg_processing_time, 2)
            },
            "temporal_capabilities": {
                "time_awareness": "Multi-scale temporal understanding",
                "causal_inference": "Automated cause-effect discovery",
                "pattern_recognition": "Recurring temporal pattern detection",
                "future_prediction": "Evidence-based event forecasting",
                "contextual_timing": "Optimal timing recommendations"
            },
            "email_specific_features": {
                "urgency_assessment": "Time-sensitive priority calculation",
                "response_timing": "Optimal response time suggestions",
                "deadline_detection": "Automatic deadline pressure analysis",
                "business_hours_awareness": "Context-aware scheduling",
                "follow_up_optimization": "Intelligent follow-up timing"
            }
        }

# Global temporal AI instance
_temporal_ai = None

def get_temporal_ai():
    """Get global temporal AI system"""
    global _temporal_ai
    if _temporal_ai is None:
        _temporal_ai = TemporalAI()
    return _temporal_ai