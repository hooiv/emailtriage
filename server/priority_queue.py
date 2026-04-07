"""
Intelligent Priority Queue System for Email Triage Environment.

Implements ML-based intelligent email scheduling with:
- Dynamic priority scoring
- Deadline-aware scheduling
- Workload balancing
- Context-aware reordering
- Batch optimization
- Resource-aware execution
- Priority inheritance
"""

import heapq
import logging
import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import secrets

logger = logging.getLogger(__name__)


class QueueStrategy(str, Enum):
    """Queue processing strategies."""
    FIFO = "fifo"  # First In First Out
    PRIORITY = "priority"  # Pure priority-based
    DEADLINE = "deadline"  # Deadline-first
    HYBRID = "hybrid"  # ML-based hybrid
    FAIR = "fair"  # Fair scheduling across categories
    ADAPTIVE = "adaptive"  # Adapts based on load


class PriorityClass(str, Enum):
    """Priority classification."""
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"
    BACKGROUND = "background"


@dataclass
class QueueItem:
    """Item in the priority queue."""
    item_id: str
    email_id: str
    priority_score: float  # 0.0 - 1.0, higher = more urgent
    priority_class: PriorityClass
    deadline: Optional[datetime]
    category: str
    sender_importance: float
    created_at: datetime
    processing_time_estimate_ms: float
    dependencies: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Scheduling state
    attempts: int = 0
    last_attempt: Optional[datetime] = None
    scheduled_at: Optional[datetime] = None
    
    def __lt__(self, other):
        """Comparison for heap ordering (higher priority = lower value for min-heap)."""
        return self.priority_score > other.priority_score
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "item_id": self.item_id,
            "email_id": self.email_id,
            "priority_score": self.priority_score,
            "priority_class": self.priority_class.value,
            "deadline": self.deadline.isoformat() if self.deadline else None,
            "category": self.category,
            "sender_importance": self.sender_importance,
            "created_at": self.created_at.isoformat(),
            "processing_time_estimate_ms": self.processing_time_estimate_ms,
            "attempts": self.attempts,
            "metadata": self.metadata
        }


@dataclass
class SchedulingDecision:
    """Decision about which item to process next."""
    item: QueueItem
    reason: str
    confidence: float
    alternatives: List[QueueItem]
    estimated_completion: datetime
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class QueueMetrics:
    """Queue performance metrics."""
    total_items: int
    items_by_priority: Dict[str, int]
    avg_wait_time_ms: float
    avg_processing_time_ms: float
    deadline_violations: int
    throughput_per_minute: float
    queue_depth_trend: str  # growing, stable, shrinking


class PriorityScorer:
    """ML-based priority scoring engine."""
    
    def __init__(self):
        self.feature_weights = {
            'urgency': 0.25,
            'sender_importance': 0.20,
            'deadline_proximity': 0.20,
            'category_priority': 0.15,
            'age_penalty': 0.10,
            'thread_importance': 0.05,
            'historical_response': 0.05
        }
        
        self.category_priorities = {
            'urgent': 1.0,
            'customer_support': 0.85,
            'billing': 0.80,
            'sales': 0.70,
            'technical': 0.65,
            'internal': 0.50,
            'newsletter': 0.20,
            'spam': 0.05
        }
        
        # Adaptive learning
        self.scoring_history: deque = deque(maxlen=1000)
        self.category_response_times: Dict[str, deque] = {}
    
    def calculate_score(
        self,
        email_id: str,
        urgency: float,
        sender_importance: float,
        deadline: Optional[datetime],
        category: str,
        created_at: datetime,
        thread_size: int = 1,
        historical_response_time: Optional[float] = None
    ) -> Tuple[float, Dict[str, float]]:
        """Calculate priority score with feature breakdown."""
        now = datetime.now()
        features = {}
        
        # Urgency factor
        features['urgency'] = min(1.0, urgency)
        
        # Sender importance
        features['sender_importance'] = min(1.0, sender_importance)
        
        # Deadline proximity (exponential increase as deadline approaches)
        if deadline:
            time_to_deadline = (deadline - now).total_seconds()
            if time_to_deadline <= 0:
                features['deadline_proximity'] = 1.0  # Past deadline
            elif time_to_deadline < 3600:  # Less than 1 hour
                features['deadline_proximity'] = 0.9
            elif time_to_deadline < 86400:  # Less than 1 day
                features['deadline_proximity'] = 0.6 * (1 - time_to_deadline / 86400)
            else:
                features['deadline_proximity'] = 0.2
        else:
            features['deadline_proximity'] = 0.3  # No deadline = moderate priority
        
        # Category priority
        features['category_priority'] = self.category_priorities.get(category.lower(), 0.5)
        
        # Age penalty (older emails get boosted to prevent starvation)
        age_hours = (now - created_at).total_seconds() / 3600
        features['age_penalty'] = min(1.0, age_hours / 24)  # Max boost after 24 hours
        
        # Thread importance (larger threads often more important)
        features['thread_importance'] = min(1.0, math.log(thread_size + 1) / math.log(10))
        
        # Historical response expectation
        if historical_response_time is not None:
            features['historical_response'] = min(1.0, 1 / (1 + historical_response_time / 60))
        else:
            features['historical_response'] = 0.5
        
        # Calculate weighted score
        score = sum(
            features[feature] * weight
            for feature, weight in self.feature_weights.items()
        )
        
        # Record for learning
        self.scoring_history.append({
            'email_id': email_id,
            'score': score,
            'features': features,
            'timestamp': now
        })
        
        return min(1.0, max(0.0, score)), features
    
    def adapt_weights(self, feedback: Dict[str, float]):
        """Adapt feature weights based on feedback."""
        learning_rate = 0.01
        
        for feature, adjustment in feedback.items():
            if feature in self.feature_weights:
                new_weight = self.feature_weights[feature] + learning_rate * adjustment
                self.feature_weights[feature] = min(0.5, max(0.05, new_weight))
        
        # Normalize weights
        total = sum(self.feature_weights.values())
        self.feature_weights = {k: v/total for k, v in self.feature_weights.items()}


class IntelligentPriorityQueue:
    """
    Intelligent email priority queue with ML-based scheduling.
    
    Features:
    - Dynamic priority scoring
    - Multiple scheduling strategies
    - Deadline-aware processing
    - Workload balancing
    - Starvation prevention
    - Batch optimization
    """
    
    def __init__(self, environment_ref=None):
        """Initialize the priority queue."""
        self.environment_ref = environment_ref
        self.strategy = QueueStrategy.HYBRID
        
        # Queue storage
        self.heap: List[QueueItem] = []
        self.item_lookup: Dict[str, QueueItem] = {}  # Fast lookup by item_id
        self.email_lookup: Dict[str, str] = {}  # email_id -> item_id
        
        # Processing state
        self.processing: Dict[str, QueueItem] = {}  # Currently processing
        self.completed: deque = deque(maxlen=1000)
        self.failed: deque = deque(maxlen=100)
        
        # Scoring engine
        self.scorer = PriorityScorer()
        
        # Fair scheduling state
        self.category_last_served: Dict[str, datetime] = {}
        self.category_counts: Dict[str, int] = {}
        
        # Performance tracking
        self.wait_times: deque = deque(maxlen=1000)
        self.processing_times: deque = deque(maxlen=1000)
        self.throughput_window: deque = deque(maxlen=60)  # Per-second counts
        
        # Analytics
        self.analytics = {
            "total_enqueued": 0,
            "total_processed": 0,
            "total_failed": 0,
            "deadline_violations": 0,
            "priority_inversions": 0,
            "reorderings": 0
        }
        
        logger.info("Intelligent Priority Queue initialized")
    
    def enqueue(
        self,
        email_id: str,
        urgency: float = 0.5,
        sender_importance: float = 0.5,
        deadline: Optional[datetime] = None,
        category: str = "general",
        processing_time_estimate_ms: float = 100,
        dependencies: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> QueueItem:
        """Add an email to the priority queue."""
        # Check for duplicates
        if email_id in self.email_lookup:
            existing_item_id = self.email_lookup[email_id]
            return self.item_lookup[existing_item_id]
        
        # Calculate priority score
        score, features = self.scorer.calculate_score(
            email_id=email_id,
            urgency=urgency,
            sender_importance=sender_importance,
            deadline=deadline,
            category=category,
            created_at=datetime.now()
        )
        
        # Determine priority class
        if score >= 0.85:
            priority_class = PriorityClass.CRITICAL
        elif score >= 0.70:
            priority_class = PriorityClass.HIGH
        elif score >= 0.40:
            priority_class = PriorityClass.NORMAL
        elif score >= 0.20:
            priority_class = PriorityClass.LOW
        else:
            priority_class = PriorityClass.BACKGROUND
        
        # Create queue item
        item = QueueItem(
            item_id=f"qi_{secrets.token_hex(8)}",
            email_id=email_id,
            priority_score=score,
            priority_class=priority_class,
            deadline=deadline,
            category=category,
            sender_importance=sender_importance,
            created_at=datetime.now(),
            processing_time_estimate_ms=processing_time_estimate_ms,
            dependencies=dependencies or [],
            metadata={
                **(metadata or {}),
                "score_features": features
            }
        )
        
        # Add to queue
        heapq.heappush(self.heap, item)
        self.item_lookup[item.item_id] = item
        self.email_lookup[email_id] = item.item_id
        
        # Update category counts
        self.category_counts[category] = self.category_counts.get(category, 0) + 1
        
        self.analytics["total_enqueued"] += 1
        
        logger.debug(f"Enqueued email {email_id} with priority {score:.3f}")
        
        return item
    
    def dequeue(self) -> Optional[SchedulingDecision]:
        """Get the next item to process based on current strategy."""
        if not self.heap:
            return None
        
        # Get candidates based on strategy
        candidates = self._get_candidates()
        
        if not candidates:
            return None
        
        # Select best candidate
        selected, reason, confidence = self._select_best(candidates)
        
        # Remove from heap
        self.heap = [item for item in self.heap if item.item_id != selected.item_id]
        heapq.heapify(self.heap)
        
        # Mark as processing
        selected.scheduled_at = datetime.now()
        self.processing[selected.item_id] = selected
        
        # Update category tracking
        self.category_last_served[selected.category] = datetime.now()
        
        # Calculate wait time
        wait_time = (datetime.now() - selected.created_at).total_seconds() * 1000
        self.wait_times.append(wait_time)
        
        # Create scheduling decision
        alternatives = [c for c in candidates if c.item_id != selected.item_id][:3]
        
        return SchedulingDecision(
            item=selected,
            reason=reason,
            confidence=confidence,
            alternatives=alternatives,
            estimated_completion=datetime.now() + timedelta(
                milliseconds=selected.processing_time_estimate_ms
            ),
            context={
                "queue_depth": len(self.heap),
                "processing_count": len(self.processing),
                "wait_time_ms": wait_time
            }
        )
    
    def _get_candidates(self, limit: int = 10) -> List[QueueItem]:
        """Get candidate items for scheduling."""
        if self.strategy == QueueStrategy.FIFO:
            # Simple FIFO - return oldest items
            return sorted(self.heap, key=lambda x: x.created_at)[:limit]
        
        elif self.strategy == QueueStrategy.PRIORITY:
            # Pure priority - return highest priority items
            return heapq.nsmallest(limit, self.heap)
        
        elif self.strategy == QueueStrategy.DEADLINE:
            # Deadline-first
            deadline_items = [i for i in self.heap if i.deadline]
            no_deadline = [i for i in self.heap if not i.deadline]
            
            deadline_sorted = sorted(deadline_items, key=lambda x: x.deadline)
            return (deadline_sorted + no_deadline)[:limit]
        
        elif self.strategy == QueueStrategy.FAIR:
            # Fair scheduling - round robin by category
            candidates = []
            categories = list(set(item.category for item in self.heap))
            
            for category in sorted(categories, 
                                 key=lambda c: self.category_last_served.get(c, datetime.min)):
                category_items = [i for i in self.heap if i.category == category]
                if category_items:
                    candidates.append(max(category_items, key=lambda x: x.priority_score))
            
            return candidates[:limit]
        
        elif self.strategy == QueueStrategy.ADAPTIVE:
            # Adaptive - adjust based on current load
            queue_depth = len(self.heap)
            
            if queue_depth > 100:
                # High load - focus on critical items
                critical = [i for i in self.heap if i.priority_class == PriorityClass.CRITICAL]
                return (critical + self.heap)[:limit]
            else:
                # Normal load - balanced approach
                return heapq.nsmallest(limit, self.heap)
        
        else:  # HYBRID
            # ML-based hybrid - consider multiple factors
            now = datetime.now()
            
            def hybrid_score(item: QueueItem) -> float:
                score = item.priority_score
                
                # Deadline boost
                if item.deadline:
                    time_to_deadline = (item.deadline - now).total_seconds()
                    if time_to_deadline < 3600:
                        score += 0.3
                    elif time_to_deadline < 0:
                        score += 0.5  # Past deadline
                
                # Age boost (prevent starvation)
                age_hours = (now - item.created_at).total_seconds() / 3600
                score += min(0.2, age_hours * 0.02)
                
                # Fair scheduling boost
                last_served = self.category_last_served.get(item.category)
                if last_served:
                    since_served = (now - last_served).total_seconds()
                    score += min(0.1, since_served * 0.001)
                
                return score
            
            return sorted(self.heap, key=hybrid_score, reverse=True)[:limit]
    
    def _select_best(self, candidates: List[QueueItem]) -> Tuple[QueueItem, str, float]:
        """Select the best candidate with reasoning."""
        if not candidates:
            return None, "", 0.0
        
        now = datetime.now()
        best = candidates[0]
        reason = "Highest priority score"
        confidence = 0.8
        
        # Check for deadline urgency
        for item in candidates:
            if item.deadline and item.deadline < now + timedelta(minutes=30):
                if item.deadline < now:
                    best = item
                    reason = "Past deadline - urgent processing"
                    confidence = 0.95
                    break
                elif not best.deadline or item.deadline < best.deadline:
                    best = item
                    reason = "Approaching deadline"
                    confidence = 0.9
        
        # Check for critical priority override
        for item in candidates:
            if item.priority_class == PriorityClass.CRITICAL:
                if best.priority_class != PriorityClass.CRITICAL:
                    best = item
                    reason = "Critical priority classification"
                    confidence = 0.92
                    break
        
        # Check for starvation (items waiting too long)
        for item in candidates:
            age_hours = (now - item.created_at).total_seconds() / 3600
            if age_hours > 4:  # More than 4 hours old
                best = item
                reason = f"Starvation prevention (waiting {age_hours:.1f}h)"
                confidence = 0.85
                break
        
        return best, reason, confidence
    
    def complete(self, item_id: str, processing_time_ms: float):
        """Mark an item as completed."""
        if item_id not in self.processing:
            return
        
        item = self.processing.pop(item_id)
        
        # Record completion
        self.completed.append({
            "item": item,
            "completed_at": datetime.now(),
            "processing_time_ms": processing_time_ms
        })
        
        # Update metrics
        self.processing_times.append(processing_time_ms)
        self.analytics["total_processed"] += 1
        
        # Check deadline violation
        if item.deadline and datetime.now() > item.deadline:
            self.analytics["deadline_violations"] += 1
        
        # Update category counts
        self.category_counts[item.category] = max(0, 
            self.category_counts.get(item.category, 0) - 1)
        
        # Clean up lookups
        if item.item_id in self.item_lookup:
            del self.item_lookup[item.item_id]
        if item.email_id in self.email_lookup:
            del self.email_lookup[item.email_id]
        
        logger.debug(f"Completed item {item_id} in {processing_time_ms:.1f}ms")
    
    def fail(self, item_id: str, reason: str, requeue: bool = True):
        """Mark an item as failed."""
        if item_id not in self.processing:
            return
        
        item = self.processing.pop(item_id)
        item.attempts += 1
        item.last_attempt = datetime.now()
        
        if requeue and item.attempts < 3:
            # Reduce priority slightly on retry
            item.priority_score *= 0.9
            heapq.heappush(self.heap, item)
            logger.warning(f"Requeued item {item_id} after failure (attempt {item.attempts})")
        else:
            self.failed.append({
                "item": item,
                "failed_at": datetime.now(),
                "reason": reason,
                "attempts": item.attempts
            })
            self.analytics["total_failed"] += 1
            
            # Clean up lookups
            if item.item_id in self.item_lookup:
                del self.item_lookup[item.item_id]
            if item.email_id in self.email_lookup:
                del self.email_lookup[item.email_id]
    
    def reprioritize(self, email_id: str, new_urgency: float):
        """Reprioritize an item in the queue."""
        if email_id not in self.email_lookup:
            return False
        
        item_id = self.email_lookup[email_id]
        
        # Find and update item
        for item in self.heap:
            if item.item_id == item_id:
                old_score = item.priority_score
                
                # Recalculate score
                new_score, features = self.scorer.calculate_score(
                    email_id=email_id,
                    urgency=new_urgency,
                    sender_importance=item.sender_importance,
                    deadline=item.deadline,
                    category=item.category,
                    created_at=item.created_at
                )
                
                item.priority_score = new_score
                item.metadata["score_features"] = features
                
                # Re-heapify
                heapq.heapify(self.heap)
                
                if old_score != new_score:
                    self.analytics["reorderings"] += 1
                
                logger.debug(f"Reprioritized {email_id}: {old_score:.3f} -> {new_score:.3f}")
                return True
        
        return False
    
    def get_batch(self, max_items: int = 10, max_time_ms: float = 5000) -> List[QueueItem]:
        """Get a batch of items for efficient processing."""
        batch = []
        total_time = 0
        
        while (len(batch) < max_items and 
               total_time < max_time_ms and 
               self.heap):
            
            decision = self.dequeue()
            if decision:
                batch.append(decision.item)
                total_time += decision.item.processing_time_estimate_ms
        
        return batch
    
    def set_strategy(self, strategy: QueueStrategy):
        """Change the scheduling strategy."""
        old_strategy = self.strategy
        self.strategy = strategy
        logger.info(f"Queue strategy changed: {old_strategy.value} -> {strategy.value}")
    
    def get_metrics(self) -> QueueMetrics:
        """Get current queue metrics."""
        # Items by priority
        items_by_priority = {}
        for item in self.heap:
            pc = item.priority_class.value
            items_by_priority[pc] = items_by_priority.get(pc, 0) + 1
        
        # Average wait time
        avg_wait = statistics.mean(self.wait_times) if self.wait_times else 0
        
        # Average processing time
        avg_proc = statistics.mean(self.processing_times) if self.processing_times else 0
        
        # Throughput
        now = datetime.now()
        recent_completed = [
            c for c in self.completed
            if c["completed_at"] > now - timedelta(minutes=1)
        ]
        throughput = len(recent_completed)
        
        # Queue depth trend
        if len(self.heap) > self.analytics.get("last_queue_depth", 0):
            trend = "growing"
        elif len(self.heap) < self.analytics.get("last_queue_depth", 0):
            trend = "shrinking"
        else:
            trend = "stable"
        self.analytics["last_queue_depth"] = len(self.heap)
        
        return QueueMetrics(
            total_items=len(self.heap),
            items_by_priority=items_by_priority,
            avg_wait_time_ms=avg_wait,
            avg_processing_time_ms=avg_proc,
            deadline_violations=self.analytics["deadline_violations"],
            throughput_per_minute=throughput,
            queue_depth_trend=trend
        )
    
    def get_queue_state(self) -> Dict[str, Any]:
        """Get current queue state."""
        return {
            "strategy": self.strategy.value,
            "queue_depth": len(self.heap),
            "processing_count": len(self.processing),
            "items": [item.to_dict() for item in heapq.nsmallest(20, self.heap)],
            "processing": [item.to_dict() for item in self.processing.values()],
            "metrics": {
                "total_enqueued": self.analytics["total_enqueued"],
                "total_processed": self.analytics["total_processed"],
                "total_failed": self.analytics["total_failed"],
                "deadline_violations": self.analytics["deadline_violations"]
            },
            "scorer_weights": self.scorer.feature_weights
        }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get queue analytics."""
        metrics = self.get_metrics()
        
        return {
            "current_metrics": {
                "total_items": metrics.total_items,
                "items_by_priority": metrics.items_by_priority,
                "avg_wait_time_ms": metrics.avg_wait_time_ms,
                "avg_processing_time_ms": metrics.avg_processing_time_ms,
                "throughput_per_minute": metrics.throughput_per_minute,
                "queue_depth_trend": metrics.queue_depth_trend
            },
            "historical": self.analytics,
            "strategy": self.strategy.value,
            "scorer": {
                "weights": self.scorer.feature_weights,
                "category_priorities": self.scorer.category_priorities
            }
        }


# Factory function
def create_priority_queue(environment_ref=None) -> IntelligentPriorityQueue:
    """Create priority queue instance."""
    return IntelligentPriorityQueue(environment_ref)


# Global instance
priority_queue = IntelligentPriorityQueue()
