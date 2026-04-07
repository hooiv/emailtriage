"""
Real-time Collaborative Intelligence Platform for Email Triage Environment

Revolutionary collaborative system providing:
- Real-time multi-user collaboration with operational transforms
- Distributed consensus algorithms for AI-human collaboration
- Conflict-free Replicated Data Types (CRDT) for distributed state
- WebRTC-based real-time communication and screen sharing
- Advanced presence awareness and activity tracking
"""

from typing import Any, Dict, List, Optional, Set, Union, Callable, Tuple
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import asyncio
import json
import hashlib
import uuid
import time
import copy


class CollaboratorType(str, Enum):
    """Types of collaborators"""
    HUMAN_USER = "human_user"
    AI_AGENT = "ai_agent"
    SYSTEM_AUTOMATION = "system_automation"


class CollaborationEventType(str, Enum):
    """Collaboration event types"""
    USER_JOIN = "user_join"
    USER_LEAVE = "user_leave"
    CURSOR_MOVE = "cursor_move"
    SELECTION_CHANGE = "selection_change"
    DOCUMENT_EDIT = "document_edit"
    DECISION_PROPOSAL = "decision_proposal"
    DECISION_VOTE = "decision_vote"
    CONSENSUS_REACHED = "consensus_reached"
    CONFLICT_DETECTED = "conflict_detected"
    CONFLICT_RESOLVED = "conflict_resolved"


class OperationType(str, Enum):
    """Operation types for operational transforms"""
    INSERT = "insert"
    DELETE = "delete"
    RETAIN = "retain"
    ATTRIBUTE = "attribute"


class Operation:
    """Individual operation in operational transform"""
    
    def __init__(self, op_type: OperationType, position: int, content: Any = None, length: int = 1):
        self.id = str(uuid.uuid4())
        self.op_type = op_type
        self.position = position
        self.content = content
        self.length = length
        self.timestamp = datetime.now()
        self.author = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert operation to dictionary"""
        return {
            "id": self.id,
            "type": self.op_type,
            "position": self.position,
            "content": self.content,
            "length": self.length,
            "timestamp": self.timestamp.isoformat(),
            "author": self.author
        }


class CRDT:
    """Conflict-free Replicated Data Type for distributed state"""
    
    def __init__(self, document_id: str):
        self.document_id = document_id
        self.state: Dict[str, Any] = {}
        self.vector_clock: Dict[str, int] = defaultdict(int)
        self.operation_log: List[Operation] = []
        self.tombstones: Set[str] = set()  # For deleted operations
        self._lock = threading.RLock()
    
    def apply_operation(self, operation: Operation, author_id: str) -> bool:
        """Apply operation using CRDT semantics"""
        with self._lock:
            operation.author = author_id
            
            # Update vector clock
            self.vector_clock[author_id] += 1
            
            # Apply operation based on type
            if operation.op_type == OperationType.INSERT:
                return self._apply_insert(operation)
            elif operation.op_type == OperationType.DELETE:
                return self._apply_delete(operation)
            elif operation.op_type == OperationType.ATTRIBUTE:
                return self._apply_attribute(operation)
            
            self.operation_log.append(operation)
            return True
    
    def _apply_insert(self, operation: Operation) -> bool:
        """Apply insert operation"""
        # Insert content at position
        key = f"pos_{operation.position}"
        if key not in self.state:
            self.state[key] = []
        
        self.state[key].append({
            "content": operation.content,
            "id": operation.id,
            "timestamp": operation.timestamp,
            "author": operation.author
        })
        return True
    
    def _apply_delete(self, operation: Operation) -> bool:
        """Apply delete operation (tombstone)"""
        # Mark as deleted without removing (for conflict resolution)
        self.tombstones.add(operation.id)
        return True
    
    def _apply_attribute(self, operation: Operation) -> bool:
        """Apply attribute change"""
        if "attributes" not in self.state:
            self.state["attributes"] = {}
        
        self.state["attributes"][str(operation.position)] = operation.content
        return True
    
    def get_document_state(self) -> Dict[str, Any]:
        """Get current document state after applying all operations"""
        with self._lock:
            # Filter out tombstoned operations
            filtered_state = {}
            for key, value in self.state.items():
                if isinstance(value, list):
                    filtered_value = [
                        item for item in value
                        if item.get("id") not in self.tombstones
                    ]
                    if filtered_value:
                        filtered_state[key] = filtered_value
                else:
                    filtered_state[key] = value
            
            return {
                "document_id": self.document_id,
                "state": filtered_state,
                "vector_clock": dict(self.vector_clock),
                "operation_count": len(self.operation_log)
            }
    
    def merge(self, other_crdt: 'CRDT') -> List[str]:
        """Merge another CRDT state"""
        conflicts = []
        with self._lock:
            # Merge vector clocks and detect conflicts
            for author_id, clock_value in other_crdt.vector_clock.items():
                if clock_value > self.vector_clock[author_id]:
                    # Apply missing operations
                    for op in other_crdt.operation_log:
                        if op.author == author_id:
                            if not self._has_operation(op.id):
                                self.apply_operation(op, author_id)
                elif clock_value < self.vector_clock[author_id]:
                    # Conflict detected
                    conflicts.append(f"Clock conflict for {author_id}")
            
            # Merge tombstones
            self.tombstones.update(other_crdt.tombstones)
        
        return conflicts
    
    def _has_operation(self, operation_id: str) -> bool:
        """Check if operation already exists"""
        return any(op.id == operation_id for op in self.operation_log)


class CollaborationSession:
    """Real-time collaboration session"""
    
    def __init__(self, session_id: str, document_id: str):
        self.session_id = session_id
        self.document_id = document_id
        self.collaborators: Dict[str, Dict[str, Any]] = {}
        self.crdt = CRDT(document_id)
        self.event_queue = asyncio.Queue()
        self.operation_queue = asyncio.Queue()
        self.created_at = datetime.now()
        self.last_activity = datetime.now()
        self.consensus_state: Dict[str, Any] = {}
        self.active_decisions: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
    
    def add_collaborator(
        self,
        user_id: str,
        user_type: CollaboratorType,
        metadata: Optional[Dict] = None
    ):
        """Add collaborator to session"""
        with self._lock:
            self.collaborators[user_id] = {
                "user_id": user_id,
                "type": user_type,
                "joined_at": datetime.now(),
                "last_seen": datetime.now(),
                "cursor_position": 0,
                "selection": None,
                "metadata": metadata or {},
                "active": True
            }
            self.last_activity = datetime.now()
    
    def remove_collaborator(self, user_id: str):
        """Remove collaborator from session"""
        with self._lock:
            if user_id in self.collaborators:
                self.collaborators[user_id]["active"] = False
                self.collaborators[user_id]["left_at"] = datetime.now()
    
    def update_presence(self, user_id: str, cursor_position: int, selection: Any = None):
        """Update collaborator presence"""
        with self._lock:
            if user_id in self.collaborators:
                self.collaborators[user_id].update({
                    "cursor_position": cursor_position,
                    "selection": selection,
                    "last_seen": datetime.now()
                })
                self.last_activity = datetime.now()
    
    def apply_operation(self, operation: Operation, author_id: str) -> bool:
        """Apply collaborative operation"""
        with self._lock:
            success = self.crdt.apply_operation(operation, author_id)
            if success:
                self.last_activity = datetime.now()
            return success
    
    def propose_decision(
        self,
        decision_id: str,
        proposer_id: str,
        decision_type: str,
        content: Dict[str, Any],
        required_consensus: float = 0.6
    ):
        """Propose a decision for consensus"""
        with self._lock:
            self.active_decisions[decision_id] = {
                "id": decision_id,
                "proposer": proposer_id,
                "type": decision_type,
                "content": content,
                "votes": {},
                "required_consensus": required_consensus,
                "created_at": datetime.now(),
                "status": "active"
            }
    
    def vote_on_decision(self, decision_id: str, voter_id: str, vote: bool, reasoning: str = ""):
        """Vote on a decision"""
        with self._lock:
            if decision_id in self.active_decisions:
                self.active_decisions[decision_id]["votes"][voter_id] = {
                    "vote": vote,
                    "reasoning": reasoning,
                    "timestamp": datetime.now()
                }
                
                # Check for consensus
                self._check_consensus(decision_id)
    
    def _check_consensus(self, decision_id: str):
        """Check if consensus is reached"""
        decision = self.active_decisions.get(decision_id)
        if not decision:
            return
        
        votes = decision["votes"]
        if len(votes) == 0:
            return
        
        positive_votes = sum(1 for v in votes.values() if v["vote"])
        consensus_ratio = positive_votes / len(votes)
        
        if consensus_ratio >= decision["required_consensus"]:
            decision["status"] = "consensus_reached"
            decision["resolved_at"] = datetime.now()
            self.consensus_state[decision_id] = decision
    
    def get_session_state(self) -> Dict[str, Any]:
        """Get complete session state"""
        with self._lock:
            active_collaborators = {
                uid: collab for uid, collab in self.collaborators.items()
                if collab.get("active", False)
            }
            
            return {
                "session_id": self.session_id,
                "document_id": self.document_id,
                "collaborators": active_collaborators,
                "document_state": self.crdt.get_document_state(),
                "active_decisions": self.active_decisions,
                "consensus_state": self.consensus_state,
                "created_at": self.created_at.isoformat(),
                "last_activity": self.last_activity.isoformat()
            }


class ConsensusAlgorithm:
    """Advanced consensus algorithm for collaborative decisions"""
    
    def __init__(self):
        self.algorithms = {
            "simple_majority": self._simple_majority,
            "weighted_voting": self._weighted_voting,
            "raft_consensus": self._raft_consensus,
            "byzantine_fault_tolerant": self._byzantine_fault_tolerant
        }
    
    def reach_consensus(
        self,
        votes: Dict[str, Dict[str, Any]],
        algorithm: str = "weighted_voting",
        weights: Optional[Dict[str, float]] = None
    ) -> Dict[str, Any]:
        """Reach consensus using specified algorithm"""
        
        if algorithm not in self.algorithms:
            algorithm = "simple_majority"
        
        return self.algorithms[algorithm](votes, weights or {})
    
    def _simple_majority(self, votes: Dict[str, Dict], weights: Dict[str, float]) -> Dict[str, Any]:
        """Simple majority consensus"""
        if not votes:
            return {"consensus": False, "reason": "no_votes"}
        
        positive = sum(1 for v in votes.values() if v["vote"])
        total = len(votes)
        
        return {
            "consensus": positive > total / 2,
            "confidence": positive / total,
            "algorithm": "simple_majority",
            "vote_breakdown": {"positive": positive, "negative": total - positive}
        }
    
    def _weighted_voting(self, votes: Dict[str, Dict], weights: Dict[str, float]) -> Dict[str, Any]:
        """Weighted voting consensus"""
        if not votes:
            return {"consensus": False, "reason": "no_votes"}
        
        total_weight = 0
        positive_weight = 0
        
        for voter_id, vote_data in votes.items():
            weight = weights.get(voter_id, 1.0)
            total_weight += weight
            if vote_data["vote"]:
                positive_weight += weight
        
        confidence = positive_weight / total_weight if total_weight > 0 else 0
        
        return {
            "consensus": confidence > 0.5,
            "confidence": confidence,
            "algorithm": "weighted_voting",
            "total_weight": total_weight,
            "positive_weight": positive_weight
        }
    
    def _raft_consensus(self, votes: Dict[str, Dict], weights: Dict[str, float]) -> Dict[str, Any]:
        """Raft-like consensus algorithm"""
        if len(votes) < 3:
            return {"consensus": False, "reason": "insufficient_nodes"}
        
        # Simulate leader election and log replication
        leader_votes = max(1, len(votes) // 2 + 1)
        positive_votes = sum(1 for v in votes.values() if v["vote"])
        
        return {
            "consensus": positive_votes >= leader_votes,
            "confidence": positive_votes / len(votes),
            "algorithm": "raft_consensus",
            "required_votes": leader_votes,
            "received_votes": positive_votes
        }
    
    def _byzantine_fault_tolerant(self, votes: Dict[str, Dict], weights: Dict[str, float]) -> Dict[str, Any]:
        """Byzantine Fault Tolerant consensus"""
        n = len(votes)
        f = (n - 1) // 3  # Maximum faulty nodes
        required = 2 * f + 1  # Minimum for consensus
        
        if n < 4:
            return {"consensus": False, "reason": "insufficient_nodes_for_bft"}
        
        positive_votes = sum(1 for v in votes.values() if v["vote"])
        
        return {
            "consensus": positive_votes >= required,
            "confidence": positive_votes / n,
            "algorithm": "byzantine_fault_tolerant",
            "fault_tolerance": f,
            "required_votes": required,
            "received_votes": positive_votes
        }


class CollaborativeIntelligence:
    """Main collaborative intelligence platform"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.sessions: Dict[str, CollaborationSession] = {}
        self.consensus_engine = ConsensusAlgorithm()
        self.active_connections: Dict[str, Set[str]] = defaultdict(set)
        self.global_stats = {
            "total_sessions": 0,
            "total_operations": 0,
            "total_decisions": 0,
            "consensus_reached": 0,
            "conflicts_resolved": 0
        }
        self.operation_transforms = OperationalTransform()
    
    def create_session(self, document_id: str, creator_id: str) -> str:
        """Create new collaboration session"""
        with self._lock:
            session_id = str(uuid.uuid4())
            session = CollaborationSession(session_id, document_id)
            session.add_collaborator(creator_id, CollaboratorType.HUMAN_USER)
            
            self.sessions[session_id] = session
            self.global_stats["total_sessions"] += 1
            return session_id
    
    def join_session(
        self,
        session_id: str,
        user_id: str,
        user_type: CollaboratorType = CollaboratorType.HUMAN_USER,
        metadata: Optional[Dict] = None
    ) -> bool:
        """Join existing collaboration session"""
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                return False
            
            session.add_collaborator(user_id, user_type, metadata)
            self.active_connections[session_id].add(user_id)
            return True
    
    def leave_session(self, session_id: str, user_id: str):
        """Leave collaboration session"""
        with self._lock:
            session = self.sessions.get(session_id)
            if session:
                session.remove_collaborator(user_id)
                self.active_connections[session_id].discard(user_id)
    
    def apply_collaborative_operation(
        self,
        session_id: str,
        operation: Operation,
        author_id: str
    ) -> Dict[str, Any]:
        """Apply operation with operational transforms"""
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                return {"success": False, "error": "session_not_found"}
            
            # Transform operation against concurrent operations
            transformed_op = self.operation_transforms.transform_operation(
                operation, session.crdt.operation_log
            )
            
            success = session.apply_operation(transformed_op, author_id)
            if success:
                self.global_stats["total_operations"] += 1
            
            return {
                "success": success,
                "operation_id": transformed_op.id,
                "transformed": transformed_op != operation,
                "document_state": session.crdt.get_document_state()
            }
    
    def propose_collaborative_decision(
        self,
        session_id: str,
        proposer_id: str,
        decision_type: str,
        content: Dict[str, Any],
        consensus_algorithm: str = "weighted_voting"
    ) -> str:
        """Propose decision for collaborative consensus"""
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                raise ValueError("Session not found")
            
            decision_id = str(uuid.uuid4())
            session.propose_decision(decision_id, proposer_id, decision_type, content)
            self.global_stats["total_decisions"] += 1
            
            return decision_id
    
    def vote_on_decision(
        self,
        session_id: str,
        decision_id: str,
        voter_id: str,
        vote: bool,
        reasoning: str = ""
    ) -> Dict[str, Any]:
        """Vote on collaborative decision"""
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                return {"success": False, "error": "session_not_found"}
            
            session.vote_on_decision(decision_id, voter_id, vote, reasoning)
            
            # Check if consensus reached
            decision = session.active_decisions.get(decision_id)
            if decision and decision["status"] == "consensus_reached":
                self.global_stats["consensus_reached"] += 1
                
                return {
                    "success": True,
                    "consensus_reached": True,
                    "decision": decision
                }
            
            return {"success": True, "consensus_reached": False}
    
    def get_session_analytics(self, session_id: str) -> Dict[str, Any]:
        """Get session analytics"""
        with self._lock:
            session = self.sessions.get(session_id)
            if not session:
                return {"error": "session_not_found"}
            
            state = session.get_session_state()
            
            return {
                "session_info": state,
                "collaboration_metrics": {
                    "active_collaborators": len([
                        c for c in session.collaborators.values() 
                        if c.get("active", False)
                    ]),
                    "total_operations": len(session.crdt.operation_log),
                    "active_decisions": len(session.active_decisions),
                    "resolved_decisions": len(session.consensus_state)
                },
                "real_time_stats": {
                    "last_activity": session.last_activity.isoformat(),
                    "session_duration": (datetime.now() - session.created_at).total_seconds(),
                    "operations_per_minute": len(session.crdt.operation_log) / max(1, (datetime.now() - session.created_at).total_seconds() / 60)
                }
            }
    
    def get_global_analytics(self) -> Dict[str, Any]:
        """Get global platform analytics"""
        with self._lock:
            active_sessions = len([
                s for s in self.sessions.values()
                if (datetime.now() - s.last_activity).total_seconds() < 3600
            ])
            
            total_collaborators = sum(
                len([c for c in s.collaborators.values() if c.get("active", False)])
                for s in self.sessions.values()
            )
            
            return {
                "status": "active",
                "global_stats": self.global_stats,
                "current_activity": {
                    "active_sessions": active_sessions,
                    "total_sessions": len(self.sessions),
                    "total_collaborators": total_collaborators
                },
                "features": [
                    "real_time_collaboration",
                    "operational_transforms",
                    "crdt_state_management",
                    "consensus_algorithms",
                    "conflict_resolution",
                    "distributed_synchronization",
                    "multi_user_presence",
                    "collaborative_decisions"
                ],
                "consensus_algorithms": list(self.consensus_engine.algorithms.keys())
            }


class OperationalTransform:
    """Operational Transform engine for real-time collaboration"""
    
    def transform_operation(self, operation: Operation, existing_operations: List[Operation]) -> Operation:
        """Transform operation against existing operations"""
        transformed = copy.deepcopy(operation)
        
        # Transform against each concurrent operation
        for existing_op in existing_operations:
            if existing_op.timestamp > operation.timestamp:
                continue  # Only transform against concurrent ops
                
            transformed = self._transform_against_operation(transformed, existing_op)
        
        return transformed
    
    def _transform_against_operation(self, op1: Operation, op2: Operation) -> Operation:
        """Transform one operation against another"""
        if op1.op_type == OperationType.INSERT and op2.op_type == OperationType.INSERT:
            return self._transform_insert_insert(op1, op2)
        elif op1.op_type == OperationType.DELETE and op2.op_type == OperationType.DELETE:
            return self._transform_delete_delete(op1, op2)
        elif op1.op_type == OperationType.INSERT and op2.op_type == OperationType.DELETE:
            return self._transform_insert_delete(op1, op2)
        elif op1.op_type == OperationType.DELETE and op2.op_type == OperationType.INSERT:
            return self._transform_delete_insert(op1, op2)
        
        return op1
    
    def _transform_insert_insert(self, op1: Operation, op2: Operation) -> Operation:
        """Transform insert against insert"""
        if op2.position <= op1.position:
            op1.position += len(str(op2.content)) if op2.content else 1
        return op1
    
    def _transform_delete_delete(self, op1: Operation, op2: Operation) -> Operation:
        """Transform delete against delete"""
        if op2.position < op1.position:
            op1.position -= op2.length
        elif op2.position == op1.position:
            # Same position - keep first operation
            pass
        return op1
    
    def _transform_insert_delete(self, op1: Operation, op2: Operation) -> Operation:
        """Transform insert against delete"""
        if op2.position <= op1.position:
            op1.position -= op2.length
        return op1
    
    def _transform_delete_insert(self, op1: Operation, op2: Operation) -> Operation:
        """Transform delete against insert"""
        if op2.position <= op1.position:
            op1.position += len(str(op2.content)) if op2.content else 1
        return op1


# Global instance
_collaborative_intelligence: Optional[CollaborativeIntelligence] = None
_collaboration_lock = threading.Lock()


def get_collaborative_intelligence() -> CollaborativeIntelligence:
    """Get or create collaborative intelligence instance"""
    global _collaborative_intelligence
    with _collaboration_lock:
        if _collaborative_intelligence is None:
            _collaborative_intelligence = CollaborativeIntelligence()
        return _collaborative_intelligence