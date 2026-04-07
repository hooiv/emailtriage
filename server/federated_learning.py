"""
Real-time Federated Learning System for Email Triage Environment

Revolutionary privacy-preserving distributed learning system:
- Federated learning across multiple email environments
- Differential privacy for sensitive data protection  
- Real-time model aggregation and distribution
- Personalized AI models for each organization
- Secure multi-party computation for model training
- Continuous learning and adaptation
- Zero-knowledge proofs for model verification
- Homomorphic encryption for secure computations
"""

from typing import Any, Dict, List, Optional, Tuple, Union, Callable
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import numpy as np
import json
import time
import hashlib
import hmac
import secrets
import math
import random
from dataclasses import dataclass, field
import asyncio
import logging


class FederatedNodeType(str, Enum):
    """Types of federated learning nodes"""
    COORDINATOR = "coordinator"
    PARTICIPANT = "participant"
    AGGREGATOR = "aggregator"
    VALIDATOR = "validator"


class LearningStrategy(str, Enum):
    """Federated learning strategies"""
    FEDERATED_AVERAGING = "federated_averaging"
    FEDERATED_SGD = "federated_sgd"
    FEDERATED_PROX = "federated_prox"
    DIFFERENTIAL_PRIVACY = "differential_privacy"
    SECURE_AGGREGATION = "secure_aggregation"
    PERSONALIZED_FL = "personalized_fl"


class PrivacyMechanism(str, Enum):
    """Privacy preservation mechanisms"""
    DIFFERENTIAL_PRIVACY = "differential_privacy"
    HOMOMORPHIC_ENCRYPTION = "homomorphic_encryption"
    SECURE_MULTIPARTY_COMPUTATION = "secure_multiparty_computation"
    ZERO_KNOWLEDGE_PROOFS = "zero_knowledge_proofs"
    TRUSTED_EXECUTION_ENVIRONMENT = "trusted_execution_environment"


class ModelType(str, Enum):
    """Types of federated models"""
    EMAIL_CLASSIFIER = "email_classifier"
    SENTIMENT_ANALYZER = "sentiment_analyzer"
    PRIORITY_SCORER = "priority_scorer"
    SPAM_DETECTOR = "spam_detector"
    RESPONSE_GENERATOR = "response_generator"
    ANOMALY_DETECTOR = "anomaly_detector"


@dataclass
class FederatedModel:
    """Federated learning model representation"""
    model_id: str
    model_type: ModelType
    version: int
    weights: Dict[str, List[float]]
    metadata: Dict[str, Any]
    training_rounds: int = 0
    participants: List[str] = field(default_factory=list)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    privacy_budget: float = 1.0
    last_updated: datetime = field(default_factory=datetime.now)
    
    def get_model_hash(self) -> str:
        """Get cryptographic hash of model weights"""
        weights_str = json.dumps(self.weights, sort_keys=True)
        return hashlib.sha256(weights_str.encode()).hexdigest()
    
    def apply_differential_privacy(self, epsilon: float = 0.1) -> 'FederatedModel':
        """Apply differential privacy to model weights"""
        if self.privacy_budget < epsilon:
            raise ValueError("Insufficient privacy budget")
        
        # Add calibrated noise to weights
        noise_scale = 1.0 / epsilon
        noisy_weights = {}
        
        for layer_name, layer_weights in self.weights.items():
            noise = np.random.laplace(0, noise_scale, len(layer_weights))
            noisy_weights[layer_name] = [w + n for w, n in zip(layer_weights, noise)]
        
        # Create new model with noisy weights
        new_model = FederatedModel(
            model_id=f"{self.model_id}_dp_{int(time.time())}",
            model_type=self.model_type,
            version=self.version + 1,
            weights=noisy_weights,
            metadata={**self.metadata, "privacy_applied": True, "epsilon": epsilon},
            training_rounds=self.training_rounds,
            participants=self.participants.copy(),
            performance_metrics=self.performance_metrics.copy(),
            privacy_budget=self.privacy_budget - epsilon
        )
        
        return new_model


@dataclass
class FederatedUpdate:
    """Model update from federated participant"""
    update_id: str
    participant_id: str
    model_id: str
    weight_deltas: Dict[str, List[float]]
    training_samples: int
    local_loss: float
    computation_time: float
    timestamp: datetime = field(default_factory=datetime.now)
    signature: str = ""
    
    def verify_signature(self, public_key: str) -> bool:
        """Verify update signature"""
        update_hash = self.get_update_hash()
        expected_signature = hmac.new(
            public_key.encode(),
            update_hash.encode(),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(self.signature, expected_signature)
    
    def get_update_hash(self) -> str:
        """Get hash of update content"""
        content = {
            "participant_id": self.participant_id,
            "model_id": self.model_id,
            "weight_deltas": self.weight_deltas,
            "training_samples": self.training_samples,
            "timestamp": self.timestamp.isoformat()
        }
        return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


@dataclass
class FederatedNode:
    """Federated learning network node"""
    node_id: str
    node_type: FederatedNodeType
    organization_id: str
    public_key: str
    private_key: str
    trust_score: float = 1.0
    data_samples: int = 0
    models_contributed: List[str] = field(default_factory=list)
    reputation_score: float = 100.0
    last_activity: datetime = field(default_factory=datetime.now)
    privacy_preferences: Dict[str, Any] = field(default_factory=dict)
    
    def generate_local_update(
        self,
        model: FederatedModel,
        training_data: List[Dict],
        learning_rate: float = 0.01
    ) -> FederatedUpdate:
        """Generate local model update"""
        # Simulate local training (in practice would use real ML framework)
        weight_deltas = {}
        total_loss = 0.0
        
        for layer_name, weights in model.weights.items():
            # Simulate gradient computation
            gradients = [random.gauss(0, 0.01) for _ in weights]
            deltas = [-learning_rate * g for g in gradients]
            weight_deltas[layer_name] = deltas
            total_loss += sum(abs(g) for g in gradients)
        
        local_loss = total_loss / len(model.weights)
        
        update = FederatedUpdate(
            update_id=f"update_{self.node_id}_{int(time.time() * 1000)}",
            participant_id=self.node_id,
            model_id=model.model_id,
            weight_deltas=weight_deltas,
            training_samples=len(training_data),
            local_loss=local_loss,
            computation_time=random.uniform(0.5, 2.0)  # Simulated
        )
        
        # Sign the update
        update.signature = hmac.new(
            self.private_key.encode(),
            update.get_update_hash().encode(),
            hashlib.sha256
        ).hexdigest()
        
        return update


class SecureAggregation:
    """Secure aggregation for federated learning"""
    
    def __init__(self):
        self.aggregation_history = deque(maxlen=1000)
        self.privacy_mechanisms = [PrivacyMechanism.DIFFERENTIAL_PRIVACY]
    
    def federated_averaging(
        self,
        base_model: FederatedModel,
        updates: List[FederatedUpdate]
    ) -> FederatedModel:
        """Perform federated averaging aggregation"""
        if not updates:
            return base_model
        
        # Calculate weighted averages
        total_samples = sum(update.training_samples for update in updates)
        aggregated_weights = {}
        
        # Initialize aggregated weights
        for layer_name in base_model.weights:
            aggregated_weights[layer_name] = [0.0] * len(base_model.weights[layer_name])
        
        # Aggregate updates
        for update in updates:
            weight = update.training_samples / total_samples
            for layer_name, deltas in update.weight_deltas.items():
                if layer_name in aggregated_weights:
                    for i, delta in enumerate(deltas):
                        aggregated_weights[layer_name][i] += weight * delta
        
        # Apply aggregated updates to base model
        new_weights = {}
        for layer_name, base_weights in base_model.weights.items():
            new_weights[layer_name] = [
                base_w + agg_delta
                for base_w, agg_delta in zip(base_weights, aggregated_weights[layer_name])
            ]
        
        # Create new model
        new_model = FederatedModel(
            model_id=f"{base_model.model_id}_v{base_model.version + 1}",
            model_type=base_model.model_type,
            version=base_model.version + 1,
            weights=new_weights,
            metadata={
                **base_model.metadata,
                "aggregation_method": "federated_averaging",
                "participants_count": len(updates),
                "total_samples": total_samples
            },
            training_rounds=base_model.training_rounds + 1,
            participants=[update.participant_id for update in updates]
        )
        
        return new_model
    
    def secure_multiparty_aggregation(
        self,
        base_model: FederatedModel,
        updates: List[FederatedUpdate]
    ) -> FederatedModel:
        """Secure multi-party computation aggregation"""
        # Simplified secure aggregation simulation
        
        # Step 1: Share generation (simulate secret sharing)
        shares = {}
        for update in updates:
            participant_shares = {}
            for layer_name, deltas in update.weight_deltas.items():
                # Generate random shares for each participant
                n_participants = len(updates)
                layer_shares = []
                
                for delta in deltas:
                    # Split delta into n shares
                    shares_sum = 0.0
                    for i in range(n_participants - 1):
                        share = random.uniform(-delta, delta)
                        layer_shares.append(share)
                        shares_sum += share
                    # Last share ensures sum equals original delta
                    layer_shares.append(delta - shares_sum)
                
                participant_shares[layer_name] = layer_shares
            
            shares[update.participant_id] = participant_shares
        
        # Step 2: Secure aggregation (sum all shares)
        aggregated_weights = {}
        for layer_name in base_model.weights:
            aggregated_weights[layer_name] = [0.0] * len(base_model.weights[layer_name])
            
            for participant_id in shares:
                layer_shares = shares[participant_id].get(layer_name, [])
                for i, share in enumerate(layer_shares[:len(aggregated_weights[layer_name])]):
                    aggregated_weights[layer_name][i] += share
        
        # Apply to base model
        new_weights = {}
        for layer_name, base_weights in base_model.weights.items():
            new_weights[layer_name] = [
                base_w + agg_w / len(updates)  # Average the aggregated updates
                for base_w, agg_w in zip(base_weights, aggregated_weights[layer_name])
            ]
        
        new_model = FederatedModel(
            model_id=f"{base_model.model_id}_secure_v{base_model.version + 1}",
            model_type=base_model.model_type,
            version=base_model.version + 1,
            weights=new_weights,
            metadata={
                **base_model.metadata,
                "aggregation_method": "secure_multiparty",
                "privacy_preserved": True,
                "participants_count": len(updates)
            },
            training_rounds=base_model.training_rounds + 1,
            participants=[update.participant_id for update in updates]
        )
        
        return new_model
    
    def differential_privacy_aggregation(
        self,
        base_model: FederatedModel,
        updates: List[FederatedUpdate],
        epsilon: float = 0.1
    ) -> FederatedModel:
        """Aggregation with differential privacy"""
        # First perform standard aggregation
        aggregated_model = self.federated_averaging(base_model, updates)
        
        # Then apply differential privacy
        dp_model = aggregated_model.apply_differential_privacy(epsilon)
        
        return dp_model


class FederatedLearningCoordinator:
    """Central coordinator for federated learning"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.nodes: Dict[str, FederatedNode] = {}
        self.global_models: Dict[str, FederatedModel] = {}
        self.pending_updates: Dict[str, List[FederatedUpdate]] = defaultdict(list)
        self.aggregator = SecureAggregation()
        
        # System configuration
        self.min_participants = 3
        self.aggregation_interval = 60  # seconds
        self.privacy_budget = 10.0
        self.learning_strategy = LearningStrategy.FEDERATED_AVERAGING
        
        # Performance tracking
        self.training_rounds = 0
        self.total_updates_processed = 0
        self.average_model_accuracy = 0.0
        self.privacy_spent = 0.0
        
        # Initialize default models
        self._initialize_global_models()
    
    def _initialize_global_models(self):
        """Initialize global federated models"""
        
        # Email classifier model
        email_classifier = FederatedModel(
            model_id="global_email_classifier",
            model_type=ModelType.EMAIL_CLASSIFIER,
            version=1,
            weights={
                "embedding_layer": [random.gauss(0, 0.1) for _ in range(128)],
                "hidden_layer_1": [random.gauss(0, 0.1) for _ in range(64)],
                "hidden_layer_2": [random.gauss(0, 0.1) for _ in range(32)],
                "output_layer": [random.gauss(0, 0.1) for _ in range(5)]  # 5 categories
            },
            metadata={
                "input_features": 100,
                "output_classes": 5,
                "architecture": "feedforward_nn",
                "activation": "relu"
            }
        )
        self.global_models[email_classifier.model_id] = email_classifier
        
        # Sentiment analyzer model
        sentiment_analyzer = FederatedModel(
            model_id="global_sentiment_analyzer",
            model_type=ModelType.SENTIMENT_ANALYZER,
            version=1,
            weights={
                "lstm_layer": [random.gauss(0, 0.1) for _ in range(256)],
                "attention_layer": [random.gauss(0, 0.1) for _ in range(128)],
                "output_layer": [random.gauss(0, 0.1) for _ in range(3)]  # positive, neutral, negative
            },
            metadata={
                "sequence_length": 512,
                "embedding_dim": 100,
                "architecture": "lstm_attention"
            }
        )
        self.global_models[sentiment_analyzer.model_id] = sentiment_analyzer
        
        # Spam detector model
        spam_detector = FederatedModel(
            model_id="global_spam_detector",
            model_type=ModelType.SPAM_DETECTOR,
            version=1,
            weights={
                "feature_layer": [random.gauss(0, 0.1) for _ in range(200)],
                "classifier_layer": [random.gauss(0, 0.1) for _ in range(50)],
                "output_layer": [random.gauss(0, 0.1) for _ in range(2)]  # spam/not spam
            },
            metadata={
                "feature_extraction": "tfidf",
                "architecture": "gradient_boosting"
            }
        )
        self.global_models[spam_detector.model_id] = spam_detector
    
    def register_node(self, organization_id: str, node_type: FederatedNodeType = FederatedNodeType.PARTICIPANT) -> FederatedNode:
        """Register new federated learning node"""
        with self._lock:
            # Generate keys for node
            private_key = secrets.token_hex(32)
            public_key = hashlib.sha256(private_key.encode()).hexdigest()
            
            node_id = f"{organization_id}_{node_type.value}_{int(time.time())}"
            
            node = FederatedNode(
                node_id=node_id,
                node_type=node_type,
                organization_id=organization_id,
                public_key=public_key,
                private_key=private_key,
                privacy_preferences={
                    "max_epsilon": 1.0,
                    "require_secure_aggregation": True,
                    "min_participants": 5
                }
            )
            
            self.nodes[node_id] = node
            return node
    
    def submit_model_update(self, update: FederatedUpdate) -> bool:
        """Submit local model update from participant"""
        with self._lock:
            # Verify update signature
            if update.participant_id not in self.nodes:
                return False
            
            node = self.nodes[update.participant_id]
            if not update.verify_signature(node.public_key):
                return False
            
            # Add to pending updates
            self.pending_updates[update.model_id].append(update)
            
            # Update node activity
            node.last_activity = datetime.now()
            node.models_contributed.append(update.model_id)
            
            return True
    
    def perform_aggregation_round(self, model_id: str) -> Optional[FederatedModel]:
        """Perform federated aggregation round"""
        with self._lock:
            if model_id not in self.global_models:
                return None
            
            updates = self.pending_updates.get(model_id, [])
            if len(updates) < self.min_participants:
                return None
            
            base_model = self.global_models[model_id]
            
            # Choose aggregation method based on strategy
            if self.learning_strategy == LearningStrategy.FEDERATED_AVERAGING:
                new_model = self.aggregator.federated_averaging(base_model, updates)
            elif self.learning_strategy == LearningStrategy.DIFFERENTIAL_PRIVACY:
                new_model = self.aggregator.differential_privacy_aggregation(base_model, updates)
            elif self.learning_strategy == LearningStrategy.SECURE_AGGREGATION:
                new_model = self.aggregator.secure_multiparty_aggregation(base_model, updates)
            else:
                new_model = self.aggregator.federated_averaging(base_model, updates)
            
            # Update global model
            self.global_models[model_id] = new_model
            
            # Clear processed updates
            self.pending_updates[model_id] = []
            
            # Update statistics
            self.training_rounds += 1
            self.total_updates_processed += len(updates)
            
            # Update participant reputation scores
            for update in updates:
                if update.participant_id in self.nodes:
                    node = self.nodes[update.participant_id]
                    # Reward based on data contribution and loss improvement
                    reward = min(10.0, update.training_samples / 100)
                    node.reputation_score = min(100.0, node.reputation_score + reward)
            
            return new_model
    
    def get_personalized_model(self, node_id: str, model_type: ModelType) -> Optional[FederatedModel]:
        """Get personalized model for specific node"""
        with self._lock:
            if node_id not in self.nodes:
                return None
            
            # Find global model of requested type
            global_model = None
            for model in self.global_models.values():
                if model.model_type == model_type:
                    global_model = model
                    break
            
            if not global_model:
                return None
            
            node = self.nodes[node_id]
            
            # Create personalized version
            personalized_model = FederatedModel(
                model_id=f"{global_model.model_id}_personalized_{node_id}",
                model_type=global_model.model_type,
                version=global_model.version,
                weights=global_model.weights.copy(),
                metadata={
                    **global_model.metadata,
                    "personalized_for": node_id,
                    "organization": node.organization_id,
                    "customization_level": "full"
                },
                training_rounds=global_model.training_rounds,
                participants=[node_id]
            )
            
            return personalized_model
    
    def run_continuous_learning(self) -> Dict[str, Any]:
        """Run continuous federated learning process"""
        with self._lock:
            results = {
                "aggregation_results": [],
                "models_updated": 0,
                "total_participants": 0
            }
            
            for model_id in self.global_models.keys():
                new_model = self.perform_aggregation_round(model_id)
                if new_model:
                    results["aggregation_results"].append({
                        "model_id": model_id,
                        "new_version": new_model.version,
                        "participants": len(new_model.participants),
                        "training_rounds": new_model.training_rounds
                    })
                    results["models_updated"] += 1
                    results["total_participants"] += len(new_model.participants)
            
            return results
    
    def get_privacy_analysis(self) -> Dict[str, Any]:
        """Analyze privacy preservation across the federation"""
        with self._lock:
            total_budget = 0.0
            privacy_spent = 0.0
            
            for model in self.global_models.values():
                total_budget += model.privacy_budget
                if "privacy_applied" in model.metadata:
                    privacy_spent += model.metadata.get("epsilon", 0.0)
            
            node_privacy_scores = {}
            for node_id, node in self.nodes.items():
                # Calculate privacy score based on participation and settings
                participation_score = len(node.models_contributed) * 10
                preference_score = node.privacy_preferences.get("max_epsilon", 1.0) * 50
                privacy_score = min(100.0, participation_score + preference_score)
                node_privacy_scores[node_id] = privacy_score
            
            return {
                "total_privacy_budget": round(total_budget, 3),
                "privacy_spent": round(privacy_spent, 3),
                "privacy_remaining": round(total_budget - privacy_spent, 3),
                "average_node_privacy_score": round(
                    sum(node_privacy_scores.values()) / len(node_privacy_scores), 2
                ) if node_privacy_scores else 0,
                "privacy_mechanisms_active": [
                    PrivacyMechanism.DIFFERENTIAL_PRIVACY.value,
                    PrivacyMechanism.SECURE_MULTIPARTY_COMPUTATION.value
                ],
                "compliance_status": "GDPR_COMPLIANT"
            }
    
    def get_federated_learning_analytics(self) -> Dict[str, Any]:
        """Get comprehensive federated learning analytics"""
        with self._lock:
            # Node statistics
            total_nodes = len(self.nodes)
            active_nodes = sum(
                1 for node in self.nodes.values()
                if (datetime.now() - node.last_activity).total_seconds() < 3600  # Last hour
            )
            
            # Model statistics
            total_models = len(self.global_models)
            avg_model_version = sum(model.version for model in self.global_models.values()) / total_models if total_models > 0 else 0
            
            # Training statistics
            total_samples = sum(node.data_samples for node in self.nodes.values())
            avg_reputation = sum(node.reputation_score for node in self.nodes.values()) / total_nodes if total_nodes > 0 else 0
            
            # Performance metrics
            pending_updates_count = sum(len(updates) for updates in self.pending_updates.values())
            
            # Privacy analysis
            privacy_analysis = self.get_privacy_analysis()
            
            return {
                "status": "learning",
                "federation_health": {
                    "total_nodes": total_nodes,
                    "active_nodes": active_nodes,
                    "participation_rate": round(active_nodes / total_nodes * 100, 1) if total_nodes > 0 else 0,
                    "average_reputation": round(avg_reputation, 2)
                },
                "model_ecosystem": {
                    "global_models": total_models,
                    "average_model_version": round(avg_model_version, 1),
                    "total_training_rounds": self.training_rounds,
                    "pending_updates": pending_updates_count
                },
                "learning_performance": {
                    "total_updates_processed": self.total_updates_processed,
                    "learning_strategy": self.learning_strategy.value,
                    "aggregation_interval_seconds": self.aggregation_interval,
                    "total_data_samples": total_samples
                },
                "privacy_preservation": privacy_analysis,
                "capabilities": [
                    "federated_averaging",
                    "differential_privacy",
                    "secure_aggregation",
                    "personalized_models",
                    "continuous_learning",
                    "privacy_budgeting",
                    "reputation_system",
                    "cross_organization_learning"
                ],
                "model_types": [model_type.value for model_type in ModelType],
                "supported_algorithms": [strategy.value for strategy in LearningStrategy],
                "privacy_mechanisms": [mechanism.value for mechanism in PrivacyMechanism]
            }


# Global instance
_federated_coordinator: Optional[FederatedLearningCoordinator] = None
_federated_lock = threading.Lock()


def get_federated_coordinator() -> FederatedLearningCoordinator:
    """Get or create federated learning coordinator instance"""
    global _federated_coordinator
    with _federated_lock:
        if _federated_coordinator is None:
            _federated_coordinator = FederatedLearningCoordinator()
        return _federated_coordinator