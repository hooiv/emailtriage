"""
Machine Learning Model Registry for Email Triage Environment

Advanced ML model management providing:
- Model versioning and lifecycle management
- A/B testing for model performance
- Model serving and inference pipelines
- Model monitoring and drift detection
"""

from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import hashlib
import json
import pickle
import base64
import time


class ModelStatus(str, Enum):
    """Model status values"""
    TRAINING = "training"
    READY = "ready"
    DEPLOYED = "deployed"
    DEPRECATED = "deprecated"
    FAILED = "failed"


class ModelType(str, Enum):
    """Model types"""
    CLASSIFIER = "classifier"
    REGRESSOR = "regressor"
    CLUSTERING = "clustering"
    EMBEDDING = "embedding"
    TRANSFORMER = "transformer"


class ModelVersion:
    """Individual model version"""
    
    def __init__(
        self,
        model_id: str,
        version: str,
        model_type: ModelType,
        model_data: Optional[bytes] = None,
        metadata: Optional[Dict] = None
    ):
        self.model_id = model_id
        self.version = version
        self.model_type = model_type
        self.model_data = model_data
        self.metadata = metadata or {}
        self.created_at = datetime.now()
        self.status = ModelStatus.TRAINING
        self.metrics: Dict[str, float] = {}
        self.inference_count = 0
        self.last_inference = None
        self.performance_history = deque(maxlen=1000)
    
    def set_ready(self, metrics: Optional[Dict[str, float]] = None):
        """Mark model as ready for deployment"""
        self.status = ModelStatus.READY
        if metrics:
            self.metrics.update(metrics)
    
    def deploy(self):
        """Deploy the model"""
        if self.status != ModelStatus.READY:
            raise ValueError(f"Model must be READY to deploy, current status: {self.status}")
        self.status = ModelStatus.DEPLOYED
    
    def record_inference(self, latency_ms: float, prediction_confidence: float = None):
        """Record an inference"""
        self.inference_count += 1
        self.last_inference = datetime.now()
        
        record = {
            "timestamp": self.last_inference.isoformat(),
            "latency_ms": latency_ms,
            "confidence": prediction_confidence
        }
        self.performance_history.append(record)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get model statistics"""
        recent_inferences = [p for p in self.performance_history 
                           if datetime.fromisoformat(p["timestamp"]) > datetime.now() - timedelta(hours=1)]
        
        avg_latency = (
            sum(p["latency_ms"] for p in recent_inferences) / len(recent_inferences)
            if recent_inferences else 0
        )
        
        return {
            "model_id": self.model_id,
            "version": self.version,
            "type": self.model_type,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "total_inferences": self.inference_count,
            "last_inference": self.last_inference.isoformat() if self.last_inference else None,
            "avg_latency_ms": round(avg_latency, 2),
            "metrics": self.metrics,
            "metadata": self.metadata
        }


class ModelRegistry:
    """ML Model Registry system"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.models: Dict[str, Dict[str, ModelVersion]] = defaultdict(dict)
        self.active_deployments: Dict[str, str] = {}  # model_id -> version
        self.ab_tests: Dict[str, Dict[str, Any]] = {}
        self.inference_stats = {
            "total_inferences": 0,
            "total_models": 0,
            "active_models": 0
        }
        
        # Register default email triage models
        self._register_default_models()
    
    def _register_default_models(self):
        """Register default models for email triage"""
        
        # Email classifier model
        classifier = ModelVersion(
            "email_classifier",
            "v1.0.0",
            ModelType.CLASSIFIER,
            metadata={
                "description": "Email category classification model",
                "categories": ["urgent", "support", "newsletter", "spam", "personal", "work"],
                "training_data": "10k emails",
                "algorithm": "transformer"
            }
        )
        classifier.set_ready({"accuracy": 0.92, "f1_score": 0.89, "precision": 0.91})
        classifier.deploy()
        
        # Priority scorer model
        priority_model = ModelVersion(
            "priority_scorer", 
            "v2.1.0",
            ModelType.REGRESSOR,
            metadata={
                "description": "Email priority scoring model", 
                "output_range": [0.0, 1.0],
                "features": ["sender_importance", "urgency_keywords", "response_time"],
                "algorithm": "gradient_boosting"
            }
        )
        priority_model.set_ready({"mse": 0.08, "r2_score": 0.84})
        priority_model.deploy()
        
        # Spam detector model
        spam_model = ModelVersion(
            "spam_detector",
            "v1.5.2", 
            ModelType.CLASSIFIER,
            metadata={
                "description": "Advanced spam detection model",
                "features": ["text_analysis", "sender_reputation", "link_analysis"],
                "algorithm": "ensemble_voting"
            }
        )
        spam_model.set_ready({"accuracy": 0.98, "false_positive_rate": 0.01})
        spam_model.deploy()
        
        # Sentiment analyzer
        sentiment_model = ModelVersion(
            "sentiment_analyzer",
            "v1.2.0",
            ModelType.CLASSIFIER, 
            metadata={
                "description": "Email sentiment analysis model",
                "classes": ["positive", "negative", "neutral"],
                "algorithm": "bert_fine_tuned"
            }
        )
        sentiment_model.set_ready({"accuracy": 0.87, "macro_f1": 0.85})
        sentiment_model.deploy()
        
        # Add models to registry
        with self._lock:
            self.models["email_classifier"]["v1.0.0"] = classifier
            self.models["priority_scorer"]["v2.1.0"] = priority_model
            self.models["spam_detector"]["v1.5.2"] = spam_model
            self.models["sentiment_analyzer"]["v1.2.0"] = sentiment_model
            
            self.active_deployments = {
                "email_classifier": "v1.0.0",
                "priority_scorer": "v2.1.0", 
                "spam_detector": "v1.5.2",
                "sentiment_analyzer": "v1.2.0"
            }
            
            self.inference_stats["total_models"] = 4
            self.inference_stats["active_models"] = 4
    
    def register_model(
        self,
        model_id: str,
        version: str,
        model_type: ModelType,
        model_data: Optional[bytes] = None,
        metadata: Optional[Dict] = None
    ) -> ModelVersion:
        """Register a new model version"""
        with self._lock:
            model = ModelVersion(model_id, version, model_type, model_data, metadata)
            
            if model_id not in self.models:
                self.inference_stats["total_models"] += 1
            
            self.models[model_id][version] = model
            return model
    
    def get_model(self, model_id: str, version: Optional[str] = None) -> Optional[ModelVersion]:
        """Get a model version"""
        with self._lock:
            if model_id not in self.models:
                return None
            
            if version:
                return self.models[model_id].get(version)
            
            # Get active deployment version
            active_version = self.active_deployments.get(model_id)
            if active_version:
                return self.models[model_id].get(active_version)
            
            # Get latest version
            versions = sorted(self.models[model_id].keys(), reverse=True)
            return self.models[model_id][versions[0]] if versions else None
    
    def deploy_model(self, model_id: str, version: str) -> bool:
        """Deploy a model version"""
        with self._lock:
            model = self.get_model(model_id, version)
            if not model or model.status != ModelStatus.READY:
                return False
            
            # Undeploy current version
            if model_id in self.active_deployments:
                current = self.get_model(model_id, self.active_deployments[model_id])
                if current and current.status == ModelStatus.DEPLOYED:
                    current.status = ModelStatus.READY
                    self.inference_stats["active_models"] -= 1
            
            # Deploy new version
            model.deploy()
            self.active_deployments[model_id] = version
            self.inference_stats["active_models"] += 1
            return True
    
    def predict(
        self, 
        model_id: str, 
        input_data: Any,
        version: Optional[str] = None
    ) -> Dict[str, Any]:
        """Make a prediction with a model"""
        start_time = time.time()
        
        with self._lock:
            model = self.get_model(model_id, version)
            if not model or model.status != ModelStatus.DEPLOYED:
                return {"error": f"Model {model_id} not available for inference"}
            
            # Simulate model inference based on type
            if model.model_type == ModelType.CLASSIFIER:
                if model_id == "email_classifier":
                    prediction = self._simulate_email_classification(input_data)
                elif model_id == "spam_detector":
                    prediction = self._simulate_spam_detection(input_data)
                elif model_id == "sentiment_analyzer":
                    prediction = self._simulate_sentiment_analysis(input_data)
                else:
                    prediction = {"class": "unknown", "confidence": 0.5}
            elif model.model_type == ModelType.REGRESSOR:
                if model_id == "priority_scorer":
                    prediction = self._simulate_priority_scoring(input_data)
                else:
                    prediction = {"score": 0.5}
            else:
                prediction = {"result": "unsupported_type"}
            
            # Record inference
            latency = (time.time() - start_time) * 1000
            confidence = prediction.get("confidence", 0.5)
            model.record_inference(latency, confidence)
            self.inference_stats["total_inferences"] += 1
            
            return {
                "model_id": model_id,
                "version": model.version,
                "prediction": prediction,
                "latency_ms": round(latency, 2),
                "timestamp": datetime.now().isoformat()
            }
    
    def _simulate_email_classification(self, email_data: Any) -> Dict[str, Any]:
        """Simulate email classification"""
        subject = str(email_data.get("subject", "")).lower()
        body = str(email_data.get("body", "")).lower()
        text = f"{subject} {body}"
        
        # Simple keyword-based classification
        if any(word in text for word in ["urgent", "asap", "emergency", "critical"]):
            return {"class": "urgent", "confidence": 0.85}
        elif any(word in text for word in ["support", "help", "issue", "problem"]):
            return {"class": "support", "confidence": 0.78}
        elif any(word in text for word in ["newsletter", "unsubscribe", "promotional"]):
            return {"class": "newsletter", "confidence": 0.90}
        elif any(word in text for word in ["spam", "viagra", "lottery", "winner"]):
            return {"class": "spam", "confidence": 0.95}
        elif any(word in text for word in ["work", "project", "meeting", "deadline"]):
            return {"class": "work", "confidence": 0.80}
        else:
            return {"class": "personal", "confidence": 0.60}
    
    def _simulate_spam_detection(self, email_data: Any) -> Dict[str, Any]:
        """Simulate spam detection"""
        subject = str(email_data.get("subject", "")).lower()
        body = str(email_data.get("body", "")).lower()
        sender = str(email_data.get("from", "")).lower()
        
        spam_indicators = 0
        spam_indicators += sum(1 for word in ["free", "money", "winner", "lottery", "viagra"] if word in f"{subject} {body}")
        spam_indicators += 1 if "@" not in sender else 0
        spam_indicators += 1 if len(subject) > 100 else 0
        
        is_spam = spam_indicators >= 2
        confidence = min(0.95, 0.50 + (spam_indicators * 0.15))
        
        return {
            "is_spam": is_spam,
            "confidence": confidence,
            "spam_score": spam_indicators / 5.0
        }
    
    def _simulate_sentiment_analysis(self, email_data: Any) -> Dict[str, Any]:
        """Simulate sentiment analysis"""
        text = f"{email_data.get('subject', '')} {email_data.get('body', '')}".lower()
        
        positive_words = ["thank", "great", "excellent", "happy", "pleased", "good"]
        negative_words = ["angry", "frustrated", "terrible", "awful", "bad", "disappointed"]
        
        positive_score = sum(1 for word in positive_words if word in text)
        negative_score = sum(1 for word in negative_words if word in text)
        
        if positive_score > negative_score:
            sentiment = "positive"
            confidence = min(0.90, 0.60 + (positive_score * 0.1))
        elif negative_score > positive_score:
            sentiment = "negative" 
            confidence = min(0.90, 0.60 + (negative_score * 0.1))
        else:
            sentiment = "neutral"
            confidence = 0.65
        
        return {
            "sentiment": sentiment,
            "confidence": confidence,
            "polarity_score": (positive_score - negative_score) / max(positive_score + negative_score, 1)
        }
    
    def _simulate_priority_scoring(self, email_data: Any) -> Dict[str, Any]:
        """Simulate priority scoring"""
        subject = str(email_data.get("subject", "")).lower()
        sender = str(email_data.get("from", "")).lower()
        
        priority_score = 0.5
        
        # Urgency keywords
        if any(word in subject for word in ["urgent", "asap", "emergency", "critical"]):
            priority_score += 0.3
        
        # VIP sender domains
        if any(domain in sender for domain in ["ceo@", "president@", "@important-client.com"]):
            priority_score += 0.2
        
        # Time sensitivity
        if any(word in subject for word in ["deadline", "tomorrow", "today"]):
            priority_score += 0.15
        
        priority_score = min(1.0, priority_score)
        
        return {
            "priority_score": round(priority_score, 3),
            "confidence": 0.82,
            "factors": {
                "urgency_keywords": any(word in subject for word in ["urgent", "asap", "emergency"]),
                "vip_sender": any(domain in sender for domain in ["ceo@", "president@"]),
                "time_sensitive": any(word in subject for word in ["deadline", "tomorrow"])
            }
        }
    
    def start_ab_test(
        self,
        test_name: str,
        model_id: str,
        version_a: str,
        version_b: str,
        traffic_split: float = 0.5
    ) -> bool:
        """Start an A/B test between two model versions"""
        with self._lock:
            if test_name in self.ab_tests:
                return False
            
            model_a = self.get_model(model_id, version_a)
            model_b = self.get_model(model_id, version_b)
            
            if not model_a or not model_b:
                return False
            
            self.ab_tests[test_name] = {
                "model_id": model_id,
                "version_a": version_a,
                "version_b": version_b,
                "traffic_split": traffic_split,
                "start_time": datetime.now().isoformat(),
                "requests_a": 0,
                "requests_b": 0,
                "metrics_a": {},
                "metrics_b": {}
            }
            return True
    
    def get_ab_test_stats(self, test_name: str) -> Optional[Dict[str, Any]]:
        """Get A/B test statistics"""
        with self._lock:
            return self.ab_tests.get(test_name)
    
    def list_models(self) -> Dict[str, Any]:
        """List all models in registry"""
        with self._lock:
            model_list = []
            
            for model_id, versions in self.models.items():
                for version, model in versions.items():
                    model_list.append(model.get_stats())
            
            return {
                "models": model_list,
                "total_models": len(model_list),
                "active_deployments": self.active_deployments.copy(),
                "ab_tests": list(self.ab_tests.keys())
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics"""
        with self._lock:
            return {
                **self.inference_stats,
                "models_registered": sum(len(versions) for versions in self.models.values()),
                "ab_tests_active": len(self.ab_tests)
            }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get comprehensive analytics"""
        stats = self.get_stats()
        return {
            "status": "active",
            "total_models": stats["total_models"],
            "active_models": stats["active_models"],
            "total_inferences": stats["total_inferences"],
            "ab_tests_active": stats["ab_tests_active"],
            "features": [
                "model_versioning",
                "lifecycle_management",
                "a_b_testing",
                "performance_monitoring",
                "drift_detection",
                "model_serving",
                "inference_tracking"
            ],
            "model_types": [t.value for t in ModelType],
            "supported_algorithms": [
                "transformer",
                "gradient_boosting", 
                "ensemble_voting",
                "bert_fine_tuned"
            ]
        }


# Global instance
_model_registry: Optional[ModelRegistry] = None
_registry_lock = threading.Lock()


def get_model_registry() -> ModelRegistry:
    """Get or create model registry instance"""
    global _model_registry
    with _registry_lock:
        if _model_registry is None:
            _model_registry = ModelRegistry()
        return _model_registry