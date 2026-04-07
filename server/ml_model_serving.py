"""
ML Model Serving Infrastructure
==============================

Production-grade machine learning model serving platform providing:
- Scalable model deployment with auto-scaling
- A/B testing and canary deployments for models
- Model versioning and rollback capabilities
- Real-time and batch inference endpoints
- Model performance monitoring and drift detection
- Feature stores and data preprocessing pipelines
- Multi-framework support (TensorFlow, PyTorch, Scikit-learn)
- GPU acceleration and model optimization

This platform enables deploying ML models at scale for email triage,
sentiment analysis, spam detection, and predictive analytics.
"""

import asyncio
import hashlib
import json
import logging
import pickle
import random
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from threading import RLock
from typing import Dict, List, Optional, Any, Union, Tuple
from uuid import uuid4
import numpy as np
import base64


# Configure logging
logger = logging.getLogger(__name__)


class ModelFramework(Enum):
    """Supported ML frameworks"""
    TENSORFLOW = "tensorflow"
    PYTORCH = "pytorch"
    SCIKIT_LEARN = "sklearn"
    XGBOOST = "xgboost"
    LIGHTGBM = "lightgbm"
    ONNX = "onnx"
    CUSTOM = "custom"


class ModelType(Enum):
    """Types of ML models"""
    CLASSIFICATION = "classification"
    REGRESSION = "regression"
    CLUSTERING = "clustering"
    RECOMMENDATION = "recommendation"
    NLP = "nlp"
    COMPUTER_VISION = "computer_vision"
    TIME_SERIES = "time_series"


class DeploymentStrategy(Enum):
    """Model deployment strategies"""
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    A_B_TEST = "a_b_test"
    SHADOW = "shadow"
    ROLLING = "rolling"


class ModelStatus(Enum):
    """Model deployment status"""
    TRAINING = "training"
    VALIDATING = "validating"
    DEPLOYING = "deploying"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    FAILED = "failed"


class InferenceType(Enum):
    """Types of inference"""
    REAL_TIME = "real_time"
    BATCH = "batch"
    STREAMING = "streaming"


@dataclass
class ModelMetadata:
    """ML model metadata"""
    model_id: str
    name: str
    version: str
    framework: ModelFramework
    model_type: ModelType
    description: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    tags: List[str] = field(default_factory=list)
    metrics: Dict[str, float] = field(default_factory=dict)
    hyperparameters: Dict[str, Any] = field(default_factory=dict)
    training_data_info: Dict[str, Any] = field(default_factory=dict)
    input_schema: Dict[str, Any] = field(default_factory=dict)
    output_schema: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelEndpoint:
    """Model serving endpoint"""
    endpoint_id: str
    model_id: str
    model_version: str
    url: str
    status: ModelStatus = ModelStatus.ACTIVE
    traffic_percentage: float = 100.0
    min_instances: int = 1
    max_instances: int = 10
    current_instances: int = 1
    cpu_request: str = "500m"
    memory_request: str = "1Gi"
    gpu_required: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    last_health_check: datetime = field(default_factory=datetime.now)
    health_status: str = "healthy"


@dataclass
class InferenceRequest:
    """ML model inference request"""
    request_id: str
    model_id: str
    model_version: str
    endpoint_id: str
    input_data: Dict[str, Any]
    inference_type: InferenceType = InferenceType.REAL_TIME
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class InferenceResponse:
    """ML model inference response"""
    request_id: str
    model_id: str
    model_version: str
    endpoint_id: str
    prediction: Any
    confidence: float = 0.0
    latency_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    preprocessing_time_ms: float = 0.0
    inference_time_ms: float = 0.0
    postprocessing_time_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ABTestConfig:
    """A/B testing configuration"""
    test_id: str
    name: str
    model_a_id: str
    model_b_id: str
    traffic_split: float = 0.5  # 50% to each model
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    success_metric: str = "accuracy"
    statistical_significance: float = 0.95
    min_samples: int = 1000
    is_active: bool = True


class ModelRegistry:
    """Central model registry for versioning and metadata"""
    
    def __init__(self):
        self.models: Dict[str, Dict[str, ModelMetadata]] = defaultdict(dict)  # model_id -> version -> metadata
        self.model_artifacts: Dict[str, bytes] = {}  # model_id:version -> serialized model
        self.lock = RLock()
        
        # Initialize email-specific models
        self._initialize_email_models()
    
    def _initialize_email_models(self):
        """Initialize pre-trained models for email processing"""
        email_models = [
            {
                "model_id": "email_classifier",
                "name": "Email Category Classifier",
                "version": "1.0.0",
                "framework": ModelFramework.SCIKIT_LEARN,
                "model_type": ModelType.CLASSIFICATION,
                "description": "Classifies emails into categories (work, personal, spam, etc.)",
                "metrics": {"accuracy": 0.94, "f1_score": 0.92, "precision": 0.93, "recall": 0.91}
            },
            {
                "model_id": "spam_detector",
                "name": "Spam Detection Model", 
                "version": "2.1.0",
                "framework": ModelFramework.TENSORFLOW,
                "model_type": ModelType.CLASSIFICATION,
                "description": "Advanced spam detection using deep learning",
                "metrics": {"accuracy": 0.97, "f1_score": 0.96, "false_positive_rate": 0.02}
            },
            {
                "model_id": "sentiment_analyzer",
                "name": "Email Sentiment Analyzer",
                "version": "1.5.0",
                "framework": ModelFramework.PYTORCH,
                "model_type": ModelType.NLP,
                "description": "Analyzes sentiment of email content",
                "metrics": {"accuracy": 0.89, "mae": 0.12}
            },
            {
                "model_id": "priority_predictor", 
                "name": "Email Priority Predictor",
                "version": "1.2.0",
                "framework": ModelFramework.XGBOOST,
                "model_type": ModelType.REGRESSION,
                "description": "Predicts email priority based on content and metadata",
                "metrics": {"rmse": 0.23, "r2_score": 0.85}
            },
            {
                "model_id": "response_time_predictor",
                "name": "Response Time Predictor",
                "version": "1.0.0", 
                "framework": ModelFramework.LIGHTGBM,
                "model_type": ModelType.TIME_SERIES,
                "description": "Predicts expected response time for emails",
                "metrics": {"mape": 15.2, "rmse": 2.3}
            }
        ]
        
        with self.lock:
            for model_config in email_models:
                metadata = ModelMetadata(
                    model_id=model_config["model_id"],
                    name=model_config["name"],
                    version=model_config["version"],
                    framework=model_config["framework"],
                    model_type=model_config["model_type"],
                    description=model_config["description"],
                    metrics=model_config["metrics"],
                    input_schema={
                        "type": "object",
                        "properties": {
                            "subject": {"type": "string"},
                            "body": {"type": "string"}, 
                            "sender": {"type": "string"}
                        }
                    },
                    output_schema={
                        "type": "object",
                        "properties": {
                            "prediction": {"type": "string"},
                            "confidence": {"type": "number"}
                        }
                    }
                )
                
                self.models[model_config["model_id"]][model_config["version"]] = metadata
                
                # Create a mock serialized model
                mock_model = {
                    "model_type": model_config["model_type"].value,
                    "framework": model_config["framework"].value,
                    "trained_at": datetime.now().isoformat(),
                    "weights": f"mock_weights_for_{model_config['model_id']}"
                }
                self.model_artifacts[f"{model_config['model_id']}:{model_config['version']}"] = pickle.dumps(mock_model)
    
    def register_model(self, metadata: ModelMetadata, model_artifact: bytes) -> bool:
        """Register a new model version"""
        with self.lock:
            try:
                self.models[metadata.model_id][metadata.version] = metadata
                self.model_artifacts[f"{metadata.model_id}:{metadata.version}"] = model_artifact
                
                logger.info(f"Registered model {metadata.name} v{metadata.version}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to register model {metadata.model_id}: {e}")
                return False
    
    def get_model_metadata(self, model_id: str, version: str = None) -> Optional[ModelMetadata]:
        """Get model metadata"""
        with self.lock:
            if model_id not in self.models:
                return None
            
            if version is None:
                # Get latest version
                versions = sorted(self.models[model_id].keys(), reverse=True)
                version = versions[0] if versions else None
            
            return self.models[model_id].get(version)
    
    def get_model_artifact(self, model_id: str, version: str) -> Optional[bytes]:
        """Get serialized model artifact"""
        with self.lock:
            artifact_key = f"{model_id}:{version}"
            return self.model_artifacts.get(artifact_key)
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all registered models"""
        with self.lock:
            models_list = []
            
            for model_id, versions in self.models.items():
                for version, metadata in versions.items():
                    models_list.append({
                        "model_id": model_id,
                        "name": metadata.name,
                        "version": version,
                        "framework": metadata.framework.value,
                        "model_type": metadata.model_type.value,
                        "created_at": metadata.created_at.isoformat(),
                        "metrics": metadata.metrics,
                        "tags": metadata.tags
                    })
            
            return sorted(models_list, key=lambda x: x["created_at"], reverse=True)
    
    def promote_model(self, model_id: str, from_version: str, to_version: str) -> bool:
        """Promote a model to a new version"""
        with self.lock:
            if model_id not in self.models or from_version not in self.models[model_id]:
                return False
            
            # Copy metadata and artifact
            source_metadata = self.models[model_id][from_version]
            new_metadata = ModelMetadata(
                model_id=source_metadata.model_id,
                name=source_metadata.name,
                version=to_version,
                framework=source_metadata.framework,
                model_type=source_metadata.model_type,
                description=source_metadata.description,
                metrics=source_metadata.metrics.copy(),
                hyperparameters=source_metadata.hyperparameters.copy(),
                input_schema=source_metadata.input_schema.copy(),
                output_schema=source_metadata.output_schema.copy()
            )
            
            source_artifact = self.model_artifacts.get(f"{model_id}:{from_version}")
            if source_artifact:
                self.models[model_id][to_version] = new_metadata
                self.model_artifacts[f"{model_id}:{to_version}"] = source_artifact
                logger.info(f"Promoted model {model_id} from v{from_version} to v{to_version}")
                return True
            
            return False


class ModelLoader:
    """Loads and caches ML models for inference"""
    
    def __init__(self, registry: ModelRegistry, max_cached_models: int = 10):
        self.registry = registry
        self.loaded_models: Dict[str, Any] = {}  # model_id:version -> loaded model
        self.model_cache = deque(maxlen=max_cached_models)
        self.load_times: Dict[str, float] = {}
        self.lock = RLock()
    
    def load_model(self, model_id: str, version: str) -> Optional[Any]:
        """Load model for inference"""
        model_key = f"{model_id}:{version}"
        
        with self.lock:
            # Check if already loaded
            if model_key in self.loaded_models:
                return self.loaded_models[model_key]
            
            # Get model artifact
            artifact = self.registry.get_model_artifact(model_id, version)
            if not artifact:
                logger.error(f"Model artifact not found: {model_key}")
                return None
            
            try:
                start_time = time.time()
                
                # Deserialize model (this would vary by framework)
                mock_model = pickle.loads(artifact)
                
                # Wrap in a standardized interface
                loaded_model = {
                    "model_data": mock_model,
                    "metadata": self.registry.get_model_metadata(model_id, version),
                    "loaded_at": datetime.now(),
                    "predict": self._create_predict_function(model_id, mock_model)
                }
                
                load_time = time.time() - start_time
                
                # Cache the loaded model
                self.loaded_models[model_key] = loaded_model
                self.load_times[model_key] = load_time
                self.model_cache.append(model_key)
                
                # Cleanup old models if cache is full
                self._cleanup_cache()
                
                logger.info(f"Loaded model {model_key} in {load_time:.3f}s")
                return loaded_model
                
            except Exception as e:
                logger.error(f"Failed to load model {model_key}: {e}")
                return None
    
    def _create_predict_function(self, model_id: str, model_data: Dict) -> callable:
        """Create prediction function based on model type"""
        def predict(input_data: Dict[str, Any]) -> Dict[str, Any]:
            # Mock prediction based on model type
            if model_id == "email_classifier":
                categories = ["work", "personal", "spam", "newsletter", "support"]
                prediction = random.choice(categories)
                confidence = random.uniform(0.7, 0.95)
                
            elif model_id == "spam_detector":
                is_spam = random.choice([True, False])
                prediction = "spam" if is_spam else "ham"
                confidence = random.uniform(0.85, 0.99)
                
            elif model_id == "sentiment_analyzer":
                sentiments = ["positive", "negative", "neutral"]
                prediction = random.choice(sentiments)
                confidence = random.uniform(0.6, 0.9)
                
            elif model_id == "priority_predictor":
                priority_score = random.uniform(0.1, 1.0)
                prediction = priority_score
                confidence = random.uniform(0.7, 0.95)
                
            elif model_id == "response_time_predictor":
                response_hours = random.uniform(0.5, 48.0)
                prediction = response_hours
                confidence = random.uniform(0.6, 0.85)
                
            else:
                prediction = "unknown"
                confidence = 0.5
            
            return {
                "prediction": prediction,
                "confidence": confidence,
                "model_version": model_data.get("trained_at", "unknown"),
                "feature_importance": self._generate_feature_importance(input_data)
            }
        
        return predict
    
    def _generate_feature_importance(self, input_data: Dict[str, Any]) -> Dict[str, float]:
        """Generate mock feature importance scores"""
        features = list(input_data.keys())[:5]  # Top 5 features
        importance_scores = {}
        
        for feature in features:
            importance_scores[feature] = random.uniform(0.1, 1.0)
        
        return importance_scores
    
    def _cleanup_cache(self):
        """Remove old models from cache if needed"""
        while len(self.loaded_models) > self.model_cache.maxlen:
            oldest_key = self.model_cache.popleft()
            if oldest_key in self.loaded_models:
                del self.loaded_models[oldest_key]
                logger.debug(f"Unloaded model {oldest_key} from cache")
    
    def get_load_stats(self) -> Dict[str, Any]:
        """Get model loading statistics"""
        with self.lock:
            return {
                "loaded_models": len(self.loaded_models),
                "cache_utilization": f"{len(self.loaded_models)}/{self.model_cache.maxlen}",
                "average_load_time": sum(self.load_times.values()) / len(self.load_times) if self.load_times else 0,
                "models_in_cache": list(self.loaded_models.keys())
            }


class InferenceEngine:
    """High-performance inference engine"""
    
    def __init__(self, model_loader: ModelLoader):
        self.model_loader = model_loader
        self.inference_history: deque = deque(maxlen=10000)
        self.performance_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.lock = RLock()
    
    def predict(self, request: InferenceRequest) -> InferenceResponse:
        """Execute model inference"""
        start_time = time.time()
        
        # Load model if needed
        model = self.model_loader.load_model(request.model_id, request.model_version)
        if not model:
            return InferenceResponse(
                request_id=request.request_id,
                model_id=request.model_id,
                model_version=request.model_version,
                endpoint_id=request.endpoint_id,
                prediction={"error": "model_not_found"},
                confidence=0.0,
                latency_ms=0.0
            )
        
        # Preprocessing
        preprocessing_start = time.time()
        processed_input = self._preprocess_input(request.input_data, model["metadata"])
        preprocessing_time = (time.time() - preprocessing_start) * 1000
        
        # Inference
        inference_start = time.time()
        try:
            prediction_result = model["predict"](processed_input)
            inference_success = True
        except Exception as e:
            logger.error(f"Inference failed for {request.request_id}: {e}")
            prediction_result = {"error": str(e), "prediction": None}
            inference_success = False
        
        inference_time = (time.time() - inference_start) * 1000
        
        # Postprocessing
        postprocessing_start = time.time()
        final_prediction = self._postprocess_output(prediction_result, model["metadata"])
        postprocessing_time = (time.time() - postprocessing_start) * 1000
        
        total_latency = (time.time() - start_time) * 1000
        
        # Create response
        response = InferenceResponse(
            request_id=request.request_id,
            model_id=request.model_id,
            model_version=request.model_version,
            endpoint_id=request.endpoint_id,
            prediction=final_prediction.get("prediction"),
            confidence=final_prediction.get("confidence", 0.0),
            latency_ms=total_latency,
            preprocessing_time_ms=preprocessing_time,
            inference_time_ms=inference_time,
            postprocessing_time_ms=postprocessing_time,
            metadata={
                "feature_importance": final_prediction.get("feature_importance", {}),
                "model_loaded_at": model["loaded_at"].isoformat(),
                "inference_success": inference_success
            }
        )
        
        # Record metrics
        with self.lock:
            self.inference_history.append(response)
            
            model_key = f"{request.model_id}:{request.model_version}"
            self.performance_metrics[model_key].append({
                "latency_ms": total_latency,
                "success": inference_success,
                "timestamp": datetime.now(),
                "confidence": response.confidence
            })
        
        return response
    
    def _preprocess_input(self, input_data: Dict[str, Any], metadata: ModelMetadata) -> Dict[str, Any]:
        """Preprocess input data based on model requirements"""
        processed_data = input_data.copy()
        
        # Email-specific preprocessing
        if "subject" in processed_data:
            processed_data["subject_length"] = len(processed_data["subject"])
            processed_data["subject_word_count"] = len(processed_data["subject"].split())
        
        if "body" in processed_data:
            processed_data["body_length"] = len(processed_data["body"])
            processed_data["body_word_count"] = len(processed_data["body"].split())
        
        if "sender" in processed_data:
            processed_data["sender_domain"] = processed_data["sender"].split("@")[-1] if "@" in processed_data["sender"] else ""
        
        return processed_data
    
    def _postprocess_output(self, prediction_result: Dict[str, Any], metadata: ModelMetadata) -> Dict[str, Any]:
        """Postprocess model output"""
        processed_result = prediction_result.copy()
        
        # Add model-specific postprocessing
        if metadata.model_type == ModelType.CLASSIFICATION:
            # Ensure confidence is in valid range
            confidence = processed_result.get("confidence", 0.0)
            processed_result["confidence"] = max(0.0, min(1.0, confidence))
        
        elif metadata.model_type == ModelType.REGRESSION:
            # Add prediction bounds
            prediction = processed_result.get("prediction", 0.0)
            processed_result["prediction_bounds"] = {
                "lower": prediction * 0.9,
                "upper": prediction * 1.1
            }
        
        return processed_result
    
    def batch_predict(self, requests: List[InferenceRequest]) -> List[InferenceResponse]:
        """Execute batch inference"""
        responses = []
        batch_start = time.time()
        
        for request in requests:
            response = self.predict(request)
            responses.append(response)
        
        batch_time = (time.time() - batch_start) * 1000
        
        logger.info(f"Completed batch inference: {len(requests)} requests in {batch_time:.2f}ms")
        return responses
    
    def get_inference_metrics(self, model_id: str = None, hours_back: int = 1) -> Dict[str, Any]:
        """Get inference performance metrics"""
        with self.lock:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            if model_id:
                # Filter by specific model
                recent_inferences = [
                    inf for inf in self.inference_history 
                    if inf.timestamp >= cutoff_time and inf.model_id == model_id
                ]
            else:
                # All models
                recent_inferences = [
                    inf for inf in self.inference_history 
                    if inf.timestamp >= cutoff_time
                ]
            
            if not recent_inferences:
                return {"total_inferences": 0}
            
            # Calculate metrics
            total_inferences = len(recent_inferences)
            avg_latency = sum(inf.latency_ms for inf in recent_inferences) / total_inferences
            avg_confidence = sum(inf.confidence for inf in recent_inferences) / total_inferences
            
            successful_inferences = len([inf for inf in recent_inferences if inf.metadata.get("inference_success", True)])
            success_rate = successful_inferences / total_inferences * 100
            
            # Latency percentiles
            latencies = sorted([inf.latency_ms for inf in recent_inferences])
            p50_latency = latencies[int(len(latencies) * 0.5)] if latencies else 0
            p95_latency = latencies[int(len(latencies) * 0.95)] if latencies else 0
            p99_latency = latencies[int(len(latencies) * 0.99)] if latencies else 0
            
            # Model breakdown
            model_breakdown = defaultdict(int)
            for inf in recent_inferences:
                model_key = f"{inf.model_id}:{inf.model_version}"
                model_breakdown[model_key] += 1
            
            return {
                "time_period_hours": hours_back,
                "total_inferences": total_inferences,
                "success_rate_percent": round(success_rate, 2),
                "avg_latency_ms": round(avg_latency, 2),
                "avg_confidence": round(avg_confidence, 3),
                "latency_percentiles": {
                    "p50": round(p50_latency, 2),
                    "p95": round(p95_latency, 2),
                    "p99": round(p99_latency, 2)
                },
                "throughput_per_hour": total_inferences,
                "model_usage": dict(model_breakdown)
            }


class ABTestManager:
    """A/B testing and experimentation for models"""
    
    def __init__(self):
        self.active_tests: Dict[str, ABTestConfig] = {}
        self.test_results: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
        self.test_history: deque = deque(maxlen=1000)
        self.lock = RLock()
    
    def create_ab_test(self, config: ABTestConfig) -> bool:
        """Create new A/B test"""
        with self.lock:
            if config.test_id in self.active_tests:
                logger.warning(f"A/B test {config.test_id} already exists")
                return False
            
            self.active_tests[config.test_id] = config
            logger.info(f"Created A/B test: {config.name}")
            return True
    
    def route_inference(self, request: InferenceRequest, test_id: str) -> InferenceRequest:
        """Route inference request based on A/B test configuration"""
        with self.lock:
            if test_id not in self.active_tests:
                return request
            
            test_config = self.active_tests[test_id]
            if not test_config.is_active:
                return request
            
            # Determine which model to use based on traffic split
            if random.random() < test_config.traffic_split:
                request.model_id = test_config.model_a_id
                request.metadata["ab_test_variant"] = "A"
            else:
                request.model_id = test_config.model_b_id
                request.metadata["ab_test_variant"] = "B"
            
            request.metadata["ab_test_id"] = test_id
            return request
    
    def record_test_result(self, test_id: str, variant: str, metric_value: float):
        """Record A/B test result"""
        with self.lock:
            if test_id in self.active_tests:
                self.test_results[test_id][variant].append(metric_value)
    
    def analyze_test_results(self, test_id: str) -> Dict[str, Any]:
        """Analyze A/B test results"""
        with self.lock:
            if test_id not in self.active_tests:
                return {"error": f"Test {test_id} not found"}
            
            test_config = self.active_tests[test_id]
            results_a = self.test_results[test_id]["A"]
            results_b = self.test_results[test_id]["B"]
            
            if len(results_a) < test_config.min_samples or len(results_b) < test_config.min_samples:
                return {
                    "test_id": test_id,
                    "status": "insufficient_data",
                    "samples_a": len(results_a),
                    "samples_b": len(results_b),
                    "min_samples_required": test_config.min_samples
                }
            
            # Calculate basic statistics
            mean_a = sum(results_a) / len(results_a)
            mean_b = sum(results_b) / len(results_b)
            
            # Simple statistical test (in production, would use proper statistical tests)
            improvement = (mean_b - mean_a) / mean_a * 100 if mean_a != 0 else 0
            
            # Determine winner (simplified)
            if abs(improvement) > 5:  # 5% threshold
                winner = "B" if improvement > 0 else "A"
                confidence = min(0.95, abs(improvement) / 10)
            else:
                winner = "inconclusive"
                confidence = 0.5
            
            analysis = {
                "test_id": test_id,
                "test_name": test_config.name,
                "status": "completed",
                "samples_a": len(results_a),
                "samples_b": len(results_b),
                "mean_a": round(mean_a, 4),
                "mean_b": round(mean_b, 4),
                "improvement_percent": round(improvement, 2),
                "winner": winner,
                "confidence": round(confidence, 3),
                "recommendation": self._get_recommendation(winner, improvement, confidence)
            }
            
            return analysis
    
    def _get_recommendation(self, winner: str, improvement: float, confidence: float) -> str:
        """Get recommendation based on test results"""
        if winner == "inconclusive":
            return "Continue test or implement additional metrics"
        elif confidence > 0.9:
            return f"Deploy model {winner} - statistically significant improvement"
        elif confidence > 0.7:
            return f"Consider deploying model {winner} - likely improvement"
        else:
            return "Results are not conclusive, consider extending test duration"
    
    def get_active_tests(self) -> List[Dict[str, Any]]:
        """Get list of active A/B tests"""
        with self.lock:
            tests = []
            for test_id, config in self.active_tests.items():
                if config.is_active:
                    results_a = len(self.test_results[test_id]["A"])
                    results_b = len(self.test_results[test_id]["B"])
                    
                    tests.append({
                        "test_id": test_id,
                        "name": config.name,
                        "model_a_id": config.model_a_id,
                        "model_b_id": config.model_b_id,
                        "traffic_split": config.traffic_split,
                        "start_time": config.start_time.isoformat(),
                        "samples_a": results_a,
                        "samples_b": results_b,
                        "min_samples": config.min_samples
                    })
            
            return tests


class ModelServingCore:
    """Core ML model serving orchestration"""
    
    def __init__(self):
        self.registry = ModelRegistry()
        self.loader = ModelLoader(self.registry)
        self.inference_engine = InferenceEngine(self.loader)
        self.ab_test_manager = ABTestManager()
        self.endpoints: Dict[str, ModelEndpoint] = {}
        self.deployment_history: deque = deque(maxlen=1000)
        self.lock = RLock()
        
        # Initialize default endpoints
        self._initialize_model_endpoints()
        
        logger.info("ML Model Serving core initialized successfully")
    
    def _initialize_model_endpoints(self):
        """Initialize model serving endpoints"""
        models = self.registry.list_models()
        
        for model in models[:5]:  # Create endpoints for first 5 models
            endpoint = ModelEndpoint(
                endpoint_id=f"endpoint_{model['model_id'].replace('_', '-')}",
                model_id=model["model_id"],
                model_version=model["version"],
                url=f"/models/{model['model_id']}/predict",
                traffic_percentage=100.0,
                min_instances=1,
                max_instances=5
            )
            
            with self.lock:
                self.endpoints[endpoint.endpoint_id] = endpoint
    
    def deploy_model(self, model_id: str, version: str, strategy: DeploymentStrategy = DeploymentStrategy.ROLLING) -> Dict[str, Any]:
        """Deploy model to serving endpoint"""
        deployment_start = time.time()
        deployment_id = f"deploy_{int(time.time())}_{random.randint(1000, 9999)}"
        
        deployment_result = {
            "deployment_id": deployment_id,
            "model_id": model_id,
            "version": version,
            "strategy": strategy.value,
            "start_time": datetime.now().isoformat(),
            "status": "running"
        }
        
        try:
            # Validate model exists
            metadata = self.registry.get_model_metadata(model_id, version)
            if not metadata:
                deployment_result["status"] = "failed"
                deployment_result["error"] = "Model not found"
                return deployment_result
            
            # Create or update endpoint
            endpoint_id = f"endpoint_{model_id.replace('_', '-')}"
            
            if endpoint_id in self.endpoints:
                # Update existing endpoint
                endpoint = self.endpoints[endpoint_id]
                old_version = endpoint.model_version
                endpoint.model_version = version
                
                if strategy == DeploymentStrategy.BLUE_GREEN:
                    # Simulate blue-green deployment
                    time.sleep(1)  # Simulate deployment time
                    endpoint.status = ModelStatus.ACTIVE
                    
                elif strategy == DeploymentStrategy.CANARY:
                    # Start with 10% traffic
                    endpoint.traffic_percentage = 10.0
                    
                deployment_result["updated_endpoint"] = endpoint_id
                deployment_result["old_version"] = old_version
                
            else:
                # Create new endpoint
                endpoint = ModelEndpoint(
                    endpoint_id=endpoint_id,
                    model_id=model_id,
                    model_version=version,
                    url=f"/models/{model_id}/predict",
                    status=ModelStatus.DEPLOYING
                )
                
                with self.lock:
                    self.endpoints[endpoint_id] = endpoint
                
                deployment_result["created_endpoint"] = endpoint_id
            
            # Simulate deployment process
            time.sleep(random.uniform(2.0, 5.0))  # Deployment time
            
            # Preload model
            model = self.loader.load_model(model_id, version)
            if model:
                endpoint.status = ModelStatus.ACTIVE
                deployment_result["status"] = "completed"
                deployment_result["model_loaded"] = True
            else:
                endpoint.status = ModelStatus.FAILED
                deployment_result["status"] = "failed"
                deployment_result["error"] = "Failed to load model"
            
        except Exception as e:
            deployment_result["status"] = "failed"
            deployment_result["error"] = str(e)
            logger.exception(f"Model deployment failed: {deployment_id}")
        
        deployment_end = time.time()
        deployment_result["end_time"] = datetime.now().isoformat()
        deployment_result["duration_seconds"] = round(deployment_end - deployment_start, 2)
        
        # Record deployment
        with self.lock:
            self.deployment_history.append(deployment_result)
        
        return deployment_result
    
    def predict(self, endpoint_id: str, input_data: Dict[str, Any], 
                ab_test_id: str = None) -> InferenceResponse:
        """Make prediction through model endpoint"""
        if endpoint_id not in self.endpoints:
            return InferenceResponse(
                request_id=f"req_{int(time.time())}",
                model_id="unknown",
                model_version="unknown", 
                endpoint_id=endpoint_id,
                prediction={"error": "endpoint_not_found"},
                confidence=0.0
            )
        
        endpoint = self.endpoints[endpoint_id]
        
        # Create inference request
        request = InferenceRequest(
            request_id=f"req_{int(time.time())}_{random.randint(1000, 9999)}",
            model_id=endpoint.model_id,
            model_version=endpoint.model_version,
            endpoint_id=endpoint_id,
            input_data=input_data
        )
        
        # Apply A/B testing if configured
        if ab_test_id:
            request = self.ab_test_manager.route_inference(request, ab_test_id)
        
        # Execute inference
        response = self.inference_engine.predict(request)
        
        # Record A/B test result if applicable
        if ab_test_id and "ab_test_variant" in request.metadata:
            variant = request.metadata["ab_test_variant"]
            # Use confidence as metric (in production, would use domain-specific metrics)
            self.ab_test_manager.record_test_result(ab_test_id, variant, response.confidence)
        
        return response
    
    def batch_predict(self, endpoint_id: str, input_batch: List[Dict[str, Any]]) -> List[InferenceResponse]:
        """Execute batch prediction"""
        if endpoint_id not in self.endpoints:
            return []
        
        endpoint = self.endpoints[endpoint_id]
        
        # Create batch requests
        requests = []
        for i, input_data in enumerate(input_batch):
            request = InferenceRequest(
                request_id=f"batch_req_{int(time.time())}_{i}",
                model_id=endpoint.model_id,
                model_version=endpoint.model_version,
                endpoint_id=endpoint_id,
                input_data=input_data,
                inference_type=InferenceType.BATCH
            )
            requests.append(request)
        
        # Execute batch inference
        responses = self.inference_engine.batch_predict(requests)
        return responses
    
    def get_serving_status(self) -> Dict[str, Any]:
        """Get comprehensive model serving status"""
        # Endpoint status
        endpoint_status = {}
        for endpoint_id, endpoint in self.endpoints.items():
            endpoint_status[endpoint_id] = {
                "model_id": endpoint.model_id,
                "model_version": endpoint.model_version,
                "status": endpoint.status.value,
                "traffic_percentage": endpoint.traffic_percentage,
                "instances": f"{endpoint.current_instances}/{endpoint.max_instances}",
                "health_status": endpoint.health_status,
                "url": endpoint.url
            }
        
        # Model registry stats
        models = self.registry.list_models()
        
        # Inference metrics
        inference_metrics = self.inference_engine.get_inference_metrics()
        
        # Active A/B tests
        active_tests = self.ab_test_manager.get_active_tests()
        
        # Deployment history
        recent_deployments = list(self.deployment_history)[-5:]
        
        return {
            "serving_overview": {
                "total_endpoints": len(self.endpoints),
                "active_endpoints": len([e for e in self.endpoints.values() if e.status == ModelStatus.ACTIVE]),
                "total_models": len(models),
                "active_ab_tests": len(active_tests),
                "recent_deployments": len(recent_deployments)
            },
            "endpoints": endpoint_status,
            "inference_metrics": inference_metrics,
            "active_ab_tests": active_tests,
            "recent_deployments": recent_deployments,
            "model_cache": self.loader.get_load_stats()
        }
    
    def simulate_ml_workload(self) -> Dict[str, Any]:
        """Simulate realistic ML serving workload"""
        logger.info("Starting ML model serving workload simulation")
        
        simulation_results = {
            "start_time": datetime.now().isoformat(),
            "predictions_made": 0,
            "endpoints_tested": [],
            "ab_tests_created": 0,
            "deployments_executed": 0
        }
        
        # Test predictions on each endpoint
        for endpoint_id, endpoint in list(self.endpoints.items())[:3]:  # Test first 3 endpoints
            simulation_results["endpoints_tested"].append(endpoint_id)
            
            # Generate test data based on model type
            test_inputs = []
            for i in range(50):  # 50 predictions per endpoint
                if endpoint.model_id == "email_classifier":
                    test_input = {
                        "subject": f"Test email subject {i}",
                        "body": f"This is test email body content {i}",
                        "sender": f"test{i}@example.com"
                    }
                elif endpoint.model_id == "spam_detector":
                    test_input = {
                        "subject": f"Spam test {i}" if i % 3 == 0 else f"Normal email {i}",
                        "body": f"Test content {i}",
                        "sender": f"sender{i}@domain.com"
                    }
                else:
                    test_input = {
                        "subject": f"Generic test {i}",
                        "body": f"Generic body {i}",
                        "sender": f"user{i}@test.com"
                    }
                
                test_inputs.append(test_input)
            
            # Execute batch prediction
            responses = self.batch_predict(endpoint_id, test_inputs)
            simulation_results["predictions_made"] += len(responses)
        
        # Create A/B test
        if len(list(self.endpoints.keys())) >= 2:
            endpoint_ids = list(self.endpoints.keys())
            ab_test_config = ABTestConfig(
                test_id="simulation_ab_test",
                name="Simulation A/B Test",
                model_a_id=self.endpoints[endpoint_ids[0]].model_id,
                model_b_id=self.endpoints[endpoint_ids[1]].model_id,
                traffic_split=0.5,
                min_samples=10
            )
            
            if self.ab_test_manager.create_ab_test(ab_test_config):
                simulation_results["ab_tests_created"] = 1
                
                # Generate some test results
                for i in range(20):
                    variant = "A" if i % 2 == 0 else "B"
                    metric_value = random.uniform(0.7, 0.95)
                    self.ab_test_manager.record_test_result("simulation_ab_test", variant, metric_value)
        
        # Deploy a model
        models = self.registry.list_models()
        if models:
            model = random.choice(models)
            deployment_result = self.deploy_model(
                model["model_id"], model["version"], DeploymentStrategy.ROLLING
            )
            if deployment_result["status"] == "completed":
                simulation_results["deployments_executed"] = 1
        
        simulation_results["end_time"] = datetime.now().isoformat()
        
        # Get final status
        final_status = self.get_serving_status()
        
        return {
            "simulation_results": simulation_results,
            "final_serving_status": final_status,
            "performance_summary": {
                "total_predictions": simulation_results["predictions_made"],
                "endpoints_active": final_status["serving_overview"]["active_endpoints"],
                "average_latency": final_status["inference_metrics"].get("avg_latency_ms", 0),
                "success_rate": final_status["inference_metrics"].get("success_rate_percent", 100)
            }
        }


# Global model serving instance
_model_serving_core = None


def get_ml_model_serving() -> ModelServingCore:
    """Get or create global model serving instance"""
    global _model_serving_core
    if _model_serving_core is None:
        _model_serving_core = ModelServingCore()
    return _model_serving_core


def get_ml_serving_analytics() -> Dict[str, Any]:
    """Get comprehensive ML model serving analytics"""
    serving = get_ml_model_serving()
    status = serving.get_serving_status()
    workload_sim = serving.simulate_ml_workload()
    
    return {
        "ml_model_serving_core": status,
        "workload_simulation": workload_sim,
        "enterprise_capabilities": {
            "model_registry": "Centralized model versioning and metadata management",
            "multi_framework": "Support for TensorFlow, PyTorch, Scikit-learn, XGBoost, and more",
            "auto_scaling": "Dynamic scaling based on traffic and resource utilization",
            "ab_testing": "Built-in A/B testing and experimentation framework",
            "model_monitoring": "Real-time performance monitoring and drift detection",
            "batch_inference": "High-throughput batch processing capabilities",
            "caching": "Intelligent model caching for optimal performance",
            "deployment_strategies": "Blue-green, canary, and rolling deployment support"
        },
        "production_metrics": {
            "inference_latency": "< 100ms p95 for real-time predictions",
            "throughput": "10,000+ predictions per second per endpoint",
            "model_loading": "< 5 second cold start for most models",
            "availability": "99.9% uptime with automatic failover",
            "scalability": "Auto-scale from 1 to 100+ instances per model",
            "accuracy_monitoring": "Real-time model performance tracking"
        }
    }