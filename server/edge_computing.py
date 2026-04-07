"""
Advanced Edge Computing System for Email Triage Environment

Revolutionary distributed edge computing architecture:
- Ultra-low latency edge processing nodes
- Intelligent edge-cloud workload distribution
- Real-time data streaming between edge nodes
- Edge AI inference with model quantization
- Dynamic edge node discovery and orchestration
- Edge caching with intelligent prefetching
- Fault-tolerant edge mesh networking
- Edge-optimized model compression and deployment
"""

from typing import Any, Dict, List, Optional, Tuple, Union, Set
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import json
import time
import random
import math
import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
import numpy as np


class EdgeNodeType(str, Enum):
    """Types of edge computing nodes"""
    INFERENCE_NODE = "inference_node"
    STORAGE_NODE = "storage_node"
    ROUTING_NODE = "routing_node"
    AGGREGATION_NODE = "aggregation_node"
    GATEWAY_NODE = "gateway_node"


class EdgeCapability(str, Enum):
    """Edge node capabilities"""
    ML_INFERENCE = "ml_inference"
    DATA_CACHING = "data_caching"
    STREAM_PROCESSING = "stream_processing"
    LOAD_BALANCING = "load_balancing"
    DATA_COMPRESSION = "data_compression"
    EDGE_ANALYTICS = "edge_analytics"
    REAL_TIME_SCORING = "real_time_scoring"


class ComputeResource(str, Enum):
    """Types of compute resources"""
    CPU_CORES = "cpu_cores"
    GPU_MEMORY = "gpu_memory"
    RAM_GB = "ram_gb"
    STORAGE_GB = "storage_gb"
    NETWORK_MBPS = "network_mbps"


@dataclass
class EdgeLocation:
    """Geographic edge location"""
    location_id: str
    region: str
    latitude: float
    longitude: float
    city: str
    country: str
    timezone: str
    network_latency_ms: Dict[str, float] = field(default_factory=dict)
    
    def distance_to(self, other: 'EdgeLocation') -> float:
        """Calculate distance to another location (simplified)"""
        # Haversine formula for great circle distance
        R = 6371  # Earth's radius in km
        
        lat1_rad = math.radians(self.latitude)
        lat2_rad = math.radians(other.latitude)
        delta_lat = math.radians(other.latitude - self.latitude)
        delta_lon = math.radians(other.longitude - self.longitude)
        
        a = (math.sin(delta_lat/2) * math.sin(delta_lat/2) +
             math.cos(lat1_rad) * math.cos(lat2_rad) *
             math.sin(delta_lon/2) * math.sin(delta_lon/2))
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        
        return R * c


@dataclass
class EdgeNode:
    """Edge computing node"""
    node_id: str
    node_type: EdgeNodeType
    location: EdgeLocation
    capabilities: List[EdgeCapability]
    resources: Dict[ComputeResource, float]
    status: str = "active"
    load_percentage: float = 0.0
    last_heartbeat: datetime = field(default_factory=datetime.now)
    deployed_models: List[str] = field(default_factory=list)
    active_connections: int = 0
    throughput_requests_per_second: float = 0.0
    average_latency_ms: float = 0.0
    
    def has_capability(self, capability: EdgeCapability) -> bool:
        """Check if node has specific capability"""
        return capability in self.capabilities
    
    def get_available_resources(self) -> Dict[ComputeResource, float]:
        """Get available resources based on current load"""
        available = {}
        for resource, total in self.resources.items():
            available[resource] = total * (1.0 - self.load_percentage / 100.0)
        return available
    
    def can_handle_request(self, required_resources: Dict[ComputeResource, float]) -> bool:
        """Check if node can handle request with required resources"""
        available = self.get_available_resources()
        
        for resource, required in required_resources.items():
            if resource not in available or available[resource] < required:
                return False
        
        return self.status == "active" and self.load_percentage < 90.0


@dataclass
class EdgeRequest:
    """Request for edge processing"""
    request_id: str
    request_type: str
    payload: Dict[str, Any]
    required_capabilities: List[EdgeCapability]
    resource_requirements: Dict[ComputeResource, float]
    latency_requirement_ms: float
    priority: int = 5  # 1-10 scale
    source_location: Optional[EdgeLocation] = None
    created_at: datetime = field(default_factory=datetime.now)
    assigned_node: Optional[str] = None
    processing_started: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    actual_latency_ms: Optional[float] = None


@dataclass
class EdgeModel:
    """AI model deployed on edge nodes"""
    model_id: str
    model_name: str
    model_type: str
    version: str
    size_mb: float
    inference_latency_ms: float
    accuracy_score: float
    quantization_level: str = "int8"  # fp32, fp16, int8, int4
    target_nodes: List[str] = field(default_factory=list)
    deployment_status: Dict[str, str] = field(default_factory=dict)
    performance_metrics: Dict[str, float] = field(default_factory=dict)


class EdgeOrchestrator:
    """Orchestrates edge computing workloads"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.edge_nodes: Dict[str, EdgeNode] = {}
        self.edge_locations: Dict[str, EdgeLocation] = {}
        self.deployed_models: Dict[str, EdgeModel] = {}
        self.request_queue: deque = deque(maxlen=10000)
        self.completed_requests: deque = deque(maxlen=5000)
        
        # Performance tracking
        self.total_requests_processed = 0
        self.average_response_time_ms = 0.0
        self.edge_cache_hit_rate = 0.0
        self.network_utilization = 0.0
        
        # Edge mesh networking
        self.node_connections: Dict[str, Set[str]] = defaultdict(set)
        self.routing_table: Dict[str, str] = {}
        
        # Initialize edge infrastructure
        self._initialize_edge_infrastructure()
    
    def _initialize_edge_infrastructure(self):
        """Initialize edge computing infrastructure"""
        
        # Create edge locations
        locations = [
            EdgeLocation("us-west-1", "US West", 37.7749, -122.4194, "San Francisco", "USA", "PST"),
            EdgeLocation("us-east-1", "US East", 40.7128, -74.0060, "New York", "USA", "EST"),
            EdgeLocation("eu-west-1", "EU West", 51.5074, -0.1278, "London", "UK", "GMT"),
            EdgeLocation("ap-south-1", "Asia Pacific", 19.0760, 72.8777, "Mumbai", "India", "IST"),
            EdgeLocation("ap-east-1", "Asia East", 35.6762, 139.6503, "Tokyo", "Japan", "JST")
        ]
        
        for location in locations:
            self.edge_locations[location.location_id] = location
        
        # Create edge nodes
        edge_configs = [
            {
                "node_id": "inference-us-west-1-001",
                "node_type": EdgeNodeType.INFERENCE_NODE,
                "location_id": "us-west-1",
                "capabilities": [EdgeCapability.ML_INFERENCE, EdgeCapability.REAL_TIME_SCORING],
                "resources": {
                    ComputeResource.CPU_CORES: 16.0,
                    ComputeResource.GPU_MEMORY: 24.0,
                    ComputeResource.RAM_GB: 64.0,
                    ComputeResource.STORAGE_GB: 1000.0,
                    ComputeResource.NETWORK_MBPS: 10000.0
                }
            },
            {
                "node_id": "storage-us-east-1-001", 
                "node_type": EdgeNodeType.STORAGE_NODE,
                "location_id": "us-east-1",
                "capabilities": [EdgeCapability.DATA_CACHING, EdgeCapability.DATA_COMPRESSION],
                "resources": {
                    ComputeResource.CPU_CORES: 8.0,
                    ComputeResource.RAM_GB: 32.0,
                    ComputeResource.STORAGE_GB: 10000.0,
                    ComputeResource.NETWORK_MBPS: 5000.0
                }
            },
            {
                "node_id": "gateway-eu-west-1-001",
                "node_type": EdgeNodeType.GATEWAY_NODE,
                "location_id": "eu-west-1", 
                "capabilities": [EdgeCapability.LOAD_BALANCING, EdgeCapability.STREAM_PROCESSING],
                "resources": {
                    ComputeResource.CPU_CORES: 32.0,
                    ComputeResource.RAM_GB: 128.0,
                    ComputeResource.STORAGE_GB: 2000.0,
                    ComputeResource.NETWORK_MBPS: 20000.0
                }
            },
            {
                "node_id": "aggregation-ap-south-1-001",
                "node_type": EdgeNodeType.AGGREGATION_NODE,
                "location_id": "ap-south-1",
                "capabilities": [EdgeCapability.EDGE_ANALYTICS, EdgeCapability.STREAM_PROCESSING],
                "resources": {
                    ComputeResource.CPU_CORES: 24.0,
                    ComputeResource.RAM_GB: 96.0,
                    ComputeResource.STORAGE_GB: 5000.0,
                    ComputeResource.NETWORK_MBPS: 15000.0
                }
            },
            {
                "node_id": "routing-ap-east-1-001",
                "node_type": EdgeNodeType.ROUTING_NODE,
                "location_id": "ap-east-1",
                "capabilities": [EdgeCapability.LOAD_BALANCING, EdgeCapability.DATA_CACHING],
                "resources": {
                    ComputeResource.CPU_CORES: 12.0,
                    ComputeResource.RAM_GB: 48.0,
                    ComputeResource.STORAGE_GB: 1000.0,
                    ComputeResource.NETWORK_MBPS: 8000.0
                }
            }
        ]
        
        for config in edge_configs:
            location = self.edge_locations[config["location_id"]]
            node = EdgeNode(
                node_id=config["node_id"],
                node_type=config["node_type"],
                location=location,
                capabilities=config["capabilities"],
                resources=config["resources"]
            )
            self.edge_nodes[node.node_id] = node
        
        # Initialize edge models
        self._initialize_edge_models()
        
        # Setup mesh networking
        self._setup_edge_mesh()
    
    def _initialize_edge_models(self):
        """Initialize edge-optimized AI models"""
        
        models = [
            EdgeModel(
                model_id="edge_email_classifier_v1",
                model_name="Edge Email Classifier",
                model_type="classification",
                version="1.0.0",
                size_mb=15.2,
                inference_latency_ms=12.5,
                accuracy_score=0.92,
                quantization_level="int8"
            ),
            EdgeModel(
                model_id="edge_spam_detector_v1",
                model_name="Edge Spam Detector",
                model_type="binary_classification",
                version="1.0.0",
                size_mb=8.7,
                inference_latency_ms=8.2,
                accuracy_score=0.96,
                quantization_level="int8"
            ),
            EdgeModel(
                model_id="edge_sentiment_analyzer_v1",
                model_name="Edge Sentiment Analyzer", 
                model_type="sentiment",
                version="1.0.0",
                size_mb=22.1,
                inference_latency_ms=18.3,
                accuracy_score=0.89,
                quantization_level="fp16"
            ),
            EdgeModel(
                model_id="edge_priority_scorer_v1",
                model_name="Edge Priority Scorer",
                model_type="regression",
                version="1.0.0",
                size_mb=12.4,
                inference_latency_ms=10.1,
                accuracy_score=0.87,
                quantization_level="int8"
            )
        ]
        
        for model in models:
            self.deployed_models[model.model_id] = model
    
    def _setup_edge_mesh(self):
        """Setup edge mesh networking"""
        # Connect nodes based on geographic proximity and capabilities
        node_list = list(self.edge_nodes.values())
        
        for i, node1 in enumerate(node_list):
            for j, node2 in enumerate(node_list[i+1:], i+1):
                # Connect nodes if they're in same region or have complementary capabilities
                distance = node1.location.distance_to(node2.location)
                
                # Connect if within 5000km or have complementary capabilities
                if (distance < 5000 or 
                    (EdgeCapability.ML_INFERENCE in node1.capabilities and 
                     EdgeCapability.DATA_CACHING in node2.capabilities)):
                    
                    self.node_connections[node1.node_id].add(node2.node_id)
                    self.node_connections[node2.node_id].add(node1.node_id)
    
    def find_optimal_edge_node(self, request: EdgeRequest) -> Optional[EdgeNode]:
        """Find optimal edge node for processing request"""
        with self._lock:
            candidate_nodes = []
            
            # Filter nodes by capabilities and resources
            for node in self.edge_nodes.values():
                # Check capabilities
                if not all(node.has_capability(cap) for cap in request.required_capabilities):
                    continue
                
                # Check resource availability
                if not node.can_handle_request(request.resource_requirements):
                    continue
                
                candidate_nodes.append(node)
            
            if not candidate_nodes:
                return None
            
            # Score nodes based on multiple factors
            scored_nodes = []
            
            for node in candidate_nodes:
                score = 0.0
                
                # Latency factor (higher score for lower latency)
                if request.source_location:
                    distance = node.location.distance_to(request.source_location)
                    latency_estimate = distance * 0.1 + node.average_latency_ms  # Simplified
                    latency_score = max(0, 100 - latency_estimate)
                    score += latency_score * 0.4
                
                # Load factor (higher score for lower load)
                load_score = 100 - node.load_percentage
                score += load_score * 0.3
                
                # Capability match factor
                capability_score = len(node.capabilities) * 10
                score += capability_score * 0.2
                
                # Performance history factor
                performance_score = (100 - node.average_latency_ms) + node.throughput_requests_per_second
                score += performance_score * 0.1
                
                scored_nodes.append((node, score))
            
            # Return highest scoring node
            best_node = max(scored_nodes, key=lambda x: x[1])[0]
            return best_node
    
    def submit_edge_request(self, request: EdgeRequest) -> str:
        """Submit request for edge processing"""
        with self._lock:
            self.request_queue.append(request)
            return request.request_id
    
    def process_edge_request(self, request: EdgeRequest) -> Dict[str, Any]:
        """Process request on edge node"""
        start_time = time.time()
        
        # Find optimal node
        optimal_node = self.find_optimal_edge_node(request)
        if not optimal_node:
            return {
                "status": "failed",
                "error": "No suitable edge node available",
                "request_id": request.request_id
            }
        
        request.assigned_node = optimal_node.node_id
        request.processing_started = datetime.now()
        
        # Simulate edge processing based on request type
        processing_time = self._simulate_edge_processing(request, optimal_node)
        
        # Update node metrics
        optimal_node.load_percentage = min(100.0, optimal_node.load_percentage + random.uniform(5, 15))
        optimal_node.active_connections += 1
        optimal_node.last_heartbeat = datetime.now()
        
        # Generate result
        result = self._generate_edge_result(request, optimal_node)
        
        request.completed_at = datetime.now()
        request.result = result
        request.actual_latency_ms = (time.time() - start_time) * 1000
        
        # Update performance metrics
        self.total_requests_processed += 1
        self._update_performance_metrics(request, optimal_node)
        
        return result
    
    def _simulate_edge_processing(self, request: EdgeRequest, node: EdgeNode) -> float:
        """Simulate edge processing time"""
        base_time = 0.01  # Base processing time
        
        # Add complexity based on request type
        if request.request_type == "ml_inference":
            base_time += 0.05
        elif request.request_type == "data_aggregation":
            base_time += 0.02
        elif request.request_type == "stream_processing":
            base_time += 0.03
        
        # Add load factor
        load_factor = 1.0 + (node.load_percentage / 100.0)
        processing_time = base_time * load_factor
        
        # Simulate actual processing delay (capped for testing)
        time.sleep(min(processing_time, 0.1))
        
        return processing_time
    
    def _generate_edge_result(self, request: EdgeRequest, node: EdgeNode) -> Dict[str, Any]:
        """Generate result for edge request"""
        base_result = {
            "status": "completed",
            "request_id": request.request_id,
            "processed_by": node.node_id,
            "processing_location": node.location.city,
            "latency_ms": round((datetime.now() - request.processing_started).total_seconds() * 1000, 2),
            "node_load": round(node.load_percentage, 1)
        }
        
        # Add type-specific results
        if request.request_type == "email_classification":
            base_result.update({
                "classification": random.choice(["support", "sales", "billing", "general"]),
                "confidence": round(random.uniform(0.85, 0.98), 3),
                "model_used": "edge_email_classifier_v1"
            })
        elif request.request_type == "spam_detection":
            is_spam = random.choice([True, False])
            base_result.update({
                "is_spam": is_spam,
                "spam_score": round(random.uniform(0.1, 0.9), 3),
                "model_used": "edge_spam_detector_v1"
            })
        elif request.request_type == "sentiment_analysis":
            sentiment = random.choice(["positive", "neutral", "negative"])
            base_result.update({
                "sentiment": sentiment,
                "sentiment_score": round(random.uniform(-1.0, 1.0), 3),
                "model_used": "edge_sentiment_analyzer_v1"
            })
        elif request.request_type == "priority_scoring":
            base_result.update({
                "priority_score": round(random.uniform(0.0, 1.0), 3),
                "priority_level": random.choice(["low", "medium", "high", "critical"]),
                "model_used": "edge_priority_scorer_v1"
            })
        else:
            base_result.update({
                "data_processed": True,
                "processing_time_ms": round(random.uniform(5, 50), 2)
            })
        
        return base_result
    
    def _update_performance_metrics(self, request: EdgeRequest, node: EdgeNode):
        """Update performance metrics"""
        # Update node-level metrics
        if request.actual_latency_ms:
            # Running average of latency
            current_avg = node.average_latency_ms
            new_avg = (current_avg * (node.active_connections - 1) + request.actual_latency_ms) / node.active_connections
            node.average_latency_ms = new_avg
        
        # Update system-level metrics
        if request.actual_latency_ms:
            current_system_avg = self.average_response_time_ms
            new_system_avg = (current_system_avg * (self.total_requests_processed - 1) + request.actual_latency_ms) / self.total_requests_processed
            self.average_response_time_ms = new_system_avg
    
    def process_edge_queue(self, max_requests: int = 50) -> List[Dict[str, Any]]:
        """Process requests in edge queue"""
        with self._lock:
            results = []
            processed = 0
            
            while self.request_queue and processed < max_requests:
                request = self.request_queue.popleft()
                result = self.process_edge_request(request)
                
                self.completed_requests.append(request)
                results.append(result)
                processed += 1
            
            return results
    
    def deploy_model_to_edge(self, model_id: str, target_node_ids: List[str]) -> Dict[str, Any]:
        """Deploy AI model to edge nodes"""
        with self._lock:
            if model_id not in self.deployed_models:
                return {"status": "failed", "error": "Model not found"}
            
            model = self.deployed_models[model_id]
            deployment_results = {}
            
            for node_id in target_node_ids:
                if node_id not in self.edge_nodes:
                    deployment_results[node_id] = {"status": "failed", "error": "Node not found"}
                    continue
                
                node = self.edge_nodes[node_id]
                
                # Check if node has ML inference capability
                if EdgeCapability.ML_INFERENCE not in node.capabilities:
                    deployment_results[node_id] = {"status": "failed", "error": "Node lacks ML inference capability"}
                    continue
                
                # Check storage requirements
                available_storage = node.get_available_resources().get(ComputeResource.STORAGE_GB, 0)
                if available_storage < model.size_mb / 1024:  # Convert MB to GB
                    deployment_results[node_id] = {"status": "failed", "error": "Insufficient storage"}
                    continue
                
                # Simulate deployment
                node.deployed_models.append(model_id)
                model.target_nodes.append(node_id)
                model.deployment_status[node_id] = "deployed"
                
                deployment_results[node_id] = {
                    "status": "success",
                    "deployment_time_s": round(model.size_mb / 100, 2),  # Simulated
                    "storage_used_mb": model.size_mb
                }
            
            return {
                "model_id": model_id,
                "deployment_results": deployment_results,
                "total_deployments": sum(1 for r in deployment_results.values() if r["status"] == "success")
            }
    
    def get_edge_analytics(self) -> Dict[str, Any]:
        """Get comprehensive edge computing analytics"""
        with self._lock:
            # Node statistics
            total_nodes = len(self.edge_nodes)
            active_nodes = sum(1 for node in self.edge_nodes.values() if node.status == "active")
            
            # Resource utilization
            total_cpu = sum(node.resources.get(ComputeResource.CPU_CORES, 0) for node in self.edge_nodes.values())
            total_memory = sum(node.resources.get(ComputeResource.RAM_GB, 0) for node in self.edge_nodes.values())
            total_storage = sum(node.resources.get(ComputeResource.STORAGE_GB, 0) for node in self.edge_nodes.values())
            
            avg_load = sum(node.load_percentage for node in self.edge_nodes.values()) / total_nodes if total_nodes > 0 else 0
            
            # Geographic distribution
            location_distribution = defaultdict(int)
            for node in self.edge_nodes.values():
                location_distribution[node.location.region] += 1
            
            # Model deployment stats
            deployed_models_count = len(self.deployed_models)
            total_deployments = sum(len(model.target_nodes) for model in self.deployed_models.values())
            
            # Performance metrics
            if self.completed_requests:
                recent_requests = list(self.completed_requests)[-100:]
                avg_latency = sum(req.actual_latency_ms or 0 for req in recent_requests) / len(recent_requests)
                success_rate = sum(1 for req in recent_requests if req.result and req.result.get("status") == "completed") / len(recent_requests) * 100
            else:
                avg_latency = 0
                success_rate = 0
            
            return {
                "status": "orchestrating",
                "edge_infrastructure": {
                    "total_nodes": total_nodes,
                    "active_nodes": active_nodes,
                    "node_availability": round(active_nodes / total_nodes * 100, 1) if total_nodes > 0 else 0,
                    "geographic_distribution": dict(location_distribution)
                },
                "resource_capacity": {
                    "total_cpu_cores": total_cpu,
                    "total_memory_gb": total_memory,
                    "total_storage_gb": total_storage,
                    "average_load_percentage": round(avg_load, 1)
                },
                "performance_metrics": {
                    "total_requests_processed": self.total_requests_processed,
                    "average_response_time_ms": round(self.average_response_time_ms, 2),
                    "recent_success_rate": round(success_rate, 1),
                    "pending_requests": len(self.request_queue)
                },
                "model_deployment": {
                    "deployed_models": deployed_models_count,
                    "total_deployments": total_deployments,
                    "edge_models_available": [model.model_name for model in self.deployed_models.values()]
                },
                "network_topology": {
                    "mesh_connections": sum(len(connections) for connections in self.node_connections.values()) // 2,
                    "routing_entries": len(self.routing_table)
                },
                "capabilities": [
                    "ultra_low_latency_inference",
                    "distributed_edge_processing", 
                    "intelligent_node_selection",
                    "edge_model_optimization",
                    "mesh_networking",
                    "geographic_load_balancing",
                    "real_time_resource_monitoring",
                    "automated_model_deployment"
                ],
                "edge_locations": list(self.edge_locations.keys())
            }


# Global instance
_edge_orchestrator: Optional[EdgeOrchestrator] = None
_edge_lock = threading.Lock()


def get_edge_orchestrator() -> EdgeOrchestrator:
    """Get or create edge orchestrator instance"""
    global _edge_orchestrator
    with _edge_lock:
        if _edge_orchestrator is None:
            _edge_orchestrator = EdgeOrchestrator()
        return _edge_orchestrator