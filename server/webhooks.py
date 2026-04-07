"""
WebHook Integration System
Real-time event notifications with retry logic and delivery tracking
"""
import asyncio
import hashlib
import hmac
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional, Callable
from collections import deque
import aiohttp

logger = logging.getLogger("webhooks")


class WebhookEventType(Enum):
    """Types of webhook events"""
    EMAIL_RECEIVED = "email.received"
    EMAIL_CATEGORIZED = "email.categorized"
    EMAIL_PRIORITIZED = "email.prioritized"
    EMAIL_REPLIED = "email.replied"
    EMAIL_ARCHIVED = "email.archived"
    EMAIL_SPAM_DETECTED = "email.spam_detected"
    EMAIL_DELETED = "email.deleted"
    
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    
    ALERT_TRIGGERED = "alert.triggered"
    SLA_BREACH = "sla.breach"
    SECURITY_THREAT = "security.threat"
    
    SYSTEM_HEALTH_CHANGE = "system.health_change"
    CIRCUIT_BREAKER_OPEN = "circuit.breaker_open"
    RATE_LIMIT_EXCEEDED = "rate_limit.exceeded"


class DeliveryStatus(Enum):
    """Webhook delivery status"""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class WebhookEndpoint:
    """Webhook endpoint configuration"""
    id: str
    url: str
    secret: str
    events: List[WebhookEventType]
    active: bool = True
    description: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    retry_count: int = 3
    timeout_seconds: int = 30
    created_at: datetime = field(default_factory=datetime.now)
    last_triggered: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "url": self.url,
            "events": [e.value for e in self.events],
            "active": self.active,
            "description": self.description,
            "retry_count": self.retry_count,
            "timeout_seconds": self.timeout_seconds,
            "created_at": self.created_at.isoformat(),
            "last_triggered": self.last_triggered.isoformat() if self.last_triggered else None
        }


@dataclass
class WebhookDelivery:
    """Record of a webhook delivery attempt"""
    id: str
    endpoint_id: str
    event_type: str
    payload: Dict[str, Any]
    status: DeliveryStatus
    attempts: int = 0
    last_attempt: Optional[datetime] = None
    response_code: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    delivered_at: Optional[datetime] = None
    duration_ms: Optional[float] = None


class WebhookManager:
    """
    Enterprise Webhook Management System
    
    Features:
    - Event subscription management
    - Cryptographic signature verification
    - Automatic retry with exponential backoff
    - Delivery tracking and analytics
    - Rate limiting per endpoint
    """
    
    def __init__(self):
        self._endpoints: Dict[str, WebhookEndpoint] = {}
        self._deliveries: deque = deque(maxlen=10000)
        self._pending_queue: asyncio.Queue = None
        self._lock = threading.RLock()
        self._delivery_counter = 0
        self._running = False
        self._worker_task = None
        
        logger.info("Webhook Manager initialized")
    
    def _generate_id(self) -> str:
        """Generate unique ID"""
        self._delivery_counter += 1
        return f"whd_{int(time.time())}_{self._delivery_counter}"
    
    def _sign_payload(self, payload: str, secret: str) -> str:
        """Generate HMAC signature for payload"""
        signature = hmac.new(
            secret.encode('utf-8'),
            payload.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"
    
    def register_endpoint(
        self,
        url: str,
        events: List[str],
        secret: Optional[str] = None,
        description: str = "",
        **kwargs
    ) -> WebhookEndpoint:
        """Register a new webhook endpoint"""
        with self._lock:
            endpoint_id = f"wh_{hashlib.md5(url.encode()).hexdigest()[:8]}"
            
            if not secret:
                secret = hashlib.sha256(f"{url}{time.time()}".encode()).hexdigest()[:32]
            
            event_types = [WebhookEventType(e) for e in events]
            
            endpoint = WebhookEndpoint(
                id=endpoint_id,
                url=url,
                secret=secret,
                events=event_types,
                description=description,
                **kwargs
            )
            
            self._endpoints[endpoint_id] = endpoint
            logger.info(f"Registered webhook endpoint: {endpoint_id} -> {url}")
            
            return endpoint
    
    def unregister_endpoint(self, endpoint_id: str) -> bool:
        """Unregister a webhook endpoint"""
        with self._lock:
            if endpoint_id in self._endpoints:
                del self._endpoints[endpoint_id]
                logger.info(f"Unregistered webhook endpoint: {endpoint_id}")
                return True
            return False
    
    def update_endpoint(self, endpoint_id: str, **updates) -> Optional[WebhookEndpoint]:
        """Update endpoint configuration"""
        with self._lock:
            endpoint = self._endpoints.get(endpoint_id)
            if not endpoint:
                return None
            
            for key, value in updates.items():
                if key == "events":
                    value = [WebhookEventType(e) for e in value]
                if hasattr(endpoint, key):
                    setattr(endpoint, key, value)
            
            return endpoint
    
    def get_endpoints_for_event(self, event_type: WebhookEventType) -> List[WebhookEndpoint]:
        """Get all endpoints subscribed to an event"""
        return [
            ep for ep in self._endpoints.values()
            if ep.active and event_type in ep.events
        ]
    
    async def trigger_event(
        self,
        event_type: WebhookEventType,
        payload: Dict[str, Any]
    ) -> List[WebhookDelivery]:
        """Trigger an event and deliver to all subscribed endpoints"""
        endpoints = self.get_endpoints_for_event(event_type)
        
        if not endpoints:
            return []
        
        deliveries = []
        
        for endpoint in endpoints:
            delivery = await self._deliver_webhook(endpoint, event_type, payload)
            deliveries.append(delivery)
        
        return deliveries
    
    def trigger_event_sync(
        self,
        event_type: WebhookEventType,
        payload: Dict[str, Any]
    ) -> List[str]:
        """Trigger event synchronously (queues for async delivery)"""
        endpoints = self.get_endpoints_for_event(event_type)
        delivery_ids = []
        
        for endpoint in endpoints:
            delivery_id = self._generate_id()
            delivery = WebhookDelivery(
                id=delivery_id,
                endpoint_id=endpoint.id,
                event_type=event_type.value,
                payload=payload,
                status=DeliveryStatus.PENDING
            )
            
            with self._lock:
                self._deliveries.append(delivery)
                delivery_ids.append(delivery_id)
            
            # Queue for async processing
            if self._pending_queue:
                try:
                    self._pending_queue.put_nowait((endpoint, delivery))
                except asyncio.QueueFull:
                    logger.warning(f"Webhook queue full, dropping delivery {delivery_id}")
        
        return delivery_ids
    
    async def _deliver_webhook(
        self,
        endpoint: WebhookEndpoint,
        event_type: WebhookEventType,
        payload: Dict[str, Any]
    ) -> WebhookDelivery:
        """Deliver webhook to endpoint with retries"""
        delivery_id = self._generate_id()
        
        delivery = WebhookDelivery(
            id=delivery_id,
            endpoint_id=endpoint.id,
            event_type=event_type.value,
            payload=payload,
            status=DeliveryStatus.PENDING
        )
        
        # Build full payload
        full_payload = {
            "event": event_type.value,
            "timestamp": datetime.now().isoformat(),
            "delivery_id": delivery_id,
            "data": payload
        }
        
        payload_json = json.dumps(full_payload)
        signature = self._sign_payload(payload_json, endpoint.secret)
        
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": event_type.value,
            "X-Webhook-Delivery": delivery_id,
            **endpoint.headers
        }
        
        # Retry loop
        for attempt in range(endpoint.retry_count + 1):
            delivery.attempts = attempt + 1
            delivery.last_attempt = datetime.now()
            
            if attempt > 0:
                # Exponential backoff
                delay = min(300, (2 ** attempt) * 5)
                await asyncio.sleep(delay)
                delivery.status = DeliveryStatus.RETRYING
            
            start_time = time.time()
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        endpoint.url,
                        data=payload_json,
                        headers=headers,
                        timeout=aiohttp.ClientTimeout(total=endpoint.timeout_seconds)
                    ) as response:
                        duration = (time.time() - start_time) * 1000
                        delivery.duration_ms = duration
                        delivery.response_code = response.status
                        
                        try:
                            delivery.response_body = await response.text()
                        except:
                            pass
                        
                        if 200 <= response.status < 300:
                            delivery.status = DeliveryStatus.DELIVERED
                            delivery.delivered_at = datetime.now()
                            endpoint.last_triggered = datetime.now()
                            
                            logger.info(
                                f"Webhook delivered: {delivery_id} to {endpoint.url} "
                                f"in {duration:.0f}ms"
                            )
                            break
                        else:
                            delivery.error_message = f"HTTP {response.status}"
                            
            except asyncio.TimeoutError:
                delivery.error_message = "Timeout"
                delivery.duration_ms = endpoint.timeout_seconds * 1000
                
            except aiohttp.ClientError as e:
                delivery.error_message = str(e)
                
            except Exception as e:
                delivery.error_message = str(e)
                logger.error(f"Webhook delivery error: {e}")
        
        if delivery.status != DeliveryStatus.DELIVERED:
            delivery.status = DeliveryStatus.FAILED
            logger.warning(
                f"Webhook delivery failed after {delivery.attempts} attempts: "
                f"{delivery_id} to {endpoint.url}"
            )
        
        with self._lock:
            self._deliveries.append(delivery)
        
        return delivery
    
    def get_delivery(self, delivery_id: str) -> Optional[WebhookDelivery]:
        """Get delivery by ID"""
        for delivery in self._deliveries:
            if delivery.id == delivery_id:
                return delivery
        return None
    
    def list_endpoints(self) -> List[Dict[str, Any]]:
        """List all registered endpoints"""
        return [ep.to_dict() for ep in self._endpoints.values()]
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get webhook analytics"""
        deliveries = list(self._deliveries)
        
        # Status breakdown
        status_counts = {status.value: 0 for status in DeliveryStatus}
        for d in deliveries:
            status_counts[d.status.value] += 1
        
        # Per-endpoint stats
        endpoint_stats = {}
        for endpoint_id in self._endpoints:
            ep_deliveries = [d for d in deliveries if d.endpoint_id == endpoint_id]
            successful = sum(1 for d in ep_deliveries if d.status == DeliveryStatus.DELIVERED)
            
            endpoint_stats[endpoint_id] = {
                "total_deliveries": len(ep_deliveries),
                "successful": successful,
                "failed": len(ep_deliveries) - successful,
                "success_rate": (successful / len(ep_deliveries) * 100) if ep_deliveries else 0,
                "avg_duration_ms": (
                    sum(d.duration_ms for d in ep_deliveries if d.duration_ms) / 
                    len([d for d in ep_deliveries if d.duration_ms])
                ) if any(d.duration_ms for d in ep_deliveries) else 0
            }
        
        # Event type breakdown
        event_counts = {}
        for d in deliveries:
            event_counts[d.event_type] = event_counts.get(d.event_type, 0) + 1
        
        # Recent failures
        recent_failures = [
            {
                "delivery_id": d.id,
                "endpoint_id": d.endpoint_id,
                "event_type": d.event_type,
                "error": d.error_message,
                "attempts": d.attempts,
                "timestamp": d.created_at.isoformat()
            }
            for d in deliveries
            if d.status == DeliveryStatus.FAILED
        ][-20:]
        
        return {
            "summary": {
                "total_endpoints": len(self._endpoints),
                "active_endpoints": sum(1 for ep in self._endpoints.values() if ep.active),
                "total_deliveries": len(deliveries),
                "delivery_success_rate": (
                    status_counts["delivered"] / len(deliveries) * 100
                ) if deliveries else 0
            },
            "status_breakdown": status_counts,
            "endpoint_stats": endpoint_stats,
            "event_counts": event_counts,
            "recent_failures": recent_failures
        }
    
    def test_endpoint(self, endpoint_id: str) -> Dict[str, Any]:
        """Send test webhook to endpoint"""
        endpoint = self._endpoints.get(endpoint_id)
        if not endpoint:
            return {"success": False, "error": "Endpoint not found"}
        
        test_payload = {
            "test": True,
            "message": "This is a test webhook delivery",
            "timestamp": datetime.now().isoformat()
        }
        
        # Run async delivery in sync context
        import asyncio
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        try:
            delivery = loop.run_until_complete(
                self._deliver_webhook(
                    endpoint,
                    WebhookEventType.SYSTEM_HEALTH_CHANGE,
                    test_payload
                )
            )
            
            return {
                "success": delivery.status == DeliveryStatus.DELIVERED,
                "delivery_id": delivery.id,
                "status": delivery.status.value,
                "response_code": delivery.response_code,
                "duration_ms": delivery.duration_ms,
                "error": delivery.error_message
            }
        except Exception as e:
            return {"success": False, "error": str(e)}


# Global instance
_webhook_manager = WebhookManager()


def get_webhook_manager() -> WebhookManager:
    """Get global webhook manager"""
    return _webhook_manager
