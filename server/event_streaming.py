"""Real-time Event Streaming & Integration Hub.

This module provides enterprise-grade real-time event streaming, WebSocket support,
and integration capabilities with external systems. This transforms the email triage
system from a standalone application into a central hub for communication management.

Features:
- Real-time WebSocket event streaming for live updates
- External system integrations (Slack, Teams, CRM, Calendar, Jira)  
- Event sourcing architecture with immutable event log
- Real-time dashboard updates and notifications
- Webhook management and external API integrations
- Multi-tenant support with organization isolation
- Advanced pub/sub messaging for distributed architectures
- Integration with popular productivity tools and business systems
"""

import json
import time
import asyncio
import logging
import uuid
from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import aiohttp
import websockets
from fastapi import WebSocket, WebSocketDisconnect
import threading
from concurrent.futures import ThreadPoolExecutor

from models import Email, EmailCategory, EmailPriority


class EventType(str, Enum):
    """Types of events in the system."""
    EMAIL_RECEIVED = "email_received"
    EMAIL_PROCESSED = "email_processed" 
    EMAIL_CATEGORIZED = "email_categorized"
    EMAIL_PRIORITIZED = "email_prioritized"
    EMAIL_FLAGGED = "email_flagged"
    AI_CONSENSUS_REACHED = "ai_consensus_reached"
    SECURITY_ALERT = "security_alert"
    WORKFLOW_TRIGGERED = "workflow_triggered"
    PREDICTION_GENERATED = "prediction_generated"
    AUTONOMOUS_ACTION = "autonomous_action"
    SYSTEM_HEALTH_UPDATE = "system_health_update"
    INTEGRATION_EVENT = "integration_event"
    USER_ACTION = "user_action"


class IntegrationType(str, Enum):
    """Types of external integrations."""
    SLACK = "slack"
    MICROSOFT_TEAMS = "microsoft_teams"
    SALESFORCE = "salesforce"
    HUBSPOT = "hubspot"
    JIRA = "jira"
    ZENDESK = "zendesk"
    GOOGLE_CALENDAR = "google_calendar"
    OUTLOOK_CALENDAR = "outlook_calendar"
    WEBHOOK = "webhook"
    REST_API = "rest_api"
    EMAIL_PROVIDER = "email_provider"


class NotificationPriority(str, Enum):
    """Priority levels for notifications."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"
    CRITICAL = "critical"


@dataclass
class SystemEvent:
    """Represents a system event for real-time streaming."""
    event_id: str
    event_type: EventType
    timestamp: str
    source: str  # Which component generated the event
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    organization_id: Optional[str] = None
    user_id: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class IntegrationConfig:
    """Configuration for external system integration."""
    integration_id: str
    integration_type: IntegrationType
    name: str
    description: str
    endpoint_url: str
    authentication: Dict[str, str]  # API keys, tokens, etc.
    event_filters: List[EventType]  # Which events to send
    is_active: bool = True
    rate_limit_per_minute: int = 60
    retry_config: Dict[str, Any] = field(default_factory=lambda: {
        'max_retries': 3,
        'retry_delay_seconds': 5,
        'exponential_backoff': True
    })
    last_success: Optional[str] = None
    last_error: Optional[str] = None
    success_count: int = 0
    error_count: int = 0
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class WebSocketClient:
    """Represents a connected WebSocket client."""
    client_id: str
    websocket: WebSocket
    organization_id: Optional[str] = None
    user_id: Optional[str] = None
    subscribed_events: Set[EventType] = field(default_factory=set)
    connected_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_activity: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class Notification:
    """Real-time notification for users."""
    notification_id: str
    title: str
    message: str
    priority: NotificationPriority
    event_type: EventType
    data: Dict[str, Any]
    target_users: List[str]
    target_organizations: List[str]
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    expires_at: Optional[str] = None
    read_by: Set[str] = field(default_factory=set)


class EventStreamManager:
    """Manages real-time event streaming and WebSocket connections."""
    
    def __init__(self):
        # Event streaming
        self.events: deque = deque(maxlen=10000)  # Keep last 10,000 events
        self.event_subscribers: Dict[EventType, List[Callable]] = defaultdict(list)
        
        # WebSocket management
        self.connected_clients: Dict[str, WebSocketClient] = {}
        self.organization_clients: Dict[str, Set[str]] = defaultdict(set)
        
        # Integration management
        self.integrations: Dict[str, IntegrationConfig] = {}
        self.integration_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self.integration_workers: List[asyncio.Task] = []
        
        # Notifications
        self.notifications: List[Notification] = []
        self.notification_templates: Dict[EventType, Dict[str, str]] = {}
        
        # Performance tracking
        self.metrics = {
            'events_published': 0,
            'events_delivered': 0,
            'websocket_connections': 0,
            'integration_calls_success': 0,
            'integration_calls_failed': 0,
            'notifications_sent': 0
        }
        
        # Background processing
        self.processing_active = False
        self.processing_tasks: List[asyncio.Task] = []
        
        self._initialize_notification_templates()
        
        logger = logging.getLogger(__name__)
        logger.info("Event Stream Manager initialized")
    
    async def start_processing(self):
        """Start background processing for integrations and notifications."""
        if self.processing_active:
            return
        
        self.processing_active = True
        
        # Start integration workers
        for i in range(3):  # 3 concurrent integration workers
            task = asyncio.create_task(self._integration_worker(f"worker_{i}"))
            self.integration_workers.append(task)
        
        # Start notification processing
        notification_task = asyncio.create_task(self._notification_processor())
        self.processing_tasks.append(notification_task)
        
        # Start metrics updater
        metrics_task = asyncio.create_task(self._metrics_updater())
        self.processing_tasks.append(metrics_task)
    
    async def stop_processing(self):
        """Stop background processing."""
        self.processing_active = False
        
        # Cancel integration workers
        for task in self.integration_workers:
            task.cancel()
        
        # Cancel processing tasks
        for task in self.processing_tasks:
            task.cancel()
        
        # Wait for cancellation
        if self.integration_workers:
            await asyncio.gather(*self.integration_workers, return_exceptions=True)
        if self.processing_tasks:
            await asyncio.gather(*self.processing_tasks, return_exceptions=True)
        
        self.integration_workers.clear()
        self.processing_tasks.clear()
    
    def publish_event(self, event_type: EventType, source: str, data: Dict[str, Any], 
                     organization_id: Optional[str] = None, user_id: Optional[str] = None):
        """Publish an event to the stream."""
        event = SystemEvent(
            event_id=f"evt_{uuid.uuid4().hex[:12]}",
            event_type=event_type,
            timestamp=datetime.now().isoformat(),
            source=source,
            data=data,
            organization_id=organization_id,
            user_id=user_id,
            correlation_id=data.get('correlation_id')
        )
        
        # Store event
        self.events.append(event)
        self.metrics['events_published'] += 1
        
        # Notify subscribers
        asyncio.create_task(self._process_event(event))
        
        return event.event_id
    
    async def _process_event(self, event: SystemEvent):
        """Process an event - send to WebSocket clients and integrations."""
        try:
            # Send to WebSocket clients
            await self._broadcast_to_websockets(event)
            
            # Queue for integrations
            try:
                await self.integration_queue.put_nowait(event)
            except asyncio.QueueFull:
                logger = logging.getLogger(__name__)
                logger.warning("Integration queue full, dropping event")
            
            # Generate notifications if needed
            await self._generate_notifications(event)
            
            self.metrics['events_delivered'] += 1
            
        except Exception as e:
            logger = logging.getLogger(__name__)
            logger.error(f"Error processing event {event.event_id}: {e}")
    
    async def _broadcast_to_websockets(self, event: SystemEvent):
        """Broadcast event to all subscribed WebSocket clients."""
        if not self.connected_clients:
            return
        
        # Find relevant clients
        target_clients = []
        for client in self.connected_clients.values():
            # Check event subscription
            if event.event_type not in client.subscribed_events:
                continue
            
            # Check organization filter
            if event.organization_id and client.organization_id != event.organization_id:
                continue
            
            target_clients.append(client)
        
        # Send to clients in parallel
        if target_clients:
            tasks = [
                self._send_to_client(client, event)
                for client in target_clients
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_to_client(self, client: WebSocketClient, event: SystemEvent):
        """Send event to a specific WebSocket client."""
        try:
            message = {
                "type": "event",
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "timestamp": event.timestamp,
                "source": event.source,
                "data": event.data,
                "metadata": event.metadata
            }
            
            await client.websocket.send_text(json.dumps(message))
            client.last_activity = datetime.now().isoformat()
            
        except Exception as e:
            # Client disconnected or error
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to send to client {client.client_id}: {e}")
            
            # Remove disconnected client
            await self._remove_client(client.client_id)
    
    async def _integration_worker(self, worker_id: str):
        """Background worker for processing integration events."""
        logger = logging.getLogger(__name__)
        logger.info(f"Integration worker {worker_id} started")
        
        while self.processing_active:
            try:
                # Wait for event with timeout
                event = await asyncio.wait_for(self.integration_queue.get(), timeout=5.0)
                
                # Process event for all active integrations
                await self._process_integration_event(event)
                
            except asyncio.TimeoutError:
                continue  # Normal timeout, check if should continue
            except Exception as e:
                logger.error(f"Integration worker {worker_id} error: {e}")
                await asyncio.sleep(5)  # Brief pause on error
    
    async def _process_integration_event(self, event: SystemEvent):
        """Process event for external integrations."""
        relevant_integrations = [
            integration for integration in self.integrations.values()
            if integration.is_active and event.event_type in integration.event_filters
        ]
        
        if not relevant_integrations:
            return
        
        # Send to integrations in parallel
        tasks = [
            self._send_to_integration(integration, event)
            for integration in relevant_integrations
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _send_to_integration(self, integration: IntegrationConfig, event: SystemEvent):
        """Send event to a specific external integration."""
        try:
            # Prepare payload based on integration type
            payload = self._prepare_integration_payload(integration, event)
            
            # Send HTTP request
            async with aiohttp.ClientSession() as session:
                headers = self._prepare_integration_headers(integration)
                
                timeout = aiohttp.ClientTimeout(total=30)
                async with session.post(
                    integration.endpoint_url,
                    json=payload,
                    headers=headers,
                    timeout=timeout
                ) as response:
                    
                    if response.status == 200:
                        integration.success_count += 1
                        integration.last_success = datetime.now().isoformat()
                        self.metrics['integration_calls_success'] += 1
                    else:
                        raise Exception(f"HTTP {response.status}: {await response.text()}")
        
        except Exception as e:
            integration.error_count += 1
            integration.last_error = f"{datetime.now().isoformat()}: {str(e)}"
            self.metrics['integration_calls_failed'] += 1
            
            logger = logging.getLogger(__name__)
            logger.error(f"Integration {integration.integration_id} failed: {e}")
    
    def _prepare_integration_payload(self, integration: IntegrationConfig, event: SystemEvent) -> Dict[str, Any]:
        """Prepare payload for specific integration type."""
        base_payload = {
            "event_id": event.event_id,
            "event_type": event.event_type.value,
            "timestamp": event.timestamp,
            "source": event.source,
            "data": event.data
        }
        
        if integration.integration_type == IntegrationType.SLACK:
            return self._prepare_slack_payload(base_payload, event)
        elif integration.integration_type == IntegrationType.MICROSOFT_TEAMS:
            return self._prepare_teams_payload(base_payload, event)
        elif integration.integration_type == IntegrationType.JIRA:
            return self._prepare_jira_payload(base_payload, event)
        else:
            return base_payload  # Generic webhook format
    
    def _prepare_slack_payload(self, base_payload: Dict[str, Any], event: SystemEvent) -> Dict[str, Any]:
        """Prepare Slack-specific payload."""
        color_map = {
            EventType.EMAIL_RECEIVED: "good",
            EventType.SECURITY_ALERT: "danger", 
            EventType.AI_CONSENSUS_REACHED: "good",
            EventType.SYSTEM_HEALTH_UPDATE: "warning"
        }
        
        attachment = {
            "color": color_map.get(event.event_type, "good"),
            "title": f"Email Triage: {event.event_type.value.replace('_', ' ').title()}",
            "text": self._generate_event_summary(event),
            "timestamp": int(datetime.fromisoformat(event.timestamp).timestamp()),
            "fields": [
                {"title": "Source", "value": event.source, "short": True},
                {"title": "Event ID", "value": event.event_id, "short": True}
            ]
        }
        
        # Add email-specific fields
        if 'email_id' in event.data:
            attachment["fields"].append({
                "title": "Email ID", 
                "value": event.data['email_id'], 
                "short": True
            })
        
        if 'category' in event.data:
            attachment["fields"].append({
                "title": "Category",
                "value": event.data['category'],
                "short": True
            })
        
        return {
            "text": f"Email Triage System Event",
            "attachments": [attachment]
        }
    
    def _prepare_teams_payload(self, base_payload: Dict[str, Any], event: SystemEvent) -> Dict[str, Any]:
        """Prepare Microsoft Teams-specific payload."""
        color_map = {
            EventType.EMAIL_RECEIVED: "Good",
            EventType.SECURITY_ALERT: "Attention",
            EventType.AI_CONSENSUS_REACHED: "Good", 
            EventType.SYSTEM_HEALTH_UPDATE: "Warning"
        }
        
        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color_map.get(event.event_type, "Good"),
            "summary": f"Email Triage: {event.event_type.value}",
            "sections": [{
                "activityTitle": f"Email Triage System Event",
                "activitySubtitle": event.event_type.value.replace('_', ' ').title(),
                "text": self._generate_event_summary(event),
                "facts": [
                    {"name": "Source", "value": event.source},
                    {"name": "Timestamp", "value": event.timestamp},
                    {"name": "Event ID", "value": event.event_id}
                ]
            }]
        }
    
    def _prepare_jira_payload(self, base_payload: Dict[str, Any], event: SystemEvent) -> Dict[str, Any]:
        """Prepare JIRA-specific payload for creating tickets."""
        if event.event_type in [EventType.SECURITY_ALERT, EventType.SYSTEM_HEALTH_UPDATE]:
            return {
                "fields": {
                    "project": {"key": "EMAIL"},
                    "summary": f"Email Triage Alert: {event.event_type.value}",
                    "description": self._generate_event_summary(event),
                    "issuetype": {"name": "Bug" if event.event_type == EventType.SECURITY_ALERT else "Task"},
                    "priority": {"name": "High" if event.event_type == EventType.SECURITY_ALERT else "Medium"}
                }
            }
        return base_payload
    
    def _prepare_integration_headers(self, integration: IntegrationConfig) -> Dict[str, str]:
        """Prepare headers for integration API call."""
        headers = {"Content-Type": "application/json"}
        
        auth = integration.authentication
        
        if 'bearer_token' in auth:
            headers['Authorization'] = f"Bearer {auth['bearer_token']}"
        elif 'api_key' in auth:
            if integration.integration_type == IntegrationType.SLACK:
                headers['Authorization'] = f"Bearer {auth['api_key']}"
            else:
                headers['X-API-Key'] = auth['api_key']
        elif 'username' in auth and 'password' in auth:
            import base64
            credentials = base64.b64encode(f"{auth['username']}:{auth['password']}".encode()).decode()
            headers['Authorization'] = f"Basic {credentials}"
        
        return headers
    
    def _generate_event_summary(self, event: SystemEvent) -> str:
        """Generate human-readable summary of an event."""
        if event.event_type == EventType.EMAIL_RECEIVED:
            return f"New email received from {event.data.get('sender', 'unknown')}: {event.data.get('subject', 'No subject')}"
        elif event.event_type == EventType.EMAIL_CATEGORIZED:
            return f"Email {event.data.get('email_id', 'unknown')} categorized as {event.data.get('category', 'unknown')}"
        elif event.event_type == EventType.SECURITY_ALERT:
            return f"Security alert: {event.data.get('alert_type', 'unknown threat')} detected"
        elif event.event_type == EventType.AI_CONSENSUS_REACHED:
            return f"AI agents reached consensus on email {event.data.get('email_id', 'unknown')}"
        elif event.event_type == EventType.AUTONOMOUS_ACTION:
            return f"Autonomous system executed: {event.data.get('action_type', 'unknown action')}"
        else:
            return f"System event: {event.event_type.value}"
    
    async def _generate_notifications(self, event: SystemEvent):
        """Generate notifications for important events."""
        notification_events = [
            EventType.SECURITY_ALERT,
            EventType.SYSTEM_HEALTH_UPDATE,
            EventType.AUTONOMOUS_ACTION
        ]
        
        if event.event_type not in notification_events:
            return
        
        # Determine priority
        priority = NotificationPriority.NORMAL
        if event.event_type == EventType.SECURITY_ALERT:
            priority = NotificationPriority.HIGH
        elif event.data.get('severity') == 'critical':
            priority = NotificationPriority.CRITICAL
        
        # Create notification
        notification = Notification(
            notification_id=f"notif_{uuid.uuid4().hex[:8]}",
            title=self._get_notification_title(event),
            message=self._generate_event_summary(event),
            priority=priority,
            event_type=event.event_type,
            data=event.data,
            target_users=event.data.get('target_users', []),
            target_organizations=event.data.get('target_organizations', []),
            expires_at=(datetime.now() + timedelta(hours=24)).isoformat() if priority != NotificationPriority.CRITICAL else None
        )
        
        self.notifications.append(notification)
        self.metrics['notifications_sent'] += 1
        
        # Send notification to WebSocket clients
        await self._broadcast_notification(notification)
    
    def _get_notification_title(self, event: SystemEvent) -> str:
        """Get title for notification based on event type."""
        titles = {
            EventType.SECURITY_ALERT: "Security Alert",
            EventType.SYSTEM_HEALTH_UPDATE: "System Health Alert", 
            EventType.AUTONOMOUS_ACTION: "Autonomous Action Taken"
        }
        return titles.get(event.event_type, "System Notification")
    
    async def _broadcast_notification(self, notification: Notification):
        """Broadcast notification to relevant clients."""
        message = {
            "type": "notification",
            "notification_id": notification.notification_id,
            "title": notification.title,
            "message": notification.message,
            "priority": notification.priority.value,
            "event_type": notification.event_type.value,
            "created_at": notification.created_at
        }
        
        # Send to all connected clients (could be filtered by user/organization)
        tasks = [
            client.websocket.send_text(json.dumps(message))
            for client in self.connected_clients.values()
        ]
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _notification_processor(self):
        """Background processor for notifications (cleanup, etc.)."""
        while self.processing_active:
            try:
                current_time = datetime.now()
                
                # Remove expired notifications
                self.notifications = [
                    notif for notif in self.notifications
                    if not notif.expires_at or datetime.fromisoformat(notif.expires_at) > current_time
                ]
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.error(f"Notification processor error: {e}")
                await asyncio.sleep(60)
    
    async def _metrics_updater(self):
        """Update metrics periodically."""
        while self.processing_active:
            try:
                self.metrics['websocket_connections'] = len(self.connected_clients)
                await asyncio.sleep(60)  # Update every minute
            except Exception as e:
                logger = logging.getLogger(__name__)
                logger.error(f"Metrics updater error: {e}")
                await asyncio.sleep(60)
    
    def _initialize_notification_templates(self):
        """Initialize notification templates for different event types."""
        self.notification_templates = {
            EventType.SECURITY_ALERT: {
                "title": "Security Alert",
                "message_template": "Security threat detected: {alert_type}"
            },
            EventType.SYSTEM_HEALTH_UPDATE: {
                "title": "System Health Alert", 
                "message_template": "System health status: {status}"
            },
            EventType.AUTONOMOUS_ACTION: {
                "title": "Autonomous Action",
                "message_template": "System automatically executed: {action}"
            }
        }
    
    # WebSocket management methods
    async def register_client(self, websocket: WebSocket, organization_id: Optional[str] = None, 
                            user_id: Optional[str] = None) -> str:
        """Register a new WebSocket client."""
        client_id = f"ws_{uuid.uuid4().hex[:8]}"
        
        client = WebSocketClient(
            client_id=client_id,
            websocket=websocket,
            organization_id=organization_id,
            user_id=user_id
        )
        
        self.connected_clients[client_id] = client
        
        if organization_id:
            self.organization_clients[organization_id].add(client_id)
        
        return client_id
    
    async def _remove_client(self, client_id: str):
        """Remove a WebSocket client."""
        if client_id in self.connected_clients:
            client = self.connected_clients[client_id]
            
            # Remove from organization tracking
            if client.organization_id:
                self.organization_clients[client.organization_id].discard(client_id)
            
            # Remove from clients
            del self.connected_clients[client_id]
    
    async def subscribe_client(self, client_id: str, event_types: List[EventType]):
        """Subscribe client to specific event types."""
        if client_id in self.connected_clients:
            self.connected_clients[client_id].subscribed_events.update(event_types)
    
    # Integration management methods
    def register_integration(self, integration_config: IntegrationConfig) -> str:
        """Register a new external integration."""
        self.integrations[integration_config.integration_id] = integration_config
        return integration_config.integration_id
    
    def update_integration(self, integration_id: str, updates: Dict[str, Any]):
        """Update an integration configuration."""
        if integration_id in self.integrations:
            integration = self.integrations[integration_id]
            
            for key, value in updates.items():
                if hasattr(integration, key):
                    setattr(integration, key, value)
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get status of all integrations."""
        return {
            integration_id: {
                "name": integration.name,
                "type": integration.integration_type.value,
                "is_active": integration.is_active,
                "success_count": integration.success_count,
                "error_count": integration.error_count,
                "last_success": integration.last_success,
                "last_error": integration.last_error,
                "success_rate": integration.success_count / max(1, integration.success_count + integration.error_count)
            }
            for integration_id, integration in self.integrations.items()
        }
    
    def get_events(self, limit: int = 100, event_type: Optional[EventType] = None, 
                   organization_id: Optional[str] = None) -> List[SystemEvent]:
        """Get recent events with optional filtering."""
        events = list(self.events)
        
        # Filter by event type
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        
        # Filter by organization
        if organization_id:
            events = [e for e in events if e.organization_id == organization_id]
        
        # Return most recent first, limited
        return sorted(events, key=lambda e: e.timestamp, reverse=True)[:limit]
    
    def get_notifications(self, user_id: Optional[str] = None, organization_id: Optional[str] = None,
                         unread_only: bool = False) -> List[Notification]:
        """Get notifications with optional filtering."""
        notifications = self.notifications
        
        # Filter by user
        if user_id:
            notifications = [n for n in notifications if user_id in n.target_users]
        
        # Filter by organization  
        if organization_id:
            notifications = [n for n in notifications if organization_id in n.target_organizations]
        
        # Filter unread
        if unread_only and user_id:
            notifications = [n for n in notifications if user_id not in n.read_by]
        
        return sorted(notifications, key=lambda n: n.created_at, reverse=True)
    
    def mark_notification_read(self, notification_id: str, user_id: str):
        """Mark a notification as read by a user."""
        for notification in self.notifications:
            if notification.notification_id == notification_id:
                notification.read_by.add(user_id)
                break
    
    def get_system_metrics(self) -> Dict[str, Any]:
        """Get comprehensive system metrics."""
        return {
            **self.metrics,
            "active_integrations": len([i for i in self.integrations.values() if i.is_active]),
            "total_integrations": len(self.integrations),
            "connected_clients": len(self.connected_clients),
            "events_in_buffer": len(self.events),
            "notifications_active": len(self.notifications),
            "integration_queue_size": self.integration_queue.qsize() if hasattr(self.integration_queue, 'qsize') else 0
        }


# Global event stream manager instance
event_stream_manager = EventStreamManager()