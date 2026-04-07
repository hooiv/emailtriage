"""Pydantic models for the Email Triage OpenEnv environment."""

from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field, computed_field


class EmailPriority(str, Enum):
    """Email priority levels."""
    URGENT = "urgent"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


class EmailCategory(str, Enum):
    """Email categories for classification."""
    CUSTOMER_SUPPORT = "customer_support"
    SALES = "sales"
    BILLING = "billing"
    TECHNICAL = "technical"
    SPAM = "spam"
    INTERNAL = "internal"
    NEWSLETTER = "newsletter"


class SenderType(str, Enum):
    """Sender classification for reputation system."""
    VIP = "vip"
    KNOWN = "known"
    UNKNOWN = "unknown"
    SUSPICIOUS = "suspicious"


class ActionType(str, Enum):
    """Available actions the agent can take."""
    CATEGORIZE = "categorize"
    PRIORITIZE = "prioritize"
    REPLY = "reply"
    FORWARD = "forward"
    ARCHIVE = "archive"
    FLAG = "flag"
    MARK_SPAM = "mark_spam"
    SNOOZE = "snooze"
    BATCH = "batch"
    UNDO = "undo"
    TAG = "tag"
    DONE = "done"


class AttachmentType(str, Enum):
    """Attachment content types."""
    IMAGE = "image"
    PDF = "pdf"
    DOCUMENT = "document"
    LOG = "log"


class Attachment(BaseModel):
    """Email attachment metadata."""
    attachment_id: str = Field(..., description="Unique attachment identifier")
    filename: str = Field(..., description="Attachment filename")
    mime_type: str = Field(..., description="MIME type")
    attachment_type: AttachmentType = Field(..., description="Attachment type")
    content_summary: str = Field(default="", description="Short summary of attachment content")
    ocr_text: Optional[str] = Field(default=None, description="OCR text extracted from image/PDF")


class SenderInfo(BaseModel):
    """Information about an email sender for reputation tracking."""
    email: str = Field(..., description="Sender email address")
    name: str = Field(..., description="Sender display name")
    sender_type: SenderType = Field(default=SenderType.UNKNOWN, description="Sender classification")
    domain: str = Field(default="", description="Email domain")
    previous_emails: int = Field(default=0, description="Number of previous emails from this sender")
    avg_response_time_hours: Optional[float] = Field(default=None, description="Average response time to this sender")
    is_internal: bool = Field(default=False, description="Whether sender is internal to organization")
    trust_score: float = Field(default=0.5, ge=0.0, le=1.0, description="Trust score 0-1")


class Email(BaseModel):
    """Represents an email in the inbox."""
    id: str = Field(..., description="Unique email identifier")
    sender: str = Field(..., description="Email sender address")
    sender_name: str = Field(..., description="Sender display name")
    subject: str = Field(..., description="Email subject line")
    body: str = Field(..., description="Email body content")
    received_at: str = Field(..., description="ISO timestamp when received")
    is_read: bool = Field(default=False, description="Whether email has been read")
    has_attachments: bool = Field(default=False, description="Whether email has attachments")
    attachments: List[Attachment] = Field(default_factory=list, description="Attachment metadata")
    
    # Threading support
    thread_id: Optional[str] = Field(default=None, description="Thread ID for conversation")
    in_reply_to: Optional[str] = Field(default=None, description="ID of email this replies to")
    thread_position: int = Field(default=0, description="Position in thread (0 = original)")
    thread_size: int = Field(default=1, description="Total emails in thread")
    
    # Sender reputation
    sender_info: Optional[SenderInfo] = Field(default=None, description="Sender reputation info")
    
    # SLA tracking
    sla_deadline: Optional[str] = Field(default=None, description="SLA response deadline (ISO)")
    sla_priority: Optional[str] = Field(default=None, description="SLA priority tier")
    time_in_inbox_hours: float = Field(default=0.0, description="Hours since received")
    
    # Agent-assigned attributes
    category: Optional[EmailCategory] = Field(default=None, description="Agent-assigned category")
    priority: Optional[EmailPriority] = Field(default=None, description="Agent-assigned priority")
    is_flagged: bool = Field(default=False, description="Whether email is flagged")
    is_archived: bool = Field(default=False, description="Whether email is archived")
    is_spam: bool = Field(default=False, description="Whether marked as spam")
    is_snoozed: bool = Field(default=False, description="Whether email is snoozed")
    snooze_until: Optional[str] = Field(default=None, description="Snooze until timestamp")
    forwarded_to: Optional[str] = Field(default=None, description="Email address forwarded to")
    reply_sent: Optional[str] = Field(default=None, description="Reply content if sent")
    
    # Tags and organization
    tags: List[str] = Field(default_factory=list, description="User-defined tags for flexible organization")
    importance_score: int = Field(default=50, ge=0, le=100, description="Calculated importance score (0-100)")
    
    # Smart suggestions
    suggested_category: Optional[EmailCategory] = Field(default=None, description="AI-suggested category")
    suggested_priority: Optional[EmailPriority] = Field(default=None, description="AI-suggested priority")
    suggested_actions: List[str] = Field(default_factory=list, description="Suggested next actions")
    confidence_score: float = Field(default=0.0, ge=0.0, le=1.0, description="Suggestion confidence")
    
    # Sentiment analysis
    sentiment_score: float = Field(default=0.0, ge=-1.0, le=1.0, description="Sentiment -1.0 (negative) to +1.0 (positive)")
    sentiment_label: str = Field(default="neutral", description="Sentiment label: very_negative, negative, neutral, positive, very_positive")


class ThreadSummary(BaseModel):
    """Summary of an email thread."""
    thread_id: str = Field(..., description="Thread identifier")
    subject: str = Field(..., description="Thread subject")
    participants: List[str] = Field(..., description="All participants in thread")
    email_count: int = Field(..., description="Number of emails in thread")
    latest_email_id: str = Field(..., description="Most recent email ID")
    oldest_email_id: str = Field(..., description="Original email ID")
    is_resolved: bool = Field(default=False, description="Whether thread is resolved")
    requires_response: bool = Field(default=False, description="Whether response is needed")


class TaskConfig(BaseModel):
    """Configuration for a specific task."""
    task_id: str = Field(..., description="Unique task identifier")
    task_name: str = Field(..., description="Human-readable task name")
    description: str = Field(..., description="Task description")
    difficulty: str = Field(..., description="easy, medium, or hard")
    max_steps: int = Field(..., description="Maximum steps allowed")
    email_count: int = Field(..., description="Number of emails in inbox")
    thread_count: int = Field(default=0, description="Number of email threads")
    success_criteria: Dict[str, Any] = Field(..., description="Criteria for success")
    sla_enabled: bool = Field(default=False, description="Whether SLA tracking is enabled")


class BatchAction(BaseModel):
    """A single action within a batch."""
    action_type: ActionType = Field(..., description="Type of action")
    email_id: str = Field(..., description="Target email ID")
    category: Optional[EmailCategory] = Field(default=None)
    priority: Optional[EmailPriority] = Field(default=None)
    reply_content: Optional[str] = Field(default=None)
    forward_to: Optional[str] = Field(default=None)
    tags: Optional[List[str]] = Field(default=None, description="Tags to add/remove")


class AuditLogEntry(BaseModel):
    """Audit log entry for compliance and debugging."""
    timestamp: str = Field(..., description="ISO timestamp of action")
    action_type: ActionType = Field(..., description="Type of action performed")
    email_id: Optional[str] = Field(default=None, description="Target email ID")
    user_agent: str = Field(default="api", description="Source of action")
    details: Dict[str, Any] = Field(default_factory=dict, description="Action-specific details")
    success: bool = Field(default=True, description="Whether action succeeded")
    error_message: Optional[str] = Field(default=None, description="Error message if failed")


class CannedResponse(BaseModel):
    """Pre-written response template."""
    id: str = Field(..., description="Template ID")
    title: str = Field(..., description="Template title")
    content: str = Field(..., description="Template content with placeholders")
    category: EmailCategory = Field(..., description="Applicable category")
    variables: List[str] = Field(default_factory=list, description="Available variables like {name}")


class SavedFilter(BaseModel):
    """Saved email filter configuration."""
    id: str = Field(..., description="Filter ID")
    name: str = Field(..., description="Filter name")
    query: Optional[str] = Field(default=None, description="Text search query")
    category: Optional[EmailCategory] = Field(default=None, description="Category filter")
    priority: Optional[EmailPriority] = Field(default=None, description="Priority filter")
    tags: List[str] = Field(default_factory=list, description="Tag filters")
    is_spam: Optional[bool] = Field(default=None, description="Spam filter")
    is_read: Optional[bool] = Field(default=None, description="Read status filter")
    has_attachments: Optional[bool] = Field(default=None, description="Attachment filter")
    created_at: str = Field(..., description="Creation timestamp")


class EnvironmentMetrics(BaseModel):
    """Metrics for observability."""
    total_requests: int = Field(default=0, description="Total API requests")
    avg_response_time_ms: float = Field(default=0.0, description="Average response time")
    emails_processed: int = Field(default=0, description="Emails processed this episode")
    actions_taken: int = Field(default=0, description="Actions taken this episode")
    sla_violations: int = Field(default=0, description="SLA violations")
    spam_detected: int = Field(default=0, description="Spam emails detected")
    threads_resolved: int = Field(default=0, description="Threads resolved")


class Observation(BaseModel):
    """Observation returned by the environment."""
    inbox: List[Email] = Field(..., description="Current state of inbox")
    threads: List[ThreadSummary] = Field(default_factory=list, description="Email thread summaries")
    current_email_id: Optional[str] = Field(default=None, description="Currently focused email")
    step_count: int = Field(..., description="Current step number")
    max_steps: int = Field(..., description="Maximum steps allowed")
    task_description: str = Field(..., description="Description of the current task")
    available_actions: List[str] = Field(..., description="List of valid action types")
    last_action_result: Optional[str] = Field(default=None, description="Result of last action")
    last_action_error: Optional[str] = Field(default=None, description="Error from last action if any")
    
    # Enhanced context
    unread_count: int = Field(default=0, description="Number of unread emails")
    urgent_count: int = Field(default=0, description="Number of urgent emails")
    sla_at_risk_count: int = Field(default=0, description="Emails approaching SLA deadline")
    pending_replies: int = Field(default=0, description="Emails awaiting reply")
    
    # Smart suggestions
    recommended_actions: List[Dict[str, Any]] = Field(default_factory=list, description="Recommended next actions")
    learning_hints: List[str] = Field(default_factory=list, description="Adaptive hints based on previous mistakes")
    
    # Performance metrics
    metrics: Optional[EnvironmentMetrics] = Field(default=None, description="Current metrics")


class Action(BaseModel):
    """Action submitted by the agent."""
    action_type: ActionType = Field(..., description="Type of action to perform")
    email_id: Optional[str] = Field(default=None, description="Target email ID")
    email_ids: Optional[List[str]] = Field(default=None, description="Multiple email IDs for batch actions")
    category: Optional[EmailCategory] = Field(default=None, description="Category for categorize action")
    priority: Optional[EmailPriority] = Field(default=None, description="Priority for prioritize action")
    reply_content: Optional[str] = Field(default=None, description="Content for reply action")
    forward_to: Optional[str] = Field(default=None, description="Address for forward action")
    snooze_hours: Optional[int] = Field(default=None, description="Hours to snooze email")
    batch_actions: Optional[List[BatchAction]] = Field(default=None, description="Actions for batch processing")
    tags: Optional[List[str]] = Field(default=None, description="Tags to add for tag action")
    template_id: Optional[str] = Field(default=None, description="Canned response template ID")


class Reward(BaseModel):
    """Reward signal from the environment."""
    value: float = Field(..., description="Reward value", ge=-1.0, le=1.0)
    breakdown: Dict[str, float] = Field(default_factory=dict, description="Reward breakdown")
    message: str = Field(default="", description="Human-readable reward explanation")


class StepResult(BaseModel):
    """Result of a step() call."""
    observation: Observation = Field(..., description="New observation")
    reward: Reward = Field(..., description="Reward for this step")
    done: bool = Field(..., description="Whether episode is complete")
    info: Dict[str, Any] = Field(default_factory=dict, description="Additional info")


class ResetResult(BaseModel):
    """Result of a reset() call."""
    observation: Observation = Field(..., description="Initial observation")
    info: Dict[str, Any] = Field(default_factory=dict, description="Additional info")


class State(BaseModel):
    """Full environment state for state() call."""
    task_id: str = Field(..., description="Current task ID")
    task_name: str = Field(..., description="Current task name")
    step_count: int = Field(..., description="Current step count")
    max_steps: int = Field(..., description="Maximum steps")
    done: bool = Field(..., description="Whether episode is done")
    total_reward: float = Field(..., description="Cumulative reward")
    inbox: List[Email] = Field(..., description="Current inbox state")
    threads: List[ThreadSummary] = Field(default_factory=list, description="Thread summaries")
    action_history: List[Dict[str, Any]] = Field(..., description="History of actions taken")
    ground_truth: Dict[str, Any] = Field(..., description="Ground truth for grading")
    metrics: EnvironmentMetrics = Field(default_factory=EnvironmentMetrics, description="Episode metrics")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Service status")
    version: str = Field(..., description="API version")
    uptime_seconds: float = Field(..., description="Server uptime")
    environment: str = Field(default="email-triage", description="Environment name")


class ErrorResponse(BaseModel):
    """Error response model."""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(default=None, description="Additional details")


class EmailRule(BaseModel):
    """AI-generated email rule for automation."""
    id: str = Field(default_factory=lambda: f"rule_{uuid.uuid4().hex[:8]}")
    name: str
    description: str
    pattern_type: str  # "sender_domain", "subject_keywords", "content_pattern", "attachment_type"
    pattern_value: str  # The actual pattern to match
    action_type: ActionType
    action_params: Optional[Dict[str, Any]] = None
    confidence: float  # AI confidence in rule accuracy (0.0-1.0)
    created_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())
    last_applied: Optional[str] = None
    applications_count: int = 0
    success_rate: float = 1.0  # Track how often rule produces good results
    is_active: bool = True


class PerformanceMetrics(BaseModel):
    """Real-time performance tracking."""
    cpu_usage_percent: float = 0.0
    memory_usage_mb: float = 0.0
    response_times: List[float] = Field(default_factory=list)  # Last 100 requests
    api_calls_per_minute: float = 0.0
    errors_per_minute: float = 0.0
    cache_hit_ratio: float = 0.0
    optimization_suggestions: List[str] = Field(default_factory=list)


class TeamMember(BaseModel):
    """Team collaboration member."""
    id: str = Field(default_factory=lambda: f"member_{uuid.uuid4().hex[:8]}")
    name: str
    email: str
    role: str  # "admin", "manager", "agent", "readonly"
    permissions: List[str] = Field(default_factory=list)  # ["read", "write", "delete", "admin"]
    last_active: Optional[str] = None
    emails_processed: int = 0
    avg_processing_time: float = 0.0
