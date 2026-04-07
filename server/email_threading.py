"""
Email Threading and SLA System for production-grade email triage.

This module provides:
- Realistic email conversation threading
- SLA tracking with time-based escalation
- Sender reputation management
- Smart action suggestions
"""

import random
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from models import (
    Email, EmailCategory, EmailPriority, SenderInfo, SenderType,
    ThreadSummary
)


# SLA Configuration by category
SLA_CONFIG = {
    EmailCategory.CUSTOMER_SUPPORT: {
        "urgent": timedelta(hours=1),
        "high": timedelta(hours=4),
        "normal": timedelta(hours=24),
        "low": timedelta(hours=72)
    },
    EmailCategory.SALES: {
        "urgent": timedelta(hours=2),
        "high": timedelta(hours=8),
        "normal": timedelta(hours=48),
        "low": timedelta(hours=168)
    },
    EmailCategory.BILLING: {
        "urgent": timedelta(hours=2),
        "high": timedelta(hours=8),
        "normal": timedelta(hours=24),
        "low": timedelta(hours=48)
    },
    EmailCategory.TECHNICAL: {
        "urgent": timedelta(minutes=30),
        "high": timedelta(hours=2),
        "normal": timedelta(hours=12),
        "low": timedelta(hours=48)
    }
}

# VIP senders (known important contacts)
VIP_DOMAINS = ["enterprise.com", "fortune500.com", "bigclient.com", "vip.org"]
VIP_NAMES = ["CEO", "CTO", "VP", "Director", "President", "Chief"]

# Suspicious patterns
SUSPICIOUS_PATTERNS = [
    "lottery", "winner", "million", "click here", "verify now",
    "suspended", "bitcoin", "inheritance", "prince", "urgent transfer"
]

# Internal domains
INTERNAL_DOMAINS = ["company.com", "internal.corp", "team.local"]


def generate_sender_info(email_address: str, sender_name: str, seed: int) -> SenderInfo:
    """Generate sender reputation information."""
    rng = random.Random(seed)
    
    domain = email_address.split("@")[-1] if "@" in email_address else "unknown.com"
    
    # Determine sender type
    sender_type = SenderType.UNKNOWN
    trust_score = 0.5
    
    # Check for VIP
    if any(vip in sender_name for vip in VIP_NAMES) or domain in VIP_DOMAINS:
        sender_type = SenderType.VIP
        trust_score = 0.95
    # Check for internal
    elif domain in INTERNAL_DOMAINS:
        sender_type = SenderType.KNOWN
        trust_score = 0.9
        is_internal = True
    # Check for suspicious
    elif any(pattern in email_address.lower() or pattern in sender_name.lower() 
             for pattern in SUSPICIOUS_PATTERNS):
        sender_type = SenderType.SUSPICIOUS
        trust_score = 0.1
    # Random known sender
    elif rng.random() < 0.3:
        sender_type = SenderType.KNOWN
        trust_score = 0.7 + rng.random() * 0.2
    
    return SenderInfo(
        email=email_address,
        name=sender_name,
        sender_type=sender_type,
        domain=domain,
        previous_emails=rng.randint(0, 50) if sender_type != SenderType.UNKNOWN else 0,
        avg_response_time_hours=rng.uniform(0.5, 24) if sender_type == SenderType.KNOWN else None,
        is_internal=domain in INTERNAL_DOMAINS,
        trust_score=trust_score
    )


def calculate_sla_deadline(
    category: EmailCategory,
    priority: EmailPriority,
    received_at: datetime
) -> Optional[datetime]:
    """Calculate SLA deadline based on category and priority."""
    if category not in SLA_CONFIG:
        return None
    
    sla_times = SLA_CONFIG[category]
    sla_delta = sla_times.get(priority.value, timedelta(hours=24))
    
    return received_at + sla_delta


def calculate_time_in_inbox(received_at: str, current_time: Optional[datetime] = None) -> float:
    """Calculate hours since email was received."""
    if current_time is None:
        current_time = datetime.now()
    
    try:
        received = datetime.fromisoformat(received_at.replace('Z', '+00:00'))
        if received.tzinfo:
            received = received.replace(tzinfo=None)
        delta = current_time - received
        return delta.total_seconds() / 3600
    except (ValueError, TypeError):
        return 0.0


def generate_smart_suggestions(email: Email, ground_truth: Dict[str, Any]) -> Tuple[Optional[EmailCategory], Optional[EmailPriority], List[str], float]:
    """Generate smart suggestions for an email based on content analysis."""
    body_lower = email.body.lower()
    subject_lower = email.subject.lower()
    combined = body_lower + " " + subject_lower
    
    suggested_category = None
    suggested_priority = None
    suggested_actions = []
    confidence = 0.0
    
    # Category detection heuristics
    category_scores = {
        EmailCategory.SPAM: 0,
        EmailCategory.CUSTOMER_SUPPORT: 0,
        EmailCategory.SALES: 0,
        EmailCategory.BILLING: 0,
        EmailCategory.TECHNICAL: 0,
        EmailCategory.INTERNAL: 0,
        EmailCategory.NEWSLETTER: 0
    }
    
    # Spam indicators
    spam_keywords = ["won", "lottery", "million", "click here", "verify", "suspended", "95% off", "free", "congratulations winner"]
    for kw in spam_keywords:
        if kw in combined:
            category_scores[EmailCategory.SPAM] += 2
    
    # Customer support indicators
    support_keywords = ["order", "refund", "help", "issue", "problem", "complaint", "not working", "disappointed"]
    for kw in support_keywords:
        if kw in combined:
            category_scores[EmailCategory.CUSTOMER_SUPPORT] += 1
    
    # Sales indicators
    sales_keywords = ["enterprise", "pricing", "demo", "partnership", "bulk", "quote", "interested in"]
    for kw in sales_keywords:
        if kw in combined:
            category_scores[EmailCategory.SALES] += 1
    
    # Billing indicators
    billing_keywords = ["invoice", "payment", "charge", "billing", "subscription", "credit card"]
    for kw in billing_keywords:
        if kw in combined:
            category_scores[EmailCategory.BILLING] += 1
    
    # Technical indicators
    tech_keywords = ["bug", "error", "crash", "api", "security", "vulnerability", "500", "production"]
    for kw in tech_keywords:
        if kw in combined:
            category_scores[EmailCategory.TECHNICAL] += 1
    
    # Internal indicators
    internal_keywords = ["team meeting", "standup", "pto", "internal", "sync", "sprint"]
    for kw in internal_keywords:
        if kw in combined:
            category_scores[EmailCategory.INTERNAL] += 1
    
    # Newsletter indicators
    newsletter_keywords = ["newsletter", "digest", "unsubscribe", "weekly", "monthly update"]
    for kw in newsletter_keywords:
        if kw in combined:
            category_scores[EmailCategory.NEWSLETTER] += 1
    
    # Find best category
    max_score = max(category_scores.values())
    if max_score > 0:
        for cat, score in category_scores.items():
            if score == max_score:
                suggested_category = cat
                confidence = min(0.9, 0.3 + (score * 0.15))
                break
    
    # Priority detection (consider sentiment for escalation)
    # Run quick sentiment check
    sentiment_score, _ = analyze_sentiment(email)
    
    if any(kw in combined for kw in ["urgent", "critical", "emergency", "asap", "immediately", "p1", "production down"]):
        suggested_priority = EmailPriority.URGENT
    elif any(kw in combined for kw in ["important", "priority", "soon", "deadline", "blocking"]):
        suggested_priority = EmailPriority.HIGH
    elif sentiment_score <= -0.4:
        # Very negative sentiment escalates priority
        suggested_priority = EmailPriority.HIGH
    elif sentiment_score <= -0.2:
        # Negative sentiment at least normal priority
        suggested_priority = EmailPriority.NORMAL
    elif any(kw in combined for kw in ["fyi", "newsletter", "digest", "low priority"]):
        suggested_priority = EmailPriority.LOW
    else:
        suggested_priority = EmailPriority.NORMAL
    
    # Generate action suggestions
    if suggested_category == EmailCategory.SPAM:
        suggested_actions = ["mark_spam", "archive"]
    elif suggested_category == EmailCategory.CUSTOMER_SUPPORT:
        if sentiment_score <= -0.3:
            # Angry customer - prioritize reply and flag
            suggested_actions = ["categorize", "reply", "flag", "prioritize"]
        else:
            suggested_actions = ["categorize", "prioritize", "reply", "flag"]
    elif suggested_category == EmailCategory.TECHNICAL:
        suggested_actions = ["categorize", "prioritize", "forward", "flag"]
    elif suggested_category == EmailCategory.NEWSLETTER:
        suggested_actions = ["categorize", "archive"]
    elif suggested_category == EmailCategory.SALES:
        suggested_actions = ["categorize", "prioritize", "reply"]
    else:
        suggested_actions = ["categorize", "prioritize"]
    
    return suggested_category, suggested_priority, suggested_actions, confidence


class ThreadManager:
    """Manages email conversation threads."""
    
    def __init__(self):
        self.threads: Dict[str, List[Email]] = defaultdict(list)
    
    def add_email(self, email: Email) -> str:
        """Add email to a thread, creating new thread if needed."""
        if email.thread_id:
            thread_id = email.thread_id
        elif email.in_reply_to:
            # Find parent thread
            for tid, emails in self.threads.items():
                if any(e.id == email.in_reply_to for e in emails):
                    thread_id = tid
                    break
            else:
                thread_id = f"thread_{email.id}"
        else:
            thread_id = f"thread_{email.id}"
        
        email.thread_id = thread_id
        email.thread_position = len(self.threads[thread_id])
        self.threads[thread_id].append(email)
        
        # Update thread sizes
        for e in self.threads[thread_id]:
            e.thread_size = len(self.threads[thread_id])
        
        return thread_id
    
    def get_thread(self, thread_id: str) -> List[Email]:
        """Get all emails in a thread."""
        return self.threads.get(thread_id, [])
    
    def get_thread_summary(self, thread_id: str) -> Optional[ThreadSummary]:
        """Generate summary for a thread."""
        emails = self.threads.get(thread_id)
        if not emails:
            return None
        
        participants = list(set(e.sender for e in emails))
        sorted_emails = sorted(emails, key=lambda e: e.received_at)
        
        # Check if thread needs response (last email not from us)
        requires_response = not any(e.reply_sent for e in emails[-1:])
        
        # Check if resolved (has reply to customer)
        is_resolved = any(e.reply_sent for e in emails)
        
        return ThreadSummary(
            thread_id=thread_id,
            subject=sorted_emails[0].subject,
            participants=participants,
            email_count=len(emails),
            latest_email_id=sorted_emails[-1].id,
            oldest_email_id=sorted_emails[0].id,
            is_resolved=is_resolved,
            requires_response=requires_response
        )
    
    def get_all_summaries(self) -> List[ThreadSummary]:
        """Get summaries for all threads."""
        summaries = []
        for thread_id in self.threads:
            summary = self.get_thread_summary(thread_id)
            if summary:
                summaries.append(summary)
        return summaries


def generate_thread_emails(
    base_email: Email,
    thread_length: int,
    seed: int,
    ground_truth: Dict[str, Any]
) -> List[Email]:
    """Generate a thread of emails based on a base email."""
    rng = random.Random(seed)
    
    emails = [base_email]
    base_email.thread_id = f"thread_{base_email.id}"
    base_email.thread_position = 0
    
    # Generate replies
    for i in range(1, thread_length):
        # Alternate between customer and support
        if i % 2 == 1:
            # Customer follow-up
            sender = base_email.sender
            sender_name = base_email.sender_name
            bodies = [
                f"Following up on my previous email. Any update on this issue?\n\nThanks,\n{sender_name}",
                f"Hi, I'm still waiting for a response. This is quite urgent.\n\nRegards,\n{sender_name}",
                f"Just checking in - have you had a chance to look at this?\n\nBest,\n{sender_name}"
            ]
        else:
            # Support response
            sender = "support@company.com"
            sender_name = "Support Team"
            bodies = [
                "Thank you for your patience. We're looking into this and will get back to you shortly.\n\nBest regards,\nSupport Team",
                "We've escalated your issue to our specialist team. You should hear back within 24 hours.\n\nThank you for your understanding.",
                "Good news! We've resolved the issue. Please let us know if you need anything else.\n\nBest,\nSupport Team"
            ]
        
        reply_email = Email(
            id=f"{base_email.id}_r{i}",
            sender=sender,
            sender_name=sender_name,
            subject=f"Re: {base_email.subject}",
            body=rng.choice(bodies),
            received_at=(datetime.fromisoformat(base_email.received_at.replace('Z', '')) + 
                        timedelta(hours=i * rng.randint(1, 8))).isoformat(),
            thread_id=base_email.thread_id,
            in_reply_to=emails[-1].id,
            thread_position=i,
            has_attachments=rng.random() < 0.1
        )
        
        emails.append(reply_email)
    
    # Update thread sizes
    for email in emails:
        email.thread_size = len(emails)
    
    return emails


def enrich_email_with_metadata(
    email: Email,
    ground_truth: Dict[str, Any],
    current_time: Optional[datetime] = None
) -> Email:
    """Enrich email with sender info, SLA, and suggestions."""
    seed = hash(email.id) % (2**32)
    
    # Add sender info
    email.sender_info = generate_sender_info(email.sender, email.sender_name, seed)
    
    # Calculate time in inbox
    email.time_in_inbox_hours = calculate_time_in_inbox(email.received_at, current_time)
    
    # Get correct category for SLA calculation
    correct_category = ground_truth.get(email.id, {}).get("correct_category")
    correct_priority = ground_truth.get(email.id, {}).get("correct_priority")
    
    # Calculate SLA if applicable
    if correct_category and correct_priority:
        try:
            received = datetime.fromisoformat(email.received_at.replace('Z', ''))
            deadline = calculate_sla_deadline(correct_category, correct_priority, received)
            if deadline:
                email.sla_deadline = deadline.isoformat()
                email.sla_priority = correct_priority.value
        except (ValueError, TypeError):
            pass
    
    # Generate smart suggestions
    suggested_cat, suggested_pri, suggested_actions, confidence = generate_smart_suggestions(
        email, ground_truth
    )
    email.suggested_category = suggested_cat
    email.suggested_priority = suggested_pri
    email.suggested_actions = suggested_actions
    email.confidence_score = confidence
    
    # Add sentiment analysis
    email.sentiment_score, email.sentiment_label = analyze_sentiment(email)
    
    return email


def analyze_sentiment(email: Email) -> Tuple[float, str]:
    """
    Analyze email sentiment using keyword-based heuristics.
    
    Returns:
        Tuple of (sentiment_score, sentiment_label)
        - sentiment_score: -1.0 (very negative) to +1.0 (very positive)
        - sentiment_label: "very_negative", "negative", "neutral", "positive", "very_positive"
    """
    text = f"{email.subject} {email.body}".lower()
    
    # Negative sentiment indicators
    negative_words = {
        "angry": -0.4, "furious": -0.5, "frustrated": -0.3, "disappointed": -0.3,
        "terrible": -0.4, "awful": -0.4, "horrible": -0.5, "worst": -0.5,
        "unacceptable": -0.4, "outrageous": -0.4, "ridiculous": -0.3,
        "never": -0.2, "hate": -0.4, "broken": -0.2, "failed": -0.2,
        "useless": -0.3, "waste": -0.3, "scam": -0.4, "fraud": -0.4,
        "lawsuit": -0.4, "legal action": -0.4, "complaint": -0.2,
        "cancel": -0.2, "refund": -0.2, "demand": -0.2,
        "!!!": -0.2, "??": -0.1, "asap": -0.1, "immediately": -0.1
    }
    
    # Positive sentiment indicators  
    positive_words = {
        "thank": 0.3, "thanks": 0.3, "appreciate": 0.3, "grateful": 0.3,
        "great": 0.3, "excellent": 0.4, "amazing": 0.4, "wonderful": 0.4,
        "love": 0.3, "fantastic": 0.4, "perfect": 0.3, "awesome": 0.3,
        "helpful": 0.2, "impressed": 0.3, "satisfied": 0.2,
        "best": 0.2, "quick": 0.1, "easy": 0.1, "smooth": 0.1,
        "pleasure": 0.2, "delight": 0.3, "recommend": 0.2
    }
    
    # Calculate sentiment score
    score = 0.0
    matches = 0
    
    for word, weight in negative_words.items():
        if word in text:
            score += weight
            matches += 1
            
    for word, weight in positive_words.items():
        if word in text:
            score += weight
            matches += 1
    
    # Check for urgency amplifiers that intensify negative sentiment
    urgency_amplifiers = ["urgent", "critical", "emergency", "now", "immediate"]
    has_urgency = any(amp in text for amp in urgency_amplifiers)
    
    if has_urgency and score < 0:
        score *= 1.3  # Intensify negative sentiment when urgent
    
    # Check for ALL CAPS sections (indicates shouting/frustration)
    caps_ratio = sum(1 for c in email.body if c.isupper()) / max(len(email.body), 1)
    if caps_ratio > 0.3:
        score -= 0.2
    
    # Normalize to -1 to +1 range
    score = max(-1.0, min(1.0, score))
    
    # Determine label
    if score <= -0.4:
        label = "very_negative"
    elif score <= -0.15:
        label = "negative"
    elif score >= 0.4:
        label = "very_positive"
    elif score >= 0.15:
        label = "positive"
    else:
        label = "neutral"
    
    return score, label


def calculate_importance_score(email: "Email") -> int:
    """
    Calculate importance score for an email (0-100).
    
    Factors considered:
    - Sender reputation (VIP=+30, Suspicious=-20)
    - Sentiment (negative=+15, very negative=+25)
    - SLA urgency (+20 for urgent, +10 for high)
    - Keywords (urgent=+20, ASAP=+15, etc.)
    - Attachment presence (+5)
    - Thread activity (+5 per email in thread)
    """
    score = 50  # Base score
    
    # Sender reputation
    if hasattr(email, 'sender_info') and email.sender_info:
        if email.sender_info.sender_type == 'vip':
            score += 30
        elif email.sender_info.sender_type == 'suspicious':
            score -= 20
        elif email.sender_info.sender_type == 'known':
            score += 5
    
    # Sentiment impact
    if hasattr(email, 'sentiment_score') and email.sentiment_score:
        if email.sentiment_score <= -0.6:  # Very negative
            score += 25
        elif email.sentiment_score <= -0.3:  # Negative
            score += 15
        elif email.sentiment_score >= 0.4:  # Positive
            score += 5
    
    # SLA priority
    if hasattr(email, 'sla_priority') and email.sla_priority:
        if email.sla_priority == 'urgent':
            score += 20
        elif email.sla_priority == 'high':
            score += 10
    
    # Keyword analysis
    subject_body = f"{email.subject} {email.body}".lower()
    urgent_keywords = ['urgent', 'asap', 'immediately', 'critical', 'emergency', 'broken', 'down']
    important_keywords = ['important', 'deadline', 'meeting', 'call', 'interview']
    
    for keyword in urgent_keywords:
        if keyword in subject_body:
            score += 20
            break
    else:
        for keyword in important_keywords:
            if keyword in subject_body:
                score += 10
                break
    
    # Attachments
    if email.has_attachments:
        score += 5
    
    # Thread activity
    if hasattr(email, 'thread_size') and email.thread_size > 1:
        score += min(email.thread_size * 2, 15)  # Cap at +15
    
    # Time pressure (hours in inbox)
    if hasattr(email, 'time_in_inbox_hours') and email.time_in_inbox_hours:
        if email.time_in_inbox_hours > 24:
            score += 10
        elif email.time_in_inbox_hours > 8:
            score += 5
    
    # Ensure score is within bounds
    return max(0, min(100, score))


# Canned response templates
CANNED_RESPONSES = {
    "acknowledge_receipt": {
        "id": "acknowledge_receipt",
        "title": "Acknowledge Receipt",
        "content": "Thank you for your email. We have received your request and will respond within {response_time}. Reference: {ticket_id}",
        "category": "customer_support",
        "variables": ["response_time", "ticket_id", "name"]
    },
    "escalate_technical": {
        "id": "escalate_technical", 
        "title": "Escalate to Technical Team",
        "content": "I'm forwarding your technical issue to our specialist team. They will contact you within {response_time} with a solution. Ticket: {ticket_id}",
        "category": "technical",
        "variables": ["response_time", "ticket_id", "name"]
    },
    "sales_followup": {
        "id": "sales_followup",
        "title": "Sales Follow-up",
        "content": "Thank you for your interest in {product}. I've attached our pricing information. Would you like to schedule a demo? Best regards, {agent_name}",
        "category": "sales",
        "variables": ["product", "agent_name", "company"]
    },
    "billing_clarification": {
        "id": "billing_clarification",
        "title": "Billing Clarification", 
        "content": "I've reviewed your account and found the issue with invoice {invoice_id}. A corrected invoice will be sent within 24 hours. Apologies for the inconvenience.",
        "category": "billing",
        "variables": ["invoice_id", "amount", "date"]
    },
    "schedule_meeting": {
        "id": "schedule_meeting",
        "title": "Schedule Meeting",
        "content": "I'd be happy to schedule a meeting to discuss {topic}. I'm available {availability}. Please let me know what works best for you.",
        "category": "internal", 
        "variables": ["topic", "availability", "duration"]
    }
}
