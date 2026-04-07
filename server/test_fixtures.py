"""
Advanced Test Fixtures
Reusable test data for comprehensive testing
"""
import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import string

from models import (
    Email, Action, ActionType, EmailPriority, 
    EmailCategory, Attachment, Observation
)


# ============================================================================
# Email Fixtures
# ============================================================================

@pytest.fixture
def urgent_vip_email() -> Email:
    """VIP sender with urgent request"""
    return Email(
        id="vip-urgent-001",
        sender="ceo@company.com",
        recipient="agent@company.com",
        subject="URGENT: Board Meeting Tomorrow",
        body="Please prepare Q4 financials immediately. This is critical for tomorrow's board presentation.",
        timestamp=datetime.now(),
        category=EmailCategory.URGENT,
        priority=EmailPriority.HIGH,
        is_spam=False,
        sentiment_score=0.2,
        vip_sender=True,
        thread_id="thread-vip-001"
    )


@pytest.fixture
def spam_phishing_email() -> Email:
    """Obvious phishing spam"""
    return Email(
        id="spam-phishing-001",
        sender="prince@nigeria-totally-legit.com",
        recipient="agent@company.com",
        subject="URGENT!!! YOU WON $10,000,000 USD!!!",
        body="""
        Dear Beneficiary,
        
        You have been selected to receive $10,000,000 USD. 
        Click here NOW: http://suspicious-phishing-site.ru/steal-credentials
        
        Send your SSN, credit card, and bank account details immediately!
        """,
        timestamp=datetime.now(),
        category=EmailCategory.SPAM,
        priority=EmailPriority.LOW,
        is_spam=True,
        sentiment_score=-0.9,
        vip_sender=False
    )


@pytest.fixture
def invoice_with_attachment() -> Email:
    """Invoice email with PDF attachment"""
    return Email(
        id="invoice-001",
        sender="billing@vendor.com",
        recipient="accounting@company.com",
        subject="Invoice #INV-2024-00123 - Payment Due",
        body="Please find attached invoice for services rendered in March 2024. Payment due in 30 days.",
        timestamp=datetime.now(),
        category=EmailCategory.BILLING,
        priority=EmailPriority.MEDIUM,
        is_spam=False,
        sentiment_score=0.0,
        attachments=[
            Attachment(
                filename="invoice_00123.pdf",
                content_type="application/pdf",
                size_bytes=245680,
                data_uri="data:application/pdf;base64,JVBERi0xLjQKJeLjz9MKMSAw..."
            )
        ]
    )


@pytest.fixture
def customer_support_angry() -> Email:
    """Angry customer support request"""
    return Email(
        id="support-angry-001",
        sender="frustrated.customer@gmail.com",
        recipient="support@company.com",
        subject="TERRIBLE SERVICE - DEMAND REFUND",
        body="""
        I've been trying to use your product for 3 days and it DOESN'T WORK!
        Your support team hasn't responded to my 5 emails.
        This is completely unacceptable. I want a FULL REFUND immediately!
        """,
        timestamp=datetime.now(),
        category=EmailCategory.SUPPORT,
        priority=EmailPriority.HIGH,
        is_spam=False,
        sentiment_score=-0.85,
        vip_sender=False
    )


@pytest.fixture
def newsletter_marketing() -> Email:
    """Marketing newsletter"""
    return Email(
        id="newsletter-001",
        sender="newsletter@techcrunch.com",
        recipient="agent@company.com",
        subject="TechCrunch Daily: Today's Top Stories",
        body="Here are today's top tech stories: AI advances, startup funding, product launches...",
        timestamp=datetime.now(),
        category=EmailCategory.NEWSLETTER,
        priority=EmailPriority.LOW,
        is_spam=False,
        sentiment_score=0.1,
        vip_sender=False
    )


@pytest.fixture
def meeting_invitation() -> Email:
    """Meeting invitation"""
    return Email(
        id="meeting-001",
        sender="colleague@company.com",
        recipient="agent@company.com",
        subject="Meeting: Project Kickoff - Tomorrow 2PM",
        body="""
        Hi team,
        
        Let's meet tomorrow at 2PM in Conference Room A to kick off the new project.
        
        Agenda:
        - Project overview
        - Timeline discussion
        - Resource allocation
        
        Please confirm attendance.
        """,
        timestamp=datetime.now(),
        category=EmailCategory.WORK,
        priority=EmailPriority.MEDIUM,
        is_spam=False,
        sentiment_score=0.4,
        vip_sender=False
    )


@pytest.fixture
def pii_sensitive_email() -> Email:
    """Email containing PII"""
    return Email(
        id="pii-001",
        sender="hr@company.com",
        recipient="agent@company.com",
        subject="Employee Records - CONFIDENTIAL",
        body="""
        Employee SSN: 123-45-6789
        Credit Card: 4532-1234-5678-9010
        Phone: 555-123-4567
        Address: 123 Main St, Anytown, CA 90210
        """,
        timestamp=datetime.now(),
        category=EmailCategory.PERSONAL,
        priority=EmailPriority.HIGH,
        is_spam=False,
        sentiment_score=0.0,
        vip_sender=True
    )


@pytest.fixture
def malformed_email() -> Email:
    """Email with unusual/malformed content"""
    return Email(
        id="malformed-001",
        sender="weird@example.com",
        recipient="agent@company.com",
        subject="�����",  # Unicode issues
        body="<script>alert('XSS')</script>" * 100,  # XSS attempt
        timestamp=datetime.now(),
        category=EmailCategory.SPAM,
        priority=EmailPriority.LOW,
        is_spam=True,
        sentiment_score=-1.0,
        vip_sender=False
    )


@pytest.fixture
def massive_attachment_email() -> Email:
    """Email with very large attachment"""
    return Email(
        id="large-attachment-001",
        sender="design@partner.com",
        recipient="agent@company.com",
        subject="Design Files for Review",
        body="Please review these design mockups.",
        timestamp=datetime.now(),
        category=EmailCategory.WORK,
        priority=EmailPriority.MEDIUM,
        is_spam=False,
        sentiment_score=0.2,
        attachments=[
            Attachment(
                filename="mockups.zip",
                content_type="application/zip",
                size_bytes=50 * 1024 * 1024,  # 50MB
                data_uri="data:application/zip;base64,..." + "A" * 1000
            )
        ]
    )


# ============================================================================
# Email Batch Fixtures
# ============================================================================

@pytest.fixture
def diverse_inbox() -> List[Email]:
    """Diverse set of emails for testing"""
    return [
        Email(
            id=f"batch-{i}",
            sender=random.choice([
                "ceo@company.com",
                "spam@scam.com",
                "customer@client.com",
                "newsletter@news.com",
                "colleague@company.com"
            ]),
            recipient="agent@company.com",
            subject=random.choice([
                "URGENT: Action Required",
                "Invoice Payment",
                "Meeting Tomorrow",
                "Newsletter: Weekly Update",
                "Question about product"
            ]),
            body="".join(random.choices(string.ascii_letters + " ", k=200)),
            timestamp=datetime.now() - timedelta(hours=random.randint(0, 48)),
            category=random.choice(list(EmailCategory)),
            priority=random.choice(list(EmailPriority)),
            is_spam=random.random() < 0.1,
            sentiment_score=random.uniform(-1.0, 1.0),
            vip_sender=random.random() < 0.05
        )
        for i in range(100)
    ]


@pytest.fixture
def threaded_emails() -> List[Email]:
    """Email thread for testing threading"""
    thread_id = "thread-conversation-001"
    
    return [
        Email(
            id="thread-1",
            sender="alice@company.com",
            recipient="bob@company.com",
            subject="Project Discussion",
            body="Let's discuss the new project.",
            timestamp=datetime.now() - timedelta(hours=3),
            category=EmailCategory.WORK,
            priority=EmailPriority.MEDIUM,
            is_spam=False,
            thread_id=thread_id
        ),
        Email(
            id="thread-2",
            sender="bob@company.com",
            recipient="alice@company.com",
            subject="RE: Project Discussion",
            body="Great idea! When should we start?",
            timestamp=datetime.now() - timedelta(hours=2),
            category=EmailCategory.WORK,
            priority=EmailPriority.MEDIUM,
            is_spam=False,
            thread_id=thread_id
        ),
        Email(
            id="thread-3",
            sender="alice@company.com",
            recipient="bob@company.com",
            subject="RE: Project Discussion",
            body="How about tomorrow at 2PM?",
            timestamp=datetime.now() - timedelta(hours=1),
            category=EmailCategory.WORK,
            priority=EmailPriority.MEDIUM,
            is_spam=False,
            thread_id=thread_id
        )
    ]


# ============================================================================
# Action Fixtures
# ============================================================================

@pytest.fixture
def categorize_action() -> Action:
    """Basic categorize action"""
    return Action(
        action_type=ActionType.CATEGORIZE,
        email_id="test-email-001",
        category="work"
    )


@pytest.fixture
def prioritize_action() -> Action:
    """Basic prioritize action"""
    return Action(
        action_type=ActionType.PRIORITIZE,
        email_id="test-email-001",
        priority="high"
    )


@pytest.fixture
def flag_spam_action() -> Action:
    """Flag as spam action"""
    return Action(
        action_type=ActionType.FLAG_SPAM,
        email_id="spam-email-001"
    )


@pytest.fixture
def reply_action() -> Action:
    """Reply action"""
    return Action(
        action_type=ActionType.REPLY,
        email_id="test-email-001",
        reply_text="Thank you for your email. We'll get back to you soon."
    )


@pytest.fixture
def batch_actions() -> List[Action]:
    """Batch of actions"""
    return [
        Action(action_type=ActionType.CATEGORIZE, email_id=f"email-{i}", category="work")
        for i in range(10)
    ]


# ============================================================================
# Edge Case Fixtures
# ============================================================================

@pytest.fixture
def edge_case_emails() -> List[Dict[str, Any]]:
    """Collection of edge case scenarios"""
    return [
        {
            "name": "Empty subject",
            "email": Email(
                id="edge-empty-subject",
                sender="test@example.com",
                recipient="agent@company.com",
                subject="",
                body="Email with no subject",
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        },
        {
            "name": "Empty body",
            "email": Email(
                id="edge-empty-body",
                sender="test@example.com",
                recipient="agent@company.com",
                subject="Subject only",
                body="",
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        },
        {
            "name": "Very long subject",
            "email": Email(
                id="edge-long-subject",
                sender="test@example.com",
                recipient="agent@company.com",
                subject="A" * 500,  # 500 characters
                body="Normal body",
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        },
        {
            "name": "Very long body",
            "email": Email(
                id="edge-long-body",
                sender="test@example.com",
                recipient="agent@company.com",
                subject="Normal subject",
                body="B" * 50000,  # 50K characters
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        },
        {
            "name": "Special characters",
            "email": Email(
                id="edge-special-chars",
                sender="test@example.com",
                recipient="agent@company.com",
                subject="Special: <>&\"'`!@#$%^&*()",
                body="Unicode: 你好 مرحبا שלום",
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        },
        {
            "name": "Future timestamp",
            "email": Email(
                id="edge-future-time",
                sender="time-traveler@future.com",
                recipient="agent@company.com",
                subject="From the future",
                body="This email is from tomorrow",
                timestamp=datetime.now() + timedelta(days=1),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        },
        {
            "name": "Very old timestamp",
            "email": Email(
                id="edge-old-time",
                sender="archival@past.com",
                recipient="agent@company.com",
                subject="From the past",
                body="This email is 10 years old",
                timestamp=datetime.now() - timedelta(days=3650),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
        }
    ]


# ============================================================================
# Mock Data Generators
# ============================================================================

@pytest.fixture
def email_generator():
    """Factory for generating test emails"""
    
    def generate(count: int = 10, **kwargs) -> List[Email]:
        emails = []
        for i in range(count):
            email = Email(
                id=kwargs.get('id', f"generated-{i}"),
                sender=kwargs.get('sender', f"sender{i}@example.com"),
                recipient=kwargs.get('recipient', "agent@company.com"),
                subject=kwargs.get('subject', f"Test Email {i}"),
                body=kwargs.get('body', f"Test body content {i}"),
                timestamp=kwargs.get('timestamp', datetime.now()),
                category=kwargs.get('category', EmailCategory.WORK),
                priority=kwargs.get('priority', EmailPriority.MEDIUM),
                is_spam=kwargs.get('is_spam', False),
                sentiment_score=kwargs.get('sentiment_score', 0.0),
                vip_sender=kwargs.get('vip_sender', False)
            )
            emails.append(email)
        return emails
    
    return generate


# ============================================================================
# Performance Test Data
# ============================================================================

@pytest.fixture
def large_dataset() -> List[Email]:
    """Large dataset for performance testing"""
    return [
        Email(
            id=f"perf-{i}",
            sender=f"sender{i % 100}@company.com",
            recipient="agent@company.com",
            subject=f"Performance Test Email {i}",
            body="This is a performance test email body. " * 50,
            timestamp=datetime.now() - timedelta(seconds=i),
            category=random.choice(list(EmailCategory)),
            priority=random.choice(list(EmailPriority)),
            is_spam=random.random() < 0.05,
            sentiment_score=random.uniform(-1.0, 1.0),
            vip_sender=random.random() < 0.02
        )
        for i in range(10000)
    ]
