"""Task definitions and email generators for the Email Triage environment."""

import random
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

from models import (
    Email, EmailCategory, EmailPriority, TaskConfig, Attachment, AttachmentType
)


# Seed for reproducibility
RANDOM_SEED = 42


def deterministic_hash(task_id: str, index: int) -> int:
    """Generate deterministic hash for reproducible randomness."""
    h = hashlib.md5(f"{task_id}:{index}".encode()).hexdigest()
    return int(h, 16)


# Sentiment indicators for more realistic classification
URGENT_KEYWORDS = ["URGENT", "ASAP", "immediately", "critical", "emergency", "blocked", "down", "broken"]
HIGH_PRIORITY_KEYWORDS = ["important", "priority", "soon", "needed", "deadline", "waiting"]
SPAM_INDICATORS = ["won", "winner", "lottery", "million", "click here", "verify now", "suspended", "congratulations", "free", "limited time"]


# Email templates organized by category
EMAIL_TEMPLATES = {
    EmailCategory.CUSTOMER_SUPPORT: [
        {
            "subjects": [
                "URGENT: Order #{order_id} not delivered - Need immediate help",
                "Help needed with my account - Cannot login",
                "Product not working as expected - Very disappointed",
                "Refund request for order #{order_id} - Defective item",
                "CRITICAL: Cannot access my account - Locked out for 3 days"
            ],
            "bodies": [
                "Hi Support Team,\n\nI placed an order #{order_id} last week but it STILL hasn't arrived. My tracking shows it's stuck in transit for 5 days now. This is extremely frustrating as I needed this for an important event.\n\nOrder details:\n- Order ID: {order_id}\n- Ordered on: {date}\n- Expected delivery: 3 days ago\n\nI've been a loyal customer for 2 years and this has never happened before. Please resolve this ASAP or I will have to dispute the charge.\n\nThanks,\n{name}\nPhone: +1-555-{order_id}",
                "Hello,\n\nI'm having serious trouble with my account. Every time I try to log in, I get an error message saying 'Account temporarily suspended.' I've tried resetting my password 4 times but nothing works.\n\nThis is blocking me from accessing important data. I need this resolved urgently.\n\nAccount email: {email}\nAccount created: 2022\n\nBest,\n{name}",
                "Dear Support Team,\n\nI'm writing to express my disappointment with my recent purchase. The product I received doesn't match the description on your website AT ALL:\n\n- Listed color: Blue\n- Received: Gray\n- Missing features: Bluetooth connectivity, LED display\n\nI paid ${amount} for this and expected quality. I would like either a replacement or a full refund immediately.\n\nOrder #{order_id}\nPhotos attached showing the discrepancy.\n\nRegards,\n{name}",
                "To Whom It May Concern,\n\nI am extremely frustrated with your service. I've been trying to reach someone for a week now about order #{order_id}.\n\nThe item arrived damaged - the screen is cracked and the packaging was torn. I paid for express shipping and this is what I get?\n\nI demand:\n1. Full refund including shipping\n2. Return label at your expense\n3. Compensation for my wasted time\n\nIf I don't hear back within 24 hours, I will file a complaint with the BBB.\n\nDisappointed customer,\n{name}",
            ],
            "priority": EmailPriority.HIGH,
            "requires_reply": True
        },
        {
            "subjects": [
                "Question about my recent order #{order_id}",
                "Shipping inquiry",
                "Product question before purchase"
            ],
            "bodies": [
                "Hi,\n\nQuick question - I ordered item #{order_id} yesterday. When can I expect it to ship?\n\nThanks,\n{name}",
                "Hello,\n\nDo you ship internationally? I'm located in Canada and wondering about shipping costs and times.\n\nBest,\n{name}",
            ],
            "priority": EmailPriority.NORMAL,
            "requires_reply": True
        }
    ],
    EmailCategory.SALES: [
        {
            "subjects": [
                "Enterprise pricing inquiry - {company}",
                "Request for product demo - {num_users} user team",
                "Partnership opportunity with {company}",
                "Bulk order inquiry - 500+ units",
                "RFP Response needed by {date}"
            ],
            "bodies": [
                "Hello Sales Team,\n\nI'm the VP of Operations at {company}, a Fortune 500 company. We're evaluating solutions for our {use_case} needs and your product came highly recommended.\n\nOur requirements:\n- {num_users} users across 12 offices\n- SSO integration with Okta\n- 99.9% uptime SLA\n- Dedicated support\n\nCould you please send over:\n1. Enterprise pricing sheet\n2. Case studies from similar companies\n3. Security/compliance documentation\n\nWe're looking to make a decision within 2 weeks.\n\nBest regards,\n{name}\nVP Operations, {company}\n{email}",
                "Hi there,\n\nI discovered your product at TechCrunch Disrupt and I'm impressed. I'm the Head of Product at {company} and we're actively looking for a solution to improve our {use_case}.\n\nWould love to schedule a demo for my team of 15 stakeholders. We have budget allocated and are ready to move fast if the product fits our needs.\n\nAvailable times: Mon-Wed, 2-5pm EST\n\nThanks,\n{name}\n{company}",
                "Dear Team,\n\nWe're interested in becoming a reseller/partner for your products in the APAC region. {company} has over 200 enterprise clients who could benefit from your solution.\n\nOur partnership history:\n- 10 years in enterprise software\n- $50M+ in annual reseller revenue\n- Dedicated implementation team\n\nLet's discuss mutual benefits.\n\n{name}\nPartnership Director\n{company}",
            ],
            "priority": EmailPriority.HIGH,
            "requires_reply": True
        },
        {
            "subjects": [
                "Quick question about pricing",
                "Free trial request",
                "Comparing your product to competitors"
            ],
            "bodies": [
                "Hi,\n\nI'm interested in your product but couldn't find pricing on the website. Can you send me a quick quote for a 10-person team?\n\nThanks,\n{name}",
                "Hello,\n\nDo you offer a free trial? I'd like to test before committing.\n\nBest,\n{name}",
            ],
            "priority": EmailPriority.NORMAL,
            "requires_reply": True
        }
    ],
    EmailCategory.BILLING: [
        {
            "subjects": [
                "URGENT: Invoice #{invoice_id} - Payment issue",
                "Billing discrepancy - Double charged ${amount}",
                "Update payment method - Card expiring",
                "Question about unexpected charges on my account",
                "Need invoice for tax purposes - #{invoice_id}"
            ],
            "bodies": [
                "Hi Billing Team,\n\nI noticed I was charged TWICE for the same invoice #{invoice_id}. The duplicate charge of ${amount} appeared on {date}.\n\nTransaction details:\n- Original charge: ${amount} on {date}\n- Duplicate charge: ${amount} on {date}\n- Card ending: 4242\n\nPlease refund the duplicate charge immediately. I've attached my bank statement showing both transactions.\n\nAccount: {email}\n\nThis is urgent as it's affecting my cash flow.\n\nThanks,\n{name}",
                "Hello,\n\nI need to update the payment method on my account before my current card expires on {date}. I tried doing it through the website but getting an error.\n\nPlease either:\n1. Send instructions for updating billing info\n2. Or call me to update it over the phone: +1-555-{order_id}\n\nAccount email: {email}\n\nRegards,\n{name}",
                "Dear Billing,\n\nI'm doing my quarterly tax filing and need proper invoices for all charges from Q{quarter}.\n\nOur company details:\n- {company}\n- Tax ID: 12-3456789\n- Address: 123 Business St, New York, NY 10001\n\nPlease send itemized invoices to this email.\n\nThanks,\n{name}\nFinance Manager\n{company}",
            ],
            "priority": EmailPriority.HIGH,
            "requires_reply": True
        }
    ],
    EmailCategory.TECHNICAL: [
        {
            "subjects": [
                "CRITICAL BUG: Production system down",
                "P1: Application crashes on startup - Blocking release",
                "API integration failing - 500 errors",
                "Security vulnerability discovered - CVE-2024-{order_id}",
                "Performance degradation - 10x slower since update"
            ],
            "bodies": [
                "PRIORITY 1 - PRODUCTION DOWN\n\nOur production environment is completely down. The application crashes immediately after launching. This is affecting all {num_users} of our users.\n\nError logs attached.\n\nEnvironment:\n- OS: {os}\n- Version: {version}\n- Last working: {date}\n- Error: NullPointerException in UserAuthService.java:142\n\nSteps to reproduce:\n1. Start application\n2. Crash occurs within 5 seconds\n\nThis is CRITICAL - we're losing $10k/hour in downtime.\n\nNeed immediate assistance.\n\n{name}\nSenior DevOps Engineer\n{company}",
                "Hi Technical Support,\n\nWe're experiencing 500 errors when calling your /api/v1/users endpoint. Started happening after your maintenance window on {date}.\n\nDetails:\n- Endpoint: GET /api/v1/users\n- Auth: Bearer {api_key}\n- Error response: {{\"error\": \"internal_server_error\", \"code\": 500}}\n- Frequency: 100% of requests\n\nOur integration was working perfectly before. Is there an API change we weren't notified about?\n\nThis is blocking our product launch scheduled for tomorrow.\n\nBest,\n{name}\nLead Backend Engineer",
                "SECURITY ALERT\n\nOur security team discovered a potential vulnerability in your authentication module:\n\n- Type: SQL Injection\n- Severity: HIGH\n- Location: /api/v1/login endpoint\n- Proof of concept: ' OR '1'='1' --\n\nWe have NOT exploited this, only discovered during our security audit.\n\nPlease acknowledge receipt and provide an ETA for a patch. We may need to temporarily disable our integration if not fixed within 48 hours.\n\n{name}\nSecurity Engineer\n{company}",
            ],
            "priority": EmailPriority.URGENT,
            "requires_reply": True
        },
        {
            "subjects": [
                "Feature request: Dark mode support",
                "Question about API rate limits",
                "Documentation clarification needed"
            ],
            "bodies": [
                "Hi,\n\nOur users have been requesting dark mode. Is this on your roadmap?\n\nThanks,\n{name}",
                "Hello,\n\nWhat are the API rate limits for the Pro tier? I couldn't find this in the docs.\n\nBest,\n{name}",
            ],
            "priority": EmailPriority.LOW,
            "requires_reply": False
        }
    ],
    EmailCategory.SPAM: [
        {
            "subjects": [
                "🎉 YOU'VE WON $1,000,000!!! Claim NOW!!!",
                "⚠️ URGENT: Your account will be SUSPENDED",
                "LIMITED TIME: 95% OFF Everything - Today Only!!!",
                "Your PayPaI payment of $499.99 is pending",
                "Congratulations! You've been selected as a WINNER",
                "Re: Your Bitcoin investment has matured - $50,000 ready",
                "FINAL NOTICE: Verify your identity immediately",
                "Hot singles in your area want to meet you"
            ],
            "bodies": [
                "CONGRATULATIONS LUCKY WINNER!!!\n\nYou have been randomly selected from 10 MILLION entries to receive $1,000,000.00 USD!!!\n\n💰💰💰 CLAIM YOUR PRIZE NOW 💰💰💰\n\nClick the link below IMMEDIATELY to claim your winnings:\n\n>>> http://totally-not-a-scam-site.ru/claim-prize <<<\n\n⚠️ WARNING: This offer expires in 24 hours! Act NOW or lose your prize FOREVER!\n\nTo process your winnings, we just need:\n- Your full name\n- Social Security Number\n- Bank account details\n- A small processing fee of $99\n\nThis is 100% LEGITIMATE! We are certified by the International Lottery Commission.\n\nCongratulations again,\nThe Prize Team\n\nP.S. Forward this to 10 friends to win an EXTRA $100,000!!!",
                "ATTENTION VALUED CUSTOMER!\n\n⚠️ SECURITY ALERT ⚠️\n\nYour account has been compromised! We detected suspicious activity and your account will be PERMANENTLY SUSPENDED within 24 hours unless you verify your identity.\n\nCLICK HERE NOW TO VERIFY: http://secure-bank-login.phishing-site.com\n\nIf you do not verify, you will:\n❌ Lose access to all funds\n❌ Have your credit score damaged\n❌ Face potential legal action\n\nThis is your FINAL WARNING!\n\n- Security Team\n(This is an automated message, do not reply)",
                "🔥 FLASH SALE - 95% OFF EVERYTHING! 🔥\n\nDear Valued Customer,\n\nYou've been selected for an EXCLUSIVE offer:\n\n✅ Louis Vuitton bags - $49 (was $2,000)\n✅ Rolex watches - $79 (was $10,000)\n✅ iPhone 15 Pro - $99 (was $1,199)\n\nThis offer is ONLY for the next 2 HOURS!\n\nSHOP NOW: http://cheap-luxury-goods.scam\n\nWe accept:\n- Wire transfer\n- Bitcoin\n- Gift cards\n\nNO REFUNDS - ALL SALES FINAL\n\n*Not affiliated with any real brands*",
                "Dear Customer,\n\nYour PayPaI (PayPal) account has been charged $499.99 for:\n\n- Norton Antivirus 3-Year License\n\nIf you did NOT authorize this transaction, call us IMMEDIATELY at:\n📞 1-800-SCAM-NOW\n\nOur technicians will:\n1. Refund your money\n2. Secure your account\n3. Install remote access software (for your protection)\n\nDO NOT IGNORE THIS EMAIL!\n\nPayPaI Security Team\nReference: TXN-{order_id}",
            ],
            "priority": EmailPriority.LOW,
            "requires_reply": False
        }
    ],
    EmailCategory.INTERNAL: [
        {
            "subjects": [
                "Team meeting - {day} at {time}",
                "Q{quarter} Planning Document - Review by EOD",
                "PTO Request: {date_range}",
                "Weekly standup notes - {date}",
                "RE: Project Alpha timeline update",
                "FYI: Office closed {day} for maintenance"
            ],
            "bodies": [
                "Hi team,\n\nReminder that we have our weekly sync on {day} at {time} in Conference Room B.\n\nAgenda:\n1. Sprint review (15 min)\n2. Blockers discussion (10 min)\n3. Next sprint planning (20 min)\n4. Q&A (5 min)\n\nPlease come prepared with your updates. If you can't attend, send your status to the Slack channel beforehand.\n\nJoin via Zoom: https://zoom.us/j/123456789\nPassword: standup\n\nCheers,\n{name}",
                "Hello everyone,\n\nPlease review the attached Q{quarter} planning document before our strategy meeting on Friday.\n\nKey discussion points:\n- Budget allocation ($2.5M available)\n- Hiring plan (3 engineers, 1 PM)\n- Product roadmap priorities\n- OKRs for next quarter\n\nI need everyone's comments by EOD Thursday so I can consolidate feedback.\n\nLet me know if you have questions.\n\nBest,\n{name}\nDirector of Engineering",
                "Hi Manager,\n\nI'd like to request PTO for {date_range}.\n\nReason: Family vacation\n\nI've already:\n- Cleared my calendar\n- Set up OOO auto-reply\n- Briefed {name} on my ongoing tasks\n\nPlease approve when you have a chance.\n\nThanks,\n{name}",
            ],
            "priority": EmailPriority.NORMAL,
            "requires_reply": False
        }
    ],
    EmailCategory.NEWSLETTER: [
        {
            "subjects": [
                "Weekly Digest: Top tech stories this week",
                "Your {month} newsletter from TechInsider",
                "🚀 New features announcement - What's new in v3.0",
                "Industry insights: {topic} trends for 2024",
                "📰 The Morning Brief - {date}",
                "[Newsletter] 5 things you missed this week"
            ],
            "bodies": [
                "Hello {name},\n\nHere's your weekly digest from TechInsider!\n\n📰 TOP STORIES THIS WEEK:\n\n1. AI advances in healthcare revolutionize diagnosis\n   Read more →\n\n2. New regulations reshape tech industry landscape\n   Read more →\n\n3. Startup funding hits record highs in Q{quarter}\n   Read more →\n\n4. Remote work trends: What companies are doing now\n   Read more →\n\n📊 QUICK STATS:\n- AI market growth: +34% YoY\n- Tech layoffs: Down 50% from peak\n- VC funding: $12B this quarter\n\nRead the full digest on our blog.\n\nBest,\nThe TechInsider Team\n\n---\nYou're receiving this because you signed up at techinsider.com\nUnsubscribe: https://techinsider.com/unsubscribe?email={email}",
                "Hi {name},\n\n✨ EXCITING NEWS! ✨\n\nWe've just released version 3.0 with amazing new features:\n\n🌙 Dark mode support\n  Finally! Your eyes will thank you.\n\n⚡ 2x performance improvements\n  Everything is faster now.\n\n🔗 15 new integrations\n  Slack, Notion, GitHub, and more!\n\n🔒 Enhanced security\n  SOC 2 Type II certified.\n\nUpdate now to get all these features!\n\nQuestions? Reply to this email or visit our help center.\n\nHappy exploring,\nThe Product Team\n\n---\nUnsubscribe from product updates",
            ],
            "priority": EmailPriority.LOW,
            "requires_reply": False
        }
    ]
}

SENDER_NAMES = [
    "John Smith", "Jane Doe", "Michael Johnson", "Emily Williams", "David Brown",
    "Sarah Davis", "Robert Wilson", "Lisa Anderson", "James Taylor", "Jennifer Martinez",
    "William Garcia", "Elizabeth Rodriguez", "Thomas Lee", "Margaret White", "Charles Harris",
    "Amanda Wilson", "Christopher Moore", "Jessica Thompson", "Andrew Miller", "Michelle White",
    "Kevin Jackson", "Nicole Turner", "Ryan Clark", "Stephanie Lewis", "Brian Walker",
    "Kimberly Hall", "Jonathan Allen", "Laura Young", "Matthew King", "Rachel Green",
    "Daniel Scott", "Ashley Adams", "Joseph Baker", "Mary Nelson", "Mark Hill"
]

# VIP senders - C-level executives and enterprise domains
VIP_SENDERS = [
    ("Sarah Chen", "CEO", "fortune500corp.com"),
    ("Michael Rodriguez", "CTO", "enterprise-global.com"),
    ("Jennifer Kim", "CFO", "megacorp-inc.com"),
    ("David Thompson", "VP Engineering", "bigtech-solutions.com"),
    ("Amanda Richards", "Chief Marketing Officer", "global-enterprise.com"),
    ("Robert Chang", "VP Sales", "industry-leader.com"),
]

# Suspicious/spam senders - suspicious domains and patterns
SUSPICIOUS_SENDERS = [
    ("Lottery Winner", "noreply@suspiciouslottery.xyz"),
    ("Account Verification", "verify@secure-account-check.info"),
    ("Prize Committee", "claims@winner-notification.biz"),
    ("Banking Alert", "alert@bank-security-team.tk"),
    ("Security Team", "urgent@account-security.ml"),
    ("PayPal Service", "verify@paypal-security.tk"),
    ("Amazon Customer", "winner@amazon-prizes.info"),
    ("Microsoft Support", "update@windows-security.xyz"),
]

COMPANIES = [
    "TechCorp Inc.", "Global Solutions", "Innovate Labs", "DataSync Co.",
    "CloudFirst", "NextGen Systems", "Digital Ventures", "Prime Analytics",
    "Enterprise Solutions", "DataFlow Systems", "Cloud Innovations", "AI Dynamics",
    "Quantum Computing", "Blockchain Technologies", "CyberSec Pro", "DevOps Masters"
]


def generate_email_address(name: str, force_domain: str = None) -> str:
    """Generate email address from name."""
    parts = name.lower().split()
    if force_domain:
        domain = force_domain
    else:
        domains = ["gmail.com", "company.com", "outlook.com", "business.org", "tech.io"]
        domain = domains[hash(name) % len(domains)]
    return f"{parts[0]}.{parts[-1]}@{domain}"


def fill_template(template: str, seed: int) -> str:
    """Fill template with deterministic random values."""
    rng = random.Random(seed)
    
    replacements = {
        "{order_id}": str(rng.randint(100000, 999999)),
        "{invoice_id}": f"INV-{rng.randint(1000, 9999)}",
        "{date}": (datetime.now() - timedelta(days=rng.randint(1, 30))).strftime("%Y-%m-%d"),
        "{date_range}": f"{rng.randint(1, 28)}-{rng.randint(1, 28)} {rng.choice(['Jan', 'Feb', 'Mar'])}",
        "{name}": rng.choice(SENDER_NAMES),
        "{email}": f"user{rng.randint(100, 999)}@example.com",
        "{company}": rng.choice(COMPANIES),
        "{num_users}": str(rng.randint(50, 500)),
        "{use_case}": rng.choice(["workflow automation", "project management", "team collaboration"]),
        "{amount}": f"{rng.randint(10, 500)}.{rng.randint(0, 99):02d}",
        "{os}": rng.choice(["Windows 11", "macOS 14", "Ubuntu 22.04"]),
        "{version}": f"{rng.randint(1, 5)}.{rng.randint(0, 9)}.{rng.randint(0, 9)}",
        "{api_key}": f"sk-{''.join(rng.choices('abcdef0123456789', k=16))}",
        "{day}": rng.choice(["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]),
        "{time}": f"{rng.randint(9, 17)}:00",
        "{quarter}": str(rng.randint(1, 4)),
        "{month}": rng.choice(["January", "February", "March", "April"]),
        "{topic}": rng.choice(["AI Trends", "Cloud Computing", "Cybersecurity", "DevOps"])
    }
    
    result = template
    for key, value in replacements.items():
        result = result.replace(key, value)
    
    return result


def generate_attachments(
    email_id: str,
    category: EmailCategory,
    rng: random.Random
) -> List[Attachment]:
    """Generate realistic attachments for multimodal email simulation."""
    if category == EmailCategory.TECHNICAL:
        return [
            Attachment(
                attachment_id=f"{email_id}_att_0",
                filename="error-log.txt",
                mime_type="text/plain",
                attachment_type=AttachmentType.LOG,
                content_summary="Application stack trace and crash context",
                ocr_text="NullPointerException at UserAuthService.java:142"
            ),
            Attachment(
                attachment_id=f"{email_id}_att_1",
                filename="screenshot-error.png",
                mime_type="image/png",
                attachment_type=AttachmentType.IMAGE,
                content_summary="Screenshot of 500 Internal Server Error page",
                ocr_text="HTTP 500 Internal Server Error Request ID: req_9d2f"
            ),
        ]
    if category == EmailCategory.BILLING:
        return [
            Attachment(
                attachment_id=f"{email_id}_att_0",
                filename="invoice-discrepancy.pdf",
                mime_type="application/pdf",
                attachment_type=AttachmentType.PDF,
                content_summary="Invoice with highlighted duplicate billing line item",
                ocr_text="Invoice INV-4432 duplicate charge amount $249.99"
            )
        ]
    if category == EmailCategory.CUSTOMER_SUPPORT:
        return [
            Attachment(
                attachment_id=f"{email_id}_att_0",
                filename="damaged-product.jpg",
                mime_type="image/jpeg",
                attachment_type=AttachmentType.IMAGE,
                content_summary="Customer photo of damaged package/product",
                ocr_text="Package torn, screen cracked, shipping label visible"
            )
        ]
    if category == EmailCategory.SPAM:
        return [
            Attachment(
                attachment_id=f"{email_id}_att_0",
                filename="urgent-verification.png",
                mime_type="image/png",
                attachment_type=AttachmentType.IMAGE,
                content_summary="Phishing-style fake security notice image",
                ocr_text="Verify account now to avoid permanent suspension"
            )
        ]
    if category == EmailCategory.INTERNAL and rng.random() < 0.6:
        return [
            Attachment(
                attachment_id=f"{email_id}_att_0",
                filename="q-planning.docx",
                mime_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                attachment_type=AttachmentType.DOCUMENT,
                content_summary="Quarterly planning document",
                ocr_text="Q planning priorities, budget allocation, hiring plan"
            )
        ]
    return []


def generate_emails(task_id: str, count: int, category_distribution: Dict[EmailCategory, float]) -> Tuple[List[Email], Dict[str, Dict[str, Any]]]:
    """Generate a list of emails with ground truth."""
    emails = []
    ground_truth = {}
    
    base_time = datetime.now() - timedelta(hours=count)
    
    for i in range(count):
        seed = deterministic_hash(task_id, i)
        rng = random.Random(seed)
        
        # Select category based on distribution
        rand_val = rng.random()
        cumulative = 0.0
        selected_category = EmailCategory.CUSTOMER_SUPPORT
        for cat, prob in category_distribution.items():
            cumulative += prob
            if rand_val <= cumulative:
                selected_category = cat
                break
        
        # Get template for category
        templates = EMAIL_TEMPLATES[selected_category]
        template = rng.choice(templates)
        
        # Determine sender type based on category and position
        # Use VIP for high-value sales inquiries (20% chance in sales category)
        # Use suspicious for spam emails (80% chance in spam category)
        use_vip = selected_category == EmailCategory.SALES and rng.random() < 0.3
        use_suspicious = selected_category == EmailCategory.SPAM and rng.random() < 0.8
        
        if use_vip:
            vip_info = rng.choice(VIP_SENDERS)
            sender_name = f"{vip_info[0]}, {vip_info[1]}"  # e.g., "Sarah Chen, CEO"
            sender_email = generate_email_address(vip_info[0], force_domain=vip_info[2])
        elif use_suspicious:
            susp_info = rng.choice(SUSPICIOUS_SENDERS)
            sender_name = susp_info[0]
            sender_email = susp_info[1]
        else:
            sender_name = rng.choice(SENDER_NAMES)
            sender_email = generate_email_address(sender_name)
        
        subject = fill_template(rng.choice(template["subjects"]), seed)
        body = fill_template(rng.choice(template["bodies"]), seed)
        
        email_id = f"email_{i:03d}"
        received_at = (base_time + timedelta(hours=i, minutes=rng.randint(0, 59))).isoformat()
        attachment_probability = {
            EmailCategory.TECHNICAL: 0.7,
            EmailCategory.BILLING: 0.5,
            EmailCategory.CUSTOMER_SUPPORT: 0.4,
            EmailCategory.INTERNAL: 0.35,
            EmailCategory.SPAM: 0.3,
            EmailCategory.SALES: 0.2,
            EmailCategory.NEWSLETTER: 0.1,
        }.get(selected_category, 0.2)
        has_attachments = rng.random() < attachment_probability
        attachments = generate_attachments(email_id, selected_category, rng) if has_attachments else []
        
        email = Email(
            id=email_id,
            sender=sender_email,
            sender_name=sender_name,
            subject=subject,
            body=body,
            received_at=received_at,
            is_read=False,
            has_attachments=has_attachments,
            attachments=attachments
        )
        
        emails.append(email)
        
        # Store ground truth
        ground_truth[email_id] = {
            "correct_category": selected_category,
            "correct_priority": template["priority"],
            "is_spam": selected_category == EmailCategory.SPAM,
            "requires_reply": template.get("requires_reply", False),
            "should_archive": selected_category in [EmailCategory.NEWSLETTER, EmailCategory.SPAM]
        }
    
    return emails, ground_truth


# Task definitions
TASKS = {
    "task_easy_categorize": TaskConfig(
        task_id="task_easy_categorize",
        task_name="Basic Email Categorization",
        description=(
            "Categorize 5 emails into their correct categories. "
            "Focus on identifying spam vs legitimate emails and basic categorization. "
            "Mark spam emails appropriately and archive newsletters."
        ),
        difficulty="easy",
        max_steps=20,
        email_count=5,
        success_criteria={
            "min_categorization_accuracy": 0.6,
            "spam_detection_required": True,
            "all_emails_processed": True
        }
    ),
    "task_medium_triage": TaskConfig(
        task_id="task_medium_triage",
        task_name="Email Triage with Prioritization",
        description=(
            "Triage 10 emails: categorize them, set priorities, and take appropriate actions. "
            "Urgent customer support issues should be flagged. "
            "Spam should be marked and archived. Reply to high-priority customer emails."
        ),
        difficulty="medium",
        max_steps=40,
        email_count=10,
        success_criteria={
            "min_categorization_accuracy": 0.7,
            "min_prioritization_accuracy": 0.6,
            "spam_detection_required": True,
            "high_priority_flagged": True,
            "customer_support_replied": True
        }
    ),
    "task_hard_full_inbox": TaskConfig(
        task_id="task_hard_full_inbox",
        task_name="Full Inbox Management",
        description=(
            "Manage a busy inbox of 15 emails including threaded conversations. "
            "Categorize all emails, set correct priorities based on SLA requirements, "
            "flag urgent items, mark and archive spam, archive newsletters, "
            "reply to customer support and sales inquiries, and forward technical issues "
            "to tech-support@company.com. Handle email threads appropriately. "
            "Complete all tasks efficiently within the step limit."
        ),
        difficulty="hard",
        max_steps=60,
        email_count=15,
        thread_count=3,
        sla_enabled=True,
        success_criteria={
            "min_categorization_accuracy": 0.8,
            "min_prioritization_accuracy": 0.7,
            "spam_detection_accuracy": 0.9,
            "urgent_items_flagged": True,
            "newsletters_archived": True,
            "customer_support_replied": True,
            "technical_forwarded": True,
            "efficiency_bonus": True,
            "thread_handling": True
        }
    )
}


def get_task_emails(task_id: str) -> Tuple[List[Email], Dict[str, Dict[str, Any]]]:
    """Get emails for a specific task."""
    task = TASKS.get(task_id)
    if not task:
        raise ValueError(f"Unknown task: {task_id}")
    
    # Define category distribution based on difficulty
    if task.difficulty == "easy":
        distribution = {
            EmailCategory.CUSTOMER_SUPPORT: 0.3,
            EmailCategory.SPAM: 0.3,
            EmailCategory.NEWSLETTER: 0.2,
            EmailCategory.INTERNAL: 0.2
        }
    elif task.difficulty == "medium":
        distribution = {
            EmailCategory.CUSTOMER_SUPPORT: 0.25,
            EmailCategory.SALES: 0.15,
            EmailCategory.BILLING: 0.1,
            EmailCategory.SPAM: 0.2,
            EmailCategory.INTERNAL: 0.15,
            EmailCategory.NEWSLETTER: 0.15
        }
    else:  # hard
        distribution = {
            EmailCategory.CUSTOMER_SUPPORT: 0.2,
            EmailCategory.SALES: 0.15,
            EmailCategory.BILLING: 0.1,
            EmailCategory.TECHNICAL: 0.15,
            EmailCategory.SPAM: 0.15,
            EmailCategory.INTERNAL: 0.1,
            EmailCategory.NEWSLETTER: 0.15
        }
    
    return generate_emails(task_id, task.email_count, distribution)


def get_task_config(task_id: str) -> TaskConfig:
    """Get configuration for a task."""
    if task_id not in TASKS:
        raise ValueError(f"Unknown task: {task_id}")
    return TASKS[task_id]


def list_tasks() -> List[TaskConfig]:
    """List all available tasks."""
    return list(TASKS.values())
