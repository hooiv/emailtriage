"""
Intelligent Response Generator

This module implements:
1. Template-based response generation with dynamic personalization
2. Context-aware response drafting
3. Tone analysis and matching
4. Response quality scoring
5. Multi-lingual support scaffolding
6. Response history and learning
"""

import re
import random
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class ResponseTone(Enum):
    """Available response tones."""
    FORMAL = "formal"
    PROFESSIONAL = "professional"
    FRIENDLY = "friendly"
    EMPATHETIC = "empathetic"
    URGENT = "urgent"
    APOLOGETIC = "apologetic"


class ResponseType(Enum):
    """Types of response templates."""
    ACKNOWLEDGMENT = "acknowledgment"
    INFORMATION_REQUEST = "information_request"
    ISSUE_RESOLUTION = "issue_resolution"
    ESCALATION = "escalation"
    FOLLOW_UP = "follow_up"
    SALES_RESPONSE = "sales_response"
    SUPPORT_RESPONSE = "support_response"
    MEETING_RESPONSE = "meeting_response"
    DECLINE = "decline"
    CONFIRMATION = "confirmation"


@dataclass
class ResponseTemplate:
    """A response template with placeholders."""
    id: str
    type: ResponseType
    tone: ResponseTone
    subject_template: str
    body_template: str
    category_tags: List[str]
    priority_tags: List[str]
    placeholders: List[str]
    quality_score: float = 0.8
    usage_count: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "tone": self.tone.value,
            "subject_template": self.subject_template,
            "body_template": self.body_template,
            "category_tags": self.category_tags,
            "priority_tags": self.priority_tags,
            "placeholders": self.placeholders,
            "quality_score": self.quality_score,
            "usage_count": self.usage_count
        }


@dataclass
class GeneratedResponse:
    """A generated response with metadata."""
    id: str
    email_id: str
    template_id: Optional[str]
    subject: str
    body: str
    tone: ResponseTone
    response_type: ResponseType
    confidence: float
    personalization_score: float
    suggestions: List[str]
    generated_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "email_id": self.email_id,
            "template_id": self.template_id,
            "subject": self.subject,
            "body": self.body,
            "tone": self.tone.value,
            "response_type": self.response_type.value,
            "confidence": self.confidence,
            "personalization_score": self.personalization_score,
            "suggestions": self.suggestions,
            "generated_at": self.generated_at.isoformat()
        }


class ResponseGenerator:
    """
    Intelligent response generator for email triage.
    
    Features:
    - Template-based response generation
    - Context-aware personalization
    - Tone matching
    - Quality scoring
    """
    
    def __init__(self):
        """Initialize the response generator."""
        self.templates: Dict[str, ResponseTemplate] = {}
        self.generated_responses: List[GeneratedResponse] = []
        self.response_history: Dict[str, List[str]] = defaultdict(list)
        self._response_counter = 0
        
        # Initialize default templates
        self._init_default_templates()
        
        # Greeting variations by tone
        self.greetings = {
            ResponseTone.FORMAL: ["Dear {name},", "Dear {name},", "To Whom It May Concern,"],
            ResponseTone.PROFESSIONAL: ["Hi {name},", "Hello {name},", "Dear {name},"],
            ResponseTone.FRIENDLY: ["Hi {name}!", "Hey {name},", "Hello {name}!"],
            ResponseTone.EMPATHETIC: ["Dear {name},", "Hi {name},"],
            ResponseTone.URGENT: ["Hi {name},", "{name},"],
            ResponseTone.APOLOGETIC: ["Dear {name},", "Hi {name},"],
        }
        
        # Closing variations by tone
        self.closings = {
            ResponseTone.FORMAL: ["Best regards,", "Sincerely,", "Respectfully,"],
            ResponseTone.PROFESSIONAL: ["Best regards,", "Thanks,", "Best,"],
            ResponseTone.FRIENDLY: ["Cheers!", "Thanks!", "Best,"],
            ResponseTone.EMPATHETIC: ["Warm regards,", "Take care,", "Best wishes,"],
            ResponseTone.URGENT: ["Please respond ASAP.", "Looking forward to your immediate response.", "Best,"],
            ResponseTone.APOLOGETIC: ["With sincere apologies,", "Thank you for your patience,", "Best regards,"],
        }
        
        # Sentiment to tone mapping
        self.sentiment_tone_map = {
            "very_negative": ResponseTone.EMPATHETIC,
            "negative": ResponseTone.APOLOGETIC,
            "neutral": ResponseTone.PROFESSIONAL,
            "positive": ResponseTone.FRIENDLY,
            "very_positive": ResponseTone.FRIENDLY,
        }
        
        # Category to response type mapping
        self.category_response_map = {
            "customer_support": ResponseType.SUPPORT_RESPONSE,
            "sales": ResponseType.SALES_RESPONSE,
            "technical": ResponseType.ISSUE_RESOLUTION,
            "billing": ResponseType.SUPPORT_RESPONSE,
            "internal": ResponseType.MEETING_RESPONSE,
            "newsletter": ResponseType.CONFIRMATION,
        }
    
    def _init_default_templates(self) -> None:
        """Initialize default response templates."""
        templates = [
            # Customer Support - Acknowledgment
            ResponseTemplate(
                id="tmpl_support_ack",
                type=ResponseType.ACKNOWLEDGMENT,
                tone=ResponseTone.EMPATHETIC,
                subject_template="Re: {original_subject}",
                body_template="""Thank you for reaching out to us regarding {issue_summary}.

I understand how {frustration_acknowledgment} this situation must be, and I want to assure you that we're here to help.

I've logged your concern and our team is looking into it. You can expect an update within {response_time}.

In the meantime, if you have any additional information that might help us resolve this faster, please don't hesitate to share it.

Your ticket reference: {ticket_id}""",
                category_tags=["customer_support", "technical"],
                priority_tags=["high", "urgent"],
                placeholders=["issue_summary", "frustration_acknowledgment", "response_time", "ticket_id"],
                quality_score=0.9
            ),
            
            # Customer Support - Resolution
            ResponseTemplate(
                id="tmpl_support_resolved",
                type=ResponseType.ISSUE_RESOLUTION,
                tone=ResponseTone.PROFESSIONAL,
                subject_template="Re: {original_subject} - Resolved",
                body_template="""Great news! I've looked into your concern and I'm happy to report that {resolution_summary}.

Here's what was done:
{resolution_steps}

{additional_instructions}

Is there anything else I can help you with? Please don't hesitate to reach out if you have any questions.

Best regards,
Customer Support Team""",
                category_tags=["customer_support"],
                priority_tags=["normal", "high"],
                placeholders=["resolution_summary", "resolution_steps", "additional_instructions"],
                quality_score=0.9
            ),
            
            # Sales - Initial Response
            ResponseTemplate(
                id="tmpl_sales_initial",
                type=ResponseType.SALES_RESPONSE,
                tone=ResponseTone.PROFESSIONAL,
                subject_template="Re: {original_subject} - Thank You for Your Interest",
                body_template="""Thank you for reaching out about {product_service}!

I'd be happy to {offer_summary}. Based on what you've shared, I think {recommendation}.

{pricing_info}

Would you be available for a quick {meeting_duration} call this week? I have availability on:
{available_times}

Looking forward to learning more about your needs.

Best regards,
{sender_name}""",
                category_tags=["sales"],
                priority_tags=["high", "normal"],
                placeholders=["product_service", "offer_summary", "recommendation", "pricing_info", "meeting_duration", "available_times", "sender_name"],
                quality_score=0.85
            ),
            
            # Technical - Bug Acknowledgment
            ResponseTemplate(
                id="tmpl_tech_bug",
                type=ResponseType.ACKNOWLEDGMENT,
                tone=ResponseTone.PROFESSIONAL,
                subject_template="Re: {original_subject} - Bug Report Received",
                body_template="""Thank you for reporting this issue.

I've created a ticket for our engineering team to investigate: {ticket_id}

Here's what we know so far:
- Issue: {issue_description}
- Impact: {impact_assessment}
- Priority: {priority_level}

Our team will look into this and provide an update within {eta}.

If you discover any additional details or workarounds, please reply to this thread.

Thanks for helping us improve!""",
                category_tags=["technical"],
                priority_tags=["high", "urgent"],
                placeholders=["ticket_id", "issue_description", "impact_assessment", "priority_level", "eta"],
                quality_score=0.9
            ),
            
            # Meeting Response
            ResponseTemplate(
                id="tmpl_meeting_accept",
                type=ResponseType.MEETING_RESPONSE,
                tone=ResponseTone.PROFESSIONAL,
                subject_template="Re: {original_subject} - Confirmed",
                body_template="""Thanks for the invite!

I'll be there. Here's my understanding:
- When: {meeting_time}
- Where: {meeting_location}
- Agenda: {agenda_summary}

{preparation_notes}

See you then!""",
                category_tags=["internal"],
                priority_tags=["normal"],
                placeholders=["meeting_time", "meeting_location", "agenda_summary", "preparation_notes"],
                quality_score=0.85
            ),
            
            # Escalation
            ResponseTemplate(
                id="tmpl_escalation",
                type=ResponseType.ESCALATION,
                tone=ResponseTone.URGENT,
                subject_template="[ESCALATED] {original_subject}",
                body_template="""This issue has been escalated due to {escalation_reason}.

Summary:
{issue_summary}

Customer Impact: {impact_level}
Time Pending: {time_pending}

Immediate action required: {required_action}

Please prioritize and respond within {response_deadline}.

Original thread attached below.""",
                category_tags=["customer_support", "technical"],
                priority_tags=["urgent"],
                placeholders=["escalation_reason", "issue_summary", "impact_level", "time_pending", "required_action", "response_deadline"],
                quality_score=0.9
            ),
            
            # Follow-up
            ResponseTemplate(
                id="tmpl_followup",
                type=ResponseType.FOLLOW_UP,
                tone=ResponseTone.FRIENDLY,
                subject_template="Following up: {original_subject}",
                body_template="""Hi {name},

I wanted to follow up on my previous message about {topic}.

{context_reminder}

{call_to_action}

Let me know if you have any questions or if there's anything I can help with!

Best,
{sender_name}""",
                category_tags=["sales", "customer_support"],
                priority_tags=["normal"],
                placeholders=["name", "topic", "context_reminder", "call_to_action", "sender_name"],
                quality_score=0.85
            ),
            
            # Billing Response
            ResponseTemplate(
                id="tmpl_billing_response",
                type=ResponseType.SUPPORT_RESPONSE,
                tone=ResponseTone.PROFESSIONAL,
                subject_template="Re: {original_subject} - Billing Update",
                body_template="""Thank you for reaching out about your billing concern.

I've reviewed your account and here's what I found:
{billing_summary}

{resolution_or_action}

If you have any questions about your invoice or need further clarification, please don't hesitate to ask.

Best regards,
Billing Support Team

Account Reference: {account_id}""",
                category_tags=["billing"],
                priority_tags=["normal", "high"],
                placeholders=["billing_summary", "resolution_or_action", "account_id"],
                quality_score=0.85
            ),
            
            # Decline/Unable to Help
            ResponseTemplate(
                id="tmpl_decline",
                type=ResponseType.DECLINE,
                tone=ResponseTone.APOLOGETIC,
                subject_template="Re: {original_subject}",
                body_template="""Thank you for your message.

Unfortunately, {decline_reason}. I understand this may not be the answer you were hoping for.

However, {alternative_suggestion}.

If your situation changes or you have any other questions, please feel free to reach out.

Best regards,
{sender_name}""",
                category_tags=["sales", "customer_support"],
                priority_tags=["normal", "low"],
                placeholders=["decline_reason", "alternative_suggestion", "sender_name"],
                quality_score=0.8
            ),
            
            # Information Request
            ResponseTemplate(
                id="tmpl_info_request",
                type=ResponseType.INFORMATION_REQUEST,
                tone=ResponseTone.PROFESSIONAL,
                subject_template="Re: {original_subject} - Additional Information Needed",
                body_template="""Thank you for reaching out.

To better assist you, I need a bit more information:

{questions_list}

Once I have these details, I'll be able to {what_happens_next}.

Thanks for your patience!""",
                category_tags=["customer_support", "technical", "sales"],
                priority_tags=["normal"],
                placeholders=["questions_list", "what_happens_next"],
                quality_score=0.85
            ),
        ]
        
        for template in templates:
            self.templates[template.id] = template
    
    def _generate_response_id(self) -> str:
        """Generate a unique response ID."""
        self._response_counter += 1
        return f"response_{self._response_counter}"
    
    def analyze_email_context(self, email: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze email context for response generation."""
        text = f"{email.get('subject', '')} {email.get('body', '')}"
        
        # Extract key information
        context = {
            "sender_name": email.get("sender_name", "there"),
            "sender_email": email.get("sender", ""),
            "subject": email.get("subject", ""),
            "category": email.get("category") or email.get("suggested_category", ""),
            "priority": email.get("priority") or email.get("suggested_priority", "normal"),
            "sentiment": email.get("sentiment_label", "neutral"),
            "is_urgent": any(word in text.lower() for word in ["urgent", "asap", "immediately", "critical"]),
            "is_question": "?" in text,
            "has_deadline": any(word in text.lower() for word in ["deadline", "by eod", "by cob", "due"]),
            "mentions_issue": any(word in text.lower() for word in ["issue", "problem", "error", "bug", "broken"]),
            "is_complaint": any(word in text.lower() for word in ["disappointed", "frustrated", "upset", "complaint", "unacceptable"]),
            "asks_for_help": any(word in text.lower() for word in ["help", "assist", "support", "guidance"]),
            "mentions_money": bool(re.search(r'\$[\d,]+|\d+\s*(?:USD|EUR|dollars)', text, re.IGNORECASE)),
            "mentions_meeting": any(word in text.lower() for word in ["meeting", "call", "sync", "schedule"]),
        }
        
        # Determine suggested tone
        if context["is_complaint"]:
            context["suggested_tone"] = ResponseTone.EMPATHETIC
        elif context["is_urgent"]:
            context["suggested_tone"] = ResponseTone.URGENT
        else:
            context["suggested_tone"] = self.sentiment_tone_map.get(
                context["sentiment"], ResponseTone.PROFESSIONAL
            )
        
        # Determine suggested response type
        if context["mentions_issue"]:
            context["suggested_response_type"] = ResponseType.SUPPORT_RESPONSE
        elif context["asks_for_help"]:
            context["suggested_response_type"] = ResponseType.ACKNOWLEDGMENT
        elif context["is_question"]:
            context["suggested_response_type"] = ResponseType.INFORMATION_REQUEST
        elif context["mentions_meeting"]:
            context["suggested_response_type"] = ResponseType.MEETING_RESPONSE
        else:
            context["suggested_response_type"] = self.category_response_map.get(
                context["category"], ResponseType.ACKNOWLEDGMENT
            )
        
        return context
    
    def find_best_template(
        self,
        context: Dict[str, Any],
        response_type: Optional[ResponseType] = None,
        tone: Optional[ResponseTone] = None
    ) -> Optional[ResponseTemplate]:
        """Find the best matching template for the context."""
        target_type = response_type or context.get("suggested_response_type", ResponseType.ACKNOWLEDGMENT)
        target_tone = tone or context.get("suggested_tone", ResponseTone.PROFESSIONAL)
        category = context.get("category", "")
        priority = context.get("priority", "normal")
        
        best_template = None
        best_score = 0.0
        
        for template in self.templates.values():
            score = 0.0
            
            # Type match (most important)
            if template.type == target_type:
                score += 0.4
            
            # Tone match
            if template.tone == target_tone:
                score += 0.2
            
            # Category match
            if category and category in template.category_tags:
                score += 0.2
            
            # Priority match
            if priority in template.priority_tags:
                score += 0.1
            
            # Quality score boost
            score += template.quality_score * 0.1
            
            if score > best_score:
                best_score = score
                best_template = template
        
        return best_template
    
    def generate_response(
        self,
        email: Dict[str, Any],
        custom_placeholders: Optional[Dict[str, str]] = None,
        tone: Optional[ResponseTone] = None,
        response_type: Optional[ResponseType] = None
    ) -> GeneratedResponse:
        """Generate a response for an email."""
        context = self.analyze_email_context(email)
        
        # Find best template
        template = self.find_best_template(context, response_type, tone)
        
        # Override tone if specified
        actual_tone = tone or context.get("suggested_tone", ResponseTone.PROFESSIONAL)
        actual_type = response_type or context.get("suggested_response_type", ResponseType.ACKNOWLEDGMENT)
        
        # Default placeholders
        placeholders = {
            "name": context["sender_name"].split()[0] if context["sender_name"] else "there",
            "original_subject": context["subject"],
            "sender_name": "Support Team",
            "ticket_id": f"TKT-{random.randint(10000, 99999)}",
            "account_id": f"ACC-{random.randint(10000, 99999)}",
            "response_time": "24 hours",
            "eta": "24-48 hours",
        }
        
        # Auto-generate contextual placeholders
        if context["mentions_issue"]:
            placeholders["issue_summary"] = "the issue you reported"
            placeholders["frustration_acknowledgment"] = "inconvenient"
            placeholders["issue_description"] = "Issue reported by customer"
            placeholders["impact_assessment"] = "Under investigation"
            placeholders["priority_level"] = context["priority"].upper()
        
        if context["mentions_meeting"]:
            placeholders["meeting_time"] = "[TBD]"
            placeholders["meeting_location"] = "[TBD]"
            placeholders["agenda_summary"] = "As discussed"
            placeholders["preparation_notes"] = "Please bring your updates."
        
        if context["is_complaint"]:
            placeholders["frustration_acknowledgment"] = "frustrating"
        
        # Merge custom placeholders
        if custom_placeholders:
            placeholders.update(custom_placeholders)
        
        # Generate subject and body
        if template:
            subject = template.subject_template
            body = template.body_template
            template_id = template.id
            template.usage_count += 1
        else:
            # Fallback generic response
            subject = f"Re: {context['subject']}"
            body = f"""Thank you for your email.

I've received your message and will get back to you shortly.

Best regards,
Support Team"""
            template_id = None
        
        # Replace placeholders
        for key, value in placeholders.items():
            subject = subject.replace(f"{{{key}}}", str(value))
            body = body.replace(f"{{{key}}}", str(value))
        
        # Add greeting and closing
        greeting = random.choice(self.greetings.get(actual_tone, ["Hi,"])).format(
            name=placeholders.get("name", "there")
        )
        closing = random.choice(self.closings.get(actual_tone, ["Best,"]))
        
        # Only add greeting if not already present
        if not body.strip().startswith(("Dear", "Hi", "Hello", "Hey", "To Whom")):
            body = f"{greeting}\n\n{body}"
        
        # Add closing if not present
        if not any(body.strip().endswith(c.rstrip(",")) for c in self.closings.get(actual_tone, [])):
            body = f"{body}\n\n{closing}"
        
        # Calculate scores
        confidence = 0.7
        if template:
            confidence = template.quality_score * 0.8 + 0.2
        
        # Count how many placeholders were filled
        unfilled = len(re.findall(r'\{[a-z_]+\}', body))
        total_placeholders = len(template.placeholders) if template else 1
        personalization_score = 1.0 - (unfilled / max(total_placeholders, 1)) * 0.3
        
        # Generate suggestions
        suggestions = []
        if "{" in body:
            suggestions.append("Some placeholders need to be filled in manually")
        if context["is_urgent"] and actual_tone != ResponseTone.URGENT:
            suggestions.append("Consider using a more urgent tone")
        if context["is_complaint"] and actual_tone != ResponseTone.EMPATHETIC:
            suggestions.append("Consider using a more empathetic tone")
        if not template:
            suggestions.append("No matching template found - using generic response")
        
        response = GeneratedResponse(
            id=self._generate_response_id(),
            email_id=email.get("id", "unknown"),
            template_id=template_id,
            subject=subject,
            body=body,
            tone=actual_tone,
            response_type=actual_type,
            confidence=confidence,
            personalization_score=personalization_score,
            suggestions=suggestions
        )
        
        self.generated_responses.append(response)
        self.response_history[email.get("sender", "")].append(response.id)
        
        return response
    
    def get_quick_responses(
        self,
        email: Dict[str, Any],
        count: int = 3
    ) -> List[Dict[str, Any]]:
        """Get quick response options for an email."""
        context = self.analyze_email_context(email)
        responses = []
        
        # Generate responses with different tones/types
        variations = [
            (ResponseTone.PROFESSIONAL, None),
            (ResponseTone.FRIENDLY, None),
            (ResponseTone.EMPATHETIC, None),
        ]
        
        if context["is_urgent"]:
            variations.insert(0, (ResponseTone.URGENT, None))
        
        if context["mentions_issue"]:
            variations.insert(0, (None, ResponseType.ACKNOWLEDGMENT))
            variations.append((None, ResponseType.ISSUE_RESOLUTION))
        
        seen_bodies = set()
        for tone, resp_type in variations[:count + 2]:
            if len(responses) >= count:
                break
            
            response = self.generate_response(email, tone=tone, response_type=resp_type)
            
            # Avoid duplicates
            body_hash = hash(response.body[:100])
            if body_hash not in seen_bodies:
                seen_bodies.add(body_hash)
                responses.append({
                    "id": response.id,
                    "preview": response.body[:150] + "..." if len(response.body) > 150 else response.body,
                    "tone": response.tone.value,
                    "type": response.response_type.value,
                    "confidence": response.confidence,
                    "full_response": response.to_dict()
                })
        
        return responses[:count]
    
    def add_template(
        self,
        template_type: str,
        tone: str,
        subject: str,
        body: str,
        categories: List[str],
        priorities: List[str]
    ) -> ResponseTemplate:
        """Add a custom template."""
        # Extract placeholders from body
        placeholders = list(set(re.findall(r'\{([a-z_]+)\}', body)))
        
        template = ResponseTemplate(
            id=f"tmpl_custom_{len(self.templates) + 1}",
            type=ResponseType(template_type),
            tone=ResponseTone(tone),
            subject_template=subject,
            body_template=body,
            category_tags=categories,
            priority_tags=priorities,
            placeholders=placeholders
        )
        
        self.templates[template.id] = template
        return template
    
    def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get a template by ID."""
        template = self.templates.get(template_id)
        return template.to_dict() if template else None
    
    def list_templates(
        self,
        template_type: Optional[str] = None,
        tone: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List available templates."""
        results = []
        
        for template in self.templates.values():
            if template_type and template.type.value != template_type:
                continue
            if tone and template.tone.value != tone:
                continue
            
            results.append(template.to_dict())
        
        return sorted(results, key=lambda x: -x["quality_score"])
    
    def get_response_history(
        self,
        sender: Optional[str] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """Get response generation history."""
        if sender:
            response_ids = self.response_history.get(sender, [])
            responses = [
                r.to_dict() for r in self.generated_responses
                if r.id in response_ids
            ]
        else:
            responses = [r.to_dict() for r in self.generated_responses]
        
        return sorted(responses, key=lambda x: x["generated_at"], reverse=True)[:limit]
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get response generator statistics."""
        tone_counts = defaultdict(int)
        type_counts = defaultdict(int)
        
        for response in self.generated_responses:
            tone_counts[response.tone.value] += 1
            type_counts[response.response_type.value] += 1
        
        template_usage = {
            t.id: t.usage_count
            for t in self.templates.values()
        }
        
        avg_confidence = 0.0
        avg_personalization = 0.0
        if self.generated_responses:
            avg_confidence = sum(r.confidence for r in self.generated_responses) / len(self.generated_responses)
            avg_personalization = sum(r.personalization_score for r in self.generated_responses) / len(self.generated_responses)
        
        return {
            "total_templates": len(self.templates),
            "total_responses_generated": len(self.generated_responses),
            "responses_by_tone": dict(tone_counts),
            "responses_by_type": dict(type_counts),
            "template_usage": template_usage,
            "average_confidence": avg_confidence,
            "average_personalization": avg_personalization,
            "unique_senders_served": len(self.response_history)
        }
    
    def reset(self) -> None:
        """Reset the response generator state."""
        self.generated_responses.clear()
        self.response_history.clear()
        for template in self.templates.values():
            template.usage_count = 0
        self._response_counter = 0
