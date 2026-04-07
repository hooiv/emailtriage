"""
Knowledge Graph & Explainable AI System

This module implements:
1. Knowledge Graph - Extract and connect entities from emails
2. Relationship Tracking - Sender-topic-context relationships
3. Explainable AI - Decision explanations with confidence
4. Context Propagation - Use historical context for better decisions
5. Entity Recognition - Extract people, organizations, topics, dates
6. Reasoning Chain - Show step-by-step decision process
"""

import re
import math
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class EntityType(Enum):
    """Types of entities that can be extracted."""
    PERSON = "person"
    ORGANIZATION = "organization"
    EMAIL_ADDRESS = "email_address"
    PHONE = "phone"
    DATE = "date"
    TIME = "time"
    MONEY = "money"
    URL = "url"
    TOPIC = "topic"
    PROJECT = "project"
    PRODUCT = "product"
    LOCATION = "location"
    DEADLINE = "deadline"
    ACTION_ITEM = "action_item"


class RelationType(Enum):
    """Types of relationships between entities."""
    SENT_BY = "sent_by"
    SENT_TO = "sent_to"
    MENTIONS = "mentions"
    ABOUT = "about"
    WORKS_FOR = "works_for"
    RELATED_TO = "related_to"
    RESPONDS_TO = "responds_to"
    DEADLINE_FOR = "deadline_for"
    ASSIGNED_TO = "assigned_to"
    REQUESTED_BY = "requested_by"


class DecisionFactor(Enum):
    """Factors that influence decisions."""
    SENDER_REPUTATION = "sender_reputation"
    CONTENT_KEYWORDS = "content_keywords"
    HISTORICAL_PATTERN = "historical_pattern"
    ENTITY_CONTEXT = "entity_context"
    URGENCY_INDICATORS = "urgency_indicators"
    SPAM_SIGNALS = "spam_signals"
    RELATIONSHIP_STRENGTH = "relationship_strength"
    TIME_SENSITIVITY = "time_sensitivity"
    DOMAIN_TRUST = "domain_trust"
    THREAD_CONTEXT = "thread_context"


@dataclass
class Entity:
    """An entity extracted from an email."""
    id: str
    type: EntityType
    value: str
    normalized_value: str
    confidence: float
    source_email_id: str
    context: str
    extracted_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type.value,
            "value": self.value,
            "normalized_value": self.normalized_value,
            "confidence": self.confidence,
            "source_email_id": self.source_email_id,
            "context": self.context,
            "extracted_at": self.extracted_at.isoformat()
        }


@dataclass
class Relationship:
    """A relationship between two entities."""
    id: str
    source_entity_id: str
    target_entity_id: str
    relation_type: RelationType
    strength: float  # 0-1
    evidence: List[str]
    email_ids: List[str]
    created_at: datetime = field(default_factory=datetime.now)
    last_seen: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source_entity_id,
            "target": self.target_entity_id,
            "type": self.relation_type.value,
            "strength": self.strength,
            "evidence": self.evidence,
            "email_ids": self.email_ids,
            "created_at": self.created_at.isoformat(),
            "last_seen": self.last_seen.isoformat()
        }


@dataclass
class ReasoningStep:
    """A step in the reasoning chain."""
    step_number: int
    factor: DecisionFactor
    observation: str
    inference: str
    confidence: float
    supporting_evidence: List[str]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "step": self.step_number,
            "factor": self.factor.value,
            "observation": self.observation,
            "inference": self.inference,
            "confidence": self.confidence,
            "evidence": self.supporting_evidence
        }


@dataclass
class Decision:
    """An explainable decision."""
    decision_id: str
    email_id: str
    decision_type: str  # categorize, prioritize, etc.
    recommendation: str
    confidence: float
    reasoning_chain: List[ReasoningStep]
    alternative_options: List[Dict[str, Any]]
    timestamp: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "email_id": self.email_id,
            "decision_type": self.decision_type,
            "recommendation": self.recommendation,
            "confidence": self.confidence,
            "reasoning_chain": [r.to_dict() for r in self.reasoning_chain],
            "alternatives": self.alternative_options,
            "timestamp": self.timestamp.isoformat()
        }


class KnowledgeGraph:
    """
    Knowledge Graph for email intelligence.
    
    Extracts entities from emails, builds relationships, and provides
    explainable AI decisions based on accumulated knowledge.
    """
    
    def __init__(self):
        """Initialize the knowledge graph."""
        self.entities: Dict[str, Entity] = {}
        self.relationships: Dict[str, Relationship] = {}
        self.entity_index: Dict[EntityType, Set[str]] = defaultdict(set)
        self.email_entities: Dict[str, List[str]] = defaultdict(list)
        self.decisions: List[Decision] = []
        
        # Pattern matchers for entity extraction
        self.patterns = {
            EntityType.EMAIL_ADDRESS: re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'),
            EntityType.PHONE: re.compile(r'[\+]?[(]?[0-9]{1,3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}'),
            EntityType.URL: re.compile(r'https?://[^\s<>"{}|\\^`\[\]]+'),
            EntityType.MONEY: re.compile(r'\$[\d,]+(?:\.\d{2})?|\d+(?:,\d{3})*(?:\.\d{2})?\s*(?:USD|EUR|GBP|dollars?|euros?)', re.IGNORECASE),
            EntityType.DATE: re.compile(r'\b(?:\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2}|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2}(?:,?\s+\d{4})?)\b', re.IGNORECASE),
            EntityType.TIME: re.compile(r'\b\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM|am|pm)?\b'),
        }
        
        # Topic keywords
        self.topic_keywords = {
            "meeting": ["meeting", "sync", "standup", "huddle", "call", "conference"],
            "deadline": ["deadline", "due", "by EOD", "by COB", "ASAP", "urgent", "immediately"],
            "project": ["project", "initiative", "sprint", "milestone", "roadmap"],
            "sales": ["deal", "opportunity", "prospect", "pipeline", "revenue", "contract"],
            "support": ["issue", "problem", "bug", "error", "help", "assist", "ticket"],
            "billing": ["invoice", "payment", "subscription", "renewal", "charge"],
            "hiring": ["candidate", "interview", "hire", "recruit", "resume", "position"],
            "security": ["security", "vulnerability", "breach", "compliance", "audit"],
        }
        
        # Urgency indicators
        self.urgency_indicators = [
            "urgent", "asap", "immediately", "critical", "emergency",
            "time-sensitive", "deadline", "overdue", "p1", "p0",
            "production down", "outage", "blocked", "escalation"
        ]
        
        # Spam signals
        self.spam_signals = [
            "winner", "lottery", "congratulations", "click here",
            "act now", "limited time", "free money", "100% free",
            "claim your", "urgent action required", "account suspended",
            "verify your", "password expired", "lottery",
            r"\.xyz", r"\.ru", r"\.cn", "nigerian prince"
        ]
        
        # Sender reputation cache
        self.sender_reputation: Dict[str, Dict[str, Any]] = {}
        
        # Domain trust scores
        self.domain_trust: Dict[str, float] = {
            "company.com": 0.95,
            "gmail.com": 0.7,
            "outlook.com": 0.7,
            "yahoo.com": 0.65,
        }
        
        self._entity_counter = 0
        self._relationship_counter = 0
        self._decision_counter = 0
    
    def _generate_entity_id(self) -> str:
        """Generate a unique entity ID."""
        self._entity_counter += 1
        return f"entity_{self._entity_counter}"
    
    def _generate_relationship_id(self) -> str:
        """Generate a unique relationship ID."""
        self._relationship_counter += 1
        return f"rel_{self._relationship_counter}"
    
    def _generate_decision_id(self) -> str:
        """Generate a unique decision ID."""
        self._decision_counter += 1
        return f"decision_{self._decision_counter}"
    
    def extract_entities(self, email: Dict[str, Any]) -> List[Entity]:
        """Extract entities from an email."""
        entities = []
        email_id = email.get("id", "unknown")
        text = f"{email.get('subject', '')} {email.get('body', '')}"
        
        # Extract pattern-based entities
        for entity_type, pattern in self.patterns.items():
            for match in pattern.finditer(text):
                value = match.group()
                context = text[max(0, match.start()-30):min(len(text), match.end()+30)]
                
                entity = Entity(
                    id=self._generate_entity_id(),
                    type=entity_type,
                    value=value,
                    normalized_value=value.lower().strip(),
                    confidence=0.9,
                    source_email_id=email_id,
                    context=context
                )
                entities.append(entity)
                self._add_entity(entity)
        
        # Extract sender as entity
        sender = email.get("sender", "")
        if sender:
            sender_entity = Entity(
                id=self._generate_entity_id(),
                type=EntityType.EMAIL_ADDRESS,
                value=sender,
                normalized_value=sender.lower(),
                confidence=1.0,
                source_email_id=email_id,
                context=f"From: {sender}"
            )
            entities.append(sender_entity)
            self._add_entity(sender_entity)
        
        # Extract person from sender name
        sender_name = email.get("sender_name", "")
        if sender_name:
            person_entity = Entity(
                id=self._generate_entity_id(),
                type=EntityType.PERSON,
                value=sender_name,
                normalized_value=sender_name.lower(),
                confidence=0.95,
                source_email_id=email_id,
                context=f"Sender: {sender_name}"
            )
            entities.append(person_entity)
            self._add_entity(person_entity)
        
        # Extract topics
        text_lower = text.lower()
        for topic, keywords in self.topic_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    topic_entity = Entity(
                        id=self._generate_entity_id(),
                        type=EntityType.TOPIC,
                        value=topic,
                        normalized_value=topic,
                        confidence=0.7,
                        source_email_id=email_id,
                        context=f"Contains keyword: {keyword}"
                    )
                    entities.append(topic_entity)
                    self._add_entity(topic_entity)
                    break
        
        # Extract deadlines
        deadline_patterns = [
            (r'by\s+(EOD|COB|end of day|close of business)', 0.85),
            (r'deadline\s*:?\s*([^.\n]+)', 0.8),
            (r'due\s+(by|on|before)\s+([^.\n]+)', 0.8),
            (r'need\s+(by|before)\s+([^.\n]+)', 0.75),
        ]
        
        for pattern, confidence in deadline_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                deadline_entity = Entity(
                    id=self._generate_entity_id(),
                    type=EntityType.DEADLINE,
                    value=match.group(0),
                    normalized_value=match.group(0).lower(),
                    confidence=confidence,
                    source_email_id=email_id,
                    context=text[max(0, match.start()-20):min(len(text), match.end()+20)]
                )
                entities.append(deadline_entity)
                self._add_entity(deadline_entity)
        
        # Extract action items
        action_patterns = [
            (r'please\s+([^.]+)', 0.7),
            (r'could you\s+([^?]+)', 0.7),
            (r'action\s*:?\s*([^.\n]+)', 0.85),
            (r'todo\s*:?\s*([^.\n]+)', 0.85),
            (r'need\s+(?:you\s+to\s+)?([^.\n]+)', 0.65),
        ]
        
        for pattern, confidence in action_patterns:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                action_entity = Entity(
                    id=self._generate_entity_id(),
                    type=EntityType.ACTION_ITEM,
                    value=match.group(0)[:100],
                    normalized_value=match.group(0).lower()[:100],
                    confidence=confidence,
                    source_email_id=email_id,
                    context=text[max(0, match.start()-10):min(len(text), match.end()+10)]
                )
                entities.append(action_entity)
                self._add_entity(action_entity)
        
        return entities
    
    def _add_entity(self, entity: Entity) -> None:
        """Add an entity to the graph."""
        self.entities[entity.id] = entity
        self.entity_index[entity.type].add(entity.id)
        self.email_entities[entity.source_email_id].append(entity.id)
    
    def create_relationship(
        self,
        source_id: str,
        target_id: str,
        relation_type: RelationType,
        evidence: str,
        email_id: str
    ) -> Optional[Relationship]:
        """Create a relationship between two entities."""
        if source_id not in self.entities or target_id not in self.entities:
            return None
        
        # Check for existing relationship
        for rel in self.relationships.values():
            if (rel.source_entity_id == source_id and 
                rel.target_entity_id == target_id and
                rel.relation_type == relation_type):
                # Strengthen existing relationship
                rel.strength = min(1.0, rel.strength + 0.1)
                rel.evidence.append(evidence)
                rel.email_ids.append(email_id)
                rel.last_seen = datetime.now()
                return rel
        
        # Create new relationship
        relationship = Relationship(
            id=self._generate_relationship_id(),
            source_entity_id=source_id,
            target_entity_id=target_id,
            relation_type=relation_type,
            strength=0.5,
            evidence=[evidence],
            email_ids=[email_id]
        )
        
        self.relationships[relationship.id] = relationship
        return relationship
    
    def build_relationships(self, email: Dict[str, Any]) -> List[Relationship]:
        """Build relationships from email entities."""
        relationships = []
        email_id = email.get("id", "unknown")
        entities = [self.entities[eid] for eid in self.email_entities.get(email_id, [])]
        
        # Find sender entity
        sender_entities = [e for e in entities if e.type == EntityType.EMAIL_ADDRESS and "From:" in e.context]
        
        for sender in sender_entities:
            # Sender SENT_BY relationship
            for topic in [e for e in entities if e.type == EntityType.TOPIC]:
                rel = self.create_relationship(
                    sender.id, topic.id, RelationType.ABOUT,
                    f"Email about {topic.value}", email_id
                )
                if rel:
                    relationships.append(rel)
            
            # Sender to person relationships
            for person in [e for e in entities if e.type == EntityType.PERSON and e.id != sender.id]:
                rel = self.create_relationship(
                    sender.id, person.id, RelationType.MENTIONS,
                    f"Email mentions {person.value}", email_id
                )
                if rel:
                    relationships.append(rel)
            
            # Deadline relationships
            for deadline in [e for e in entities if e.type == EntityType.DEADLINE]:
                rel = self.create_relationship(
                    sender.id, deadline.id, RelationType.DEADLINE_FOR,
                    f"Deadline: {deadline.value}", email_id
                )
                if rel:
                    relationships.append(rel)
        
        return relationships
    
    def explain_categorization(self, email: Dict[str, Any]) -> Decision:
        """Generate an explainable categorization decision."""
        email_id = email.get("id", "unknown")
        reasoning_chain = []
        step = 0
        
        text = f"{email.get('subject', '')} {email.get('body', '')}".lower()
        sender = email.get("sender", "")
        sender_info = email.get("sender_info", {})
        
        # Step 1: Analyze sender reputation
        step += 1
        trust_score = sender_info.get("trust_score", 0.5)
        sender_type = sender_info.get("sender_type", "unknown")
        
        if trust_score >= 0.9:
            inference = "High trust sender - likely legitimate email"
            confidence = 0.9
        elif trust_score < 0.3:
            inference = "Low trust sender - potential spam or unknown source"
            confidence = 0.7
        else:
            inference = "Normal trust level - evaluate content"
            confidence = 0.5
        
        reasoning_chain.append(ReasoningStep(
            step_number=step,
            factor=DecisionFactor.SENDER_REPUTATION,
            observation=f"Sender {sender} has trust score {trust_score:.2f} ({sender_type})",
            inference=inference,
            confidence=confidence,
            supporting_evidence=[
                f"Previous emails: {sender_info.get('previous_emails', 0)}",
                f"Avg response time: {sender_info.get('avg_response_time_hours', 'N/A')}h"
            ]
        ))
        
        # Step 2: Check spam signals
        step += 1
        spam_matches = []
        for signal in self.spam_signals:
            if re.search(signal, text, re.IGNORECASE):
                spam_matches.append(signal)
        
        if spam_matches:
            spam_confidence = min(0.95, 0.4 + len(spam_matches) * 0.15)
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.SPAM_SIGNALS,
                observation=f"Found {len(spam_matches)} spam indicators",
                inference="Strong spam signals detected",
                confidence=spam_confidence,
                supporting_evidence=spam_matches[:5]
            ))
        else:
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.SPAM_SIGNALS,
                observation="No spam indicators found",
                inference="Content appears legitimate",
                confidence=0.7,
                supporting_evidence=["Clean content analysis"]
            ))
        
        # Step 3: Analyze content keywords for category
        step += 1
        category_scores = defaultdict(float)
        
        # Customer support signals
        support_keywords = ["issue", "problem", "help", "not working", "broken", "complaint", "refund"]
        for kw in support_keywords:
            if kw in text:
                category_scores["customer_support"] += 0.15
        
        # Sales signals
        sales_keywords = ["demo", "pricing", "enterprise", "interested in", "partnership", "opportunity"]
        for kw in sales_keywords:
            if kw in text:
                category_scores["sales"] += 0.15
        
        # Technical signals
        tech_keywords = ["bug", "error", "api", "integration", "code", "deploy", "server"]
        for kw in tech_keywords:
            if kw in text:
                category_scores["technical"] += 0.15
        
        # Internal signals
        internal_keywords = ["team", "meeting", "sync", "pto", "vacation", "internal"]
        for kw in internal_keywords:
            if kw in text:
                category_scores["internal"] += 0.15
        
        # Newsletter signals
        newsletter_keywords = ["unsubscribe", "newsletter", "digest", "weekly update", "product update"]
        for kw in newsletter_keywords:
            if kw in text:
                category_scores["newsletter"] += 0.2
        
        # Billing signals
        billing_keywords = ["invoice", "payment", "billing", "subscription", "charge", "receipt"]
        for kw in billing_keywords:
            if kw in text:
                category_scores["billing"] += 0.15
        
        top_categories = sorted(category_scores.items(), key=lambda x: -x[1])[:3]
        
        if top_categories and top_categories[0][1] > 0:
            best_cat, best_score = top_categories[0]
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.CONTENT_KEYWORDS,
                observation=f"Keyword analysis: {dict(top_categories)}",
                inference=f"Best category match: {best_cat}",
                confidence=min(0.9, best_score),
                supporting_evidence=[f"{cat}: {score:.2f}" for cat, score in top_categories]
            ))
        else:
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.CONTENT_KEYWORDS,
                observation="No strong category signals",
                inference="May need human review",
                confidence=0.3,
                supporting_evidence=["Ambiguous content"]
            ))
        
        # Step 4: Check urgency
        step += 1
        urgency_matches = [ind for ind in self.urgency_indicators if ind in text]
        
        if urgency_matches:
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.URGENCY_INDICATORS,
                observation=f"Found {len(urgency_matches)} urgency indicators",
                inference="High priority suggested",
                confidence=min(0.9, 0.5 + len(urgency_matches) * 0.15),
                supporting_evidence=urgency_matches[:5]
            ))
        
        # Step 5: Domain trust
        step += 1
        domain = sender.split("@")[-1] if "@" in sender else ""
        domain_trust = self.domain_trust.get(domain, 0.5)
        
        reasoning_chain.append(ReasoningStep(
            step_number=step,
            factor=DecisionFactor.DOMAIN_TRUST,
            observation=f"Domain {domain} has trust score {domain_trust:.2f}",
            inference="Internal domain" if domain_trust > 0.8 else "External domain",
            confidence=domain_trust,
            supporting_evidence=[f"Known domain: {domain in self.domain_trust}"]
        ))
        
        # Make final recommendation
        if spam_matches and len(spam_matches) >= 2:
            recommendation = "spam"
            final_confidence = min(0.95, 0.5 + len(spam_matches) * 0.15)
        elif top_categories and top_categories[0][1] > 0.3:
            recommendation = top_categories[0][0]
            final_confidence = min(0.9, top_categories[0][1] + trust_score * 0.2)
        else:
            recommendation = "internal" if domain_trust > 0.8 else "personal"
            final_confidence = 0.5
        
        # Generate alternatives
        alternatives = []
        for cat, score in top_categories[:3]:
            if cat != recommendation:
                alternatives.append({
                    "category": cat,
                    "confidence": score,
                    "reason": f"Alternative based on keyword score {score:.2f}"
                })
        
        decision = Decision(
            decision_id=self._generate_decision_id(),
            email_id=email_id,
            decision_type="categorize",
            recommendation=recommendation,
            confidence=final_confidence,
            reasoning_chain=reasoning_chain,
            alternative_options=alternatives
        )
        
        self.decisions.append(decision)
        return decision
    
    def explain_priority(self, email: Dict[str, Any]) -> Decision:
        """Generate an explainable priority decision."""
        email_id = email.get("id", "unknown")
        reasoning_chain = []
        step = 0
        
        text = f"{email.get('subject', '')} {email.get('body', '')}".lower()
        sender_info = email.get("sender_info", {})
        time_in_inbox = email.get("time_in_inbox_hours", 0)
        sla_priority = email.get("sla_priority")
        category = email.get("category", "")
        
        priority_score = 0.0
        
        # Step 1: SLA check
        step += 1
        if sla_priority:
            if sla_priority == "critical":
                priority_score += 0.5
                inference = "Critical SLA - highest priority"
            elif sla_priority == "high":
                priority_score += 0.3
                inference = "High SLA priority"
            else:
                inference = f"SLA priority: {sla_priority}"
            
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.TIME_SENSITIVITY,
                observation=f"SLA priority is {sla_priority}",
                inference=inference,
                confidence=0.95,
                supporting_evidence=[f"SLA defined: {sla_priority}"]
            ))
        
        # Step 2: Time in inbox
        step += 1
        if time_in_inbox > 8:
            priority_score += 0.3
            inference = "Email has been waiting too long"
        elif time_in_inbox > 4:
            priority_score += 0.15
            inference = "Email getting stale"
        else:
            inference = "Email is recent"
        
        reasoning_chain.append(ReasoningStep(
            step_number=step,
            factor=DecisionFactor.TIME_SENSITIVITY,
            observation=f"Email in inbox for {time_in_inbox:.1f} hours",
            inference=inference,
            confidence=0.8,
            supporting_evidence=[f"Age: {time_in_inbox:.1f}h"]
        ))
        
        # Step 3: Urgency keywords
        step += 1
        urgency_matches = [ind for ind in self.urgency_indicators if ind in text]
        if urgency_matches:
            priority_score += min(0.4, len(urgency_matches) * 0.1)
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.URGENCY_INDICATORS,
                observation=f"Found urgency words: {', '.join(urgency_matches[:3])}",
                inference="Content indicates urgency",
                confidence=min(0.9, 0.5 + len(urgency_matches) * 0.1),
                supporting_evidence=urgency_matches
            ))
        
        # Step 4: Sender importance
        step += 1
        trust_score = sender_info.get("trust_score", 0.5)
        is_vip = trust_score >= 0.9
        
        if is_vip:
            priority_score += 0.2
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.SENDER_REPUTATION,
                observation=f"VIP sender (trust: {trust_score:.2f})",
                inference="Prioritize VIP senders",
                confidence=0.9,
                supporting_evidence=["High trust score", "Known sender"]
            ))
        
        # Step 5: Category-based priority
        step += 1
        category_priorities = {
            "customer_support": 0.3,
            "security": 0.5,
            "billing": 0.2,
            "technical": 0.2,
            "sales": 0.15,
            "spam": -0.5,
            "newsletter": -0.3,
        }
        
        cat_boost = category_priorities.get(category, 0)
        priority_score += cat_boost
        
        if category:
            reasoning_chain.append(ReasoningStep(
                step_number=step,
                factor=DecisionFactor.ENTITY_CONTEXT,
                observation=f"Category is {category}",
                inference=f"Category priority boost: {cat_boost:+.2f}",
                confidence=0.85,
                supporting_evidence=[f"Category: {category}"]
            ))
        
        # Determine final priority
        if priority_score >= 0.6:
            recommendation = "urgent"
            final_confidence = min(0.95, priority_score)
        elif priority_score >= 0.4:
            recommendation = "high"
            final_confidence = min(0.9, priority_score + 0.3)
        elif priority_score >= 0.2:
            recommendation = "normal"
            final_confidence = 0.7
        else:
            recommendation = "low"
            final_confidence = 0.7
        
        decision = Decision(
            decision_id=self._generate_decision_id(),
            email_id=email_id,
            decision_type="prioritize",
            recommendation=recommendation,
            confidence=final_confidence,
            reasoning_chain=reasoning_chain,
            alternative_options=[
                {"priority": "urgent", "threshold": "score >= 0.6"},
                {"priority": "high", "threshold": "score >= 0.4"},
                {"priority": "normal", "threshold": "score >= 0.2"},
                {"priority": "low", "threshold": "score < 0.2"},
            ]
        )
        
        self.decisions.append(decision)
        return decision
    
    def get_context_for_email(self, email: Dict[str, Any]) -> Dict[str, Any]:
        """Get accumulated context for an email from the knowledge graph."""
        sender = email.get("sender", "")
        email_id = email.get("id", "")
        
        # Find related entities
        email_entities_list = [
            self.entities[eid].to_dict()
            for eid in self.email_entities.get(email_id, [])
        ]
        
        # Find sender history
        sender_emails = []
        sender_topics = set()
        
        for eid, entity in self.entities.items():
            if entity.type == EntityType.EMAIL_ADDRESS and sender.lower() in entity.value.lower():
                # Find relationships for this sender
                for rel in self.relationships.values():
                    if rel.source_entity_id == eid:
                        target = self.entities.get(rel.target_entity_id)
                        if target and target.type == EntityType.TOPIC:
                            sender_topics.add(target.value)
        
        # Get related decisions
        related_decisions = [
            d.to_dict() for d in self.decisions[-10:]
            if d.email_id == email_id
        ]
        
        return {
            "email_id": email_id,
            "extracted_entities": email_entities_list,
            "sender_history": {
                "topics": list(sender_topics),
                "relationship_count": len([
                    r for r in self.relationships.values()
                    if sender.lower() in str(self.entities.get(r.source_entity_id, {}))
                ])
            },
            "related_decisions": related_decisions,
            "graph_stats": {
                "total_entities": len(self.entities),
                "total_relationships": len(self.relationships),
                "email_entities_count": len(email_entities_list)
            }
        }
    
    def query_entities(
        self,
        entity_type: Optional[EntityType] = None,
        value_contains: Optional[str] = None,
        email_id: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Query entities from the knowledge graph."""
        results = []
        
        if entity_type:
            entity_ids = self.entity_index.get(entity_type, set())
        else:
            entity_ids = set(self.entities.keys())
        
        for eid in entity_ids:
            if len(results) >= limit:
                break
            
            entity = self.entities[eid]
            
            if email_id and entity.source_email_id != email_id:
                continue
            
            if value_contains and value_contains.lower() not in entity.value.lower():
                continue
            
            results.append(entity.to_dict())
        
        return results
    
    def query_relationships(
        self,
        source_type: Optional[EntityType] = None,
        target_type: Optional[EntityType] = None,
        relation_type: Optional[RelationType] = None,
        min_strength: float = 0.0,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Query relationships from the knowledge graph."""
        results = []
        
        for rel in self.relationships.values():
            if len(results) >= limit:
                break
            
            if rel.strength < min_strength:
                continue
            
            if relation_type and rel.relation_type != relation_type:
                continue
            
            source = self.entities.get(rel.source_entity_id)
            target = self.entities.get(rel.target_entity_id)
            
            if source_type and (not source or source.type != source_type):
                continue
            
            if target_type and (not target or target.type != target_type):
                continue
            
            results.append({
                **rel.to_dict(),
                "source_value": source.value if source else None,
                "target_value": target.value if target else None
            })
        
        return results
    
    def get_decision_explanation(self, decision_id: str) -> Optional[Dict[str, Any]]:
        """Get a full explanation for a specific decision."""
        for decision in self.decisions:
            if decision.decision_id == decision_id:
                explanation = decision.to_dict()
                
                # Add human-readable summary
                summary_parts = []
                for step in decision.reasoning_chain:
                    summary_parts.append(f"• {step.factor.value}: {step.inference}")
                
                explanation["human_readable_summary"] = "\n".join(summary_parts)
                explanation["confidence_level"] = (
                    "High" if decision.confidence >= 0.8 else
                    "Medium" if decision.confidence >= 0.5 else
                    "Low"
                )
                
                return explanation
        
        return None
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the knowledge graph."""
        entity_counts = {}
        for entity_type in EntityType:
            entity_counts[entity_type.value] = len(self.entity_index.get(entity_type, set()))
        
        relationship_counts = {}
        for rel in self.relationships.values():
            rel_type = rel.relation_type.value
            relationship_counts[rel_type] = relationship_counts.get(rel_type, 0) + 1
        
        decision_counts = {}
        for decision in self.decisions:
            decision_counts[decision.decision_type] = decision_counts.get(decision.decision_type, 0) + 1
        
        avg_confidence = 0.0
        if self.decisions:
            avg_confidence = sum(d.confidence for d in self.decisions) / len(self.decisions)
        
        return {
            "total_entities": len(self.entities),
            "total_relationships": len(self.relationships),
            "total_decisions": len(self.decisions),
            "entity_counts_by_type": entity_counts,
            "relationship_counts_by_type": relationship_counts,
            "decision_counts_by_type": decision_counts,
            "average_decision_confidence": avg_confidence,
            "emails_processed": len(self.email_entities)
        }
    
    def reset(self) -> None:
        """Reset the knowledge graph."""
        self.entities.clear()
        self.relationships.clear()
        self.entity_index.clear()
        self.email_entities.clear()
        self.decisions.clear()
        self._entity_counter = 0
        self._relationship_counter = 0
        self._decision_counter = 0
