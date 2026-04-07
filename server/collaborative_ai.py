"""Multi-Agent Collaborative AI System.

This module implements a sophisticated multi-agent system where specialized AI agents
collaborate to process emails more effectively than any single system could:

- Specialist Agents: Category Expert, Priority Analyst, Security Guard, Quality Controller
- Coordination Layer: Agent orchestration and conflict resolution
- Consensus Building: Multi-agent voting and confidence aggregation
- Knowledge Sharing: Cross-agent learning and expertise transfer
- Performance Monitoring: Agent performance tracking and optimization
"""

import json
import time
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from enum import Enum
import logging
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor

from models import Email, EmailCategory, EmailPriority, ActionType


class AgentRole(str, Enum):
    """Roles for different AI agents."""
    CATEGORY_EXPERT = "category_expert"
    PRIORITY_ANALYST = "priority_analyst"
    SECURITY_GUARD = "security_guard"
    QUALITY_CONTROLLER = "quality_controller"
    WORKFLOW_SPECIALIST = "workflow_specialist"
    RELATIONSHIP_ANALYZER = "relationship_analyzer"
    CONTENT_SPECIALIST = "content_specialist"


class DecisionConfidence(str, Enum):
    """Confidence levels for agent decisions."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"


@dataclass
class AgentDecision:
    """Individual agent decision on an email."""
    agent_id: str
    agent_role: AgentRole
    email_id: str
    decision_type: str  # "category", "priority", "action", "flag"
    decision_value: Any
    confidence: float  # 0.0 to 1.0
    reasoning: str
    evidence: List[str]
    processing_time_ms: float
    timestamp: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConsensusResult:
    """Result of multi-agent consensus building."""
    email_id: str
    decision_type: str
    final_decision: Any
    confidence: float
    participating_agents: List[str]
    agreement_score: float  # How much agents agreed
    dissenting_opinions: List[AgentDecision]
    processing_time_ms: float
    consensus_method: str  # "unanimous", "majority", "weighted", "expert_override"


@dataclass
class AgentPerformance:
    """Performance tracking for individual agents."""
    agent_id: str
    total_decisions: int
    correct_decisions: int
    accuracy: float
    avg_confidence: float
    avg_processing_time: float
    specialization_score: float
    collaboration_score: float
    last_updated: str


class BaseAgent:
    """Base class for all AI agents."""
    
    def __init__(self, agent_id: str, role: AgentRole, specialization_areas: List[str]):
        self.agent_id = agent_id
        self.role = role
        self.specialization_areas = specialization_areas
        self.decision_history: List[AgentDecision] = []
        self.performance = AgentPerformance(
            agent_id=agent_id,
            total_decisions=0,
            correct_decisions=0,
            accuracy=1.0,
            avg_confidence=0.5,
            avg_processing_time=0.0,
            specialization_score=1.0,
            collaboration_score=1.0,
            last_updated=datetime.now().isoformat()
        )
        self.knowledge_base: Dict[str, Any] = {}
        self.learning_rate = 0.1
    
    def make_decision(self, email: Email, context: Dict[str, Any] = None) -> AgentDecision:
        """Make a decision about the email. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement make_decision")
    
    def update_performance(self, decision: AgentDecision, actual_outcome: Any, feedback_score: float):
        """Update agent performance based on feedback."""
        self.performance.total_decisions += 1
        
        if feedback_score > 0.7:  # Consider it correct if feedback is positive
            self.performance.correct_decisions += 1
        
        # Update accuracy
        self.performance.accuracy = self.performance.correct_decisions / self.performance.total_decisions
        
        # Update confidence tracking
        total_conf = self.performance.avg_confidence * (self.performance.total_decisions - 1)
        self.performance.avg_confidence = (total_conf + decision.confidence) / self.performance.total_decisions
        
        # Update processing time
        total_time = self.performance.avg_processing_time * (self.performance.total_decisions - 1)
        self.performance.avg_processing_time = (total_time + decision.processing_time_ms) / self.performance.total_decisions
        
        self.performance.last_updated = datetime.now().isoformat()
        
        # Learn from feedback
        self._learn_from_feedback(decision, feedback_score)
    
    def _learn_from_feedback(self, decision: AgentDecision, feedback_score: float):
        """Learn from feedback to improve future decisions."""
        # Simple learning mechanism - adjust confidence in similar patterns
        pattern_key = f"{decision.decision_type}_{hash(decision.reasoning) % 1000}"
        
        if pattern_key not in self.knowledge_base:
            self.knowledge_base[pattern_key] = {'confidence': 0.5, 'occurrences': 0}
        
        kb_entry = self.knowledge_base[pattern_key]
        kb_entry['occurrences'] += 1
        
        # Adjust confidence based on feedback
        if feedback_score > 0.7:
            kb_entry['confidence'] = min(1.0, kb_entry['confidence'] + self.learning_rate)
        else:
            kb_entry['confidence'] = max(0.1, kb_entry['confidence'] - self.learning_rate)
    
    def get_specialization_confidence(self, email: Email) -> float:
        """Get confidence boost based on specialization match."""
        email_text = f"{email.subject} {email.body}".lower()
        
        specialization_boost = 0.0
        for area in self.specialization_areas:
            if area.lower() in email_text:
                specialization_boost += 0.1
        
        return min(0.3, specialization_boost)  # Max 30% boost


class CategoryExpertAgent(BaseAgent):
    """Agent specialized in email categorization."""
    
    def __init__(self):
        super().__init__(
            agent_id="category_expert_001",
            role=AgentRole.CATEGORY_EXPERT,
            specialization_areas=["customer support", "billing", "technical", "sales", "spam"]
        )
        
        # Category-specific patterns
        self.category_patterns = {
            EmailCategory.SPAM: [
                'winner', 'congratulations', 'urgent action', 'click here', 'limited time',
                'free money', 'act now', 'verify account', 'suspended'
            ],
            EmailCategory.CUSTOMER_SUPPORT: [
                'help', 'issue', 'problem', 'not working', 'error', 'support',
                'assistance', 'bug', 'unable to', 'how to'
            ],
            EmailCategory.BILLING: [
                'invoice', 'payment', 'bill', 'charge', 'refund', 'subscription',
                'billing', 'account', 'receipt', 'transaction'
            ],
            EmailCategory.SALES: [
                'proposal', 'quote', 'pricing', 'demo', 'product', 'service',
                'purchase', 'buy', 'offer', 'discount'
            ],
            EmailCategory.TECHNICAL: [
                'api', 'code', 'server', 'database', 'system', 'technical',
                'integration', 'development', 'deployment', 'configuration'
            ],
            EmailCategory.INTERNAL: [
                'meeting', 'team', 'project', 'deadline', 'update', 'report',
                'internal', 'company', 'office', 'colleagues'
            ],
            EmailCategory.NEWSLETTER: [
                'newsletter', 'unsubscribe', 'monthly', 'weekly', 'update',
                'news', 'announcement', 'digest', 'blog'
            ]
        }
    
    def make_decision(self, email: Email, context: Dict[str, Any] = None) -> AgentDecision:
        """Decide on email category based on patterns and context."""
        start_time = time.time()
        
        email_text = f"{email.subject} {email.body}".lower()
        category_scores = {}
        evidence = []
        
        # Pattern matching
        for category, patterns in self.category_patterns.items():
            score = 0
            matched_patterns = []
            
            for pattern in patterns:
                if pattern in email_text:
                    score += 1
                    matched_patterns.append(pattern)
            
            if matched_patterns:
                category_scores[category] = score / len(patterns)  # Normalize
                evidence.extend([f"Matched pattern: '{p}'" for p in matched_patterns[:3]])
        
        # Sender analysis
        sender_domain = email.sender.split('@')[-1] if '@' in email.sender else ""
        
        if sender_domain:
            if any(domain in sender_domain for domain in ['support', 'help', 'service']):
                category_scores[EmailCategory.CUSTOMER_SUPPORT] = category_scores.get(EmailCategory.CUSTOMER_SUPPORT, 0) + 0.3
                evidence.append("Support-related sender domain")
            elif any(domain in sender_domain for domain in ['billing', 'accounting', 'finance']):
                category_scores[EmailCategory.BILLING] = category_scores.get(EmailCategory.BILLING, 0) + 0.3
                evidence.append("Billing-related sender domain")
            elif any(domain in sender_domain for domain in ['no-reply', 'newsletter', 'marketing']):
                category_scores[EmailCategory.NEWSLETTER] = category_scores.get(EmailCategory.NEWSLETTER, 0) + 0.3
                evidence.append("Newsletter-type sender")
        
        # Attachment analysis
        if email.has_attachments:
            for attachment in email.attachments:
                if attachment.filename.lower().endswith(('.pdf', '.doc', '.docx')):
                    category_scores[EmailCategory.SALES] = category_scores.get(EmailCategory.SALES, 0) + 0.2
                    evidence.append("Document attachment (potential proposal/contract)")
                elif attachment.filename.lower().endswith(('.log', '.txt')):
                    category_scores[EmailCategory.TECHNICAL] = category_scores.get(EmailCategory.TECHNICAL, 0) + 0.2
                    evidence.append("Log file attachment")
        
        # Determine best category
        if category_scores:
            best_category = max(category_scores.keys(), key=lambda c: category_scores[c])
            confidence = min(1.0, category_scores[best_category] + self.get_specialization_confidence(email))
        else:
            best_category = EmailCategory.INTERNAL  # Default
            confidence = 0.3
            evidence.append("No strong patterns detected, defaulting to internal")
        
        # Build reasoning
        reasoning = f"Categorized as {best_category.value} based on pattern analysis. "
        reasoning += f"Score: {category_scores.get(best_category, 0):.2f}"
        
        processing_time = (time.time() - start_time) * 1000
        
        decision = AgentDecision(
            agent_id=self.agent_id,
            agent_role=self.role,
            email_id=email.id,
            decision_type="category",
            decision_value=best_category,
            confidence=confidence,
            reasoning=reasoning,
            evidence=evidence[:5],  # Limit evidence
            processing_time_ms=processing_time,
            timestamp=datetime.now().isoformat(),
            metadata={
                'category_scores': {c.value: score for c, score in category_scores.items()},
                'sender_domain': sender_domain
            }
        )
        
        self.decision_history.append(decision)
        return decision


class PriorityAnalystAgent(BaseAgent):
    """Agent specialized in priority assessment."""
    
    def __init__(self):
        super().__init__(
            agent_id="priority_analyst_001",
            role=AgentRole.PRIORITY_ANALYST,
            specialization_areas=["urgency", "deadline", "vip", "sla", "escalation"]
        )
        
        self.urgency_indicators = {
            'critical': ['critical', 'emergency', 'urgent', 'asap', 'immediately'],
            'high': ['high priority', 'important', 'deadline', 'time sensitive', 'soon'],
            'medium': ['please review', 'when possible', 'at your convenience'],
            'low': ['fyi', 'no rush', 'low priority', 'whenever']
        }
    
    def make_decision(self, email: Email, context: Dict[str, Any] = None) -> AgentDecision:
        """Assess email priority based on multiple factors."""
        start_time = time.time()
        
        email_text = f"{email.subject} {email.body}".lower()
        priority_score = 0.0
        evidence = []
        
        # Urgency keyword analysis
        urgency_level = 'medium'
        for level, keywords in self.urgency_indicators.items():
            if any(keyword in email_text for keyword in keywords):
                urgency_level = level
                evidence.append(f"Urgency indicator: {level}")
                break
        
        # Convert urgency level to score
        urgency_scores = {'low': 0.2, 'medium': 0.5, 'high': 0.8, 'critical': 1.0}
        priority_score = urgency_scores.get(urgency_level, 0.5)
        
        # Sender importance
        if email.sender_info:
            if email.sender_info.sender_type.value == "vip":
                priority_score = min(1.0, priority_score + 0.3)
                evidence.append("VIP sender detected")
            elif email.sender_info.sender_type.value == "known":
                priority_score = min(1.0, priority_score + 0.1)
                evidence.append("Known sender")
            elif email.sender_info.sender_type.value == "suspicious":
                priority_score = max(0.1, priority_score - 0.2)
                evidence.append("Suspicious sender - lowered priority")
            
            # Trust score influence
            trust_influence = (email.sender_info.trust_score - 0.5) * 0.2
            priority_score = max(0.1, min(1.0, priority_score + trust_influence))
            if abs(trust_influence) > 0.05:
                evidence.append(f"Trust score adjustment: {trust_influence:+.2f}")
        
        # SLA considerations
        if email.sla_deadline:
            try:
                deadline = datetime.fromisoformat(email.sla_deadline.replace('Z', '+00:00'))
                hours_remaining = (deadline - datetime.now(deadline.tzinfo)).total_seconds() / 3600
                
                if hours_remaining < 1:
                    priority_score = 1.0
                    evidence.append("SLA deadline within 1 hour - CRITICAL")
                elif hours_remaining < 4:
                    priority_score = max(priority_score, 0.9)
                    evidence.append("SLA deadline within 4 hours - HIGH")
                elif hours_remaining < 24:
                    priority_score = max(priority_score, 0.6)
                    evidence.append("SLA deadline within 24 hours")
            except (ValueError, TypeError):
                evidence.append("Invalid SLA deadline format")
        
        # Thread context (if part of ongoing conversation)
        if email.thread_size > 1:
            priority_score = min(1.0, priority_score + 0.1)
            evidence.append(f"Part of thread ({email.thread_size} emails)")
        
        # Time since received
        if email.time_in_inbox_hours > 48:
            priority_score = min(1.0, priority_score + 0.2)
            evidence.append("Email aging - needs attention")
        
        # Determine final priority
        if priority_score >= 0.8:
            final_priority = EmailPriority.URGENT
            confidence = 0.8 + min(0.2, (priority_score - 0.8) * 2)
        elif priority_score >= 0.6:
            final_priority = EmailPriority.HIGH
            confidence = 0.7 + min(0.2, (priority_score - 0.6) * 2)
        elif priority_score >= 0.4:
            final_priority = EmailPriority.NORMAL
            confidence = 0.6 + min(0.2, (priority_score - 0.4) * 2)
        else:
            final_priority = EmailPriority.LOW
            confidence = 0.5 + min(0.2, priority_score * 2)
        
        reasoning = f"Priority assessed as {final_priority.value} with score {priority_score:.2f}"
        
        processing_time = (time.time() - start_time) * 1000
        
        decision = AgentDecision(
            agent_id=self.agent_id,
            agent_role=self.role,
            email_id=email.id,
            decision_type="priority",
            decision_value=final_priority,
            confidence=confidence,
            reasoning=reasoning,
            evidence=evidence[:5],
            processing_time_ms=processing_time,
            timestamp=datetime.now().isoformat(),
            metadata={
                'priority_score': priority_score,
                'urgency_level': urgency_level,
                'sla_hours_remaining': email.time_in_inbox_hours
            }
        )
        
        self.decision_history.append(decision)
        return decision


class SecurityGuardAgent(BaseAgent):
    """Agent specialized in security and threat detection."""
    
    def __init__(self):
        super().__init__(
            agent_id="security_guard_001",
            role=AgentRole.SECURITY_GUARD,
            specialization_areas=["phishing", "malware", "spam", "security", "threats"]
        )
        
        self.threat_indicators = {
            'phishing': [
                'verify your account', 'confirm your password', 'update payment',
                'click here immediately', 'account will be suspended', 'urgent action required'
            ],
            'spam': [
                'congratulations', 'winner', 'lottery', 'free money', 'claim your prize',
                'no obligation', '100% free', 'act now', 'limited time offer'
            ],
            'malware': [
                'download attachment', 'run this file', 'install software',
                'update required', 'security patch', 'click to install'
            ]
        }
    
    def make_decision(self, email: Email, context: Dict[str, Any] = None) -> AgentDecision:
        """Assess security threats and recommend actions."""
        start_time = time.time()
        
        email_text = f"{email.subject} {email.body}".lower()
        threat_score = 0.0
        evidence = []
        threat_types = []
        
        # Threat pattern analysis
        for threat_type, indicators in self.threat_indicators.items():
            matches = [indicator for indicator in indicators if indicator in email_text]
            if matches:
                threat_score += len(matches) * 0.2
                threat_types.append(threat_type)
                evidence.extend([f"{threat_type.capitalize()} indicator: '{match}'" for match in matches[:2]])
        
        # Sender reputation analysis
        if email.sender_info:
            if email.sender_info.sender_type.value == "suspicious":
                threat_score += 0.4
                evidence.append("Known suspicious sender")
            elif email.sender_info.trust_score < 0.3:
                threat_score += 0.3
                evidence.append(f"Low trust sender (score: {email.sender_info.trust_score:.2f})")
        
        # Domain analysis
        sender_domain = email.sender.split('@')[-1] if '@' in email.sender else ""
        suspicious_tlds = ['.tk', '.ml', '.ga', '.cf', '.click']
        if any(sender_domain.endswith(tld) for tld in suspicious_tlds):
            threat_score += 0.3
            evidence.append("Suspicious domain TLD")
        
        # URL analysis
        import re
        urls = re.findall(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', email.body)
        if urls:
            for url in urls[:3]:  # Check first 3 URLs
                if any(suspicious in url.lower() for suspicious in ['bit.ly', 'tinyurl', 'short.link']):
                    threat_score += 0.2
                    evidence.append("URL shortener detected")
                if re.search(r'\d+\.\d+\.\d+\.\d+', url):  # IP address
                    threat_score += 0.3
                    evidence.append("Direct IP address in URL")
        
        # Attachment analysis
        if email.has_attachments:
            for attachment in email.attachments:
                if attachment.filename.lower().endswith(('.exe', '.scr', '.bat', '.cmd', '.zip')):
                    threat_score += 0.4
                    evidence.append(f"Suspicious attachment: {attachment.filename}")
                elif attachment.filename.count('.') > 1:  # Double extension
                    threat_score += 0.3
                    evidence.append("Double extension attachment")
        
        # Determine action recommendation
        if threat_score >= 0.8:
            recommended_action = "quarantine"
            confidence = 0.9
        elif threat_score >= 0.5:
            recommended_action = "flag_for_review"
            confidence = 0.8
        elif threat_score >= 0.3:
            recommended_action = "mark_suspicious"
            confidence = 0.7
        else:
            recommended_action = "allow"
            confidence = 0.6
        
        reasoning = f"Security assessment: {recommended_action}. Threat score: {threat_score:.2f}"
        if threat_types:
            reasoning += f". Detected threats: {', '.join(threat_types)}"
        
        processing_time = (time.time() - start_time) * 1000
        
        decision = AgentDecision(
            agent_id=self.agent_id,
            agent_role=self.role,
            email_id=email.id,
            decision_type="security_action",
            decision_value=recommended_action,
            confidence=confidence,
            reasoning=reasoning,
            evidence=evidence[:5],
            processing_time_ms=processing_time,
            timestamp=datetime.now().isoformat(),
            metadata={
                'threat_score': threat_score,
                'threat_types': threat_types,
                'urls_found': len(urls) if 'urls' in locals() else 0
            }
        )
        
        self.decision_history.append(decision)
        return decision


class QualityControllerAgent(BaseAgent):
    """Agent that validates and quality-checks other agents' decisions."""
    
    def __init__(self):
        super().__init__(
            agent_id="quality_controller_001",
            role=AgentRole.QUALITY_CONTROLLER,
            specialization_areas=["validation", "consistency", "quality", "accuracy"]
        )
    
    def validate_decisions(self, email: Email, decisions: List[AgentDecision]) -> AgentDecision:
        """Validate consistency and quality of other agents' decisions."""
        start_time = time.time()
        
        evidence = []
        quality_issues = []
        overall_confidence = 0.0
        
        if not decisions:
            return self._create_validation_decision(email, "no_decisions", 0.0, ["No decisions to validate"], [], start_time)
        
        # Check decision consistency
        category_decisions = [d for d in decisions if d.decision_type == "category"]
        priority_decisions = [d for d in decisions if d.decision_type == "priority"]
        security_decisions = [d for d in decisions if d.decision_type == "security_action"]
        
        # Validate category decisions
        if category_decisions:
            confidences = [d.confidence for d in category_decisions]
            avg_confidence = sum(confidences) / len(confidences)
            
            if avg_confidence < 0.5:
                quality_issues.append("Low confidence in category decisions")
            
            # Check for conflicting categorizations
            unique_categories = set(d.decision_value for d in category_decisions)
            if len(unique_categories) > 1:
                quality_issues.append(f"Conflicting categories: {[c.value if hasattr(c, 'value') else c for c in unique_categories]}")
        
        # Validate priority-security consistency
        if priority_decisions and security_decisions:
            priority = priority_decisions[0].decision_value
            security_action = security_decisions[0].decision_value
            
            # High priority should not be combined with quarantine
            if priority == EmailPriority.URGENT and security_action == "quarantine":
                quality_issues.append("Inconsistent: urgent priority with quarantine action")
            
            # Suspicious emails shouldn't be high priority unless explicitly justified
            if security_action in ["quarantine", "flag_for_review"] and priority in [EmailPriority.HIGH, EmailPriority.URGENT]:
                if not any("vip" in d.reasoning.lower() for d in decisions):
                    quality_issues.append("Questionable: high priority for suspicious email")
        
        # Calculate overall quality score
        total_confidence = sum(d.confidence for d in decisions)
        avg_confidence = total_confidence / len(decisions)
        consistency_penalty = len(quality_issues) * 0.1
        quality_score = max(0.0, avg_confidence - consistency_penalty)
        
        # Determine validation result
        if quality_score >= 0.8 and not quality_issues:
            validation_result = "approved"
            confidence = 0.9
            evidence.append(f"High quality decisions (score: {quality_score:.2f})")
        elif quality_score >= 0.6 and len(quality_issues) <= 1:
            validation_result = "approved_with_notes"
            confidence = 0.7
            evidence.append(f"Acceptable quality with minor issues")
        else:
            validation_result = "requires_review"
            confidence = 0.5
            evidence.append(f"Quality concerns detected (score: {quality_score:.2f})")
        
        evidence.extend(quality_issues)
        
        reasoning = f"Quality validation: {validation_result}. "
        reasoning += f"Analyzed {len(decisions)} decisions with overall quality score {quality_score:.2f}"
        
        processing_time = (time.time() - start_time) * 1000
        
        decision = AgentDecision(
            agent_id=self.agent_id,
            agent_role=self.role,
            email_id=email.id,
            decision_type="quality_validation",
            decision_value=validation_result,
            confidence=confidence,
            reasoning=reasoning,
            evidence=evidence[:5],
            processing_time_ms=processing_time,
            timestamp=datetime.now().isoformat(),
            metadata={
                'quality_score': quality_score,
                'decisions_analyzed': len(decisions),
                'quality_issues_count': len(quality_issues)
            }
        )
        
        self.decision_history.append(decision)
        return decision
    
    def _create_validation_decision(self, email: Email, result: str, confidence: float, evidence: List[str], issues: List[str], start_time: float) -> AgentDecision:
        """Helper to create validation decision."""
        processing_time = (time.time() - start_time) * 1000
        return AgentDecision(
            agent_id=self.agent_id,
            agent_role=self.role,
            email_id=email.id,
            decision_type="quality_validation",
            decision_value=result,
            confidence=confidence,
            reasoning=f"Quality validation: {result}",
            evidence=evidence,
            processing_time_ms=processing_time,
            timestamp=datetime.now().isoformat(),
            metadata={'quality_issues': issues}
        )


class MultiAgentOrchestrator:
    """Orchestrates multiple AI agents and builds consensus."""
    
    def __init__(self):
        # Initialize specialized agents
        self.agents = {
            AgentRole.CATEGORY_EXPERT: CategoryExpertAgent(),
            AgentRole.PRIORITY_ANALYST: PriorityAnalystAgent(),
            AgentRole.SECURITY_GUARD: SecurityGuardAgent(),
            AgentRole.QUALITY_CONTROLLER: QualityControllerAgent()
        }
        
        self.consensus_history: List[ConsensusResult] = []
        self.orchestration_stats = {
            'total_orchestrations': 0,
            'consensus_achieved': 0,
            'conflicts_resolved': 0,
            'avg_processing_time': 0.0
        }
        
        # Thread pool for parallel agent execution
        self.executor = ThreadPoolExecutor(max_workers=len(self.agents))
    
    def process_email(self, email: Email, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """Process email through all agents and build consensus."""
        start_time = time.time()
        
        # Get decisions from all relevant agents in parallel
        decisions = self._get_agent_decisions(email, context or {})
        
        # Quality validation
        quality_agent = self.agents[AgentRole.QUALITY_CONTROLLER]
        quality_decision = quality_agent.validate_decisions(email, decisions)
        decisions.append(quality_decision)
        
        # Build consensus
        consensus_results = self._build_consensus(email, decisions)
        
        # Update statistics
        processing_time = (time.time() - start_time) * 1000
        self.orchestration_stats['total_orchestrations'] += 1
        
        total_time = self.orchestration_stats['avg_processing_time'] * (self.orchestration_stats['total_orchestrations'] - 1)
        self.orchestration_stats['avg_processing_time'] = (total_time + processing_time) / self.orchestration_stats['total_orchestrations']
        
        return {
            'consensus_results': consensus_results,
            'individual_decisions': [self._decision_to_dict(d) for d in decisions],
            'processing_time_ms': processing_time,
            'quality_validation': quality_decision.decision_value,
            'agent_count': len(self.agents)
        }
    
    def _get_agent_decisions(self, email: Email, context: Dict[str, Any]) -> List[AgentDecision]:
        """Get decisions from all agents in parallel."""
        futures = []
        
        # Submit agent tasks to thread pool
        for role, agent in self.agents.items():
            if role != AgentRole.QUALITY_CONTROLLER:  # Quality controller runs separately
                future = self.executor.submit(agent.make_decision, email, context)
                futures.append(future)
        
        # Collect results
        decisions = []
        for future in futures:
            try:
                decision = future.result(timeout=5.0)  # 5 second timeout per agent
                decisions.append(decision)
            except Exception as e:
                logging.warning(f"Agent decision failed: {e}")
        
        return decisions
    
    def _build_consensus(self, email: Email, decisions: List[AgentDecision]) -> Dict[str, ConsensusResult]:
        """Build consensus from agent decisions."""
        consensus_results = {}
        
        # Group decisions by type
        decision_groups = defaultdict(list)
        for decision in decisions:
            if decision.decision_type != "quality_validation":  # Skip quality validation
                decision_groups[decision.decision_type].append(decision)
        
        # Build consensus for each decision type
        for decision_type, type_decisions in decision_groups.items():
            consensus = self._resolve_consensus(email.id, decision_type, type_decisions)
            consensus_results[decision_type] = consensus
            
            if consensus.agreement_score > 0.8:
                self.orchestration_stats['consensus_achieved'] += 1
            else:
                self.orchestration_stats['conflicts_resolved'] += 1
        
        return consensus_results
    
    def _resolve_consensus(self, email_id: str, decision_type: str, decisions: List[AgentDecision]) -> ConsensusResult:
        """Resolve consensus for a specific decision type."""
        start_time = time.time()
        
        if not decisions:
            return ConsensusResult(
                email_id=email_id,
                decision_type=decision_type,
                final_decision=None,
                confidence=0.0,
                participating_agents=[],
                agreement_score=0.0,
                dissenting_opinions=[],
                processing_time_ms=0.0,
                consensus_method="no_decisions"
            )
        
        if len(decisions) == 1:
            # Only one agent decided - use that decision
            decision = decisions[0]
            return ConsensusResult(
                email_id=email_id,
                decision_type=decision_type,
                final_decision=decision.decision_value,
                confidence=decision.confidence,
                participating_agents=[decision.agent_id],
                agreement_score=1.0,
                dissenting_opinions=[],
                processing_time_ms=(time.time() - start_time) * 1000,
                consensus_method="single_agent"
            )
        
        # Multiple agents - resolve conflicts
        decision_counts = Counter(str(d.decision_value) for d in decisions)
        most_common = decision_counts.most_common(1)[0]
        
        if most_common[1] == len(decisions):
            # Unanimous decision
            consensus_method = "unanimous"
            agreement_score = 1.0
            final_decision = decisions[0].decision_value
            avg_confidence = sum(d.confidence for d in decisions) / len(decisions)
            dissenting_opinions = []
            
        elif most_common[1] > len(decisions) / 2:
            # Majority rule
            consensus_method = "majority"
            agreement_score = most_common[1] / len(decisions)
            
            # Find majority decision
            majority_decisions = [d for d in decisions if str(d.decision_value) == most_common[0]]
            final_decision = majority_decisions[0].decision_value
            avg_confidence = sum(d.confidence for d in majority_decisions) / len(majority_decisions)
            
            # Dissenting opinions
            dissenting_opinions = [d for d in decisions if str(d.decision_value) != most_common[0]]
            
        else:
            # Weighted consensus based on confidence
            consensus_method = "weighted"
            
            weighted_scores = defaultdict(float)
            total_weight = 0
            
            for decision in decisions:
                weight = decision.confidence
                weighted_scores[str(decision.decision_value)] += weight
                total_weight += weight
            
            best_decision = max(weighted_scores.keys(), key=lambda k: weighted_scores[k])
            final_decision = next(d.decision_value for d in decisions if str(d.decision_value) == best_decision)
            
            agreement_score = weighted_scores[best_decision] / total_weight if total_weight > 0 else 0
            avg_confidence = agreement_score
            
            dissenting_opinions = [d for d in decisions if str(d.decision_value) != best_decision]
        
        processing_time = (time.time() - start_time) * 1000
        
        consensus = ConsensusResult(
            email_id=email_id,
            decision_type=decision_type,
            final_decision=final_decision,
            confidence=avg_confidence,
            participating_agents=[d.agent_id for d in decisions],
            agreement_score=agreement_score,
            dissenting_opinions=dissenting_opinions,
            processing_time_ms=processing_time,
            consensus_method=consensus_method
        )
        
        self.consensus_history.append(consensus)
        return consensus
    
    def _decision_to_dict(self, decision: AgentDecision) -> Dict[str, Any]:
        """Convert AgentDecision to dictionary for serialization."""
        return {
            'agent_id': decision.agent_id,
            'agent_role': decision.agent_role.value,
            'decision_type': decision.decision_type,
            'decision_value': decision.decision_value.value if hasattr(decision.decision_value, 'value') else decision.decision_value,
            'confidence': decision.confidence,
            'reasoning': decision.reasoning,
            'evidence': decision.evidence,
            'processing_time_ms': decision.processing_time_ms
        }
    
    def get_agent_performance(self) -> Dict[str, Any]:
        """Get performance statistics for all agents."""
        performance_data = {}
        
        for role, agent in self.agents.items():
            performance_data[role.value] = {
                'total_decisions': agent.performance.total_decisions,
                'accuracy': agent.performance.accuracy,
                'avg_confidence': agent.performance.avg_confidence,
                'avg_processing_time': agent.performance.avg_processing_time,
                'recent_decisions': len([d for d in agent.decision_history[-50:] if d]),
                'specialization_areas': agent.specialization_areas
            }
        
        return {
            'agent_performance': performance_data,
            'orchestration_stats': self.orchestration_stats,
            'consensus_history_size': len(self.consensus_history)
        }
    
    def update_agent_performance(self, email_id: str, feedback: Dict[str, Any]):
        """Update agent performance based on feedback."""
        # Find decisions for this email
        for agent in self.agents.values():
            relevant_decisions = [d for d in agent.decision_history if d.email_id == email_id]
            
            for decision in relevant_decisions:
                # Extract feedback score for this decision type
                feedback_score = feedback.get(f"{decision.decision_type}_satisfaction", 0.5)
                actual_outcome = feedback.get(f"{decision.decision_type}_actual", decision.decision_value)
                
                agent.update_performance(decision, actual_outcome, feedback_score)


# Global multi-agent orchestrator instance
agent_orchestrator = MultiAgentOrchestrator()