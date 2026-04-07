"""Advanced Workflow Automation Engine for Email Triage.

This module implements a sophisticated workflow automation system with:
- Complex conditional logic and triggers
- Multi-step automation sequences
- Dynamic rule creation and modification
- Performance monitoring and optimization
- Integration with ML pipeline for smart automation
"""

import json
import time
from typing import Dict, List, Any, Optional, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import re
import logging

from models import Email, EmailCategory, EmailPriority, ActionType, Action


class TriggerType(str, Enum):
    """Types of workflow triggers."""
    EMAIL_RECEIVED = "email_received"
    EMAIL_CATEGORIZED = "email_categorized"
    EMAIL_PRIORITIZED = "email_prioritized"
    SENDER_DETECTED = "sender_detected"
    KEYWORD_MATCH = "keyword_match"
    TIME_BASED = "time_based"
    SLA_DEADLINE = "sla_deadline"
    ATTACHMENT_DETECTED = "attachment_detected"
    SENTIMENT_THRESHOLD = "sentiment_threshold"
    ML_CONFIDENCE = "ml_confidence"


class ConditionOperator(str, Enum):
    """Logical operators for conditions."""
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    CONTAINS = "contains"
    NOT_CONTAINS = "not_contains"
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    BETWEEN = "between"
    IN_LIST = "in_list"
    NOT_IN_LIST = "not_in_list"
    REGEX_MATCH = "regex_match"


class ActionFrequency(str, Enum):
    """How often an action should execute."""
    ONCE = "once"
    DAILY = "daily"
    HOURLY = "hourly"
    IMMEDIATE = "immediate"


@dataclass
class WorkflowCondition:
    """A single condition in a workflow rule."""
    field: str  # e.g., "subject", "sender", "category", "priority"
    operator: ConditionOperator
    value: Any
    weight: float = 1.0  # Importance weight for this condition


@dataclass
class WorkflowAction:
    """An action to perform when workflow triggers."""
    action_type: ActionType
    parameters: Dict[str, Any] = field(default_factory=dict)
    delay_seconds: int = 0  # Delay before executing action
    frequency: ActionFrequency = ActionFrequency.IMMEDIATE
    condition_score_threshold: float = 0.7  # Minimum condition score to trigger


@dataclass
class WorkflowRule:
    """A complete workflow automation rule."""
    id: str
    name: str
    description: str
    trigger_type: TriggerType
    conditions: List[WorkflowCondition]
    actions: List[WorkflowAction]
    is_active: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_executed: Optional[str] = None
    execution_count: int = 0
    success_rate: float = 1.0
    performance_score: float = 1.0
    tags: List[str] = field(default_factory=list)


@dataclass
class WorkflowExecution:
    """Record of a workflow rule execution."""
    rule_id: str
    email_id: str
    triggered_at: str
    conditions_met: List[str]
    condition_score: float
    actions_performed: List[Dict[str, Any]]
    success: bool
    execution_time_ms: float
    errors: List[str] = field(default_factory=list)


class WorkflowEngine:
    """Main workflow automation engine."""
    
    def __init__(self):
        self.rules: Dict[str, WorkflowRule] = {}
        self.execution_history: List[WorkflowExecution] = []
        self.scheduled_actions: List[Dict[str, Any]] = []
        self.performance_metrics = {
            'total_executions': 0,
            'successful_executions': 0,
            'average_execution_time': 0.0,
            'rules_created': 0,
            'auto_generated_rules': 0
        }
        
        # Initialize with some standard workflow templates
        self._create_standard_workflows()
        
    def _create_standard_workflows(self):
        """Create standard workflow templates."""
        
        # Auto-categorize spam emails
        spam_rule = WorkflowRule(
            id="auto_categorize_spam",
            name="Auto-Categorize Spam",
            description="Automatically categorize emails with spam indicators",
            trigger_type=TriggerType.EMAIL_RECEIVED,
            conditions=[
                WorkflowCondition("subject", ConditionOperator.REGEX_MATCH, r"(urgent|winner|congratulations|prize|claim)", weight=1.5),
                WorkflowCondition("sender", ConditionOperator.NOT_CONTAINS, "@yourcompany.com", weight=1.0),
                WorkflowCondition("sentiment_score", ConditionOperator.LESS_THAN, -0.5, weight=1.2)
            ],
            actions=[
                WorkflowAction(
                    action_type=ActionType.CATEGORIZE,
                    parameters={"category": "spam"},
                    condition_score_threshold=0.8
                ),
                WorkflowAction(
                    action_type=ActionType.MARK_SPAM,
                    parameters={},
                    condition_score_threshold=0.9
                )
            ],
            tags=["spam", "security", "auto-generated"]
        )
        self.rules[spam_rule.id] = spam_rule
        
        # VIP priority escalation
        vip_rule = WorkflowRule(
            id="vip_priority_escalation",
            name="VIP Priority Escalation",
            description="Escalate emails from VIP senders to high priority",
            trigger_type=TriggerType.EMAIL_RECEIVED,
            conditions=[
                WorkflowCondition("sender_type", ConditionOperator.EQUALS, "vip", weight=2.0),
                WorkflowCondition("priority", ConditionOperator.IN_LIST, [None, "normal", "low"], weight=1.0)
            ],
            actions=[
                WorkflowAction(
                    action_type=ActionType.PRIORITIZE,
                    parameters={"priority": "high"},
                    condition_score_threshold=0.6
                ),
                WorkflowAction(
                    action_type=ActionType.FLAG,
                    parameters={},
                    condition_score_threshold=0.8
                )
            ],
            tags=["vip", "priority", "escalation"]
        )
        self.rules[vip_rule.id] = vip_rule
        
        # Customer support auto-response
        support_rule = WorkflowRule(
            id="customer_support_auto_response",
            name="Customer Support Auto-Response",
            description="Auto-respond to customer support emails with acknowledgment",
            trigger_type=TriggerType.EMAIL_CATEGORIZED,
            conditions=[
                WorkflowCondition("category", ConditionOperator.EQUALS, "customer_support", weight=2.0),
                WorkflowCondition("reply_sent", ConditionOperator.EQUALS, None, weight=1.0)
            ],
            actions=[
                WorkflowAction(
                    action_type=ActionType.REPLY,
                    parameters={
                        "template_id": "customer_support_acknowledgment",
                        "auto_reply": True
                    },
                    delay_seconds=300,  # 5-minute delay
                    condition_score_threshold=0.7
                )
            ],
            tags=["customer_support", "auto_reply"]
        )
        self.rules[support_rule.id] = support_rule
        
        # SLA deadline monitoring
        sla_rule = WorkflowRule(
            id="sla_deadline_monitoring",
            name="SLA Deadline Monitoring",
            description="Flag emails approaching SLA deadline",
            trigger_type=TriggerType.SLA_DEADLINE,
            conditions=[
                WorkflowCondition("sla_hours_remaining", ConditionOperator.LESS_THAN, 2, weight=2.0),
                WorkflowCondition("reply_sent", ConditionOperator.EQUALS, None, weight=1.0)
            ],
            actions=[
                WorkflowAction(
                    action_type=ActionType.FLAG,
                    parameters={"flag_reason": "sla_deadline_approaching"},
                    condition_score_threshold=0.6
                ),
                WorkflowAction(
                    action_type=ActionType.PRIORITIZE,
                    parameters={"priority": "urgent"},
                    condition_score_threshold=0.8
                )
            ],
            tags=["sla", "deadline", "monitoring"]
        )
        self.rules[sla_rule.id] = sla_rule
    
    def add_rule(self, rule: WorkflowRule) -> str:
        """Add a new workflow rule."""
        self.rules[rule.id] = rule
        self.performance_metrics['rules_created'] += 1
        
        logging.info(f"Added workflow rule: {rule.name} ({rule.id})")
        return rule.id
    
    def remove_rule(self, rule_id: str) -> bool:
        """Remove a workflow rule."""
        if rule_id in self.rules:
            del self.rules[rule_id]
            logging.info(f"Removed workflow rule: {rule_id}")
            return True
        return False
    
    def trigger_workflow(self, trigger_type: TriggerType, email: Email, context: Dict[str, Any] = None) -> List[WorkflowExecution]:
        """Trigger workflow rules based on event type."""
        if context is None:
            context = {}
            
        triggered_rules = [
            rule for rule in self.rules.values()
            if rule.is_active and rule.trigger_type == trigger_type
        ]
        
        executions = []
        for rule in triggered_rules:
            execution = self._execute_rule(rule, email, context)
            if execution:
                executions.append(execution)
        
        return executions
    
    def _execute_rule(self, rule: WorkflowRule, email: Email, context: Dict[str, Any]) -> Optional[WorkflowExecution]:
        """Execute a single workflow rule."""
        start_time = time.time()
        
        try:
            # Evaluate conditions
            condition_score, met_conditions = self._evaluate_conditions(rule.conditions, email, context)
            
            # Check if conditions are met
            if condition_score < 0.5:  # Minimum threshold for any execution
                return None
            
            execution = WorkflowExecution(
                rule_id=rule.id,
                email_id=email.id,
                triggered_at=datetime.now().isoformat(),
                conditions_met=met_conditions,
                condition_score=condition_score,
                actions_performed=[],
                success=True,
                execution_time_ms=0.0,
                errors=[]
            )
            
            # Execute actions that meet their thresholds
            for action in rule.actions:
                if condition_score >= action.condition_score_threshold:
                    try:
                        if action.delay_seconds > 0:
                            # Schedule delayed action
                            self._schedule_action(rule.id, email.id, action, action.delay_seconds)
                            execution.actions_performed.append({
                                "action_type": action.action_type.value,
                                "status": "scheduled",
                                "delay_seconds": action.delay_seconds
                            })
                        else:
                            # Execute immediately
                            result = self._execute_action(action, email, context)
                            execution.actions_performed.append(result)
                            
                    except Exception as e:
                        execution.errors.append(f"Action {action.action_type.value} failed: {str(e)}")
                        execution.success = False
                        logging.error(f"Workflow action failed: {e}")
            
            # Update rule statistics
            rule.execution_count += 1
            rule.last_executed = datetime.now().isoformat()
            if execution.success:
                rule.success_rate = (rule.success_rate * (rule.execution_count - 1) + 1.0) / rule.execution_count
            else:
                rule.success_rate = (rule.success_rate * (rule.execution_count - 1) + 0.0) / rule.execution_count
            
            # Calculate execution time
            execution.execution_time_ms = (time.time() - start_time) * 1000
            
            # Update performance metrics
            self.performance_metrics['total_executions'] += 1
            if execution.success:
                self.performance_metrics['successful_executions'] += 1
            
            avg_time = self.performance_metrics['average_execution_time']
            total_execs = self.performance_metrics['total_executions']
            self.performance_metrics['average_execution_time'] = (
                (avg_time * (total_execs - 1) + execution.execution_time_ms) / total_execs
            )
            
            # Store execution history
            self.execution_history.append(execution)
            if len(self.execution_history) > 1000:  # Keep last 1000 executions
                self.execution_history = self.execution_history[-500:]
            
            return execution
            
        except Exception as e:
            logging.error(f"Workflow rule execution failed: {e}")
            return None
    
    def _evaluate_conditions(self, conditions: List[WorkflowCondition], email: Email, context: Dict[str, Any]) -> tuple[float, List[str]]:
        """Evaluate workflow conditions and return score and met conditions."""
        total_weight = sum(condition.weight for condition in conditions)
        met_weight = 0.0
        met_conditions = []
        
        for condition in conditions:
            if self._evaluate_single_condition(condition, email, context):
                met_weight += condition.weight
                met_conditions.append(f"{condition.field} {condition.operator.value} {condition.value}")
        
        score = met_weight / total_weight if total_weight > 0 else 0.0
        return score, met_conditions
    
    def _evaluate_single_condition(self, condition: WorkflowCondition, email: Email, context: Dict[str, Any]) -> bool:
        """Evaluate a single workflow condition."""
        try:
            # Get field value from email or context
            field_value = self._get_field_value(condition.field, email, context)
            
            # Evaluate based on operator
            if condition.operator == ConditionOperator.EQUALS:
                return field_value == condition.value
            elif condition.operator == ConditionOperator.NOT_EQUALS:
                return field_value != condition.value
            elif condition.operator == ConditionOperator.CONTAINS:
                return str(condition.value).lower() in str(field_value).lower()
            elif condition.operator == ConditionOperator.NOT_CONTAINS:
                return str(condition.value).lower() not in str(field_value).lower()
            elif condition.operator == ConditionOperator.GREATER_THAN:
                return float(field_value) > float(condition.value)
            elif condition.operator == ConditionOperator.LESS_THAN:
                return float(field_value) < float(condition.value)
            elif condition.operator == ConditionOperator.BETWEEN:
                if isinstance(condition.value, (list, tuple)) and len(condition.value) == 2:
                    return condition.value[0] <= float(field_value) <= condition.value[1]
                return False
            elif condition.operator == ConditionOperator.IN_LIST:
                return field_value in condition.value
            elif condition.operator == ConditionOperator.NOT_IN_LIST:
                return field_value not in condition.value
            elif condition.operator == ConditionOperator.REGEX_MATCH:
                return bool(re.search(str(condition.value), str(field_value), re.IGNORECASE))
            
            return False
            
        except Exception as e:
            logging.warning(f"Condition evaluation failed: {e}")
            return False
    
    def _get_field_value(self, field: str, email: Email, context: Dict[str, Any]) -> Any:
        """Get field value from email object or context."""
        # Check context first
        if field in context:
            return context[field]
        
        # Map common field names to email attributes
        field_mapping = {
            'subject': email.subject,
            'sender': email.sender,
            'sender_name': email.sender_name,
            'body': email.body,
            'category': email.category.value if email.category else None,
            'priority': email.priority.value if email.priority else None,
            'is_spam': email.is_spam,
            'is_flagged': email.is_flagged,
            'is_archived': email.is_archived,
            'has_attachments': email.has_attachments,
            'attachment_count': len(email.attachments),
            'tags': email.tags,
            'importance_score': email.importance_score,
            'sentiment_score': email.sentiment_score,
            'sentiment_label': email.sentiment_label,
            'reply_sent': email.reply_sent,
            'forwarded_to': email.forwarded_to,
            'sender_type': email.sender_info.sender_type.value if email.sender_info else None,
            'trust_score': email.sender_info.trust_score if email.sender_info else 0.5,
            'time_in_inbox_hours': email.time_in_inbox_hours
        }
        
        if field in field_mapping:
            return field_mapping[field]
        
        # Handle special calculated fields
        if field == 'sla_hours_remaining':
            if email.sla_deadline:
                try:
                    deadline = datetime.fromisoformat(email.sla_deadline.replace('Z', '+00:00'))
                    remaining = (deadline - datetime.now(deadline.tzinfo)).total_seconds() / 3600
                    return max(0, remaining)
                except (ValueError, TypeError):
                    return 0
            return float('inf')  # No SLA deadline
        
        # Try direct attribute access
        if hasattr(email, field):
            return getattr(email, field)
        
        return None
    
    def _execute_action(self, action: WorkflowAction, email: Email, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a workflow action."""
        result = {
            "action_type": action.action_type.value,
            "parameters": action.parameters,
            "status": "success",
            "message": "",
            "timestamp": datetime.now().isoformat()
        }
        
        try:
            if action.action_type == ActionType.CATEGORIZE:
                category = EmailCategory(action.parameters.get("category"))
                email.category = category
                result["message"] = f"Email categorized as {category.value}"
                
            elif action.action_type == ActionType.PRIORITIZE:
                priority = EmailPriority(action.parameters.get("priority"))
                email.priority = priority
                result["message"] = f"Email priority set to {priority.value}"
                
            elif action.action_type == ActionType.FLAG:
                email.is_flagged = True
                flag_reason = action.parameters.get("flag_reason", "workflow")
                result["message"] = f"Email flagged: {flag_reason}"
                
            elif action.action_type == ActionType.ARCHIVE:
                email.is_archived = True
                result["message"] = "Email archived"
                
            elif action.action_type == ActionType.MARK_SPAM:
                email.is_spam = True
                email.category = EmailCategory.SPAM
                result["message"] = "Email marked as spam"
                
            elif action.action_type == ActionType.TAG:
                tags = action.parameters.get("tags", [])
                existing_tags = set(email.tags or [])
                email.tags = list(existing_tags.union(set(tags)))
                result["message"] = f"Tags added: {', '.join(tags)}"
                
            elif action.action_type == ActionType.REPLY:
                template_id = action.parameters.get("template_id")
                auto_reply = action.parameters.get("auto_reply", False)
                
                if template_id:
                    # Use template system for reply
                    reply_content = self._generate_template_reply(template_id, email, context)
                    email.reply_sent = reply_content
                    result["message"] = f"Auto-reply sent using template: {template_id}"
                else:
                    result["status"] = "error"
                    result["message"] = "No template specified for reply action"
                    
            else:
                result["status"] = "error"
                result["message"] = f"Unknown action type: {action.action_type.value}"
                
        except Exception as e:
            result["status"] = "error"
            result["message"] = f"Action execution failed: {str(e)}"
        
        return result
    
    def _generate_template_reply(self, template_id: str, email: Email, context: Dict[str, Any]) -> str:
        """Generate reply content using template."""
        templates = {
            "customer_support_acknowledgment": """
Dear {sender_name},

Thank you for contacting our customer support team. We have received your message regarding: "{subject}"

We will review your inquiry and respond within 24 hours. If this is an urgent matter, please call our support line at 1-800-SUPPORT.

Best regards,
Customer Support Team
            """.strip(),
            
            "auto_out_of_office": """
Thank you for your email. I am currently out of the office and will return on {return_date}.

For urgent matters, please contact my colleague at urgent@company.com.

I will respond to your message upon my return.

Best regards,
{recipient_name}
            """.strip(),
            
            "spam_notification": """
This email has been automatically flagged as potential spam and moved to quarantine.

If you believe this is a mistake, please contact the IT security team.

Automated Email Security System
            """.strip()
        }
        
        template = templates.get(template_id, "Thank you for your email. We will respond soon.")
        
        # Replace placeholders
        variables = {
            'sender_name': email.sender_name,
            'subject': email.subject,
            'recipient_name': context.get('recipient_name', 'System'),
            'return_date': context.get('return_date', 'soon'),
            'current_date': datetime.now().strftime('%Y-%m-%d')
        }
        
        for key, value in variables.items():
            template = template.replace(f'{{{key}}}', str(value))
        
        return template
    
    def _schedule_action(self, rule_id: str, email_id: str, action: WorkflowAction, delay_seconds: int):
        """Schedule an action for later execution."""
        scheduled_time = datetime.now() + timedelta(seconds=delay_seconds)
        
        scheduled_action = {
            'rule_id': rule_id,
            'email_id': email_id,
            'action': action,
            'scheduled_time': scheduled_time.isoformat(),
            'created_at': datetime.now().isoformat()
        }
        
        self.scheduled_actions.append(scheduled_action)
    
    def process_scheduled_actions(self, email_lookup: Callable[[str], Optional[Email]]) -> List[Dict[str, Any]]:
        """Process any scheduled actions that are due."""
        current_time = datetime.now()
        processed_actions = []
        remaining_actions = []
        
        for scheduled in self.scheduled_actions:
            try:
                scheduled_time = datetime.fromisoformat(scheduled['scheduled_time'])
                
                if current_time >= scheduled_time:
                    # Action is due
                    email = email_lookup(scheduled['email_id'])
                    if email:
                        result = self._execute_action(scheduled['action'], email, {})
                        processed_actions.append({
                            'rule_id': scheduled['rule_id'],
                            'email_id': scheduled['email_id'],
                            'action_result': result,
                            'executed_at': current_time.isoformat()
                        })
                    else:
                        # Email not found, skip
                        processed_actions.append({
                            'rule_id': scheduled['rule_id'],
                            'email_id': scheduled['email_id'],
                            'action_result': {'status': 'error', 'message': 'Email not found'},
                            'executed_at': current_time.isoformat()
                        })
                else:
                    # Action not yet due
                    remaining_actions.append(scheduled)
                    
            except Exception as e:
                logging.error(f"Failed to process scheduled action: {e}")
                processed_actions.append({
                    'rule_id': scheduled.get('rule_id', 'unknown'),
                    'email_id': scheduled.get('email_id', 'unknown'),
                    'action_result': {'status': 'error', 'message': str(e)},
                    'executed_at': current_time.isoformat()
                })
        
        self.scheduled_actions = remaining_actions
        return processed_actions
    
    def suggest_workflow_rules(self, email_history: List[Email], user_actions: List[Dict[str, Any]]) -> List[WorkflowRule]:
        """Suggest new workflow rules based on user behavior patterns."""
        suggestions = []
        
        # Analyze user action patterns
        action_patterns = self._analyze_action_patterns(email_history, user_actions)
        
        for pattern in action_patterns:
            if pattern['confidence'] > 0.7 and pattern['frequency'] > 5:
                # Create suggested rule
                rule = self._create_rule_from_pattern(pattern)
                if rule:
                    suggestions.append(rule)
        
        return suggestions[:5]  # Return top 5 suggestions
    
    def _analyze_action_patterns(self, email_history: List[Email], user_actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Analyze patterns in user actions."""
        patterns = []
        
        # Group actions by type
        action_groups = {}
        for action in user_actions:
            action_type = action.get('action_type')
            if action_type not in action_groups:
                action_groups[action_type] = []
            action_groups[action_type].append(action)
        
        # Analyze each action type for patterns
        for action_type, actions in action_groups.items():
            if len(actions) < 3:  # Need minimum occurrences
                continue
            
            # Find common patterns
            sender_patterns = self._find_sender_patterns(actions, email_history)
            subject_patterns = self._find_subject_patterns(actions, email_history)
            
            for pattern in sender_patterns + subject_patterns:
                patterns.append(pattern)
        
        return patterns
    
    def _find_sender_patterns(self, actions: List[Dict[str, Any]], email_history: List[Email]) -> List[Dict[str, Any]]:
        """Find patterns based on email senders."""
        patterns = []
        
        # Group actions by sender
        sender_actions = {}
        for action in actions:
            email_id = action.get('email_id')
            email = next((e for e in email_history if e.id == email_id), None)
            if email:
                sender = email.sender
                if sender not in sender_actions:
                    sender_actions[sender] = []
                sender_actions[sender].append(action)
        
        # Find senders with consistent actions
        for sender, sender_action_list in sender_actions.items():
            if len(sender_action_list) >= 3:
                # Check if actions are consistent
                action_types = [a.get('action_type') for a in sender_action_list]
                most_common_action = max(set(action_types), key=action_types.count)
                consistency = action_types.count(most_common_action) / len(action_types)
                
                if consistency > 0.8:  # 80% consistency
                    patterns.append({
                        'type': 'sender_pattern',
                        'pattern': f"sender_equals_{sender}",
                        'action': most_common_action,
                        'confidence': consistency,
                        'frequency': len(sender_action_list),
                        'description': f"Always {most_common_action} emails from {sender}"
                    })
        
        return patterns
    
    def _find_subject_patterns(self, actions: List[Dict[str, Any]], email_history: List[Email]) -> List[Dict[str, Any]]:
        """Find patterns based on email subjects."""
        patterns = []
        
        # Group actions by subject keywords
        keyword_actions = {}
        for action in actions:
            email_id = action.get('email_id')
            email = next((e for e in email_history if e.id == email_id), None)
            if email:
                # Extract keywords from subject
                subject_words = re.findall(r'\b\w+\b', email.subject.lower())
                for word in subject_words:
                    if len(word) > 3:  # Skip short words
                        if word not in keyword_actions:
                            keyword_actions[word] = []
                        keyword_actions[word].append(action)
        
        # Find keywords with consistent actions
        for keyword, keyword_action_list in keyword_actions.items():
            if len(keyword_action_list) >= 3:
                action_types = [a.get('action_type') for a in keyword_action_list]
                most_common_action = max(set(action_types), key=action_types.count)
                consistency = action_types.count(most_common_action) / len(action_types)
                
                if consistency > 0.75:  # 75% consistency
                    patterns.append({
                        'type': 'subject_pattern',
                        'pattern': f"subject_contains_{keyword}",
                        'action': most_common_action,
                        'confidence': consistency,
                        'frequency': len(keyword_action_list),
                        'description': f"Always {most_common_action} emails with '{keyword}' in subject"
                    })
        
        return patterns
    
    def _create_rule_from_pattern(self, pattern: Dict[str, Any]) -> Optional[WorkflowRule]:
        """Create a workflow rule from a detected pattern."""
        try:
            rule_id = f"auto_rule_{int(time.time())}"
            
            if pattern['type'] == 'sender_pattern':
                sender = pattern['pattern'].replace('sender_equals_', '')
                rule = WorkflowRule(
                    id=rule_id,
                    name=f"Auto-handle {sender}",
                    description=pattern['description'],
                    trigger_type=TriggerType.EMAIL_RECEIVED,
                    conditions=[
                        WorkflowCondition("sender", ConditionOperator.EQUALS, sender, weight=2.0)
                    ],
                    actions=[
                        WorkflowAction(
                            action_type=ActionType(pattern['action']),
                            parameters=self._get_action_parameters(pattern['action']),
                            condition_score_threshold=0.7
                        )
                    ],
                    tags=["auto-generated", "pattern-based"]
                )
                
            elif pattern['type'] == 'subject_pattern':
                keyword = pattern['pattern'].replace('subject_contains_', '')
                rule = WorkflowRule(
                    id=rule_id,
                    name=f"Auto-handle '{keyword}' emails",
                    description=pattern['description'],
                    trigger_type=TriggerType.EMAIL_RECEIVED,
                    conditions=[
                        WorkflowCondition("subject", ConditionOperator.CONTAINS, keyword, weight=1.5)
                    ],
                    actions=[
                        WorkflowAction(
                            action_type=ActionType(pattern['action']),
                            parameters=self._get_action_parameters(pattern['action']),
                            condition_score_threshold=0.7
                        )
                    ],
                    tags=["auto-generated", "pattern-based"]
                )
            else:
                return None
            
            self.performance_metrics['auto_generated_rules'] += 1
            return rule
            
        except Exception as e:
            logging.error(f"Failed to create rule from pattern: {e}")
            return None
    
    def _get_action_parameters(self, action_type: str) -> Dict[str, Any]:
        """Get default parameters for action type."""
        defaults = {
            "categorize": {"category": "internal"},
            "prioritize": {"priority": "normal"},
            "mark_spam": {},
            "archive": {},
            "flag": {},
            "tag": {"tags": ["auto-processed"]}
        }
        return defaults.get(action_type, {})
    
    def get_workflow_analytics(self) -> Dict[str, Any]:
        """Get comprehensive workflow analytics."""
        total_rules = len(self.rules)
        active_rules = len([r for r in self.rules.values() if r.is_active])
        
        # Execution statistics
        recent_executions = self.execution_history[-100:] if len(self.execution_history) >= 100 else self.execution_history
        success_rate = len([e for e in recent_executions if e.success]) / len(recent_executions) if recent_executions else 0
        
        # Rule performance
        rule_performance = {}
        for rule in self.rules.values():
            rule_performance[rule.name] = {
                'execution_count': rule.execution_count,
                'success_rate': rule.success_rate,
                'last_executed': rule.last_executed,
                'performance_score': rule.performance_score
            }
        
        # Top performing rules
        top_rules = sorted(
            self.rules.values(),
            key=lambda r: r.performance_score * r.execution_count,
            reverse=True
        )[:5]
        
        return {
            'total_rules': total_rules,
            'active_rules': active_rules,
            'performance_metrics': self.performance_metrics,
            'recent_success_rate': success_rate,
            'scheduled_actions_pending': len(self.scheduled_actions),
            'rule_performance': rule_performance,
            'top_performing_rules': [
                {
                    'name': rule.name,
                    'execution_count': rule.execution_count,
                    'success_rate': rule.success_rate,
                    'performance_score': rule.performance_score
                } for rule in top_rules
            ],
            'execution_history_size': len(self.execution_history)
        }


# Global workflow engine instance
workflow_engine = WorkflowEngine()