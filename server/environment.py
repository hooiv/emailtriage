"""Email Triage OpenEnv Environment implementation."""

import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from copy import deepcopy

from models import (
    Action, ActionType, Email, EmailCategory, EmailPriority,
    Observation, Reward, State, StepResult, ResetResult,
    ThreadSummary, EnvironmentMetrics, BatchAction
)
from tasks import get_task_emails, get_task_config, list_tasks, TASKS
from graders import grade_task, calculate_step_reward
from email_threading import (
    ThreadManager, enrich_email_with_metadata, generate_smart_suggestions,
    calculate_importance_score, CANNED_RESPONSES
)


class EmailTriageEnv:
    """
    Email Triage Environment.
    
    An OpenEnv-compliant environment where an AI agent must triage,
    categorize, prioritize, and take actions on incoming emails.
    
    Features:
    - Email conversation threading
    - SLA tracking with time-based urgency
    - Sender reputation system
    - Smart action suggestions
    - Batch action processing
    - Comprehensive metrics
    """
    
    def __init__(self, task_id: str = "task_easy_categorize"):
        """Initialize the environment with a specific task."""
        self.task_id = task_id
        self.task_config = get_task_config(task_id)
        self.emails: List[Email] = []
        self.ground_truth: Dict[str, Dict[str, Any]] = {}
        self.step_count = 0
        self.done = False
        self.total_reward = 0.0
        self.action_history: List[Dict[str, Any]] = []
        self.current_email_id: Optional[str] = None
        self._initialized = False
        
        # New production features
        self.thread_manager = ThreadManager()
        self.metrics = EnvironmentMetrics()
        self._start_time = time.time()
        self._request_times: List[float] = []
        self._undo_stack: List[Dict[str, Any]] = []
        self._learning_hints: List[str] = []
        self._mistake_counters: Dict[str, int] = {}
        
        # Additional advanced features
        self.audit_log: List[Dict[str, Any]] = []  # Audit trail
        self.saved_filters: Dict[str, Dict[str, Any]] = {}  # User-defined filters
        self.tags_used: set = set()  # Track all tags used
        
        # ML and advanced processing
        from ml_pipeline import ml_pipeline
        from security_scanner import security_scanner
        from workflow_engine import workflow_engine
        from collaborative_ai import agent_orchestrator
        from predictive_engine import predictive_engine
        from autonomous_manager import create_autonomous_manager
        from event_streaming import event_stream_manager, EventType
        from analytics_dashboard import create_analytics_engine
        
        self.ml_pipeline = ml_pipeline
        self.security_scanner = security_scanner
        self.workflow_engine = workflow_engine
        self.agent_orchestrator = agent_orchestrator
        self.predictive_engine = predictive_engine
        self.event_stream_manager = event_stream_manager
        
        # Initialize analytics engine
        self.analytics_engine = create_analytics_engine(self)
        
        # Initialize autonomous management system
        self.autonomous_manager = create_autonomous_manager(self)
        
        # Initialize blockchain audit trail
        from blockchain_audit import get_blockchain_audit
        self.blockchain_audit = get_blockchain_audit()
        
        # Initialize monitoring system
        from monitoring_system import create_monitoring_system
        self.monitoring_system = create_monitoring_system(self)
        
        # Initialize performance optimizer
        from performance_optimizer import create_performance_optimizer
        self.performance_optimizer = create_performance_optimizer(self)
        
        # Initialize intelligent priority queue
        from priority_queue import create_priority_queue
        self.priority_queue = create_priority_queue(self)

        # Initialize knowledge graph and explainable AI
        from knowledge_graph import KnowledgeGraph
        self.knowledge_graph = KnowledgeGraph()
        
        # Initialize intelligent response generator
        from response_generator import ResponseGenerator
        self.response_generator = ResponseGenerator()

        # Advanced features state
        self.ml_predictions: Dict[str, Any] = {}
        self.security_scans: Dict[str, Any] = {}
        self.workflow_automations: List[Dict[str, Any]] = []
        self.agent_consensus: Dict[str, Any] = {}  # Multi-agent consensus results
        self.autonomous_decisions: List[Dict[str, Any]] = []  # Autonomous system decisions
        self.workflow_executions: List[Any] = []
        from models import EmailRule, TeamMember, PerformanceMetrics
        self.email_rules: List[EmailRule] = []  # AI-generated automation rules
        self.performance_metrics = PerformanceMetrics()  # Real-time performance tracking
        self.team_members: Dict[str, TeamMember] = {}  # Team collaboration
    
    async def _initialize_event_streaming(self):
        """Initialize event streaming system asynchronously."""
        try:
            await self.event_stream_manager.start_processing()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to initialize event streaming: {e}")
    
    def _start_event_streaming_sync(self):
        """Start event streaming in sync context."""
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.event_stream_manager.start_processing())
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to start event streaming sync: {e}")
    
    def _publish_event(self, event_type: str, data: Dict[str, Any]):
        """Publish an event to the event stream."""
        try:
            self.event_stream_manager.publish_event(
                event_type=EventType(event_type),
                source="email_environment",
                data=data,
                organization_id="default"  # Could be made configurable
            )
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Failed to publish event: {e}")  # Don't fail the main operation
    
    def _collect_metrics(self):
        """Collect and update analytics metrics."""
        try:
            self.analytics_engine.collect_metrics()
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Failed to collect metrics: {e}")  # Don't fail the main operation
    
    def reset(self, task_id: Optional[str] = None) -> ResetResult:
        """
        Reset the environment to initial state.
        
        Args:
            task_id: Optional task ID to switch to a different task
            
        Returns:
            ResetResult with initial observation
        """
        request_start = time.time()
        
        if task_id:
            self.task_id = task_id
            self.task_config = get_task_config(task_id)
        
        # Generate fresh emails for the task
        self.emails, self.ground_truth = get_task_emails(self.task_id)
        self.thread_manager = ThreadManager()
        
        # Publish task reset event
        self._publish_event("email_received", {
            "task_id": self.task_id,
            "email_count": len(self.emails),
            "task_name": self.task_config.task_name if hasattr(self, 'task_config') else 'unknown'
        })
        
        # Enrich emails with metadata
        current_time = datetime.now()
        for email in self.emails:
            enrich_email_with_metadata(email, self.ground_truth, current_time)
            
            # Calculate importance score
            email.importance_score = calculate_importance_score(email)
            
            # Run ML predictions
            ml_result = self.ml_pipeline.process_email(email)
            self.ml_predictions[email.id] = ml_result
            
            # Publish email received event
            self._publish_event("email_received", {
                "email_id": email.id,
                "sender": email.sender,
                "subject": email.subject[:100],  # Truncate for privacy
                "category": email.category.value if email.category else None,
                "priority": email.priority.value if email.priority else None,
                "importance_score": email.importance_score
            })
            
            # Apply ML predictions to email
            if ml_result['category_prediction']['confidence'] > 0.8:
                email.suggested_category = ml_result['category_prediction']['category']
                email.confidence_score = ml_result['category_prediction']['confidence']
            
            # Run security scan
            security_result = self.security_scanner.scan_email(email)
            self.security_scans[email.id] = security_result
            
            # Apply security flags
            if security_result.risk_score > 0.7:
                email.tags = email.tags or []
                email.tags.append("high_risk")
            
            self.thread_manager.add_email(email)
            
            # Trigger workflow automation
            from workflow_engine import TriggerType
            workflow_executions = self.workflow_engine.trigger_workflow(
                trigger_type=TriggerType.EMAIL_RECEIVED,
                email=email,
                context={'task_id': self.task_id}
            )
            self.workflow_executions.extend(workflow_executions)
        
        # Record audit log entry
        self._add_audit_log("reset", None, {
            "task_id": self.task_id, 
            "email_count": len(self.emails)
        })
        
        self.step_count = 0
        self.done = False
        self.total_reward = 0.0
        self.action_history = []
        self.current_email_id = self.emails[0].id if self.emails else None
        self._initialized = True
        
        # Reset metrics
        self.metrics = EnvironmentMetrics()
        self._start_time = time.time()
        self._request_times = []
        self._undo_stack = []
        self._learning_hints = []
        self._mistake_counters = {}
        
        # Track request time
        self._track_request_time(request_start)
        self.metrics.total_requests += 1
        
        observation = self._get_observation()
        
        return ResetResult(
            observation=observation,
            info={
                "task_id": self.task_id,
                "task_name": self.task_config.task_name,
                "difficulty": self.task_config.difficulty,
                "email_count": len(self.emails),
                "thread_count": len(self.thread_manager.threads),
                "max_steps": self.task_config.max_steps,
                "sla_enabled": getattr(self.task_config, 'sla_enabled', False)
            }
        )
    
    def _add_audit_log(self, action_type: str, email_id: Optional[str] = None, 
                      details: Optional[Dict[str, Any]] = None, success: bool = True, 
                      error: Optional[str] = None):
        """Add entry to audit log and blockchain trail."""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "action_type": action_type,
            "email_id": email_id,
            "details": details or {},
            "success": success,
            "error_message": error,
            "step_count": self.step_count
        }
        self.audit_log.append(entry)
        
        # Keep last 1000 entries to prevent memory issues
        if len(self.audit_log) > 1000:
            self.audit_log = self.audit_log[-1000:]
        
        # Add to blockchain audit trail
        try:
            from blockchain_audit import AuditEventType
            
            # Map action types to audit event types
            event_type_map = {
                "reset": AuditEventType.SYSTEM_START,
                "categorize": AuditEventType.EMAIL_CATEGORIZED,
                "prioritize": AuditEventType.EMAIL_PRIORITIZED,
                "archive": AuditEventType.EMAIL_ARCHIVED,
                "flag": AuditEventType.EMAIL_FLAGGED,
                "reply": AuditEventType.EMAIL_REPLIED,
                "forward": AuditEventType.EMAIL_FORWARDED,
                "mark_spam": AuditEventType.EMAIL_DELETED,
                "tag": AuditEventType.EMAIL_ACCESSED,
                "ai_decision": AuditEventType.AI_DECISION,
                "security_alert": AuditEventType.SECURITY_ALERT
            }
            
            event_type = event_type_map.get(action_type, AuditEventType.EMAIL_ACCESSED)
            
            self.blockchain_audit.add_record(
                event_type=event_type,
                actor="system",
                action_details={
                    "action": action_type,
                    "success": success,
                    **(details or {})
                },
                resource_id=email_id,
                metadata={"step_count": self.step_count}
            )
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.debug(f"Blockchain audit failed: {e}")
    
    def step(self, action: Action) -> StepResult:
        """
        Execute an action in the environment.
        
        Args:
            action: The action to execute
            
        Returns:
            StepResult with observation, reward, done flag, and info
        """
        request_start = time.time()
        
        if not self._initialized:
            raise RuntimeError("Environment not initialized. Call reset() first.")
        
        if self.done:
            raise RuntimeError("Episode is done. Call reset() to start a new episode.")

        if action.action_type != ActionType.UNDO:
            self._push_undo_snapshot()

        self.step_count += 1
        self.metrics.total_requests += 1
        self.metrics.actions_taken += 1
        
        # Store previous state for reward calculation
        previous_state = {
            "emails": deepcopy(self.emails),
            "step_count": self.step_count - 1
        }
        
        # Handle batch actions
        if action.action_type == ActionType.BATCH and action.batch_actions:
            action_result = self._execute_batch_actions(action.batch_actions)
        else:
            # Execute single action
            action_result = self._execute_action(action)
        
        # Calculate reward
        reward = calculate_step_reward(
            action_type=action.action_type.value,
            action_result=action_result,
            emails=self.emails,
            ground_truth=self.ground_truth,
            previous_state=previous_state
        )
        
        self.total_reward += reward.value

        self._update_learning_hints(action, action_result)
        
        # Record action in history
        self.action_history.append({
            "step": self.step_count,
            "action": action.model_dump(mode="json"),
            "result": action_result,
            "reward": reward.value,
            "timestamp": datetime.now().isoformat()
        })
        
        # Check if episode is done
        if action.action_type == ActionType.DONE:
            self.done = True
        elif self.step_count >= self.task_config.max_steps:
            self.done = True
        
        # Get final grading if done
        info: Dict[str, Any] = {"action_result": action_result}
        if self.done:
            grade_result = grade_task(
                self.task_id,
                self.emails,
                self.ground_truth,
                self.step_count,
                self.task_config.max_steps
            )
            info["final_grade"] = grade_result
            info["final_score"] = grade_result["score"]
            info["metrics"] = self.metrics.model_dump()
        
        # Track request time
        self._track_request_time(request_start)
        
        # Collect analytics metrics
        self._collect_metrics()
        
        observation = self._get_observation(
            last_action_result=action_result.get("message"),
            last_action_error=action_result.get("error")
        )
        
        return StepResult(
            observation=observation,
            reward=reward,
            done=self.done,
            info=info
        )
    
    def state(self) -> State:
        """
        Get the full current state of the environment.
        
        Returns:
            State object with complete environment state
        """
        return State(
            task_id=self.task_id,
            task_name=self.task_config.task_name,
            step_count=self.step_count,
            max_steps=self.task_config.max_steps,
            done=self.done,
            total_reward=self.total_reward,
            inbox=self.emails,
            threads=self.thread_manager.get_all_summaries(),
            action_history=self.action_history,
            ground_truth=self.ground_truth,
            metrics=self.metrics
        )
    
    def _get_observation(
        self,
        last_action_result: Optional[str] = None,
        last_action_error: Optional[str] = None
    ) -> Observation:
        """Generate observation from current state."""
        # Calculate counts
        unread_count = sum(1 for e in self.emails if not e.is_read)
        urgent_count = sum(1 for e in self.emails if e.priority == EmailPriority.URGENT)
        pending_replies = sum(
            1 for e in self.emails 
            if e.category == EmailCategory.CUSTOMER_SUPPORT and not e.reply_sent
        )
        
        # Check SLA at risk
        sla_at_risk = 0
        current_time = datetime.now()
        for email in self.emails:
            if email.sla_deadline:
                try:
                    deadline = datetime.fromisoformat(email.sla_deadline)
                    hours_remaining = (deadline - current_time).total_seconds() / 3600
                    if 0 < hours_remaining < 2:  # Less than 2 hours remaining
                        sla_at_risk += 1
                except (ValueError, TypeError):
                    pass
        
        # Generate recommended actions
        recommended_actions = self._generate_recommendations()
        
        return Observation(
            inbox=self.emails,
            threads=self.thread_manager.get_all_summaries(),
            current_email_id=self.current_email_id,
            step_count=self.step_count,
            max_steps=self.task_config.max_steps,
            task_description=self.task_config.description,
            available_actions=[a.value for a in ActionType],
            last_action_result=last_action_result,
            last_action_error=last_action_error,
            unread_count=unread_count,
            urgent_count=urgent_count,
            sla_at_risk_count=sla_at_risk,
            pending_replies=pending_replies,
            recommended_actions=recommended_actions,
            learning_hints=self._learning_hints[-5:],
            metrics=self.metrics
        )
    
    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """Generate smart action recommendations."""
        recommendations = []
        
        # Find unprocessed emails
        unprocessed = [e for e in self.emails if e.category is None]
        
        # Prioritize by suggested confidence and urgency
        for email in unprocessed[:3]:
            if email.suggested_category and email.confidence_score > 0.5:
                recommendations.append({
                    "email_id": email.id,
                    "suggested_action": "categorize",
                    "suggested_category": email.suggested_category.value,
                    "confidence": email.confidence_score,
                    "reason": f"High confidence ({email.confidence_score:.0%}) match for {email.suggested_category.value}"
                })
        
        # Find emails needing replies
        needs_reply = [
            e for e in self.emails 
            if e.category == EmailCategory.CUSTOMER_SUPPORT 
            and not e.reply_sent
            and e.sender_info and e.sender_info.sender_type.value in ["vip", "known"]
        ]
        
        for email in needs_reply[:2]:
            recommendations.append({
                "email_id": email.id,
                "suggested_action": "reply",
                "reason": f"VIP/Known sender awaiting response",
                "priority": "high"
            })
        
        # Find spam to mark
        likely_spam = [
            e for e in self.emails
            if e.suggested_category == EmailCategory.SPAM
            and not e.is_spam
            and e.confidence_score > 0.7
        ]
        
        for email in likely_spam[:2]:
            recommendations.append({
                "email_id": email.id,
                "suggested_action": "mark_spam",
                "confidence": email.confidence_score,
                "reason": "High confidence spam detection"
            })
        
        return recommendations[:5]  # Return top 5 recommendations
    
    def _execute_batch_actions(self, batch_actions: List[BatchAction]) -> Dict[str, Any]:
        """Execute multiple actions in a single step."""
        results = []
        success_count = 0
        
        for batch_action in batch_actions:
            # Convert to regular Action
            action = Action(
                action_type=batch_action.action_type,
                email_id=batch_action.email_id,
                category=batch_action.category,
                priority=batch_action.priority,
                reply_content=batch_action.reply_content,
                forward_to=batch_action.forward_to
            )
            
            result = self._execute_action(action)
            results.append(result)
            
            if result.get("success"):
                success_count += 1
        
        return {
            "action_type": "batch",
            "success": success_count > 0,
            "message": f"Batch processed {success_count}/{len(batch_actions)} actions successfully",
            "results": results,
            "error": None if success_count > 0 else "All batch actions failed"
        }
    
    def _execute_action(self, action: Action) -> Dict[str, Any]:
        """Execute an action and return the result."""
        result = {
            "action_type": action.action_type.value,
            "email_id": action.email_id,
            "success": False,
            "message": "",
            "error": None
        }
        
        # Handle DONE action
        if action.action_type == ActionType.DONE:
            result["success"] = True
            result["message"] = "Episode ended by agent"
            return result

        if action.action_type == ActionType.UNDO:
            return self._undo_last_action()
        
        # Handle BATCH action (should not reach here normally)
        if action.action_type == ActionType.BATCH:
            result["error"] = "Use batch_actions field for batch processing"
            return result
        
        # Validate email_id for actions that require it
        if action.action_type not in [ActionType.DONE, ActionType.BATCH, ActionType.UNDO] and not action.email_id:
            result["error"] = "email_id is required for this action"
            return result
        
        email = self._get_email(action.email_id)
        if not email:
            result["error"] = f"Email not found: {action.email_id}"
            return result
        
        # Mark email as read
        email.is_read = True
        self.current_email_id = action.email_id
        
        # Run collaborative AI analysis if not already done
        if action.email_id not in self.agent_consensus:
            try:
                context = {
                    'current_step': self.step_count,
                    'total_emails': len(self.emails),
                    'user_history': self.action_history[-5:] if self.action_history else []
                }
                
                consensus_result = self.agent_orchestrator.process_email(email, context)
                self.agent_consensus[action.email_id] = consensus_result
                
                # Publish AI consensus event
                self._publish_event("ai_consensus_reached", {
                    "email_id": action.email_id,
                    "consensus_results": len(consensus_result.get('consensus_results', {})),
                    "quality_validation": consensus_result.get('quality_validation', 'unknown'),
                    "processing_time_ms": consensus_result.get('processing_time_ms', 0),
                    "agent_count": consensus_result.get('agent_count', 0)
                })
                
                # Store insights for potential recommendations
                if consensus_result.get('consensus_results'):
                    email_insights = {
                        'ai_category': consensus_result['consensus_results'].get('category'),
                        'ai_priority': consensus_result['consensus_results'].get('priority'),
                        'ai_security_action': consensus_result['consensus_results'].get('security_action'),
                        'confidence_scores': {
                            k: v.confidence if hasattr(v, 'confidence') else 0.5
                            for k, v in consensus_result['consensus_results'].items()
                        },
                        'processing_time': consensus_result.get('processing_time_ms', 0)
                    }
                    
                    # Add to ML pipeline for learning
                    self.ml_pipeline.add_training_data(email, email_insights, 1.0)
                    
                    # Add to predictive analytics
                    self.predictive_engine.add_email_data_point(email, {
                        'response_time_ms': consensus_result.get('processing_time_ms', 0),
                        'ai_consensus_confidence': max(email_insights['confidence_scores'].values()) if email_insights['confidence_scores'] else 0.5
                    })
                    
                    # Trigger autonomous processing if enabled
                    if hasattr(self, 'autonomous_manager') and self.autonomous_manager.autonomy_enabled:
                        autonomous_decision = self.autonomous_manager.process_email_autonomously(email)
                        self.autonomous_decisions.append({
                            'email_id': email.id,
                            'decision': autonomous_decision.decision_type,
                            'confidence': autonomous_decision.confidence,
                            'executed': autonomous_decision.executed,
                            'timestamp': autonomous_decision.created_at
                        })
                    
            except Exception as e:
                # Don't fail the action if AI processing fails
                self.agent_consensus[action.email_id] = {
                    'error': str(e),
                    'consensus_results': {},
                    'processing_time_ms': 0
                }
        
        # Execute specific action
        if action.action_type == ActionType.CATEGORIZE:
            if not action.category:
                result["error"] = "category is required for categorize action"
                return result
            email.category = action.category
            result["success"] = True
            result["message"] = f"Email categorized as {action.category.value}"
            self.metrics.emails_processed += 1
            
            # Publish categorization event
            self._publish_event("email_categorized", {
                "email_id": action.email_id,
                "category": action.category.value,
                "sender": email.sender,
                "confidence": self.agent_consensus.get(action.email_id, {}).get('consensus_results', {}).get('category', {}).confidence if hasattr(self.agent_consensus.get(action.email_id, {}).get('consensus_results', {}).get('category', {}), 'confidence') else None
            })
        
        elif action.action_type == ActionType.PRIORITIZE:
            if not action.priority:
                result["error"] = "priority is required for prioritize action"
                return result
            email.priority = action.priority
            result["success"] = True
            result["message"] = f"Email priority set to {action.priority.value}"
        
        elif action.action_type == ActionType.REPLY:
            if not action.reply_content:
                result["error"] = "reply_content is required for reply action"
                return result
            email.reply_sent = action.reply_content
            result["success"] = True
            result["message"] = "Reply sent"
            
            # Check SLA compliance
            if email.sla_deadline:
                try:
                    deadline = datetime.fromisoformat(email.sla_deadline)
                    if datetime.now() > deadline:
                        self.metrics.sla_violations += 1
                        result["message"] += " (SLA violated)"
                except (ValueError, TypeError):
                    pass
        
        elif action.action_type == ActionType.FORWARD:
            if not action.forward_to:
                result["error"] = "forward_to is required for forward action"
                return result
            email.forwarded_to = action.forward_to
            result["success"] = True
            result["message"] = f"Email forwarded to {action.forward_to}"
        
        elif action.action_type == ActionType.ARCHIVE:
            email.is_archived = True
            result["success"] = True
            result["message"] = "Email archived"
        
        elif action.action_type == ActionType.FLAG:
            email.is_flagged = True
            result["success"] = True
            result["message"] = "Email flagged"
        
        elif action.action_type == ActionType.MARK_SPAM:
            email.is_spam = True
            email.category = EmailCategory.SPAM
            result["success"] = True
            result["message"] = "Email marked as spam"
            self.metrics.spam_detected += 1
        
        elif action.action_type == ActionType.SNOOZE:
            if not action.snooze_hours:
                action.snooze_hours = 1  # Default 1 hour
            email.is_snoozed = True
            snooze_until = datetime.now() + timedelta(hours=action.snooze_hours)
            email.snooze_until = snooze_until.isoformat()
            result["success"] = True
            result["message"] = f"Email snoozed for {action.snooze_hours} hour(s)"
        
        elif action.action_type == ActionType.TAG:
            if not action.tags:
                result["error"] = "tags list is required for tag action"
                return result
            
            # Add tags to email (merge with existing tags)
            existing_tags = set(email.tags or [])
            new_tags = set(action.tags)
            email.tags = list(existing_tags.union(new_tags))
            
            # Track all tags used
            self.tags_used.update(new_tags)
            
            result["success"] = True
            result["message"] = f"Tags added: {', '.join(action.tags)}"
            
            # Add audit log entry
            self._add_audit_log("tag", action.email_id, {
                "tags_added": action.tags,
                "total_tags": len(email.tags)
            })
        
        else:
            result["error"] = f"Unknown action type: {action.action_type}"
            
        return result
    
    def _get_email(self, email_id: str) -> Optional[Email]:
        """Get email by ID."""
        for email in self.emails:
            if email.id == email_id:
                return email
        return None
    
    def _track_request_time(self, start_time: float) -> None:
        """Track request timing for metrics and monitoring."""
        elapsed_ms = (time.time() - start_time) * 1000
        self._request_times.append(elapsed_ms)
        self.metrics.avg_response_time_ms = sum(self._request_times) / len(self._request_times)
        
        # Record to monitoring system
        try:
            self.monitoring_system.record_metric("response_time_ms", elapsed_ms)
        except Exception:
            pass  # Don't fail main operation
        
        # Record to performance optimizer
        try:
            self.performance_optimizer.record_latency(elapsed_ms)
            self.performance_optimizer.record_request(success=True)
        except Exception:
            pass  # Don't fail main operation

    def _push_undo_snapshot(self) -> None:
        """Store a snapshot used by undo action."""
        snapshot = {
            "emails": deepcopy(self.emails),
            "current_email_id": self.current_email_id,
            "total_reward": self.total_reward,
        }
        self._undo_stack.append(snapshot)
        if len(self._undo_stack) > 100:
            self._undo_stack = self._undo_stack[-100:]

    def _undo_last_action(self) -> Dict[str, Any]:
        """Undo the most recent non-undo action."""
        if not self._undo_stack:
            return {
                "action_type": "undo",
                "email_id": None,
                "success": False,
                "message": "",
                "error": "No previous action to undo"
            }

        snapshot = self._undo_stack.pop()
        self.emails = snapshot["emails"]
        self.current_email_id = snapshot["current_email_id"]
        self.total_reward = snapshot["total_reward"]
        self.done = False

        undone_action = None
        for idx in range(len(self.action_history) - 1, -1, -1):
            prior = self.action_history[idx].get("action", {})
            prior_type = prior.get("action_type")
            if prior_type != ActionType.UNDO.value:
                undone_action = self.action_history.pop(idx)
                break

        self.thread_manager = ThreadManager()
        for email in self.emails:
            self.thread_manager.add_email(email)
        self._refresh_metrics_from_state()

        undone_type = (
            undone_action.get("action", {}).get("action_type", "unknown")
            if undone_action else "unknown"
        )
        return {
            "action_type": "undo",
            "email_id": None,
            "success": True,
            "message": f"Undid previous action ({undone_type})",
            "error": None
        }

    def _refresh_metrics_from_state(self) -> None:
        """Recompute derived metrics after state restoration."""
        self.metrics.emails_processed = sum(1 for e in self.emails if e.category is not None)
        self.metrics.spam_detected = sum(1 for e in self.emails if e.is_spam)
        self.metrics.threads_resolved = sum(
            1 for t in self.thread_manager.get_all_summaries() if t.is_resolved
        )
        self.metrics.actions_taken = len(self.action_history)

    def _add_learning_hint(self, hint: str) -> None:
        """Add a deduplicated learning hint."""
        if hint not in self._learning_hints:
            self._learning_hints.append(hint)
        if len(self._learning_hints) > 10:
            self._learning_hints = self._learning_hints[-10:]

    def _update_learning_hints(self, action: Action, action_result: Dict[str, Any]) -> None:
        """Adaptively generate hints based on repeated mistakes."""
        error = action_result.get("error")
        if error:
            self._mistake_counters[error] = self._mistake_counters.get(error, 0) + 1

            if "Email not found" in error and self._mistake_counters[error] >= 2:
                available = ", ".join(e.id for e in self.emails[:5])
                self._add_learning_hint(
                    f"Use valid email IDs from observation.inbox (examples: {available})."
                )
            elif "category is required" in error and self._mistake_counters[error] >= 1:
                self._add_learning_hint(
                    "For categorize actions, include `category` (e.g. spam, billing, technical)."
                )
            elif "priority is required" in error and self._mistake_counters[error] >= 1:
                self._add_learning_hint(
                    "For prioritize actions, include `priority` (urgent/high/normal/low)."
                )
            elif "reply_content is required" in error and self._mistake_counters[error] >= 1:
                self._add_learning_hint(
                    "For reply actions, include professional `reply_content` text."
                )
            elif "forward_to is required" in error and self._mistake_counters[error] >= 1:
                self._add_learning_hint(
                    "For forward actions, include a destination email in `forward_to`."
                )
            elif "No previous action to undo" in error:
                self._add_learning_hint(
                    "Undo works only after at least one successful non-undo action."
                )

        if action.action_type == ActionType.CATEGORIZE and action_result.get("success") and action.email_id:
            email = self._get_email(action.email_id)
            expected = self.ground_truth.get(action.email_id, {}).get("correct_category")
            if email and expected and email.category != expected:
                key = "wrong_category"
                self._mistake_counters[key] = self._mistake_counters.get(key, 0) + 1
                if self._mistake_counters[key] >= 2:
                    self._add_learning_hint(
                        "When uncertain, use `suggested_category` and `confidence_score` from each email."
                    )

        if action.action_type == ActionType.PRIORITIZE and action_result.get("success") and action.email_id:
            email = self._get_email(action.email_id)
            expected = self.ground_truth.get(action.email_id, {}).get("correct_priority")
            if email and expected and email.priority != expected:
                key = "wrong_priority"
                self._mistake_counters[key] = self._mistake_counters.get(key, 0) + 1
                if self._mistake_counters[key] >= 2:
                    self._add_learning_hint(
                        "Use urgency cues (critical/asap/production down) and sentiment to set priority."
                    )

        if self.step_count > int(self.task_config.max_steps * 0.7):
            self._add_learning_hint(
                "You are near the step limit; use batch actions for repetitive archive/spam operations."
            )
    
    def get_available_tasks(self) -> List[Dict[str, Any]]:
        """Get list of available tasks."""
        return [
            {
                "task_id": t.task_id,
                "task_name": t.task_name,
                "description": t.description,
                "difficulty": t.difficulty,
                "max_steps": t.max_steps,
                "email_count": t.email_count,
                "thread_count": getattr(t, 'thread_count', 0),
                "sla_enabled": getattr(t, 'sla_enabled', False)
            }
            for t in list_tasks()
        ]


# Import for timedelta
from datetime import timedelta
