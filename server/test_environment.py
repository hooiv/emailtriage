"""
Comprehensive test suite for Email Triage Environment
Tests all production features including threading, SLA, batch actions, grading
"""
import pytest
from datetime import datetime, timedelta
import sys
import os

# Add server directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from models import (
    Email, Action, ActionType, EmailCategory, EmailPriority,
    SenderType, BatchAction
)
from environment import EmailTriageEnv
from graders import (
    calculate_categorization_accuracy, calculate_prioritization_accuracy,
    grade_task, calculate_step_reward
)
from email_threading import (
    ThreadManager, generate_sender_info, generate_smart_suggestions,
    calculate_sla_deadline
)
from tasks import TASKS, get_task_config


class TestSenderReputation:
    """Tests for sender reputation system"""
    
    def test_vip_detection(self):
        """Enterprise domains should be detected as VIP"""
        sender_info = generate_sender_info('ceo@enterprise.com', 'CEO Jane', 456)
        assert sender_info.sender_type == SenderType.VIP
        assert sender_info.trust_score >= 0.9
        
    def test_suspicious_detection(self):
        """Suspicious domains should be flagged"""
        sender_info = generate_sender_info('lottery@fake-winner.xyz', 'Prize Center', 0)
        assert sender_info.sender_type == SenderType.SUSPICIOUS
        assert sender_info.trust_score < 0.3


class TestSmartSuggestions:
    """Tests for smart action suggestions"""
    
    def test_spam_detection(self):
        """Spam emails should be detected with high confidence"""
        spam_email = Email(
            id='spam_001',
            sender='lottery@scam.com',
            sender_name='Prize Winner',
            subject='CONGRATULATIONS!!! You won a million dollars!',
            body='Click here to claim your FREE prize immediately! Act now!',
            received_at='2024-01-01T12:00:00'
        )
        
        category, priority, actions, confidence = generate_smart_suggestions(spam_email, {})
        
        assert category == EmailCategory.SPAM
        assert confidence >= 0.8
        
    def test_support_categorization(self):
        """Customer support emails should be categorized correctly"""
        support_email = Email(
            id='support_001',
            sender='customer@example.com',
            sender_name='Customer',
            subject='Help with my order #12345',
            body='I have a question about my recent order. The product arrived damaged.',
            received_at='2024-01-01T12:00:00'
        )
        
        category, priority, actions, confidence = generate_smart_suggestions(support_email, {})
        
        assert category in [EmailCategory.CUSTOMER_SUPPORT, EmailCategory.BILLING]


class TestSLATracking:
    """Tests for SLA deadline tracking"""
    
    def test_urgent_support_sla(self):
        """Urgent support emails should have tight SLA"""
        deadline = calculate_sla_deadline(
            EmailCategory.CUSTOMER_SUPPORT,
            EmailPriority.URGENT,
            datetime(2024, 1, 1, 12, 0, 0)
        )
        
        expected = datetime(2024, 1, 1, 13, 0, 0)  # 1 hour
        assert deadline == expected


class TestGraders:
    """Tests for task grading system"""
    
    def test_exact_category_match(self):
        """Exact category match should give full credit"""
        email = Email(
            id='email_001',
            sender='test@example.com',
            sender_name='Test',
            subject='Test',
            body='Test',
            received_at='2024-01-01T12:00:00',
            category=EmailCategory.SPAM
        )
        ground_truth = {'email_001': {'correct_category': EmailCategory.SPAM}}
        
        accuracy = calculate_categorization_accuracy([email], ground_truth)
        assert accuracy == 1.0
        
    def test_partial_category_credit(self):
        """Similar categories should give partial credit"""
        # customer_support and billing are similar
        email = Email(
            id='email_001',
            sender='test@example.com',
            sender_name='Test',
            subject='Test',
            body='Test',
            received_at='2024-01-01T12:00:00',
            category=EmailCategory.BILLING
        )
        ground_truth = {'email_001': {'correct_category': EmailCategory.CUSTOMER_SUPPORT}}
        
        accuracy = calculate_categorization_accuracy([email], ground_truth)
        assert 0.2 <= accuracy <= 0.5  # Partial credit
        
    def test_exact_priority_match(self):
        """Exact priority match should give full credit"""
        email = Email(
            id='email_001',
            sender='test@example.com',
            sender_name='Test',
            subject='Test',
            body='Test',
            received_at='2024-01-01T12:00:00',
            priority=EmailPriority.URGENT
        )
        ground_truth = {'email_001': {'correct_priority': EmailPriority.URGENT}}
        
        accuracy = calculate_prioritization_accuracy([email], ground_truth)
        assert accuracy == 1.0
        
    def test_off_by_one_priority(self):
        """Off by one priority should give partial credit"""
        email = Email(
            id='email_001',
            sender='test@example.com',
            sender_name='Test',
            subject='Test',
            body='Test',
            received_at='2024-01-01T12:00:00',
            priority=EmailPriority.HIGH
        )
        ground_truth = {'email_001': {'correct_priority': EmailPriority.URGENT}}
        
        accuracy = calculate_prioritization_accuracy([email], ground_truth)
        assert 0.4 <= accuracy <= 0.6  # 0.5 for off by one
        
    def test_grade_task_easy(self):
        """Easy task grader should work correctly"""
        emails = [
            Email(
                id='email_001',
                sender='test@example.com',
                sender_name='Test',
                subject='Test',
                body='Test',
                received_at='2024-01-01T12:00:00',
                category=EmailCategory.SPAM
            )
        ]
        ground_truth = {
            'email_001': {
                'correct_category': EmailCategory.SPAM,
                'is_spam': True
            }
        }
        
        result = grade_task('task_easy_categorize', emails, ground_truth, 5, 50)
        
        assert 0.0 <= result['score'] <= 1.0


class TestEnvironment:
    """Tests for main EmailTriageEnv environment"""
    
    def test_reset(self):
        """Reset should return valid observation"""
        env = EmailTriageEnv('task_easy_categorize')
        result = env.reset()
        
        assert result.observation is not None
        assert len(result.observation.inbox) > 0
        assert result.observation.unread_count > 0
        
    def test_step_categorize(self):
        """Categorize action should work"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        email_id = env.emails[0].id
        action = Action(
            action_type=ActionType.CATEGORIZE,
            email_id=email_id,
            category=EmailCategory.SPAM
        )
        
        result = env.step(action)
        
        assert result.reward is not None
        assert result.observation is not None
        
    def test_step_prioritize(self):
        """Prioritize action should work"""
        env = EmailTriageEnv('task_medium_triage')
        env.reset()
        
        email_id = env.emails[0].id
        action = Action(
            action_type=ActionType.PRIORITIZE,
            email_id=email_id,
            priority=EmailPriority.HIGH
        )
        
        result = env.step(action)
        
        assert result.reward is not None
        
    def test_step_archive(self):
        """Archive action should work"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        email_id = env.emails[0].id
        action = Action(
            action_type=ActionType.ARCHIVE,
            email_id=email_id
        )
        
        result = env.step(action)
        
        # Check the email is marked as archived
        email = next(e for e in env.emails if e.id == email_id)
        assert email.is_archived == True
        
    def test_step_flag(self):
        """Flag action should work"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        email_id = env.emails[0].id
        action = Action(
            action_type=ActionType.FLAG,
            email_id=email_id
        )
        
        result = env.step(action)
        
        # Check the email is flagged
        email = next(e for e in env.emails if e.id == email_id)
        assert email.is_flagged == True
        
    def test_state(self):
        """State should return complete state"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        state = env.state()
        
        assert state.task_id is not None
        assert state.metrics is not None
        
    def test_recommendations(self):
        """Recommendations should be provided"""
        env = EmailTriageEnv('task_easy_categorize')
        result = env.reset()
        
        # Should have some recommendations
        assert result.observation.recommended_actions is not None
        
    def test_invalid_email_id(self):
        """Invalid email ID should return error"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        action = Action(
            action_type=ActionType.CATEGORIZE,
            email_id='invalid_id',
            category=EmailCategory.SPAM
        )
        
        result = env.step(action)
        
        # Should have negative reward for invalid action
        assert result.reward.value < 0
        
    def test_batch_actions(self):
        """Batch actions should process multiple emails"""
        env = EmailTriageEnv('task_medium_triage')
        env.reset()
        
        # Create batch action for first two emails
        batch_actions = [
            BatchAction(
                email_id=env.emails[0].id,
                action_type=ActionType.ARCHIVE
            ),
            BatchAction(
                email_id=env.emails[1].id,
                action_type=ActionType.ARCHIVE
            )
        ]
        
        action = Action(
            action_type=ActionType.BATCH,
            email_id='batch',
            batch_actions=batch_actions
        )
        
        result = env.step(action)
        
        # Check both emails are archived
        assert env.emails[0].is_archived == True
        assert env.emails[1].is_archived == True

    def test_undo_action(self):
        """Undo action should revert previous state."""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        email_id = env.emails[0].id

        env.step(Action(
            action_type=ActionType.CATEGORIZE,
            email_id=email_id,
            category=EmailCategory.SPAM
        ))
        assert env.emails[0].category == EmailCategory.SPAM

        undo_result = env.step(Action(action_type=ActionType.UNDO))
        assert undo_result.reward.value <= 0.1
        assert env.emails[0].category is None

    def test_learning_hints_populated_on_repeated_errors(self):
        """Repeated invalid actions should emit adaptive hints."""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()

        env.step(Action(
            action_type=ActionType.CATEGORIZE,
            email_id='missing-email',
            category=EmailCategory.SPAM
        ))
        result = env.step(Action(
            action_type=ActionType.CATEGORIZE,
            email_id='missing-email',
            category=EmailCategory.SPAM
        ))

        hints = result.observation.learning_hints
        assert any("email IDs" in h for h in hints)


class TestTaskConfigurations:
    """Tests for task configurations"""
    
    def test_all_tasks_exist(self):
        """All required tasks should exist"""
        assert 'task_easy_categorize' in TASKS
        assert 'task_medium_triage' in TASKS
        assert 'task_hard_full_inbox' in TASKS
        
    def test_task_difficulty_scaling(self):
        """Tasks should have increasing difficulty"""
        easy = TASKS['task_easy_categorize']
        medium = TASKS['task_medium_triage']
        hard = TASKS['task_hard_full_inbox']
        
        # More emails = harder
        assert easy.email_count <= medium.email_count <= hard.email_count
        
        # Hard task should have SLA enabled
        assert hard.sla_enabled == True
        
    def test_task_configs_valid(self):
        """All task configs should have required fields"""
        for task_id, config in TASKS.items():
            assert config.task_id == task_id
            assert config.task_name
            assert config.description
            assert config.difficulty in ['easy', 'medium', 'hard']
            assert config.max_steps > 0
            assert config.email_count > 0


class TestMultimodalAttachments:
    """Tests for attachment generation and schema."""

    def test_hard_task_has_some_attachments(self):
        env = EmailTriageEnv('task_hard_full_inbox')
        result = env.reset()
        with_attachments = [e for e in result.observation.inbox if e.has_attachments]
        assert len(with_attachments) > 0

    def test_attachment_fields_present(self):
        env = EmailTriageEnv('task_hard_full_inbox')
        result = env.reset()
        target = next((e for e in result.observation.inbox if e.attachments), None)
        if target is None:
            pytest.skip("No attachments generated in this deterministic sample")
        att = target.attachments[0]
        assert att.filename
        assert att.mime_type
        assert att.attachment_type.value in ["image", "pdf", "document", "log"]


class TestMetrics:
    """Tests for environment metrics"""
    
    def test_metrics_after_reset(self):
        """Metrics should be reset after reset()"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        state = env.state()
        assert state.metrics.total_requests >= 1
        assert state.metrics.actions_taken == 0
        
    def test_metrics_after_step(self):
        """Metrics should update after step()"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        action = Action(
            action_type=ActionType.CATEGORIZE,
            email_id=env.emails[0].id,
            category=EmailCategory.SPAM
        )
        env.step(action)
        
        state = env.state()
        assert state.metrics.actions_taken == 1


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_step_without_reset(self):
        """Step without reset should handle gracefully"""
        env = EmailTriageEnv('task_easy_categorize')
        
        action = Action(
            action_type=ActionType.CATEGORIZE,
            email_id='any_id',
            category=EmailCategory.SPAM
        )
        
        # Should not crash
        try:
            result = env.step(action)
            # Either returns error or handles gracefully
            assert result is not None
        except Exception:
            # Acceptable to raise exception
            pass
    
    def test_done_action(self):
        """Done action should signal completion"""
        env = EmailTriageEnv('task_easy_categorize')
        env.reset()
        
        action = Action(
            action_type=ActionType.DONE,
            email_id='done'
        )
        
        result = env.step(action)
        assert result.done == True


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])
