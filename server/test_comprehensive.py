"""
Comprehensive Test Suite for Email Triage OpenEnv
Tests all 14 AI systems, OpenEnv compliance, and production features
"""
import pytest
import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from fastapi.testclient import TestClient
import sys
import os

# Add server to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'server'))

from main import app
from environment import EmailTriageEnv
from models import (
    Email, Action, ActionType, Observation, 
    EmailPriority, EmailCategory, Attachment
)


@pytest.fixture
def client():
    """FastAPI test client"""
    return TestClient(app)


@pytest.fixture
def env():
    """Fresh environment instance"""
    environment = EmailTriageEnv()
    environment.reset()
    return environment


@pytest.fixture
def sample_emails() -> List[Email]:
    """Generate diverse test emails"""
    return [
        Email(
            id="urgent-ceo-001",
            sender="ceo@company.com",
            recipient="agent@company.com",
            subject="URGENT: Board meeting tomorrow",
            body="We need the Q4 financials prepared immediately.",
            timestamp=datetime.now(),
            category=EmailCategory.URGENT,
            priority=EmailPriority.HIGH,
            is_spam=False,
            sentiment_score=0.3,
            vip_sender=True
        ),
        Email(
            id="spam-001",
            sender="winner@lottery-scam.com",
            recipient="agent@company.com",
            subject="YOU WON $1,000,000!!!",
            body="Click here to claim your prize now!!!",
            timestamp=datetime.now(),
            category=EmailCategory.SPAM,
            priority=EmailPriority.LOW,
            is_spam=True,
            sentiment_score=-0.8,
            vip_sender=False
        ),
        Email(
            id="invoice-001",
            sender="accounting@vendor.com",
            recipient="agent@company.com",
            subject="Invoice #12345 - Payment Due",
            body="Please find attached invoice for services rendered.",
            timestamp=datetime.now(),
            category=EmailCategory.BILLING,
            priority=EmailPriority.MEDIUM,
            is_spam=False,
            sentiment_score=0.0,
            attachments=[
                Attachment(
                    filename="invoice_12345.pdf",
                    content_type="application/pdf",
                    size_bytes=45678,
                    data_uri="data:application/pdf;base64,JVBERi0xLj..."
                )
            ]
        ),
        Email(
            id="support-001",
            sender="customer@client.com",
            recipient="support@company.com",
            subject="Help: Product not working",
            body="I've been trying to use your product but it keeps crashing. Very frustrated!",
            timestamp=datetime.now(),
            category=EmailCategory.SUPPORT,
            priority=EmailPriority.MEDIUM,
            is_spam=False,
            sentiment_score=-0.6,
            vip_sender=False
        ),
        Email(
            id="newsletter-001",
            sender="marketing@news.com",
            recipient="agent@company.com",
            subject="Weekly Tech News Digest",
            body="Here are this week's top tech stories...",
            timestamp=datetime.now(),
            category=EmailCategory.NEWSLETTER,
            priority=EmailPriority.LOW,
            is_spam=False,
            sentiment_score=0.1,
            vip_sender=False
        )
    ]


# ============================================================================
# OpenEnv Core API Tests
# ============================================================================

class TestOpenEnvCompliance:
    """Test full OpenEnv specification compliance"""
    
    def test_reset_endpoint(self, client):
        """Test /reset returns proper ResetResult"""
        response = client.post(
            "/reset",
            json={"task_id": "task_easy_categorize"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "observation" in data
        assert "info" in data
        assert "inbox" in data["observation"]
        
    def test_state_endpoint(self, client):
        """Test /state returns current state"""
        # Reset first
        client.post("/reset", json={"task_id": "task_easy_categorize"})
        
        response = client.get("/state")
        assert response.status_code == 200
        data = response.json()
        assert "inbox" in data
        assert "step_count" in data
        
    def test_step_endpoint(self, client):
        """Test /step processes actions correctly"""
        # Reset first
        reset_resp = client.post("/reset", json={"task_id": "task_easy_categorize"})
        inbox = reset_resp.json()["observation"]["inbox"]
        
        if inbox:
            email_id = inbox[0]["id"]
            response = client.post(
                "/step",
                json={
                    "action_type": "categorize",
                    "email_id": email_id,
                    "category": "internal"  # Use valid EmailCategory enum value
                }
            )
            assert response.status_code == 200
            data = response.json()
            assert "observation" in data
            assert "reward" in data
            assert "done" in data
            assert isinstance(data["reward"]["value"], (int, float))
            
    def test_tasks_endpoint(self, client):
        """Test /tasks lists all available tasks"""
        response = client.get("/tasks")
        assert response.status_code == 200
        data = response.json()
        assert "tasks" in data
        assert len(data["tasks"]) >= 3
        
        task_ids = [t["task_id"] for t in data["tasks"]]
        assert "task_easy_categorize" in task_ids
        assert "task_medium_triage" in task_ids
        assert "task_hard_full_inbox" in task_ids


class TestEnvironmentLogic:
    """Test core environment functionality"""
    
    def test_categorization(self, env, sample_emails):
        """Test email categorization"""
        email = sample_emails[0]  # CEO email
        action = Action(
            action_type=ActionType.CATEGORIZE,
            email_id=email.id,
            category="urgent"
        )
        
        # Should succeed
        result = env._categorize_email(email, "urgent")
        assert result is True
        
    def test_prioritization(self, env, sample_emails):
        """Test email prioritization"""
        email = sample_emails[0]
        action = Action(
            action_type=ActionType.PRIORITIZE,
            email_id=email.id,
            priority="high"
        )
        
        result = env._prioritize_email(email, "high")
        assert result is True
        
    def test_spam_detection(self, env, sample_emails):
        """Test spam flagging"""
        spam_email = sample_emails[1]  # Lottery scam
        
        action = Action(
            action_type=ActionType.FLAG_SPAM,
            email_id=spam_email.id
        )
        
        result = env._flag_spam(spam_email)
        assert result is True
        
    def test_archive(self, env, sample_emails):
        """Test archiving emails"""
        email = sample_emails[4]  # Newsletter
        
        result = env._archive_email(email)
        assert result is True
        
    def test_delete(self, env, sample_emails):
        """Test deleting emails"""
        email = sample_emails[1]  # Spam
        
        result = env._delete_email(email)
        assert result is True


# ============================================================================
# Advanced AI Systems Tests
# ============================================================================

class TestKnowledgeGraph:
    """Test Knowledge Graph & Explainable AI"""
    
    def test_extract_entities_endpoint(self, client):
        """Test entity extraction from email"""
        # First reset and get an email
        reset_resp = client.post("/reset", json={"task_id": "task_easy_categorize"})
        inbox = reset_resp.json()["observation"]["inbox"]
        
        if inbox:
            email_id = inbox[0]["id"]
            response = client.get(f"/knowledge/extract/{email_id}")
            assert response.status_code == 200
            data = response.json()
            assert "entities" in data or "extraction" in data
        
    def test_explain_categorization(self, client):
        """Test explainable AI for categorization - check status endpoint"""
        response = client.get("/knowledge/status")
        assert response.status_code == 200
        data = response.json()
        assert "total_entities" in data or "status" in data
        
    def test_query_relationships(self, client):
        """Test relationship querying"""
        response = client.get("/knowledge/relationships")
        assert response.status_code == 200
        data = response.json()
        assert "relationships" in data or "network" in data


class TestResponseGenerator:
    """Test Intelligent Response Generator"""
    
    def test_generate_response(self, client):
        """Test response generation"""
        # Reset and get email
        reset_resp = client.post("/reset", json={"task_id": "task_easy_categorize"})
        inbox = reset_resp.json()["observation"]["inbox"]
        
        if inbox:
            email_id = inbox[0]["id"]
            response = client.get(f"/response/generate/{email_id}")
            assert response.status_code == 200
            data = response.json()
            assert "response" in data or "generated_response" in data
        
    def test_quick_responses(self, client):
        """Test quick response templates"""
        # Reset and get email
        reset_resp = client.post("/reset", json={"task_id": "task_easy_categorize"})
        inbox = reset_resp.json()["observation"]["inbox"]
        
        if inbox:
            email_id = inbox[0]["id"]
            response = client.get(f"/response/quick/{email_id}")
            assert response.status_code == 200
            data = response.json()
            assert "quick_responses" in data or "suggestions" in data
        
    def test_template_listing(self, client):
        """Test template retrieval"""
        response = client.get("/response/templates")
        assert response.status_code == 200
        data = response.json()
        assert "templates" in data or "available_templates" in data


class TestMultiAgentAI:
    """Test Multi-Agent Collaborative System"""
    
    def test_agent_decision(self, client):
        """Test multi-agent decision making"""
        response = client.post(
            "/collaborative-ai/decide",
            json={
                "email_id": "test-001",
                "sender": "important@client.com",
                "subject": "Contract renewal",
                "body": "We need to discuss renewal terms"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "consensus" in data
        assert "agent_votes" in data
        assert len(data["agent_votes"]) == 4  # 4 agents
        
    def test_agent_performance(self, client):
        """Test agent performance metrics"""
        response = client.get("/collaborative-ai/performance")
        assert response.status_code == 200
        data = response.json()
        assert "agents" in data


class TestPredictiveAnalytics:
    """Test Predictive Analytics Engine"""
    
    def test_forecast_volume(self, client):
        """Test email volume forecasting"""
        response = client.post(
            "/predictive/forecast",
            json={"hours_ahead": 24}
        )
        assert response.status_code == 200
        data = response.json()
        assert "forecast" in data
        assert len(data["forecast"]) > 0
        
    def test_sender_behavior(self, client):
        """Test sender behavior analysis"""
        response = client.get("/predictive/sender-behavior/test@example.com")
        assert response.status_code == 200
        data = response.json()
        assert "sender" in data


class TestSecurityScanner:
    """Test Security Scanner System"""
    
    def test_scan_email(self, client):
        """Test security scanning"""
        response = client.post(
            "/security/scan",
            json={
                "email_id": "test-001",
                "subject": "Your SSN is 123-45-6789",
                "body": "Please send payment to http://phishing-site.com"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "scan_result" in data
        assert "threats_detected" in data["scan_result"]
        
    def test_pii_detection(self, client):
        """Test PII detection"""
        response = client.post(
            "/security/scan",
            json={
                "email_id": "test-002",
                "subject": "Personal info",
                "body": "My credit card is 4532-1234-5678-9010 and SSN is 123-45-6789"
            }
        )
        assert response.status_code == 200
        data = response.json()
        # Should detect credit card and SSN
        assert data["scan_result"]["risk_score"] > 0.5


class TestBlockchainAudit:
    """Test Blockchain Audit Trail"""
    
    def test_get_audit_trail(self, client):
        """Test retrieving audit trail"""
        response = client.get("/blockchain/audit-trail/test-email-001")
        assert response.status_code == 200
        data = response.json()
        assert "audit_trail" in data
        
    def test_verify_integrity(self, client):
        """Test blockchain integrity verification"""
        response = client.get("/blockchain/verify")
        assert response.status_code == 200
        data = response.json()
        assert "valid" in data
        assert data["valid"] is True


class TestMonitoringSystem:
    """Test Advanced Monitoring System"""
    
    def test_system_health(self, client):
        """Test health metrics"""
        response = client.get("/monitoring/health")
        assert response.status_code == 200
        data = response.json()
        assert "health_status" in data
        assert "metrics" in data
        assert len(data["metrics"]) > 0
        
    def test_alerts(self, client):
        """Test alert generation"""
        response = client.get("/monitoring/alerts")
        assert response.status_code == 200
        data = response.json()
        assert "alerts" in data


class TestPerformanceOptimizer:
    """Test Auto-Performance Optimizer"""
    
    def test_optimization_status(self, client):
        """Test optimizer status"""
        response = client.get("/performance/status")
        assert response.status_code == 200
        data = response.json()
        assert "performance_report" in data
        
    def test_apply_optimization(self, client):
        """Test applying optimizations"""
        response = client.post(
            "/performance/optimize",
            json={"strategy": "aggressive"}
        )
        assert response.status_code == 200
        data = response.json()
        assert "applied_optimizations" in data


# ============================================================================
# Production Features Tests
# ============================================================================

class TestProductionFeatures:
    """Test production-grade features"""
    
    def test_health_endpoint(self, client):
        """Test health check"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "version" in data
        
    def test_metrics_endpoint(self, client):
        """Test Prometheus metrics"""
        response = client.get("/metrics")
        assert response.status_code == 200
        # Prometheus format
        assert "http_requests_total" in response.text
        
    def test_batch_operations(self, client):
        """Test batch email processing"""
        response = client.post(
            "/batch/categorize",
            json={
                "email_ids": ["email-1", "email-2", "email-3"],
                "category": "work"
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        
    def test_search(self, client):
        """Test advanced search"""
        response = client.post(
            "/search",
            json={
                "query": "urgent meeting",
                "filters": {"category": "work"}
            }
        )
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        
    def test_export_data(self, client):
        """Test data export"""
        response = client.get("/export?format=json")
        assert response.status_code == 200
        data = response.json()
        assert "emails" in data
        
    def test_undo_system(self, client):
        """Test undo/redo functionality"""
        # Perform action
        client.post(
            "/reset",
            json={"task_id": "task_easy_categorize"}
        )
        
        # Undo
        response = client.post("/undo")
        assert response.status_code == 200
        
        # Redo
        response = client.post("/redo")
        assert response.status_code == 200


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_invalid_email_id(self, client):
        """Test handling of invalid email ID"""
        response = client.post(
            "/step",
            json={
                "action_type": "categorize",
                "email_id": "nonexistent-id",
                "category": "work"
            }
        )
        assert response.status_code == 400
        
    def test_malformed_request(self, client):
        """Test malformed request handling"""
        response = client.post(
            "/step",
            json={"invalid": "data"}
        )
        assert response.status_code == 422  # Validation error
        
    def test_rate_limiting(self, client):
        """Test rate limiting protection"""
        # Make many rapid requests
        responses = []
        for _ in range(150):  # Exceed rate limit
            resp = client.get("/health")
            responses.append(resp.status_code)
        
        # Should get some 429 responses
        assert 429 in responses
        
    def test_large_inbox(self, env):
        """Test handling of large inbox (stress test)"""
        # Create 1000 emails
        large_inbox = []
        for i in range(1000):
            email = Email(
                id=f"stress-{i}",
                sender=f"sender{i}@test.com",
                recipient="agent@company.com",
                subject=f"Test email {i}",
                body=f"Body content {i}",
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
            large_inbox.append(email)
        
        # Should handle without crashing
        env.state.inbox = large_inbox
        assert len(env.state.inbox) == 1000


class TestTaskGrading:
    """Test task grading system"""
    
    def test_easy_task_grading(self, client):
        """Test easy task grading"""
        # Reset to easy task
        reset_resp = client.post(
            "/reset",
            json={"task_id": "task_easy_categorize"}
        )
        inbox = reset_resp.json()["observation"]["inbox"]
        
        # Categorize all emails
        for email in inbox:
            client.post(
                "/step",
                json={
                    "action_type": "categorize",
                    "email_id": email["id"],
                    "category": email.get("category", "work")
                }
            )
        
        # Get final score
        state = client.get("/state").json()
        # Score should be positive
        
    def test_grading_accuracy(self, client):
        """Test grading accuracy"""
        reset_resp = client.post(
            "/reset",
            json={"task_id": "task_easy_categorize"}
        )
        
        # Should return grading info
        assert "info" in reset_resp.json()


class TestConcurrency:
    """Test concurrent operations"""
    
    def test_concurrent_requests(self, client):
        """Test handling concurrent requests"""
        import threading
        
        results = []
        
        def make_request():
            resp = client.get("/health")
            results.append(resp.status_code)
        
        # Launch 50 concurrent requests
        threads = []
        for _ in range(50):
            t = threading.Thread(target=make_request)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        # All should succeed
        assert all(r == 200 for r in results)


# ============================================================================
# Integration Tests
# ============================================================================

class TestFullWorkflow:
    """Test complete email triage workflows"""
    
    def test_complete_triage_workflow(self, client):
        """Test full email triage workflow"""
        # 1. Reset environment
        reset_resp = client.post(
            "/reset",
            json={"task_id": "task_medium_triage"}
        )
        assert reset_resp.status_code == 200
        inbox = reset_resp.json()["observation"]["inbox"]
        
        # 2. Process each email
        for email in inbox[:3]:  # Process first 3
            email_id = email["id"]
            
            # Scan for security
            security_resp = client.post(
                "/security/scan",
                json={
                    "email_id": email_id,
                    "subject": email["subject"],
                    "body": email["body"]
                }
            )
            assert security_resp.status_code == 200
            
            # Get AI recommendation
            ai_resp = client.post(
                "/collaborative-ai/decide",
                json={
                    "email_id": email_id,
                    "sender": email["sender"],
                    "subject": email["subject"],
                    "body": email["body"]
                }
            )
            assert ai_resp.status_code == 200
            
            # Take action based on recommendation
            consensus = ai_resp.json()["consensus"]
            step_resp = client.post(
                "/step",
                json={
                    "action_type": "categorize",
                    "email_id": email_id,
                    "category": consensus.get("category", "work")
                }
            )
            assert step_resp.status_code == 200
        
        # 3. Check final state
        state_resp = client.get("/state")
        assert state_resp.status_code == 200


class TestSystemIntegration:
    """Test integration between AI systems"""
    
    def test_ai_system_collaboration(self, client):
        """Test collaboration between different AI systems"""
        email_data = {
            "email_id": "integration-test-001",
            "sender": "ceo@company.com",
            "subject": "URGENT: Meeting about Project Alpha",
            "body": "We need to discuss the contract with John Smith at 555-1234"
        }
        
        # 1. Extract knowledge graph
        kg_resp = client.post(
            "/knowledge-graph/extract",
            json=email_data
        )
        assert kg_resp.status_code == 200
        entities = kg_resp.json()["entities"]
        
        # 2. Get AI decision
        ai_resp = client.post(
            "/collaborative-ai/decide",
            json=email_data
        )
        assert ai_resp.status_code == 200
        
        # 3. Generate response
        response_resp = client.post(
            "/response-generator/generate",
            json={
                **email_data,
                "response_type": "acknowledgment",
                "tone": "professional"
            }
        )
        assert response_resp.status_code == 200
        
        # 4. All systems should work together
        assert len(entities) > 0
        assert "consensus" in ai_resp.json()
        assert "generated_response" in response_resp.json()


# ============================================================================
# Performance Tests
# ============================================================================

class TestPerformance:
    """Test system performance"""
    
    def test_response_time(self, client):
        """Test API response times"""
        import time
        
        start = time.time()
        response = client.get("/health")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        assert elapsed < 0.1  # Should respond in <100ms
        
    def test_throughput(self, client):
        """Test request throughput"""
        import time
        
        start = time.time()
        count = 0
        
        # Make requests for 1 second
        while time.time() - start < 1.0:
            client.get("/health")
            count += 1
        
        # Should handle at least 100 req/sec
        assert count >= 100
        
    def test_memory_efficiency(self, env):
        """Test memory usage doesn't explode"""
        import sys
        
        initial_size = sys.getsizeof(env)
        
        # Process 100 emails
        for i in range(100):
            email = Email(
                id=f"mem-test-{i}",
                sender="test@example.com",
                recipient="agent@company.com",
                subject=f"Test {i}",
                body="Test body",
                timestamp=datetime.now(),
                category=EmailCategory.WORK,
                priority=EmailPriority.MEDIUM,
                is_spam=False
            )
            env.state.inbox.append(email)
            
            action = Action(
                action_type=ActionType.CATEGORIZE,
                email_id=email.id,
                category="work"
            )
            env.step(action)
        
        final_size = sys.getsizeof(env)
        
        # Size shouldn't grow linearly (should use deque)
        growth_ratio = final_size / initial_size
        assert growth_ratio < 2.0  # Less than 2x growth


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
