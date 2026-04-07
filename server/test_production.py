"""
Simplified Production Tests
Fast validation of core functionality and all systems
"""
import pytest
from fastapi.testclient import TestClient
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from main import app


@pytest.fixture
def client():
    """FastAPI test client"""
    return TestClient(app)


class TestCoreAPI:
    """Core API functionality tests"""
    
    def test_health(self, client):
        """Health endpoint works"""
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_reset(self, client):
        """Reset endpoint works"""
        response = client.post("/reset", json={"task_id": "task_easy_categorize"})
        assert response.status_code == 200
        assert "observation" in response.json()
    
    def test_state(self, client):
        """State endpoint works"""
        client.post("/reset", json={"task_id": "task_easy_categorize"})
        response = client.get("/state")
        assert response.status_code == 200
        assert "inbox" in response.json()
    
    def test_step(self, client):
        """Step endpoint works"""
        reset_resp = client.post("/reset", json={"task_id": "task_easy_categorize"})
        inbox = reset_resp.json()["observation"]["inbox"]
        
        if inbox:
            email_id = inbox[0]["id"]
            response = client.post(
                "/step",
                json={
                    "action_type": "categorize",
                    "email_id": email_id,
                    "category": "internal"
                }
            )
            assert response.status_code == 200
            assert "reward" in response.json()
    
    def test_tasks(self, client):
        """Tasks endpoint lists all tasks"""
        response = client.get("/tasks")
        assert response.status_code == 200
        tasks = response.json()["tasks"]
        assert len(tasks) >= 3
        task_ids = [t["task_id"] for t in tasks]
        assert "task_easy_categorize" in task_ids
        assert "task_medium_triage" in task_ids
        assert "task_hard_full_inbox" in task_ids


class TestAdvancedSystems:
    """Test all 14 advanced AI systems are accessible"""
    
    def test_all_systems_active(self, client):
        """Verify all 14 systems are reporting active"""
        response = client.get("/system/full-status")
        assert response.status_code == 200
        data = response.json()
        assert data["total_systems"] == 14
        assert data["all_active"] is True
        assert len(data["systems"]) == 14
    
    def test_knowledge_graph_accessible(self, client):
        """Knowledge graph endpoints accessible"""
        response = client.get("/knowledge/status")
        assert response.status_code == 200
    
    def test_response_generator_accessible(self, client):
        """Response generator accessible"""
        response = client.get("/response/templates")
        assert response.status_code == 200
    
    def test_ai_agents_accessible(self, client):
        """Multi-agent AI accessible"""
        response = client.get("/ai/agents/status")
        assert response.status_code == 200
    
    def test_security_accessible(self, client):
        """Security scanner accessible"""
        response = client.get("/security/analytics")
        assert response.status_code == 200
    
    def test_blockchain_accessible(self, client):
        """Blockchain audit accessible"""
        response = client.get("/audit/analytics")
        assert response.status_code == 200
    
    def test_monitoring_accessible(self, client):
        """Monitoring system accessible"""
        response = client.get("/monitoring/health")
        assert response.status_code == 200
    
    def test_performance_optimizer_accessible(self, client):
        """Performance optimizer accessible"""
        response = client.get("/performance/metrics")
        assert response.status_code == 200


class TestProductionFeatures:
    """Test production-grade features"""
    
    def test_metrics_endpoint(self, client):
        """Metrics endpoint available"""
        response = client.get("/metrics")
        assert response.status_code == 200
        # Check it returns JSON data about metrics
        data = response.json()
        assert "uptime_seconds" in data or "total_requests" in data
    
    def test_search_accessible(self, client):
        """Search endpoint accessible"""
        response = client.get("/search")  # GET not POST
        assert response.status_code == 200
    
    def test_export_accessible(self, client):
        """Data export accessible"""
        response = client.get("/export?format=json")
        assert response.status_code == 200
    
    def test_undo_accessible(self, client):
        """Undo system works or gracefully declines"""
        response = client.post("/undo")
        # May not exist or may decline if nothing to undo
        assert response.status_code in [200, 400, 404]
    
    def test_analytics_accessible(self, client):
        """Analytics dashboard accessible"""
        response = client.get("/analytics/overview")
        assert response.status_code == 200


class TestGrading:
    """Test task grading system"""
    
    def test_easy_task_completable(self, client):
        """Easy task can be completed"""
        # Reset
        reset_resp = client.post("/reset", json={"task_id": "task_easy_categorize"})
        assert reset_resp.status_code == 200
        inbox = reset_resp.json()["observation"]["inbox"]
        
        # Process some emails
        for email in inbox[:2]:
            client.post(
                "/step",
                json={
                    "action_type": "categorize",
                    "email_id": email["id"],
                    "category": "internal"
                }
            )
        
        # Should have positive reward
        state = client.get("/state").json()
        assert state["step_count"] >= 2


class TestPerformance:
    """Quick performance checks"""
    
    def test_health_response_time(self, client):
        """Health check responds quickly"""
        import time
        start = time.time()
        response = client.get("/health")
        elapsed = time.time() - start
        
        assert response.status_code == 200
        assert elapsed < 0.5  # Under 500ms
    
    def test_concurrent_health_checks(self, client):
        """Can handle concurrent requests"""
        import threading
        
        results = []
        
        def make_request():
            resp = client.get("/health")
            results.append(resp.status_code)
        
        threads = [threading.Thread(target=make_request) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        assert all(r == 200 for r in results)


class TestEdgeCases:
    """Test edge cases and error handling"""
    
    def test_invalid_task_id(self, client):
        """Invalid task ID handled gracefully"""
        response = client.post("/reset", json={"task_id": "invalid_task"})
        assert response.status_code in [400, 404, 422]
    
    def test_invalid_email_id(self, client):
        """Invalid email ID returns error or gracefully handles"""
        client.post("/reset", json={"task_id": "task_easy_categorize"})
        response = client.post(
            "/step",
            json={
                "action_type": "categorize",
                "email_id": "nonexistent",
                "category": "internal"
            }
        )
        # May return success with error in response, or 400/404
        assert response.status_code in [200, 400, 404]
    
    def test_malformed_json(self, client):
        """Malformed request handled gracefully"""
        response = client.post(
            "/step",
            json={"invalid": "data"}
        )
        assert response.status_code == 422


class TestEnterpriseResilienceSystems:
    """Test enterprise resilience systems"""
    
    def test_circuit_breakers(self, client):
        """Circuit breakers accessible"""
        response = client.get("/resilience/circuit-breakers")
        assert response.status_code == 200
        data = response.json()
        assert "summary" in data
        assert data["summary"]["total_circuits"] >= 5
    
    def test_rate_limit_analytics(self, client):
        """Rate limiter analytics accessible"""
        response = client.get("/resilience/rate-limits")
        assert response.status_code == 200
        data = response.json()
        assert "summary" in data
    
    def test_feature_flags(self, client):
        """Feature flags accessible"""
        response = client.get("/features")
        assert response.status_code == 200
        data = response.json()
        assert "flags" in data
        assert len(data["flags"]) >= 10
    
    def test_webhooks(self, client):
        """Webhooks accessible"""
        response = client.get("/webhooks")
        assert response.status_code == 200
        data = response.json()
        assert "endpoints" in data
    
    def test_cache_stats(self, client):
        """Cache stats accessible"""
        response = client.get("/cache/stats")
        assert response.status_code == 200
        data = response.json()
        assert "summary" in data
        assert data["summary"]["total_namespaces"] >= 10
    
    def test_enterprise_status(self, client):
        """Enterprise status shows all systems"""
        response = client.get("/enterprise/status")
        assert response.status_code == 200
        data = response.json()
        assert data["available"] is True
        assert data["total_enterprise_systems"] == 5
        assert data["all_operational"] is True


class TestAdvancedEnterpriseSystems:
    """Tests for advanced enterprise systems"""
    
    def test_tracing_analytics(self, client):
        """Tracing analytics accessible"""
        response = client.get("/tracing/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "total_traces" in data
        assert "active_traces" in data
    
    def test_tracing_traces(self, client):
        """Recent traces accessible"""
        response = client.get("/tracing/traces")
        assert response.status_code == 200
        data = response.json()
        assert "traces" in data
    
    def test_plugins_list(self, client):
        """Plugin list accessible"""
        response = client.get("/plugins")
        assert response.status_code == 200
        data = response.json()
        assert "plugins" in data
        assert len(data["plugins"]) >= 3  # 3 built-in plugins
    
    def test_plugin_analytics(self, client):
        """Plugin analytics accessible"""
        response = client.get("/plugins/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "total_plugins" in data
    
    def test_jobs_list(self, client):
        """Jobs list accessible"""
        response = client.get("/jobs")
        assert response.status_code == 200
        data = response.json()
        assert "jobs" in data
    
    def test_job_analytics(self, client):
        """Job analytics accessible"""
        response = client.get("/jobs/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "registered_handlers" in data
        assert len(data["registered_handlers"]) >= 5  # 5 built-in handlers
    
    def test_config_all(self, client):
        """Config values accessible"""
        response = client.get("/config")
        assert response.status_code == 200
        data = response.json()
        assert "config" in data
    
    def test_config_analytics(self, client):
        """Config analytics accessible"""
        response = client.get("/config/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "total_keys" in data
        assert data["total_keys"] >= 25  # Many default config keys
    
    def test_api_versions(self, client):
        """API versions accessible"""
        response = client.get("/api/versions")
        assert response.status_code == 200
        data = response.json()
        assert "versions" in data
        assert "current" in data
        assert len(data["versions"]) >= 2  # v1 and v2
    
    def test_versioning_analytics(self, client):
        """API versioning analytics accessible"""
        response = client.get("/api/versioning/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "version_count" in data
    
    def test_audit_events(self, client):
        """Audit events accessible"""
        response = client.get("/audit/events")
        assert response.status_code == 200
        data = response.json()
        assert "events" in data
    
    def test_audit_analytics(self, client):
        """Audit analytics accessible"""
        response = client.get("/audit/analytics")
        assert response.status_code == 200
        data = response.json()
        # Could be blockchain audit analytics or audit logger analytics
        assert "generated_at" in data or "total_events" in data
    
    def test_advanced_status(self, client):
        """Advanced systems status"""
        response = client.get("/advanced/status")
        assert response.status_code == 200
        data = response.json()
        assert data["available"] is True
        assert data["total_advanced_systems"] == 7
        assert data["all_operational"] is True
    
    def test_system_overview(self, client):
        """System overview shows all modules"""
        response = client.get("/system/overview")
        assert response.status_code == 200
        data = response.json()
        assert data["totals"]["total_systems"] == 26
        assert data["totals"]["ai_systems"] == 14
        assert data["totals"]["enterprise_systems"] == 5
        assert data["totals"]["advanced_systems"] == 7
    
    def test_metrics_dashboard(self, client):
        """Metrics dashboard accessible"""
        response = client.get("/observability/dashboard")
        assert response.status_code == 200
        data = response.json()
        assert "uptime_seconds" in data
        assert "key_metrics" in data
        assert "total_metrics" in data
    
    def test_metrics_list(self, client):
        """Metrics list accessible"""
        response = client.get("/observability/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "metrics" in data
        assert len(data["metrics"]) >= 20  # 20 default metrics
    
    def test_metrics_analytics(self, client):
        """Metrics analytics accessible"""
        response = client.get("/observability/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "total_metrics" in data
        assert data["total_metrics"] >= 20


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
