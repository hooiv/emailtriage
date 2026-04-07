"""
Tests for Next-Gen Advanced Systems
GraphQL, Health Checks, Compression, API Analytics, Request Validation
"""

import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


class TestGraphQLAPI:
    """Test GraphQL API system"""
    
    def test_graphql_schema(self):
        """Test GraphQL schema endpoint"""
        response = client.get("/graphql/schema")
        assert response.status_code == 200
        data = response.json()
        assert "schema" in data
        assert "type Query" in data["schema"]
        assert "type Mutation" in data["schema"]
    
    def test_graphql_introspection(self):
        """Test GraphQL introspection"""
        response = client.get("/graphql/introspect")
        assert response.status_code == 200
        data = response.json()
        assert "__schema" in data
        assert "types" in data["__schema"]
    
    def test_graphql_query(self):
        """Test GraphQL query execution"""
        response = client.post("/graphql", json={
            "query": "{ tasks { id name difficulty } }"
        })
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
    
    def test_graphql_analytics(self):
        """Test GraphQL analytics"""
        response = client.get("/graphql/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "features" in data
        assert "type_system" in data["features"]


class TestHealthChecks:
    """Test health check system"""
    
    def test_liveness_probe(self):
        """Test Kubernetes liveness probe"""
        response = client.get("/health/live")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["type"] == "liveness"
    
    def test_readiness_probe(self):
        """Test Kubernetes readiness probe"""
        response = client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["type"] == "readiness"
    
    def test_startup_probe(self):
        """Test Kubernetes startup probe"""
        response = client.get("/health/startup")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert data["type"] == "startup"
    
    def test_deep_health(self):
        """Test deep health check"""
        response = client.get("/health/deep")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "checks" in data
        assert "summary" in data
        assert data["type"] == "deep"
    
    def test_mark_startup_complete(self):
        """Test marking startup as complete"""
        response = client.post("/health/startup/complete")
        assert response.status_code == 200
        assert response.json()["status"] == "startup_complete"
    
    def test_health_checks_list(self):
        """Test listing health checks"""
        response = client.get("/health/checks")
        assert response.status_code == 200
        data = response.json()
        assert "total_checks_registered" in data
        assert "checks" in data
    
    def test_health_analytics(self):
        """Test health analytics"""
        response = client.get("/health/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "features" in data
        assert "liveness_probe" in data["features"]


class TestCompression:
    """Test compression system"""
    
    def test_compress_string(self):
        """Test string compression"""
        response = client.post("/compression/compress", json={
            "data": "Hello, World!" * 100,
            "force": True
        })
        assert response.status_code == 200
        data = response.json()
        assert data["compressed"] == True
        assert "algorithm" in data
        assert "ratio" in data
        assert data["ratio"] < 1.0
    
    def test_compress_dict(self):
        """Test dictionary compression"""
        response = client.post("/compression/compress", json={
            "data": {"key": "value" * 100},
            "algorithm": "gzip",
            "force": True
        })
        assert response.status_code == 200
        data = response.json()
        assert data["compressed"] == True
    
    def test_decompress(self):
        """Test decompression"""
        # First compress
        comp_resp = client.post("/compression/compress", json={
            "data": "Test data" * 50,
            "force": True
        })
        compressed_data = comp_resp.json()["data"]
        encoding = comp_resp.json()["encoding"]
        
        # Then decompress
        response = client.post("/compression/decompress", json={
            "data": compressed_data,
            "encoding": encoding
        })
        assert response.status_code == 200
        data = response.json()
        assert data["success"] == True
        assert "data" in data
    
    def test_compression_benchmark(self):
        """Test compression benchmark"""
        response = client.post("/compression/benchmark", json={
            "data": "Benchmark test data" * 100
        })
        assert response.status_code == 200
        data = response.json()
        assert "original_size" in data
        assert "algorithms" in data
        assert "gzip" in data["algorithms"]
        assert "best_size" in data
    
    def test_compression_stats(self):
        """Test compression statistics"""
        response = client.get("/compression/stats")
        assert response.status_code == 200
        data = response.json()
        assert "compressions" in data
        assert "decompressions" in data
        assert "available_algorithms" in data
    
    def test_compression_analytics(self):
        """Test compression analytics"""
        response = client.get("/compression/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "features" in data
        assert "gzip_compression" in data["features"]


class TestAPIAnalytics:
    """Test API analytics system"""
    
    def test_analytics_summary(self):
        """Test analytics summary"""
        response = client.get("/api/analytics/summary")
        assert response.status_code == 200
        data = response.json()
        assert "total_requests" in data
        assert "error_rate" in data
        assert "average_response_time_ms" in data
    
    def test_analytics_endpoints(self):
        """Test endpoint analytics"""
        response = client.get("/api/analytics/endpoints")
        assert response.status_code == 200
        data = response.json()
        assert "endpoints" in data
        assert "total_endpoints" in data
    
    def test_analytics_consumers(self):
        """Test consumer analytics"""
        response = client.get("/api/analytics/consumers?limit=10")
        assert response.status_code == 200
        data = response.json()
        assert "consumers" in data
        assert "total_consumers" in data
    
    def test_analytics_traffic(self):
        """Test traffic patterns"""
        response = client.get("/api/analytics/traffic")
        assert response.status_code == 200
        data = response.json()
        assert "hourly_distribution" in data
        assert "current_rpm" in data
    
    def test_analytics_errors(self):
        """Test error analysis"""
        response = client.get("/api/analytics/errors")
        assert response.status_code == 200
        data = response.json()
        assert "total_errors" in data
        assert "error_rate" in data
    
    def test_analytics_requests(self):
        """Test recent requests"""
        response = client.get("/api/analytics/requests?limit=50")
        assert response.status_code == 200
        data = response.json()
        assert "requests" in data
        assert isinstance(data["requests"], list)
    
    def test_analytics_overview(self):
        """Test comprehensive analytics"""
        response = client.get("/api/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "features" in data
        assert "endpoint_tracking" in data["features"]
    
    def test_analytics_reset(self):
        """Test analytics reset"""
        response = client.post("/api/analytics/reset")
        assert response.status_code == 200
        assert response.json()["status"] == "analytics_reset"


class TestRequestValidation:
    """Test request validation system"""
    
    def test_validate_action_schema(self):
        """Test action schema validation"""
        response = client.post("/validation/validate", json={
            "schema_name": "action",
            "data": {
                "action": "categorize",
                "email_id": "test123",
                "category": "urgent"
            }
        })
        assert response.status_code == 200
        data = response.json()
        assert "valid" in data
        assert data["schema"] == "action"
    
    def test_validate_invalid_data(self):
        """Test validation with invalid data"""
        response = client.post("/validation/validate", json={
            "schema_name": "action",
            "data": {
                "action": "invalid_action",
                "email_id": ""
            }
        })
        assert response.status_code == 200
        data = response.json()
        assert data["valid"] == False
        assert len(data["errors"]) > 0
    
    def test_sanitize_request(self):
        """Test request sanitization"""
        response = client.post("/validation/sanitize", json={
            "data": {
                "key": "value\x00\x01\x02",  # Control characters
                "long_text": "x" * 20000
            }
        })
        assert response.status_code == 200
        data = response.json()
        assert "sanitized" in data
        # Control characters should be removed
        assert "\x00" not in str(data["sanitized"])
    
    def test_get_all_schemas(self):
        """Test getting all schemas"""
        response = client.get("/validation/schemas")
        assert response.status_code == 200
        data = response.json()
        assert "action" in data
        assert "reset" in data
        assert "search" in data
    
    def test_get_specific_schema(self):
        """Test getting specific schema"""
        response = client.get("/validation/schemas/action")
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "action"
        assert "fields" in data
        assert len(data["fields"]) > 0
    
    def test_get_nonexistent_schema(self):
        """Test getting non-existent schema"""
        response = client.get("/validation/schemas/nonexistent")
        assert response.status_code == 404
    
    def test_validation_stats(self):
        """Test validation statistics"""
        response = client.get("/validation/stats")
        assert response.status_code == 200
        data = response.json()
        assert "total_validations" in data
        assert "pass_rate" in data
        assert "schemas_registered" in data
    
    def test_validation_analytics(self):
        """Test validation analytics"""
        response = client.get("/validation/analytics")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "features" in data
        assert "json_schema_validation" in data["features"]


class TestSystemOverview:
    """Test system overview with all systems"""
    
    def test_system_overview_includes_next_gen(self):
        """Test that system overview includes next-gen systems"""
        response = client.get("/system/overview")
        assert response.status_code == 200
        data = response.json()
        
        # Check next-gen systems are included
        assert data["systems_enabled"]["graphql_api"] == True
        assert data["systems_enabled"]["health_checks"] == True
        assert data["systems_enabled"]["compression"] == True
        assert data["systems_enabled"]["api_analytics"] == True
        assert data["systems_enabled"]["request_validation"] == True
        
        # Check totals are updated
        assert data["totals"]["next_gen_systems"] == 5
        assert data["totals"]["total_systems"] == 31
        assert "220+" in data["totals"]["api_endpoints"]


def test_all_next_gen_systems():
    """Run all next-gen system tests"""
    # Run all test classes
    print("Testing GraphQL API...")
    TestGraphQLAPI().test_graphql_schema()
    TestGraphQLAPI().test_graphql_introspection()
    TestGraphQLAPI().test_graphql_analytics()
    
    print("Testing Health Checks...")
    TestHealthChecks().test_liveness_probe()
    TestHealthChecks().test_readiness_probe()
    TestHealthChecks().test_deep_health()
    
    print("Testing Compression...")
    TestCompression().test_compress_string()
    TestCompression().test_compression_stats()
    
    print("Testing API Analytics...")
    TestAPIAnalytics().test_analytics_summary()
    TestAPIAnalytics().test_analytics_overview()
    
    print("Testing Request Validation...")
    TestRequestValidation().test_get_all_schemas()
    TestRequestValidation().test_validation_analytics()
    
    print("✓ All next-gen system tests passed!")


if __name__ == "__main__":
    test_all_next_gen_systems()
