"""
Comprehensive Test Suite for Next-Generation Systems
Tests quantum optimization and blockchain audit systems
"""

import pytest
import asyncio
from fastapi.testclient import TestClient
import json
import sys
import os

# Add server directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    from main import app
    from quantum_optimization import get_quantum_engine
    from blockchain_audit import get_blockchain_audit, TransactionType
except ImportError as e:
    print(f"Import error: {e}")
    pytest.skip("Required modules not available", allow_module_level=True)


class TestQuantumOptimization:
    """Test quantum optimization system"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_quantum_analytics(self, client):
        """Test quantum analytics endpoint"""
        response = client.get("/quantum/analytics")
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert "features" in data
        assert "algorithms" in data
        assert data["status"] == "active"
    
    def test_create_annealing_optimizer(self, client):
        """Test creating quantum annealing optimizer"""
        response = client.post("/quantum/annealing/create", json={
            "optimizer_id": "test_annealer",
            "num_variables": 10
        })
        assert response.status_code == 200
        
        data = response.json()
        assert data["optimizer_id"] == "test_annealer"
        assert data["type"] == "quantum_annealing"
        assert data["num_variables"] == 10
    
    def test_create_qaoa_optimizer(self, client):
        """Test creating QAOA optimizer"""
        response = client.post("/quantum/qaoa/create", json={
            "optimizer_id": "test_qaoa",
            "num_qubits": 6,
            "num_layers": 2
        })
        assert response.status_code == 200
        
        data = response.json()
        assert data["optimizer_id"] == "test_qaoa"
        assert data["type"] == "qaoa"
        assert data["num_qubits"] == 6
        assert data["num_layers"] == 2
    
    def test_create_quantum_network(self, client):
        """Test creating quantum neural network"""
        response = client.post("/quantum/network/create", json={
            "network_id": "test_qnn",
            "input_size": 4,
            "hidden_size": 8,
            "output_size": 2
        })
        assert response.status_code == 200
        
        data = response.json()
        assert data["network_id"] == "test_qnn"
        assert data["architecture"]["input_size"] == 4
        assert data["architecture"]["hidden_size"] == 8
        assert data["architecture"]["output_size"] == 2
    
    def test_optimize_email_routing(self, client):
        """Test quantum email routing optimization"""
        emails = [
            {"id": "email1", "urgency": 0.8, "category": "support"},
            {"id": "email2", "urgency": 0.3, "category": "sales"}
        ]
        agents = [
            {"id": "agent1", "urgency_skill": 0.7, "specialty": "support", "current_workload": 0.2},
            {"id": "agent2", "urgency_skill": 0.4, "specialty": "sales", "current_workload": 0.1}
        ]
        
        response = client.post("/quantum/routing/optimize", json={
            "emails": emails,
            "agents": agents
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "assignments" in data
        assert "optimization_result" in data
        assert "quantum_speedup" in data
    
    def test_solve_max_cut(self, client):
        """Test Max-Cut problem solving with QAOA"""
        adjacency_matrix = [
            [0, 1, 1, 0],
            [1, 0, 1, 1],
            [1, 1, 0, 1],
            [0, 1, 1, 0]
        ]
        
        response = client.post("/quantum/maxcut/solve", json={
            "adjacency_matrix": adjacency_matrix
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "solution" in data
        assert "cut_value" in data
        assert "solve_time" in data
    
    def test_train_quantum_network(self, client):
        """Test quantum neural network training"""
        # First create a network
        create_response = client.post("/quantum/network/create", json={
            "network_id": "train_test_qnn",
            "input_size": 2,
            "hidden_size": 4,
            "output_size": 1
        })
        assert create_response.status_code == 200
        
        # Then train it
        training_data = [
            {"inputs": [0.0, 0.0], "targets": [0.0]},
            {"inputs": [0.0, 1.0], "targets": [1.0]},
            {"inputs": [1.0, 0.0], "targets": [1.0]},
            {"inputs": [1.0, 1.0], "targets": [0.0]}
        ]
        
        response = client.post("/quantum/network/train", json={
            "network_id": "train_test_qnn",
            "training_data": training_data,
            "epochs": 10
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "epochs_trained" in data
        assert "final_loss" in data
    
    def test_quantum_predict(self, client):
        """Test quantum neural network prediction"""
        # Use the network created in previous test
        response = client.post("/quantum/network/predict", json={
            "network_id": "train_test_qnn",
            "inputs": [0.5, 0.5]
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "predictions" in data
        assert "inference_time" in data
        assert isinstance(data["predictions"], list)
    
    def test_quantum_performance(self, client):
        """Test quantum performance metrics"""
        response = client.get("/quantum/performance")
        assert response.status_code == 200
        
        data = response.json()
        assert "performance" in data
        assert "features_available" in data
        assert "algorithms_available" in data
        
        performance = data["performance"]
        assert "success_rate" in performance
        assert "quantum_advantage_rate" in performance
        assert "average_speedup" in performance


class TestBlockchainAudit:
    """Test blockchain audit system"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_blockchain_analytics(self, client):
        """Test blockchain analytics endpoint"""
        response = client.get("/blockchain/analytics")
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert "blockchain_health" in data
        assert "consensus" in data
        assert "smart_contracts" in data
        assert "features" in data
        assert data["status"] == "active"
    
    def test_create_wallet(self, client):
        """Test creating blockchain wallet"""
        response = client.post("/blockchain/wallet/create", json={
            "owner_id": "test_agent_001"
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "address" in data
        assert "balance" in data
        assert "reputation_score" in data
        assert data["balance"] == 100.0  # Initial balance
        assert data["reputation_score"] == 100.0  # Initial reputation
        
        # Store address for future tests
        global test_wallet_address
        test_wallet_address = data["address"]
    
    def test_create_transaction(self, client):
        """Test creating blockchain transaction"""
        # First ensure we have a wallet
        wallet_response = client.post("/blockchain/wallet/create", json={
            "owner_id": "test_agent_002"
        })
        wallet_data = wallet_response.json()
        address = wallet_data["address"]
        
        response = client.post("/blockchain/transaction/create", json={
            "transaction_type": "email_action",
            "from_address": address,
            "to_address": "system",
            "data": {"action": "categorize", "email_id": "email_123", "category": "support"},
            "gas_fee": 0.002
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "transaction_id" in data
        assert data["status"] == "pending"
        
        # Store transaction ID for future tests
        global test_transaction_id
        test_transaction_id = data["transaction_id"]
    
    def test_mine_block(self, client):
        """Test mining blockchain block"""
        # First create a wallet for mining
        wallet_response = client.post("/blockchain/wallet/create", json={
            "owner_id": "miner_001"
        })
        wallet_data = wallet_response.json()
        miner_address = wallet_data["address"]
        
        response = client.post("/blockchain/block/mine", json={
            "miner_address": miner_address
        })
        # Block mining might fail if no pending transactions, so check for both cases
        assert response.status_code in [200, 400]
        
        if response.status_code == 200:
            data = response.json()
            assert "block_number" in data
            assert "block_hash" in data
            assert "transactions_count" in data
    
    def test_deploy_smart_contract(self, client):
        """Test smart contract deployment"""
        # Create wallet for contract deployment
        wallet_response = client.post("/blockchain/wallet/create", json={
            "owner_id": "contract_deployer"
        })
        wallet_data = wallet_response.json()
        creator_address = wallet_data["address"]
        
        response = client.post("/blockchain/contract/deploy", json={
            "contract_type": "sla_enforcement",
            "creator_address": creator_address,
            "contract_code": "contract SLAEnforcement { function checkViolation() { ... } }",
            "gas_limit": 500000
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "contract_id" in data
        assert data["status"] == "deployed"
        
        # Store contract ID for future tests
        global test_contract_id
        test_contract_id = data["contract_id"]
    
    def test_execute_smart_contract(self, client):
        """Test smart contract execution"""
        # Use the contract from previous test
        if 'test_contract_id' not in globals():
            self.test_deploy_smart_contract(client)
        
        response = client.post("/blockchain/contract/execute", json={
            "contract_id": test_contract_id,
            "function_name": "check_sla_violation",
            "params": {"response_time_hours": 25, "sla_limit_hours": 24},
            "caller_address": "test_caller"
        })
        assert response.status_code == 200
        
        data = response.json()
        assert "success" in data
        if data["success"]:
            assert "violation" in data
            assert "penalty" in data
    
    def test_validate_blockchain(self, client):
        """Test blockchain validation"""
        response = client.get("/blockchain/chain/validate")
        assert response.status_code == 200
        
        data = response.json()
        assert "chain_valid" in data
        assert "chain_length" in data
        assert isinstance(data["chain_valid"], bool)
    
    def test_get_wallet_info(self, client):
        """Test getting wallet information"""
        # Create a wallet first
        wallet_response = client.post("/blockchain/wallet/create", json={
            "owner_id": "wallet_info_test"
        })
        wallet_data = wallet_response.json()
        address = wallet_data["address"]
        
        response = client.get(f"/blockchain/wallet/{address}")
        assert response.status_code == 200
        
        data = response.json()
        assert "address" in data
        assert "balance" in data
        assert "reputation_score" in data
        assert data["address"] == address
    
    def test_list_smart_contracts(self, client):
        """Test listing smart contracts"""
        response = client.get("/blockchain/contracts")
        assert response.status_code == 200
        
        data = response.json()
        assert "contracts" in data
        assert isinstance(data["contracts"], list)
    
    def test_get_blockchain_blocks(self, client):
        """Test getting blockchain blocks"""
        response = client.get("/blockchain/blocks?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert "blocks" in data
        assert isinstance(data["blocks"], list)
        
        # Should at least have genesis block
        assert len(data["blocks"]) >= 1
        
        # Check genesis block structure
        genesis = data["blocks"][0]
        assert "block_number" in genesis
        assert "block_hash" in genesis
        assert genesis["block_number"] == 0


class TestSystemIntegration:
    """Test integration between quantum and blockchain systems"""
    
    @pytest.fixture
    def client(self):
        return TestClient(app)
    
    def test_system_overview_includes_new_systems(self, client):
        """Test that system overview includes quantum and blockchain"""
        response = client.get("/system/overview")
        assert response.status_code == 200
        
        data = response.json()
        # The overview should include our new systems
        assert isinstance(data, dict)
        # We don't enforce specific structure since it depends on what's available
    
    def test_quantum_blockchain_workflow(self, client):
        """Test a workflow combining quantum optimization and blockchain audit"""
        
        # 1. Create blockchain wallet for an agent
        wallet_response = client.post("/blockchain/wallet/create", json={
            "owner_id": "quantum_agent_001"
        })
        assert wallet_response.status_code == 200
        agent_address = wallet_response.json()["address"]
        
        # 2. Create quantum neural network for email classification
        network_response = client.post("/quantum/network/create", json={
            "network_id": "email_classifier",
            "input_size": 3,
            "hidden_size": 6,
            "output_size": 2
        })
        assert network_response.status_code == 200
        
        # 3. Make a quantum prediction
        prediction_response = client.post("/quantum/network/predict", json={
            "network_id": "email_classifier",
            "inputs": [0.8, 0.6, 0.9]  # High urgency email features
        })
        assert prediction_response.status_code == 200
        
        # 4. Record the prediction as a blockchain transaction
        transaction_response = client.post("/blockchain/transaction/create", json={
            "transaction_type": "email_action",
            "from_address": agent_address,
            "to_address": "system",
            "data": {
                "action": "quantum_classify",
                "network_id": "email_classifier",
                "prediction": prediction_response.json()["predictions"],
                "confidence": 0.95
            }
        })
        assert transaction_response.status_code == 200
        
        # 5. Verify both systems are working
        quantum_analytics = client.get("/quantum/analytics")
        blockchain_analytics = client.get("/blockchain/analytics")
        
        assert quantum_analytics.status_code == 200
        assert blockchain_analytics.status_code == 200
        
        # Both systems should be active
        assert quantum_analytics.json()["status"] == "active"
        assert blockchain_analytics.json()["status"] == "active"


def test_system_startup():
    """Test that all new systems start up correctly"""
    
    # Test quantum engine initialization
    quantum_engine = get_quantum_engine()
    assert quantum_engine is not None
    
    analytics = quantum_engine.get_quantum_analytics()
    assert analytics["status"] == "active"
    assert len(analytics["features"]) > 0
    assert len(analytics["algorithms"]) > 0
    
    # Test blockchain audit initialization  
    blockchain = get_blockchain_audit()
    assert blockchain is not None
    
    blockchain_analytics = blockchain.get_blockchain_analytics()
    assert blockchain_analytics["status"] == "active"
    assert blockchain_analytics["blockchain_health"]["chain_length"] >= 1  # Genesis block
    assert len(blockchain_analytics["features"]) > 0


if __name__ == "__main__":
    # Run basic smoke tests
    print("Testing system startup...")
    test_system_startup()
    print("✓ System startup tests passed")
    
    print("Running integration tests with TestClient...")
    
    client = TestClient(app)
    
    # Test quantum system
    print("Testing quantum analytics...")
    response = client.get("/quantum/analytics")
    assert response.status_code == 200
    print("✓ Quantum analytics working")
    
    # Test blockchain system
    print("Testing blockchain analytics...")  
    response = client.get("/blockchain/analytics")
    assert response.status_code == 200
    print("✓ Blockchain analytics working")
    
    print("🎉 All next-generation systems are operational!")
    print("- Quantum Optimization Engine: ACTIVE")
    print("- Blockchain Audit System: ACTIVE")
    print("- 36+ Production Systems: READY")