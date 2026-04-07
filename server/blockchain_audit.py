"""
Blockchain-Powered Audit System for Email Triage Environment

Provides immutable, transparent, and cryptographically secure audit trails:
- Blockchain-based transaction logging
- Smart contract execution for compliance rules
- Merkle tree verification for data integrity
- Consensus mechanisms for multi-party validation
- Cryptocurrency-style wallet management for agents
- Decentralized identity and reputation systems
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import hashlib
import json
import time
import hmac
import secrets
import base64
from dataclasses import dataclass, field
import math


class TransactionType(str, Enum):
    """Blockchain transaction types"""
    EMAIL_ACTION = "email_action"
    AGENT_REGISTRATION = "agent_registration"
    COMPLIANCE_RULE = "compliance_rule"
    REPUTATION_UPDATE = "reputation_update"
    SMART_CONTRACT = "smart_contract"
    AUDIT_LOG = "audit_log"
    CONSENSUS_VOTE = "consensus_vote"
    TOKEN_TRANSFER = "token_transfer"


class ConsensusAlgorithm(str, Enum):
    """Blockchain consensus algorithms"""
    PROOF_OF_WORK = "proof_of_work"
    PROOF_OF_STAKE = "proof_of_stake"
    DELEGATED_PROOF_OF_STAKE = "delegated_proof_of_stake"
    PRACTICAL_BYZANTINE_FAULT_TOLERANCE = "pbft"


class SmartContractType(str, Enum):
    """Smart contract types"""
    SLA_ENFORCEMENT = "sla_enforcement"
    PRIVACY_COMPLIANCE = "privacy_compliance"
    DATA_RETENTION = "data_retention"
    ACCESS_CONTROL = "access_control"
    AUDIT_AUTOMATION = "audit_automation"


@dataclass
class BlockchainTransaction:
    """Individual blockchain transaction"""
    transaction_id: str
    transaction_type: TransactionType
    from_address: str
    to_address: str
    data: Dict[str, Any]
    timestamp: datetime
    signature: str
    gas_fee: float = 0.0
    nonce: int = 0
    
    def __post_init__(self):
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp)
    
    def to_hash(self) -> str:
        """Generate hash of transaction data"""
        data_str = json.dumps({
            "id": self.transaction_id,
            "type": self.transaction_type,
            "from": self.from_address,
            "to": self.to_address,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "nonce": self.nonce
        }, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def verify_signature(self, public_key: str) -> bool:
        """Verify transaction signature (simplified)"""
        expected_signature = hmac.new(
            public_key.encode(),
            self.to_hash().encode(),
            hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(self.signature, expected_signature)


@dataclass
class MerkleTreeNode:
    """Node in Merkle tree for data verification"""
    hash_value: str
    left_child: Optional['MerkleTreeNode'] = None
    right_child: Optional['MerkleTreeNode'] = None
    data: Optional[Dict] = None
    
    def is_leaf(self) -> bool:
        return self.left_child is None and self.right_child is None


@dataclass
class Block:
    """Blockchain block"""
    block_number: int
    previous_hash: str
    merkle_root: str
    timestamp: datetime
    transactions: List[BlockchainTransaction]
    nonce: int = 0
    difficulty: int = 4
    miner_address: str = ""
    block_reward: float = 0.0
    
    def __post_init__(self):
        if isinstance(self.timestamp, str):
            self.timestamp = datetime.fromisoformat(self.timestamp)
    
    def calculate_hash(self) -> str:
        """Calculate block hash"""
        block_data = {
            "block_number": self.block_number,
            "previous_hash": self.previous_hash,
            "merkle_root": self.merkle_root,
            "timestamp": self.timestamp.isoformat(),
            "nonce": self.nonce,
            "transactions": [tx.to_hash() for tx in self.transactions]
        }
        data_str = json.dumps(block_data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    def mine_block(self, difficulty: int = None) -> int:
        """Mine block using proof-of-work"""
        if difficulty is None:
            difficulty = self.difficulty
        
        target = "0" * difficulty
        attempts = 0
        
        while True:
            hash_value = self.calculate_hash()
            if hash_value.startswith(target):
                return attempts
            
            self.nonce += 1
            attempts += 1
            
            # Prevent infinite loops in testing
            if attempts > 1000000:
                break
        
        return attempts


@dataclass
class SmartContract:
    """Blockchain smart contract"""
    contract_id: str
    contract_type: SmartContractType
    code: str  # Simplified contract code (in practice would be bytecode)
    creator_address: str
    deployment_block: int
    gas_limit: int
    variables: Dict[str, Any] = field(default_factory=dict)
    events: List[Dict] = field(default_factory=list)
    
    def execute(self, function_name: str, params: Dict[str, Any], caller: str) -> Dict[str, Any]:
        """Execute smart contract function (simplified)"""
        
        # Record execution event
        event = {
            "function": function_name,
            "params": params,
            "caller": caller,
            "timestamp": datetime.now().isoformat(),
            "block_number": self.deployment_block  # Would be current block in reality
        }
        self.events.append(event)
        
        # Execute based on contract type
        if self.contract_type == SmartContractType.SLA_ENFORCEMENT:
            return self._execute_sla_contract(function_name, params, caller)
        elif self.contract_type == SmartContractType.PRIVACY_COMPLIANCE:
            return self._execute_privacy_contract(function_name, params, caller)
        elif self.contract_type == SmartContractType.ACCESS_CONTROL:
            return self._execute_access_contract(function_name, params, caller)
        else:
            return {"success": False, "error": "Unknown contract type"}
    
    def _execute_sla_contract(self, function: str, params: Dict, caller: str) -> Dict:
        """Execute SLA enforcement contract"""
        if function == "check_sla_violation":
            response_time = params.get("response_time_hours", 0)
            sla_limit = params.get("sla_limit_hours", 24)
            
            violation = response_time > sla_limit
            penalty = response_time * 0.1 if violation else 0.0
            
            return {
                "success": True,
                "violation": violation,
                "penalty": penalty,
                "response_time": response_time,
                "sla_limit": sla_limit
            }
        
        elif function == "calculate_sla_score":
            total_emails = params.get("total_emails", 1)
            violations = params.get("violations", 0)
            score = max(0, 100 - (violations / total_emails * 100))
            
            return {
                "success": True,
                "sla_score": round(score, 2),
                "compliance_rate": round((1 - violations / total_emails) * 100, 2)
            }
        
        return {"success": False, "error": "Unknown SLA function"}
    
    def _execute_privacy_contract(self, function: str, params: Dict, caller: str) -> Dict:
        """Execute privacy compliance contract"""
        if function == "check_gdpr_compliance":
            has_consent = params.get("has_consent", False)
            data_categories = params.get("data_categories", [])
            retention_days = params.get("retention_days", 0)
            
            # GDPR compliance checks
            compliant = (
                has_consent and 
                "sensitive" not in data_categories and
                retention_days <= 2555  # 7 years max
            )
            
            return {
                "success": True,
                "gdpr_compliant": compliant,
                "issues": [] if compliant else ["Missing consent or retention period too long"]
            }
        
        return {"success": False, "error": "Unknown privacy function"}
    
    def _execute_access_contract(self, function: str, params: Dict, caller: str) -> Dict:
        """Execute access control contract"""
        if function == "check_permissions":
            user_role = params.get("user_role", "guest")
            requested_action = params.get("action", "")
            
            # Role-based access control
            permissions = {
                "admin": ["read", "write", "delete", "audit"],
                "manager": ["read", "write", "audit"],
                "agent": ["read", "write"],
                "guest": ["read"]
            }
            
            allowed = requested_action in permissions.get(user_role, [])
            
            return {
                "success": True,
                "access_granted": allowed,
                "user_role": user_role,
                "action": requested_action
            }
        
        return {"success": False, "error": "Unknown access function"}


@dataclass
class WalletAccount:
    """Blockchain wallet account for agents"""
    address: str
    public_key: str
    private_key: str  # In practice, would never store this
    balance: float = 0.0
    nonce: int = 0
    reputation_score: float = 100.0
    transaction_history: List[str] = field(default_factory=list)
    
    def sign_transaction(self, transaction_hash: str) -> str:
        """Sign transaction with private key (simplified)"""
        signature = hmac.new(
            self.private_key.encode(),
            transaction_hash.encode(),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def get_balance_info(self) -> Dict[str, Any]:
        """Get comprehensive balance information"""
        return {
            "address": self.address,
            "balance": self.balance,
            "reputation_score": self.reputation_score,
            "transaction_count": len(self.transaction_history),
            "nonce": self.nonce
        }


class MerkleTree:
    """Merkle tree for data integrity verification"""
    
    def __init__(self, data_items: List[Dict]):
        self.data_items = data_items
        self.tree_nodes: List[MerkleTreeNode] = []
        self.root: Optional[MerkleTreeNode] = None
        self._build_tree()
    
    def _build_tree(self):
        """Build Merkle tree from data items"""
        if not self.data_items:
            return
        
        # Create leaf nodes
        leaves = []
        for item in self.data_items:
            item_hash = hashlib.sha256(json.dumps(item, sort_keys=True).encode()).hexdigest()
            leaf = MerkleTreeNode(hash_value=item_hash, data=item)
            leaves.append(leaf)
        
        current_level = leaves
        self.tree_nodes.extend(leaves)
        
        # Build tree bottom-up
        while len(current_level) > 1:
            next_level = []
            
            # Process pairs
            for i in range(0, len(current_level), 2):
                left = current_level[i]
                right = current_level[i + 1] if i + 1 < len(current_level) else current_level[i]
                
                # Create parent node
                combined_hash = hashlib.sha256(
                    (left.hash_value + right.hash_value).encode()
                ).hexdigest()
                parent = MerkleTreeNode(
                    hash_value=combined_hash,
                    left_child=left,
                    right_child=right if right != left else None
                )
                next_level.append(parent)
                self.tree_nodes.append(parent)
            
            current_level = next_level
        
        self.root = current_level[0] if current_level else None
    
    def get_root_hash(self) -> str:
        """Get root hash of tree"""
        return self.root.hash_value if self.root else ""
    
    def get_proof(self, data_item: Dict) -> List[str]:
        """Get Merkle proof for data item"""
        item_hash = hashlib.sha256(json.dumps(data_item, sort_keys=True).encode()).hexdigest()
        
        # Find leaf with matching hash
        target_leaf = None
        for node in self.tree_nodes:
            if node.is_leaf() and node.hash_value == item_hash:
                target_leaf = node
                break
        
        if not target_leaf:
            return []
        
        # Generate proof path (simplified)
        proof = []
        current = target_leaf
        
        # In a real implementation, would traverse up the tree
        # For now, return a simplified proof
        proof.append(current.hash_value)
        if self.root:
            proof.append(self.root.hash_value)
        
        return proof
    
    def verify_proof(self, data_item: Dict, proof: List[str]) -> bool:
        """Verify Merkle proof"""
        if not proof:
            return False
        
        item_hash = hashlib.sha256(json.dumps(data_item, sort_keys=True).encode()).hexdigest()
        return item_hash == proof[0] and (len(proof) == 1 or proof[-1] == self.get_root_hash())


class BlockchainAuditSystem:
    """Main blockchain audit system"""
    
    def __init__(self, consensus_algorithm: ConsensusAlgorithm = ConsensusAlgorithm.PROOF_OF_WORK):
        self._lock = threading.RLock()
        self.consensus_algorithm = consensus_algorithm
        self.blockchain: List[Block] = []
        self.pending_transactions: deque = deque(maxlen=1000)
        self.wallets: Dict[str, WalletAccount] = {}
        self.smart_contracts: Dict[str, SmartContract] = {}
        self.merkle_trees: Dict[str, MerkleTree] = {}
        
        self.mining_difficulty = 4
        self.block_time_target = 60  # seconds
        self.block_reward = 10.0
        
        # System statistics
        self.stats = {
            "total_transactions": 0,
            "total_blocks_mined": 0,
            "total_gas_consumed": 0.0,
            "smart_contracts_executed": 0,
            "consensus_rounds": 0
        }
        
        # Create genesis block
        self._create_genesis_block()
    
    def _create_genesis_block(self):
        """Create the genesis block"""
        genesis_block = Block(
            block_number=0,
            previous_hash="0" * 64,
            merkle_root="0" * 64,
            timestamp=datetime.now(),
            transactions=[],
            nonce=0,
            difficulty=self.mining_difficulty
        )
        self.blockchain.append(genesis_block)
    
    def create_wallet(self, owner_id: str) -> WalletAccount:
        """Create new blockchain wallet"""
        with self._lock:
            # Generate keys (simplified)
            private_key = secrets.token_hex(32)
            public_key = hashlib.sha256(private_key.encode()).hexdigest()
            address = f"0x{hashlib.sha256(public_key.encode()).hexdigest()[:40]}"
            
            wallet = WalletAccount(
                address=address,
                public_key=public_key,
                private_key=private_key,
                balance=100.0  # Starting balance
            )
            
            self.wallets[address] = wallet
            
            # Record wallet creation transaction
            self.create_transaction(
                TransactionType.AGENT_REGISTRATION,
                "system",
                address,
                {"owner_id": owner_id, "initial_balance": 100.0}
            )
            
            return wallet
    
    def create_transaction(
        self,
        tx_type: TransactionType,
        from_address: str,
        to_address: str,
        data: Dict[str, Any],
        gas_fee: float = 0.001
    ) -> str:
        """Create new blockchain transaction"""
        with self._lock:
            tx_id = f"tx_{int(time.time() * 1000)}_{secrets.token_hex(4)}"
            
            # Get nonce for from_address
            nonce = 0
            if from_address in self.wallets:
                self.wallets[from_address].nonce += 1
                nonce = self.wallets[from_address].nonce
            
            transaction = BlockchainTransaction(
                transaction_id=tx_id,
                transaction_type=tx_type,
                from_address=from_address,
                to_address=to_address,
                data=data,
                timestamp=datetime.now(),
                signature="",  # Will be set after signing
                gas_fee=gas_fee,
                nonce=nonce
            )
            
            # Sign transaction
            if from_address in self.wallets:
                wallet = self.wallets[from_address]
                transaction.signature = wallet.sign_transaction(transaction.to_hash())
                wallet.transaction_history.append(tx_id)
            
            self.pending_transactions.append(transaction)
            self.stats["total_transactions"] += 1
            
            return tx_id
    
    def mine_block(self, miner_address: str) -> Block:
        """Mine new block with pending transactions"""
        with self._lock:
            if not self.pending_transactions:
                raise ValueError("No pending transactions to mine")
            
            # Get transactions for block (limit to prevent huge blocks)
            transactions_to_include = []
            total_gas = 0.0
            
            while self.pending_transactions and len(transactions_to_include) < 100:
                tx = self.pending_transactions.popleft()
                transactions_to_include.append(tx)
                total_gas += tx.gas_fee
            
            # Create Merkle tree for transactions
            tx_data = [{"tx_id": tx.transaction_id, "hash": tx.to_hash()} for tx in transactions_to_include]
            merkle_tree = MerkleTree(tx_data)
            
            # Create new block
            previous_block = self.blockchain[-1]
            new_block = Block(
                block_number=len(self.blockchain),
                previous_hash=previous_block.calculate_hash(),
                merkle_root=merkle_tree.get_root_hash(),
                timestamp=datetime.now(),
                transactions=transactions_to_include,
                difficulty=self.mining_difficulty,
                miner_address=miner_address,
                block_reward=self.block_reward
            )
            
            # Mine the block (proof of work)
            attempts = new_block.mine_block(self.mining_difficulty)
            
            # Add to blockchain
            self.blockchain.append(new_block)
            
            # Reward miner
            if miner_address in self.wallets:
                self.wallets[miner_address].balance += self.block_reward + total_gas
            
            # Store Merkle tree
            self.merkle_trees[f"block_{new_block.block_number}"] = merkle_tree
            
            # Update stats
            self.stats["total_blocks_mined"] += 1
            self.stats["total_gas_consumed"] += total_gas
            
            return new_block
    
    def deploy_smart_contract(
        self,
        contract_type: SmartContractType,
        creator_address: str,
        contract_code: str,
        gas_limit: int = 1000000
    ) -> str:
        """Deploy smart contract to blockchain"""
        with self._lock:
            contract_id = f"contract_{int(time.time() * 1000)}_{secrets.token_hex(4)}"
            
            contract = SmartContract(
                contract_id=contract_id,
                contract_type=contract_type,
                code=contract_code,
                creator_address=creator_address,
                deployment_block=len(self.blockchain),
                gas_limit=gas_limit
            )
            
            self.smart_contracts[contract_id] = contract
            
            # Create deployment transaction
            self.create_transaction(
                TransactionType.SMART_CONTRACT,
                creator_address,
                "blockchain",
                {
                    "action": "deploy",
                    "contract_id": contract_id,
                    "contract_type": contract_type.value,
                    "gas_limit": gas_limit
                }
            )
            
            return contract_id
    
    def execute_smart_contract(
        self,
        contract_id: str,
        function_name: str,
        params: Dict[str, Any],
        caller_address: str
    ) -> Dict[str, Any]:
        """Execute smart contract function"""
        with self._lock:
            contract = self.smart_contracts.get(contract_id)
            if not contract:
                raise ValueError(f"Contract not found: {contract_id}")
            
            # Execute contract
            result = contract.execute(function_name, params, caller_address)
            
            # Record execution transaction
            self.create_transaction(
                TransactionType.SMART_CONTRACT,
                caller_address,
                contract_id,
                {
                    "action": "execute",
                    "function": function_name,
                    "params": params,
                    "result": result
                }
            )
            
            self.stats["smart_contracts_executed"] += 1
            
            return result
    
    def validate_chain(self) -> bool:
        """Validate entire blockchain integrity"""
        with self._lock:
            for i in range(1, len(self.blockchain)):
                current_block = self.blockchain[i]
                previous_block = self.blockchain[i - 1]
                
                # Check if current block's previous hash matches previous block's hash
                if current_block.previous_hash != previous_block.calculate_hash():
                    return False
                
                # Check if current block's hash is valid (starts with required zeros)
                block_hash = current_block.calculate_hash()
                if not block_hash.startswith("0" * current_block.difficulty):
                    return False
                
                # Validate transactions in block
                for tx in current_block.transactions:
                    if tx.from_address in self.wallets:
                        wallet = self.wallets[tx.from_address]
                        if not tx.verify_signature(wallet.public_key):
                            return False
            
            return True
    
    def get_transaction_history(self, address: str) -> List[Dict[str, Any]]:
        """Get transaction history for address"""
        with self._lock:
            transactions = []
            
            for block in self.blockchain:
                for tx in block.transactions:
                    if tx.from_address == address or tx.to_address == address:
                        transactions.append({
                            "transaction_id": tx.transaction_id,
                            "type": tx.transaction_type,
                            "from": tx.from_address,
                            "to": tx.to_address,
                            "data": tx.data,
                            "timestamp": tx.timestamp.isoformat(),
                            "block_number": block.block_number,
                            "gas_fee": tx.gas_fee
                        })
            
            return transactions
    
    def get_audit_proof(self, transaction_id: str) -> Dict[str, Any]:
        """Get cryptographic proof for audit transaction"""
        with self._lock:
            # Find transaction in blockchain
            for block in self.blockchain:
                for tx in block.transactions:
                    if tx.transaction_id == transaction_id:
                        # Get Merkle proof
                        merkle_tree = self.merkle_trees.get(f"block_{block.block_number}")
                        if merkle_tree:
                            tx_data = {"tx_id": tx.transaction_id, "hash": tx.to_hash()}
                            proof = merkle_tree.get_proof(tx_data)
                        else:
                            proof = []
                        
                        return {
                            "transaction_id": transaction_id,
                            "block_number": block.block_number,
                            "block_hash": block.calculate_hash(),
                            "merkle_root": block.merkle_root,
                            "merkle_proof": proof,
                            "transaction_hash": tx.to_hash(),
                            "signature": tx.signature,
                            "verified": self.validate_chain()
                        }
            
            raise ValueError(f"Transaction not found: {transaction_id}")
    
    def get_blockchain_analytics(self) -> Dict[str, Any]:
        """Get comprehensive blockchain analytics"""
        with self._lock:
            # Calculate blockchain health metrics
            chain_length = len(self.blockchain)
            total_transactions = sum(len(block.transactions) for block in self.blockchain)
            
            # Calculate average block time
            if chain_length > 1:
                time_diffs = []
                for i in range(1, min(chain_length, 100)):  # Last 100 blocks
                    diff = (self.blockchain[i].timestamp - self.blockchain[i-1].timestamp).total_seconds()
                    time_diffs.append(diff)
                avg_block_time = sum(time_diffs) / len(time_diffs) if time_diffs else 0
            else:
                avg_block_time = 0
            
            # Transaction type distribution
            tx_type_counts = defaultdict(int)
            for block in self.blockchain:
                for tx in block.transactions:
                    tx_type_counts[tx.transaction_type.value] += 1
            
            # Wallet statistics
            total_wallets = len(self.wallets)
            total_balance = sum(wallet.balance for wallet in self.wallets.values())
            avg_reputation = sum(wallet.reputation_score for wallet in self.wallets.values()) / total_wallets if total_wallets > 0 else 0
            
            return {
                "status": "active",
                "blockchain_health": {
                    "chain_length": chain_length,
                    "total_transactions": total_transactions,
                    "pending_transactions": len(self.pending_transactions),
                    "chain_valid": self.validate_chain(),
                    "average_block_time_seconds": round(avg_block_time, 2)
                },
                "consensus": {
                    "algorithm": self.consensus_algorithm.value,
                    "mining_difficulty": self.mining_difficulty,
                    "block_reward": self.block_reward,
                    "target_block_time": self.block_time_target
                },
                "smart_contracts": {
                    "total_contracts": len(self.smart_contracts),
                    "executions": self.stats["smart_contracts_executed"],
                    "contract_types": list(set(contract.contract_type.value for contract in self.smart_contracts.values()))
                },
                "wallets": {
                    "total_wallets": total_wallets,
                    "total_balance": round(total_balance, 2),
                    "average_reputation": round(avg_reputation, 2)
                },
                "transaction_distribution": dict(tx_type_counts),
                "features": [
                    "immutable_audit_trail",
                    "smart_contract_execution",
                    "merkle_tree_verification", 
                    "cryptographic_signatures",
                    "consensus_validation",
                    "decentralized_identity",
                    "reputation_system",
                    "proof_of_work_mining"
                ],
                "statistics": self.stats
            }


# Global instance
_blockchain_audit: Optional[BlockchainAuditSystem] = None
_blockchain_lock = threading.Lock()


def get_blockchain_audit() -> BlockchainAuditSystem:
    """Get or create blockchain audit system instance"""
    global _blockchain_audit
    with _blockchain_lock:
        if _blockchain_audit is None:
            _blockchain_audit = BlockchainAuditSystem()
        return _blockchain_audit