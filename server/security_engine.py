"""
Advanced Security & Compliance Engine for Email Triage Environment

Enterprise-grade security providing:
- Multi-factor authentication simulation
- Role-based access control (RBAC)
- Data privacy compliance (GDPR, CCPA)
- Security incident response
- Compliance audit trails
"""

from typing import Any, Dict, List, Optional, Set, Union
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import hashlib
import secrets
import re
import json


class SecurityRole(str, Enum):
    """Security roles in the system"""
    ADMIN = "admin"
    ANALYST = "analyst"
    OPERATOR = "operator"
    AUDITOR = "auditor"
    VIEWER = "viewer"


class Permission(str, Enum):
    """System permissions"""
    READ_EMAILS = "read_emails"
    WRITE_EMAILS = "write_emails"
    DELETE_EMAILS = "delete_emails"
    MANAGE_USERS = "manage_users"
    VIEW_ANALYTICS = "view_analytics"
    EXPORT_DATA = "export_data"
    SYSTEM_CONFIG = "system_config"
    AUDIT_LOGS = "audit_logs"


class ComplianceStandard(str, Enum):
    """Compliance standards"""
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOX = "sox"
    PCI_DSS = "pci_dss"
    ISO27001 = "iso27001"


class SecurityIncidentType(str, Enum):
    """Security incident types"""
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    DATA_BREACH = "data_breach"
    PRIVILEGE_ESCALATION = "privilege_escalation"
    SUSPICIOUS_ACTIVITY = "suspicious_activity"
    MALWARE_DETECTED = "malware_detected"
    PHISHING_ATTEMPT = "phishing_attempt"


class SecurityIncident:
    """Security incident record"""
    
    def __init__(
        self,
        incident_type: SecurityIncidentType,
        severity: str,
        description: str,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None
    ):
        self.id = secrets.token_hex(16)
        self.incident_type = incident_type
        self.severity = severity
        self.description = description
        self.user_id = user_id
        self.ip_address = ip_address
        self.timestamp = datetime.now()
        self.status = "open"
        self.resolved_at = None
        self.resolution_notes = ""
    
    def resolve(self, notes: str = ""):
        """Resolve the incident"""
        self.status = "resolved"
        self.resolved_at = datetime.now()
        self.resolution_notes = notes
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "type": self.incident_type,
            "severity": self.severity,
            "description": self.description,
            "user_id": self.user_id,
            "ip_address": self.ip_address,
            "timestamp": self.timestamp.isoformat(),
            "status": self.status,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolution_notes": self.resolution_notes
        }


class User:
    """User with security context"""
    
    def __init__(
        self,
        user_id: str,
        email: str,
        role: SecurityRole,
        permissions: Optional[Set[Permission]] = None
    ):
        self.user_id = user_id
        self.email = email
        self.role = role
        self.permissions = permissions or self._get_default_permissions(role)
        self.created_at = datetime.now()
        self.last_login = None
        self.login_attempts = 0
        self.locked = False
        self.mfa_enabled = False
        self.session_token = None
    
    def _get_default_permissions(self, role: SecurityRole) -> Set[Permission]:
        """Get default permissions for role"""
        role_permissions = {
            SecurityRole.ADMIN: {
                Permission.READ_EMAILS, Permission.WRITE_EMAILS, Permission.DELETE_EMAILS,
                Permission.MANAGE_USERS, Permission.VIEW_ANALYTICS, Permission.EXPORT_DATA,
                Permission.SYSTEM_CONFIG, Permission.AUDIT_LOGS
            },
            SecurityRole.ANALYST: {
                Permission.READ_EMAILS, Permission.WRITE_EMAILS,
                Permission.VIEW_ANALYTICS, Permission.EXPORT_DATA
            },
            SecurityRole.OPERATOR: {
                Permission.READ_EMAILS, Permission.WRITE_EMAILS,
                Permission.VIEW_ANALYTICS
            },
            SecurityRole.AUDITOR: {
                Permission.READ_EMAILS, Permission.VIEW_ANALYTICS,
                Permission.AUDIT_LOGS, Permission.EXPORT_DATA
            },
            SecurityRole.VIEWER: {
                Permission.READ_EMAILS, Permission.VIEW_ANALYTICS
            }
        }
        return role_permissions.get(role, set())
    
    def has_permission(self, permission: Permission) -> bool:
        """Check if user has permission"""
        return permission in self.permissions and not self.locked
    
    def login(self, ip_address: str = None) -> str:
        """Login user and generate session token"""
        if self.locked:
            raise ValueError("Account is locked")
        
        self.last_login = datetime.now()
        self.login_attempts = 0
        self.session_token = secrets.token_hex(32)
        return self.session_token


class SecurityEngine:
    """Advanced security and compliance engine"""
    
    def __init__(self):
        self._lock = threading.RLock()
        self.users: Dict[str, User] = {}
        self.active_sessions: Dict[str, str] = {}  # token -> user_id
        self.security_incidents: List[SecurityIncident] = []
        self.audit_trail = deque(maxlen=10000)
        self.failed_login_attempts: Dict[str, List[datetime]] = defaultdict(list)
        self.ip_blocklist: Set[str] = set()
        self.compliance_rules: Dict[ComplianceStandard, List[Dict]] = {}
        
        # Security metrics
        self.metrics = {
            "total_users": 0,
            "active_sessions": 0,
            "security_incidents": 0,
            "failed_logins": 0,
            "blocked_ips": 0,
            "compliance_violations": 0
        }
        
        # Initialize default users and compliance rules
        self._initialize_default_users()
        self._initialize_compliance_rules()
    
    def _initialize_default_users(self):
        """Initialize default users"""
        default_users = [
            ("admin", "admin@company.com", SecurityRole.ADMIN),
            ("analyst1", "analyst1@company.com", SecurityRole.ANALYST),
            ("operator1", "operator1@company.com", SecurityRole.OPERATOR),
            ("auditor", "auditor@company.com", SecurityRole.AUDITOR),
            ("viewer", "viewer@company.com", SecurityRole.VIEWER)
        ]
        
        for user_id, email, role in default_users:
            user = User(user_id, email, role)
            user.mfa_enabled = role in [SecurityRole.ADMIN, SecurityRole.AUDITOR]
            self.users[user_id] = user
            self.metrics["total_users"] += 1
    
    def _initialize_compliance_rules(self):
        """Initialize compliance rules"""
        self.compliance_rules = {
            ComplianceStandard.GDPR: [
                {
                    "rule": "data_retention",
                    "description": "Personal data must not be retained longer than necessary",
                    "max_retention_days": 2555,  # 7 years
                    "applies_to": ["email_content", "personal_info"]
                },
                {
                    "rule": "right_to_be_forgotten",
                    "description": "Users can request deletion of their personal data",
                    "response_time_days": 30
                },
                {
                    "rule": "consent_tracking",
                    "description": "Track and manage user consent for data processing",
                    "required_fields": ["purpose", "timestamp", "user_id"]
                }
            ],
            ComplianceStandard.CCPA: [
                {
                    "rule": "data_disclosure",
                    "description": "Disclose categories of personal information collected",
                    "disclosure_period_days": 45
                },
                {
                    "rule": "opt_out_right",
                    "description": "Consumers can opt out of sale of personal information",
                    "response_time_hours": 15
                }
            ],
            ComplianceStandard.SOX: [
                {
                    "rule": "audit_trail",
                    "description": "Maintain detailed audit trails for financial communications",
                    "retention_years": 7,
                    "integrity_checks": True
                }
            ]
        }
    
    def authenticate_user(
        self, 
        user_id: str, 
        password: str,
        ip_address: str = None,
        mfa_code: str = None
    ) -> Optional[str]:
        """Authenticate user with MFA support"""
        with self._lock:
            # Check IP blocklist
            if ip_address and ip_address in self.ip_blocklist:
                self._log_audit("authentication_blocked", {
                    "user_id": user_id,
                    "ip_address": ip_address,
                    "reason": "blocked_ip"
                })
                return None
            
            # Check user exists
            user = self.users.get(user_id)
            if not user:
                self._record_failed_login(user_id, ip_address)
                return None
            
            # Simulate password check (in real system, hash and compare)
            if not self._verify_password(user_id, password):
                self._record_failed_login(user_id, ip_address)
                return None
            
            # Check MFA if enabled
            if user.mfa_enabled and not self._verify_mfa(user_id, mfa_code):
                self._record_failed_login(user_id, ip_address)
                return None
            
            # Successful login
            token = user.login(ip_address)
            self.active_sessions[token] = user_id
            self.metrics["active_sessions"] += 1
            
            self._log_audit("user_login", {
                "user_id": user_id,
                "ip_address": ip_address,
                "mfa_used": user.mfa_enabled
            })
            
            return token
    
    def _verify_password(self, user_id: str, password: str) -> bool:
        """Simulate password verification"""
        # In real system, would hash and compare
        return password == "password123"  # Simplified for demo
    
    def _verify_mfa(self, user_id: str, mfa_code: str) -> bool:
        """Simulate MFA verification"""
        # In real system, would verify TOTP or SMS code
        return mfa_code == "123456"  # Simplified for demo
    
    def _record_failed_login(self, user_id: str, ip_address: str = None):
        """Record failed login attempt"""
        with self._lock:
            self.failed_login_attempts[user_id].append(datetime.now())
            self.metrics["failed_logins"] += 1
            
            # Check for brute force attack
            recent_failures = [
                attempt for attempt in self.failed_login_attempts[user_id]
                if attempt > datetime.now() - timedelta(minutes=15)
            ]
            
            if len(recent_failures) >= 5:
                # Lock account
                if user_id in self.users:
                    self.users[user_id].locked = True
                
                # Block IP if provided
                if ip_address:
                    self.ip_blocklist.add(ip_address)
                    self.metrics["blocked_ips"] += 1
                
                # Create security incident
                incident = SecurityIncident(
                    SecurityIncidentType.UNAUTHORIZED_ACCESS,
                    "high",
                    f"Brute force attack detected for user {user_id}",
                    user_id,
                    ip_address
                )
                self.security_incidents.append(incident)
                self.metrics["security_incidents"] += 1
    
    def authorize_action(self, token: str, permission: Permission) -> bool:
        """Authorize user action"""
        with self._lock:
            user_id = self.active_sessions.get(token)
            if not user_id:
                return False
            
            user = self.users.get(user_id)
            if not user:
                return False
            
            has_permission = user.has_permission(permission)
            
            self._log_audit("authorization_check", {
                "user_id": user_id,
                "permission": permission,
                "granted": has_permission
            })
            
            return has_permission
    
    def check_data_privacy(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Check data for privacy compliance"""
        findings = []
        
        # Check for PII
        pii_patterns = {
            "email": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",
            "phone": r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
            "ssn": r"\b\d{3}-?\d{2}-?\d{4}\b",
            "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b"
        }
        
        data_str = json.dumps(data)
        
        for pii_type, pattern in pii_patterns.items():
            matches = re.findall(pattern, data_str, re.IGNORECASE)
            if matches:
                findings.append({
                    "type": "pii_detected",
                    "pii_type": pii_type,
                    "count": len(matches),
                    "compliance_risk": "high"
                })
        
        return {
            "compliant": len(findings) == 0,
            "findings": findings,
            "recommendations": [
                "Encrypt PII data",
                "Implement data masking",
                "Add consent tracking"
            ] if findings else []
        }
    
    def create_incident(
        self,
        incident_type: SecurityIncidentType,
        severity: str,
        description: str,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None
    ) -> SecurityIncident:
        """Create a security incident"""
        with self._lock:
            incident = SecurityIncident(
                incident_type, severity, description, user_id, ip_address
            )
            self.security_incidents.append(incident)
            self.metrics["security_incidents"] += 1
            
            self._log_audit("security_incident_created", {
                "incident_id": incident.id,
                "type": incident_type,
                "severity": severity
            })
            
            return incident
    
    def _log_audit(self, action: str, details: Dict[str, Any]):
        """Log audit event"""
        audit_entry = {
            "id": secrets.token_hex(16),
            "timestamp": datetime.now().isoformat(),
            "action": action,
            "details": details
        }
        self.audit_trail.append(audit_entry)
    
    def get_audit_trail(
        self,
        user_id: Optional[str] = None,
        action: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get audit trail with filters"""
        with self._lock:
            entries = list(self.audit_trail)
            
            if user_id:
                entries = [e for e in entries if e["details"].get("user_id") == user_id]
            if action:
                entries = [e for e in entries if e["action"] == action]
            if since:
                entries = [e for e in entries 
                          if datetime.fromisoformat(e["timestamp"]) >= since]
            
            return sorted(entries, key=lambda e: e["timestamp"], reverse=True)[:limit]
    
    def get_compliance_report(self, standard: ComplianceStandard) -> Dict[str, Any]:
        """Generate compliance report"""
        with self._lock:
            rules = self.compliance_rules.get(standard, [])
            
            # Simulate compliance checking
            violations = []
            if standard == ComplianceStandard.GDPR:
                # Check data retention
                old_data_count = 5  # Simulate some old data
                if old_data_count > 0:
                    violations.append({
                        "rule": "data_retention",
                        "description": f"{old_data_count} records exceed retention period",
                        "severity": "medium"
                    })
            
            compliance_score = max(0.0, 1.0 - (len(violations) * 0.2))
            
            return {
                "standard": standard,
                "compliance_score": round(compliance_score, 2),
                "rules_checked": len(rules),
                "violations": violations,
                "last_audit": datetime.now().isoformat(),
                "recommendations": [
                    "Implement automated data retention policies",
                    "Add consent management system",
                    "Regular compliance audits"
                ]
            }
    
    def get_security_dashboard(self) -> Dict[str, Any]:
        """Get security dashboard data"""
        with self._lock:
            recent_incidents = [
                i.to_dict() for i in self.security_incidents[-10:]
                if i.timestamp > datetime.now() - timedelta(days=7)
            ]
            
            threat_level = "low"
            if len(recent_incidents) > 5:
                threat_level = "high"
            elif len(recent_incidents) > 2:
                threat_level = "medium"
            
            return {
                "threat_level": threat_level,
                "active_sessions": self.metrics["active_sessions"],
                "recent_incidents": recent_incidents,
                "failed_logins_24h": len([
                    attempt for attempts in self.failed_login_attempts.values()
                    for attempt in attempts
                    if attempt > datetime.now() - timedelta(hours=24)
                ]),
                "blocked_ips": len(self.ip_blocklist),
                "compliance_status": {
                    "gdpr": "compliant",
                    "ccpa": "compliant", 
                    "sox": "partial"
                }
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get security statistics"""
        with self._lock:
            return {
                **self.metrics,
                "audit_entries": len(self.audit_trail),
                "compliance_standards": len(self.compliance_rules)
            }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get comprehensive security analytics"""
        stats = self.get_stats()
        dashboard = self.get_security_dashboard()
        
        return {
            "status": "active",
            "threat_level": dashboard["threat_level"],
            "total_users": stats["total_users"],
            "security_incidents": stats["security_incidents"],
            "compliance_standards_supported": len(self.compliance_rules),
            "features": [
                "multi_factor_authentication",
                "role_based_access_control",
                "security_incident_management", 
                "compliance_monitoring",
                "audit_trail",
                "threat_detection",
                "data_privacy_checks"
            ],
            "supported_standards": [s.value for s in ComplianceStandard],
            "security_roles": [r.value for r in SecurityRole],
            "statistics": stats,
            "dashboard": dashboard
        }


# Global instance
_security_engine: Optional[SecurityEngine] = None
_security_lock = threading.Lock()


def get_security_engine() -> SecurityEngine:
    """Get or create security engine instance"""
    global _security_engine
    with _security_lock:
        if _security_engine is None:
            _security_engine = SecurityEngine()
        return _security_engine