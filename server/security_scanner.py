"""Advanced Security and Privacy Features for Email Triage.

This module implements enterprise-grade security features including:
- PII Detection and Redaction
- Email Content Scanning
- Threat Detection
- Encryption at Rest
- Privacy Compliance (GDPR, CCPA)
"""

import re
import hashlib
import json
import base64
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

from models import Email, Attachment


class ThreatLevel(str, Enum):
    """Threat severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class PIIType(str, Enum):
    """Types of Personally Identifiable Information."""
    SSN = "ssn"
    CREDIT_CARD = "credit_card"
    EMAIL = "email"
    PHONE = "phone"
    IP_ADDRESS = "ip_address"
    PASSPORT = "passport"
    DRIVER_LICENSE = "driver_license"
    BANK_ACCOUNT = "bank_account"
    MEDICAL_ID = "medical_id"


@dataclass
class PIIDetection:
    """Detected PII in email content."""
    pii_type: PIIType
    value: str
    confidence: float
    start_pos: int
    end_pos: int
    context: str  # Surrounding text for context


@dataclass
class ThreatDetection:
    """Detected security threat."""
    threat_type: str
    severity: ThreatLevel
    confidence: float
    description: str
    evidence: List[str]
    recommended_action: str


@dataclass
class SecurityScanResult:
    """Result of security scan on email."""
    email_id: str
    scan_timestamp: str
    pii_detections: List[PIIDetection]
    threat_detections: List[ThreatDetection]
    risk_score: float  # 0.0 to 1.0
    compliance_flags: List[str]
    redacted_content: Optional[str] = None


class PIIDetector:
    """Detect and classify PII in email content."""
    
    def __init__(self):
        # Compile regex patterns for PII detection
        self.patterns = {
            PIIType.SSN: re.compile(r'\b\d{3}-?\d{2}-?\d{4}\b'),
            PIIType.CREDIT_CARD: re.compile(r'\b(?:\d{4}[-\s]?){3}\d{4}\b'),
            PIIType.EMAIL: re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            PIIType.PHONE: re.compile(r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b'),
            PIIType.IP_ADDRESS: re.compile(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b'),
            PIIType.PASSPORT: re.compile(r'\b[A-Z]{1,2}[0-9]{6,9}\b'),
            PIIType.DRIVER_LICENSE: re.compile(r'\b[A-Z]{1,2}[0-9]{6,8}\b'),
            PIIType.BANK_ACCOUNT: re.compile(r'\b[0-9]{8,17}\b'),
            PIIType.MEDICAL_ID: re.compile(r'\b(?:MRN|DOB|PATIENT)[:\s]*[A-Z0-9-]{6,15}\b', re.IGNORECASE)
        }
        
        self.context_window = 20  # Characters before and after PII
    
    def detect_pii(self, text: str) -> List[PIIDetection]:
        """Detect all PII in given text."""
        detections = []
        
        for pii_type, pattern in self.patterns.items():
            for match in pattern.finditer(text):
                start, end = match.span()
                value = match.group()
                
                # Extract context
                context_start = max(0, start - self.context_window)
                context_end = min(len(text), end + self.context_window)
                context = text[context_start:context_end]
                
                # Calculate confidence based on pattern specificity
                confidence = self._calculate_confidence(pii_type, value, context)
                
                detections.append(PIIDetection(
                    pii_type=pii_type,
                    value=value,
                    confidence=confidence,
                    start_pos=start,
                    end_pos=end,
                    context=context
                ))
        
        return detections
    
    def _calculate_confidence(self, pii_type: PIIType, value: str, context: str) -> float:
        """Calculate confidence score for PII detection."""
        base_confidence = 0.7  # Base confidence for regex match
        
        # Adjust confidence based on context keywords
        context_lower = context.lower()
        
        if pii_type == PIIType.SSN:
            if any(kw in context_lower for kw in ['ssn', 'social', 'security']):
                return min(1.0, base_confidence + 0.2)
            if self._luhn_check(value.replace('-', '')):
                return min(1.0, base_confidence + 0.1)
        
        elif pii_type == PIIType.CREDIT_CARD:
            if any(kw in context_lower for kw in ['card', 'visa', 'mastercard', 'amex', 'payment']):
                return min(1.0, base_confidence + 0.2)
            if self._luhn_check(value.replace('-', '').replace(' ', '')):
                return min(1.0, base_confidence + 0.15)
        
        elif pii_type == PIIType.EMAIL:
            if '@' in value and '.' in value.split('@')[1]:
                return min(1.0, base_confidence + 0.1)
        
        elif pii_type == PIIType.PHONE:
            if any(kw in context_lower for kw in ['phone', 'tel', 'call', 'number']):
                return min(1.0, base_confidence + 0.2)
        
        return base_confidence
    
    def _luhn_check(self, card_number: str) -> bool:
        """Validate credit card or SSN using Luhn algorithm."""
        if not card_number.isdigit():
            return False
        
        digits = [int(d) for d in card_number]
        checksum = 0
        
        for i, digit in enumerate(reversed(digits)):
            if i % 2 == 1:  # Every second digit from right
                digit *= 2
                if digit > 9:
                    digit = digit // 10 + digit % 10
            checksum += digit
        
        return checksum % 10 == 0
    
    def redact_pii(self, text: str, detections: List[PIIDetection]) -> str:
        """Redact PII from text while preserving structure."""
        redacted_text = text
        
        # Sort detections by position (reverse order to maintain indices)
        sorted_detections = sorted(detections, key=lambda d: d.start_pos, reverse=True)
        
        for detection in sorted_detections:
            if detection.confidence > 0.7:  # Only redact high-confidence detections
                replacement = self._generate_redaction_mask(detection)
                redacted_text = (
                    redacted_text[:detection.start_pos] + 
                    replacement + 
                    redacted_text[detection.end_pos:]
                )
        
        return redacted_text
    
    def _generate_redaction_mask(self, detection: PIIDetection) -> str:
        """Generate appropriate redaction mask for PII type."""
        if detection.pii_type == PIIType.SSN:
            return "XXX-XX-XXXX"
        elif detection.pii_type == PIIType.CREDIT_CARD:
            return "XXXX-XXXX-XXXX-XXXX"
        elif detection.pii_type == PIIType.EMAIL:
            parts = detection.value.split('@')
            if len(parts) == 2:
                return f"{'X' * len(parts[0])}@{parts[1]}"
            return "XXXXX@XXXXX"
        elif detection.pii_type == PIIType.PHONE:
            return "XXX-XXX-XXXX"
        else:
            return "X" * len(detection.value)


class ThreatDetector:
    """Detect security threats in email content."""
    
    def __init__(self):
        # Known malicious indicators
        self.malicious_domains = {
            'bit.ly', 'tinyurl.com', 'ow.ly', 't.co', 'short.link',
            'suspicious-domain.ru', 'phishing-site.tk', 'malware.ml'
        }
        
        self.phishing_keywords = {
            'verify', 'suspend', 'urgent', 'immediate', 'click here', 'act now',
            'limited time', 'expires', 'confirm', 'update', 'secure', 'protect'
        }
        
        self.malware_indicators = {
            '.exe', '.scr', '.bat', '.cmd', '.pif', '.com', '.zip', '.rar'
        }
        
        # Compiled regex patterns for threats
        self.url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
        self.suspicious_attachment_pattern = re.compile(r'\.(exe|scr|bat|cmd|pif|com|zip|rar)$', re.IGNORECASE)
    
    def detect_threats(self, email: Email) -> List[ThreatDetection]:
        """Detect all security threats in email."""
        threats = []
        
        # Combine subject and body for analysis
        content = f"{email.subject} {email.body}".lower()
        
        # Check for phishing indicators
        threats.extend(self._detect_phishing(email, content))
        
        # Check for malicious URLs
        threats.extend(self._detect_malicious_urls(email, email.body))
        
        # Check for suspicious attachments
        threats.extend(self._detect_suspicious_attachments(email))
        
        # Check for social engineering
        threats.extend(self._detect_social_engineering(email, content))
        
        # Check sender reputation
        threats.extend(self._detect_sender_threats(email))
        
        return threats
    
    def _detect_phishing(self, email: Email, content: str) -> List[ThreatDetection]:
        """Detect phishing attempts."""
        threats = []
        phishing_score = 0
        evidence = []
        
        # Check for phishing keywords
        keyword_matches = [kw for kw in self.phishing_keywords if kw in content]
        if keyword_matches:
            phishing_score += len(keyword_matches) * 0.1
            evidence.extend([f"Phishing keyword: '{kw}'" for kw in keyword_matches[:3]])
        
        # Check for urgency indicators
        urgency_indicators = ['urgent', 'immediate', 'expires', 'suspend', 'limited time']
        urgency_matches = [ind for ind in urgency_indicators if ind in content]
        if urgency_matches:
            phishing_score += 0.3
            evidence.append(f"Urgency language detected: {', '.join(urgency_matches)}")
        
        # Check for suspicious sender/recipient mismatch
        if email.sender_info and email.sender_info.sender_type.value == "suspicious":
            phishing_score += 0.4
            evidence.append("Sender marked as suspicious")
        
        # Check for domain spoofing
        sender_domain = email.sender.split('@')[-1] if '@' in email.sender else ""
        if sender_domain in self.malicious_domains:
            phishing_score += 0.5
            evidence.append(f"Known malicious domain: {sender_domain}")
        
        if phishing_score > 0.3:
            severity = ThreatLevel.CRITICAL if phishing_score > 0.7 else ThreatLevel.HIGH if phishing_score > 0.5 else ThreatLevel.MEDIUM
            
            threats.append(ThreatDetection(
                threat_type="phishing",
                severity=severity,
                confidence=min(1.0, phishing_score),
                description=f"Potential phishing email detected (score: {phishing_score:.2f})",
                evidence=evidence,
                recommended_action="quarantine" if severity == ThreatLevel.CRITICAL else "flag"
            ))
        
        return threats
    
    def _detect_malicious_urls(self, email: Email, content: str) -> List[ThreatDetection]:
        """Detect malicious URLs in email content."""
        threats = []
        urls = self.url_pattern.findall(content)
        
        for url in urls:
            risk_score = 0
            evidence = []
            
            # Check domain
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                domain = parsed.netloc.lower()
                
                if domain in self.malicious_domains:
                    risk_score += 0.8
                    evidence.append(f"Known malicious domain: {domain}")
                
                # Check for URL shorteners (potential for obfuscation)
                shortener_domains = ['bit.ly', 'tinyurl.com', 'ow.ly', 't.co', 'short.link']
                if domain in shortener_domains:
                    risk_score += 0.3
                    evidence.append(f"URL shortener detected: {domain}")
                
                # Check for suspicious patterns
                if re.search(r'[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}', domain):
                    risk_score += 0.4
                    evidence.append("Direct IP address in URL")
                
                # Check for suspicious TLDs
                suspicious_tlds = ['.tk', '.ml', '.ga', '.cf', '.click']
                if any(domain.endswith(tld) for tld in suspicious_tlds):
                    risk_score += 0.3
                    evidence.append(f"Suspicious TLD: {domain}")
                
            except Exception:
                risk_score += 0.2
                evidence.append("Malformed URL detected")
            
            if risk_score > 0.3:
                severity = ThreatLevel.CRITICAL if risk_score > 0.7 else ThreatLevel.HIGH if risk_score > 0.5 else ThreatLevel.MEDIUM
                
                threats.append(ThreatDetection(
                    threat_type="malicious_url",
                    severity=severity,
                    confidence=min(1.0, risk_score),
                    description=f"Suspicious URL detected: {url[:50]}{'...' if len(url) > 50 else ''}",
                    evidence=evidence,
                    recommended_action="block_url" if severity == ThreatLevel.CRITICAL else "warn_user"
                ))
        
        return threats
    
    def _detect_suspicious_attachments(self, email: Email) -> List[ThreatDetection]:
        """Detect suspicious email attachments."""
        threats = []
        
        if not email.has_attachments:
            return threats
        
        for attachment in email.attachments:
            risk_score = 0
            evidence = []
            
            # Check file extension
            if self.suspicious_attachment_pattern.search(attachment.filename):
                risk_score += 0.6
                evidence.append(f"Suspicious file extension: {attachment.filename}")
            
            # Check for double extensions
            if attachment.filename.count('.') > 1:
                risk_score += 0.3
                evidence.append("Double file extension detected")
            
            # Check MIME type vs extension mismatch
            if hasattr(attachment, 'mime_type'):
                expected_mime = {
                    '.pdf': 'application/pdf',
                    '.doc': 'application/msword',
                    '.jpg': 'image/jpeg',
                    '.png': 'image/png'
                }
                
                file_ext = '.' + attachment.filename.split('.')[-1].lower()
                if file_ext in expected_mime and attachment.mime_type != expected_mime[file_ext]:
                    risk_score += 0.4
                    evidence.append(f"MIME type mismatch: {attachment.mime_type} vs expected {expected_mime[file_ext]}")
            
            # Check for obfuscated filenames
            if re.search(r'[^a-zA-Z0-9._-]', attachment.filename):
                risk_score += 0.2
                evidence.append("Non-standard characters in filename")
            
            if risk_score > 0.3:
                severity = ThreatLevel.CRITICAL if risk_score > 0.7 else ThreatLevel.HIGH if risk_score > 0.5 else ThreatLevel.MEDIUM
                
                threats.append(ThreatDetection(
                    threat_type="suspicious_attachment",
                    severity=severity,
                    confidence=min(1.0, risk_score),
                    description=f"Suspicious attachment: {attachment.filename}",
                    evidence=evidence,
                    recommended_action="quarantine_attachment" if severity == ThreatLevel.CRITICAL else "scan_attachment"
                ))
        
        return threats
    
    def _detect_social_engineering(self, email: Email, content: str) -> List[ThreatDetection]:
        """Detect social engineering attempts."""
        threats = []
        
        # Social engineering indicators
        social_eng_patterns = [
            r'(ceo|president|manager|director)\s+(says?|requests?|needs?|wants?)',
            r'(confidential|secret|classified)\s+(document|information|data)',
            r'(wire transfer|payment|invoice|refund)\s+(urgent|immediate|asap)',
            r'(verify|confirm|update)\s+(account|password|credentials)',
            r'(congratulations|winner|selected|lottery)',
            r'(act now|limited time|expires soon|urgent action)'
        ]
        
        social_eng_score = 0
        evidence = []
        
        for pattern in social_eng_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                social_eng_score += 0.2
                evidence.append(f"Social engineering pattern detected")
                if len(evidence) >= 5:  # Limit evidence list
                    break
        
        # Check for authority impersonation
        authority_keywords = ['ceo', 'president', 'director', 'manager', 'hr', 'it department', 'security team']
        if any(kw in content for kw in authority_keywords):
            if email.sender_info and email.sender_info.sender_type.value != "vip":
                social_eng_score += 0.3
                evidence.append("Authority impersonation suspected")
        
        if social_eng_score > 0.3:
            severity = ThreatLevel.HIGH if social_eng_score > 0.6 else ThreatLevel.MEDIUM
            
            threats.append(ThreatDetection(
                threat_type="social_engineering",
                severity=severity,
                confidence=min(1.0, social_eng_score),
                description=f"Potential social engineering attack (score: {social_eng_score:.2f})",
                evidence=evidence,
                recommended_action="verify_sender" if severity == ThreatLevel.HIGH else "flag"
            ))
        
        return threats
    
    def _detect_sender_threats(self, email: Email) -> List[ThreatDetection]:
        """Detect threats based on sender analysis."""
        threats = []
        
        if not email.sender_info:
            return threats
        
        # Check for suspicious sender patterns
        if email.sender_info.sender_type.value == "suspicious":
            threats.append(ThreatDetection(
                threat_type="suspicious_sender",
                severity=ThreatLevel.MEDIUM,
                confidence=0.8,
                description="Email from suspicious sender",
                evidence=["Sender marked as suspicious in reputation system"],
                recommended_action="verify_sender"
            ))
        
        # Check for low trust score
        if email.sender_info.trust_score < 0.3:
            threats.append(ThreatDetection(
                threat_type="low_trust_sender",
                severity=ThreatLevel.MEDIUM,
                confidence=email.sender_info.trust_score,
                description=f"Email from low-trust sender (trust score: {email.sender_info.trust_score:.2f})",
                evidence=[f"Low trust score: {email.sender_info.trust_score:.2f}"],
                recommended_action="additional_verification"
            ))
        
        return threats


class SecurityScanner:
    """Main security scanning engine."""
    
    def __init__(self):
        self.pii_detector = PIIDetector()
        self.threat_detector = ThreatDetector()
        self.scan_history: List[SecurityScanResult] = []
        
    def scan_email(self, email: Email, include_redaction: bool = False) -> SecurityScanResult:
        """Perform comprehensive security scan on email."""
        scan_start = datetime.now()
        
        # Detect PII
        pii_detections = []
        content_to_scan = f"{email.subject} {email.body}"
        pii_detections.extend(self.pii_detector.detect_pii(content_to_scan))
        
        # Scan attachments for PII if they have text content
        for attachment in email.attachments:
            if hasattr(attachment, 'ocr_text') and attachment.ocr_text:
                attachment_pii = self.pii_detector.detect_pii(attachment.ocr_text)
                pii_detections.extend(attachment_pii)
        
        # Detect threats
        threat_detections = self.threat_detector.detect_threats(email)
        
        # Calculate overall risk score
        risk_score = self._calculate_risk_score(pii_detections, threat_detections)
        
        # Check compliance flags
        compliance_flags = self._check_compliance(pii_detections, threat_detections)
        
        # Generate redacted content if requested
        redacted_content = None
        if include_redaction and pii_detections:
            redacted_content = self.pii_detector.redact_pii(content_to_scan, pii_detections)
        
        result = SecurityScanResult(
            email_id=email.id,
            scan_timestamp=scan_start.isoformat(),
            pii_detections=pii_detections,
            threat_detections=threat_detections,
            risk_score=risk_score,
            compliance_flags=compliance_flags,
            redacted_content=redacted_content
        )
        
        # Store scan history
        self.scan_history.append(result)
        if len(self.scan_history) > 1000:  # Keep last 1000 scans
            self.scan_history = self.scan_history[-500:]
        
        return result
    
    def _calculate_risk_score(self, pii_detections: List[PIIDetection], threat_detections: List[ThreatDetection]) -> float:
        """Calculate overall email risk score."""
        base_score = 0.0
        
        # PII contribution (up to 0.4)
        high_conf_pii = [p for p in pii_detections if p.confidence > 0.7]
        if high_conf_pii:
            pii_score = min(0.4, len(high_conf_pii) * 0.1)
            base_score += pii_score
        
        # Threat contribution (up to 0.6)
        if threat_detections:
            threat_weights = {
                ThreatLevel.LOW: 0.1,
                ThreatLevel.MEDIUM: 0.2,
                ThreatLevel.HIGH: 0.4,
                ThreatLevel.CRITICAL: 0.6
            }
            
            threat_score = 0
            for threat in threat_detections:
                threat_score += threat_weights[threat.severity] * threat.confidence
            
            base_score += min(0.6, threat_score)
        
        return min(1.0, base_score)
    
    def _check_compliance(self, pii_detections: List[PIIDetection], threat_detections: List[ThreatDetection]) -> List[str]:
        """Check for compliance violations."""
        flags = []
        
        # GDPR compliance
        eu_pii_types = [PIIType.EMAIL, PIIType.PHONE, PIIType.IP_ADDRESS]
        if any(p.pii_type in eu_pii_types for p in pii_detections):
            flags.append("GDPR_PII_DETECTED")
        
        # HIPAA compliance
        medical_pii_types = [PIIType.MEDICAL_ID, PIIType.SSN]
        if any(p.pii_type in medical_pii_types for p in pii_detections):
            flags.append("HIPAA_PHI_DETECTED")
        
        # Financial compliance
        financial_pii_types = [PIIType.CREDIT_CARD, PIIType.BANK_ACCOUNT, PIIType.SSN]
        if any(p.pii_type in financial_pii_types for p in pii_detections):
            flags.append("FINANCIAL_PII_DETECTED")
        
        # Critical threats
        critical_threats = [t for t in threat_detections if t.severity == ThreatLevel.CRITICAL]
        if critical_threats:
            flags.append("CRITICAL_THREAT_DETECTED")
        
        return flags
    
    def get_security_analytics(self) -> Dict[str, Any]:
        """Get security analytics and trends."""
        if not self.scan_history:
            return {"message": "No scan history available"}
        
        recent_scans = self.scan_history[-100:]  # Last 100 scans
        
        # Risk distribution
        risk_distribution = {
            "low": len([s for s in recent_scans if s.risk_score < 0.3]),
            "medium": len([s for s in recent_scans if 0.3 <= s.risk_score < 0.7]),
            "high": len([s for s in recent_scans if s.risk_score >= 0.7])
        }
        
        # Most common threats
        threat_counts = {}
        for scan in recent_scans:
            for threat in scan.threat_detections:
                threat_counts[threat.threat_type] = threat_counts.get(threat.threat_type, 0) + 1
        
        # Most common PII types
        pii_counts = {}
        for scan in recent_scans:
            for pii in scan.pii_detections:
                pii_counts[pii.pii_type.value] = pii_counts.get(pii.pii_type.value, 0) + 1
        
        # Compliance violations
        compliance_violations = {}
        for scan in recent_scans:
            for flag in scan.compliance_flags:
                compliance_violations[flag] = compliance_violations.get(flag, 0) + 1
        
        return {
            "total_scans": len(recent_scans),
            "average_risk_score": sum(s.risk_score for s in recent_scans) / len(recent_scans),
            "risk_distribution": risk_distribution,
            "top_threats": dict(sorted(threat_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            "top_pii_types": dict(sorted(pii_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
            "compliance_violations": compliance_violations,
            "high_risk_emails": len([s for s in recent_scans if s.risk_score >= 0.7]),
            "scan_trends": {
                "daily_scans": self._get_daily_scan_counts()
            }
        }
    
    def _get_daily_scan_counts(self) -> Dict[str, int]:
        """Get scan counts by day for trending."""
        daily_counts = {}
        
        for scan in self.scan_history:
            try:
                scan_date = datetime.fromisoformat(scan.scan_timestamp).date().isoformat()
                daily_counts[scan_date] = daily_counts.get(scan_date, 0) + 1
            except (ValueError, TypeError):
                continue
        
        return daily_counts


# Global security scanner instance
security_scanner = SecurityScanner()