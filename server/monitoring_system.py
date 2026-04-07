"""
Advanced Monitoring System for Email Triage Environment.

Implements enterprise-grade monitoring with:
- Real-time health monitoring
- Proactive anomaly detection
- Predictive alerting
- Performance profiling
- SLA monitoring and tracking
- Resource utilization tracking
- Intelligent alert routing
- Auto-remediation triggers
"""

import hashlib
import json
import logging
import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
import secrets

logger = logging.getLogger(__name__)


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class AlertStatus(str, Enum):
    """Alert status values."""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"


class MetricType(str, Enum):
    """Types of metrics to monitor."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    RATE = "rate"
    PERCENTAGE = "percentage"


class HealthStatus(str, Enum):
    """System health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"


class MonitoringCategory(str, Enum):
    """Categories of monitoring."""
    SYSTEM = "system"
    APPLICATION = "application"
    SECURITY = "security"
    BUSINESS = "business"
    AI_OPERATIONS = "ai_operations"


@dataclass
class MetricDefinition:
    """Definition of a monitored metric."""
    metric_id: str
    name: str
    description: str
    metric_type: MetricType
    category: MonitoringCategory
    unit: str
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None
    comparison: str = "greater_than"  # greater_than, less_than, equals
    enabled: bool = True


@dataclass
class MetricDataPoint:
    """Individual metric data point."""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Alert:
    """System alert."""
    alert_id: str
    metric_id: str
    severity: AlertSeverity
    status: AlertStatus
    title: str
    description: str
    triggered_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved_by: Optional[str] = None
    value: Optional[float] = None
    threshold: Optional[float] = None
    labels: Dict[str, str] = field(default_factory=dict)
    remediation_actions: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "metric_id": self.metric_id,
            "severity": self.severity.value,
            "status": self.status.value,
            "title": self.title,
            "description": self.description,
            "triggered_at": self.triggered_at.isoformat(),
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "acknowledged_by": self.acknowledged_by,
            "resolved_by": self.resolved_by,
            "value": self.value,
            "threshold": self.threshold,
            "labels": self.labels,
            "remediation_actions": self.remediation_actions
        }


@dataclass
class SLADefinition:
    """Service Level Agreement definition."""
    sla_id: str
    name: str
    description: str
    metric_id: str
    target_value: float
    comparison: str  # greater_than, less_than
    measurement_window: timedelta
    measurement_unit: str
    penalty_threshold: float  # Percentage below target to trigger penalty
    enabled: bool = True


@dataclass
class SLAReport:
    """SLA compliance report."""
    sla_id: str
    period_start: datetime
    period_end: datetime
    target_value: float
    actual_value: float
    compliance_percentage: float
    is_compliant: bool
    violations: List[Dict[str, Any]]
    generated_at: datetime


@dataclass
class AnomalyDetection:
    """Anomaly detection result."""
    metric_id: str
    timestamp: datetime
    expected_value: float
    actual_value: float
    deviation_score: float
    is_anomaly: bool
    anomaly_type: str  # spike, drop, trend_change
    confidence: float
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class HealthCheckResult:
    """Health check result for a component."""
    component_name: str
    status: HealthStatus
    latency_ms: float
    last_check: datetime
    message: str
    details: Dict[str, Any] = field(default_factory=dict)


class AdvancedMonitoringSystem:
    """
    Enterprise-grade monitoring system with proactive alerting.
    
    Features:
    - Real-time metric collection and storage
    - Anomaly detection using statistical methods
    - Predictive alerting based on trends
    - SLA monitoring and compliance tracking
    - Intelligent alert routing and suppression
    - Auto-remediation triggers
    - Performance profiling
    """
    
    def __init__(self, environment_ref=None):
        """Initialize monitoring system."""
        self.environment_ref = environment_ref
        
        # Metric storage
        self.metrics: Dict[str, MetricDefinition] = {}
        self.metric_data: Dict[str, deque] = {}  # metric_id -> data points
        self.max_data_points = 10000
        
        # Alerting
        self.alerts: Dict[str, Alert] = {}
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        self.alert_handlers: List[Callable] = []
        self.suppression_rules: List[Dict[str, Any]] = []
        
        # SLA tracking
        self.slas: Dict[str, SLADefinition] = {}
        self.sla_reports: List[SLAReport] = []
        
        # Health monitoring
        self.health_checks: Dict[str, Callable] = {}
        self.health_results: Dict[str, HealthCheckResult] = {}
        self.health_history: deque = deque(maxlen=1000)
        
        # Anomaly detection
        self.anomaly_history: deque = deque(maxlen=1000)
        self.baseline_windows: Dict[str, deque] = {}  # For anomaly detection
        
        # Performance profiling
        self.profile_data: Dict[str, deque] = {}
        self.active_profiles: Dict[str, Dict[str, Any]] = {}
        
        # Analytics
        self.analytics = {
            "metrics_collected": 0,
            "alerts_triggered": 0,
            "alerts_resolved": 0,
            "anomalies_detected": 0,
            "sla_violations": 0,
            "health_checks_performed": 0
        }
        
        # Initialize default metrics and SLAs
        self._init_default_metrics()
        self._init_default_slas()
        self._init_default_health_checks()
        
        logger.info("Advanced Monitoring System initialized")
    
    def _init_default_metrics(self):
        """Initialize default system metrics."""
        default_metrics = [
            MetricDefinition(
                metric_id="response_time_ms",
                name="Response Time",
                description="API response time in milliseconds",
                metric_type=MetricType.GAUGE,
                category=MonitoringCategory.APPLICATION,
                unit="ms",
                warning_threshold=500,
                critical_threshold=2000
            ),
            MetricDefinition(
                metric_id="requests_per_second",
                name="Request Rate",
                description="Number of requests per second",
                metric_type=MetricType.RATE,
                category=MonitoringCategory.APPLICATION,
                unit="req/s",
                warning_threshold=1000,
                critical_threshold=5000
            ),
            MetricDefinition(
                metric_id="error_rate",
                name="Error Rate",
                description="Percentage of requests resulting in errors",
                metric_type=MetricType.PERCENTAGE,
                category=MonitoringCategory.APPLICATION,
                unit="%",
                warning_threshold=5,
                critical_threshold=10
            ),
            MetricDefinition(
                metric_id="emails_processed",
                name="Emails Processed",
                description="Total emails processed",
                metric_type=MetricType.COUNTER,
                category=MonitoringCategory.BUSINESS,
                unit="count"
            ),
            MetricDefinition(
                metric_id="ai_accuracy",
                name="AI Accuracy",
                description="AI categorization accuracy percentage",
                metric_type=MetricType.PERCENTAGE,
                category=MonitoringCategory.AI_OPERATIONS,
                unit="%",
                warning_threshold=90,
                critical_threshold=80,
                comparison="less_than"
            ),
            MetricDefinition(
                metric_id="ai_confidence",
                name="AI Confidence",
                description="Average AI prediction confidence",
                metric_type=MetricType.GAUGE,
                category=MonitoringCategory.AI_OPERATIONS,
                unit="score",
                warning_threshold=0.7,
                critical_threshold=0.5,
                comparison="less_than"
            ),
            MetricDefinition(
                metric_id="security_threats",
                name="Security Threats",
                description="Number of security threats detected",
                metric_type=MetricType.COUNTER,
                category=MonitoringCategory.SECURITY,
                unit="count",
                warning_threshold=5,
                critical_threshold=20
            ),
            MetricDefinition(
                metric_id="cpu_usage",
                name="CPU Usage",
                description="System CPU usage percentage",
                metric_type=MetricType.PERCENTAGE,
                category=MonitoringCategory.SYSTEM,
                unit="%",
                warning_threshold=70,
                critical_threshold=90
            ),
            MetricDefinition(
                metric_id="memory_usage",
                name="Memory Usage",
                description="System memory usage percentage",
                metric_type=MetricType.PERCENTAGE,
                category=MonitoringCategory.SYSTEM,
                unit="%",
                warning_threshold=75,
                critical_threshold=90
            ),
            MetricDefinition(
                metric_id="queue_depth",
                name="Queue Depth",
                description="Number of items in processing queue",
                metric_type=MetricType.GAUGE,
                category=MonitoringCategory.APPLICATION,
                unit="items",
                warning_threshold=100,
                critical_threshold=500
            ),
            MetricDefinition(
                metric_id="agent_consensus_time",
                name="Agent Consensus Time",
                description="Time for multi-agent consensus",
                metric_type=MetricType.GAUGE,
                category=MonitoringCategory.AI_OPERATIONS,
                unit="ms",
                warning_threshold=1000,
                critical_threshold=5000
            ),
            MetricDefinition(
                metric_id="prediction_latency",
                name="Prediction Latency",
                description="Time for AI predictions",
                metric_type=MetricType.GAUGE,
                category=MonitoringCategory.AI_OPERATIONS,
                unit="ms",
                warning_threshold=100,
                critical_threshold=500
            )
        ]
        
        for metric in default_metrics:
            self.register_metric(metric)
    
    def _init_default_slas(self):
        """Initialize default SLAs."""
        default_slas = [
            SLADefinition(
                sla_id="response_time_sla",
                name="Response Time SLA",
                description="99% of requests must respond within 2 seconds",
                metric_id="response_time_ms",
                target_value=2000,
                comparison="less_than",
                measurement_window=timedelta(hours=1),
                measurement_unit="ms",
                penalty_threshold=99.0
            ),
            SLADefinition(
                sla_id="availability_sla",
                name="Availability SLA",
                description="System must be available 99.9% of the time",
                metric_id="error_rate",
                target_value=0.1,
                comparison="less_than",
                measurement_window=timedelta(hours=24),
                measurement_unit="%",
                penalty_threshold=99.9
            ),
            SLADefinition(
                sla_id="ai_accuracy_sla",
                name="AI Accuracy SLA",
                description="AI must maintain 85% accuracy",
                metric_id="ai_accuracy",
                target_value=85.0,
                comparison="greater_than",
                measurement_window=timedelta(hours=24),
                measurement_unit="%",
                penalty_threshold=95.0
            )
        ]
        
        for sla in default_slas:
            self.register_sla(sla)
    
    def _init_default_health_checks(self):
        """Initialize default health checks."""
        # Environment health check
        def check_environment():
            if self.environment_ref and self.environment_ref._initialized:
                return HealthCheckResult(
                    component_name="environment",
                    status=HealthStatus.HEALTHY,
                    latency_ms=0.5,
                    last_check=datetime.now(),
                    message="Environment operational",
                    details={"email_count": len(self.environment_ref.emails) if hasattr(self.environment_ref, 'emails') else 0}
                )
            return HealthCheckResult(
                component_name="environment",
                status=HealthStatus.UNHEALTHY,
                latency_ms=0,
                last_check=datetime.now(),
                message="Environment not initialized"
            )
        
        # AI System health check
        def check_ai_system():
            try:
                if self.environment_ref and hasattr(self.environment_ref, 'agent_orchestrator'):
                    agent_count = len(self.environment_ref.agent_orchestrator.agents)
                    return HealthCheckResult(
                        component_name="ai_system",
                        status=HealthStatus.HEALTHY if agent_count > 0 else HealthStatus.DEGRADED,
                        latency_ms=1.0,
                        last_check=datetime.now(),
                        message=f"{agent_count} AI agents operational",
                        details={"agent_count": agent_count}
                    )
            except Exception as e:
                return HealthCheckResult(
                    component_name="ai_system",
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=0,
                    last_check=datetime.now(),
                    message=f"AI system error: {str(e)}"
                )
            return HealthCheckResult(
                component_name="ai_system",
                status=HealthStatus.UNHEALTHY,
                latency_ms=0,
                last_check=datetime.now(),
                message="AI system not available"
            )
        
        # Analytics health check
        def check_analytics():
            try:
                if self.environment_ref and hasattr(self.environment_ref, 'analytics_engine'):
                    dashboard_count = len(self.environment_ref.analytics_engine.dashboards)
                    return HealthCheckResult(
                        component_name="analytics",
                        status=HealthStatus.HEALTHY,
                        latency_ms=0.8,
                        last_check=datetime.now(),
                        message=f"Analytics operational with {dashboard_count} dashboards",
                        details={"dashboard_count": dashboard_count}
                    )
            except Exception as e:
                return HealthCheckResult(
                    component_name="analytics",
                    status=HealthStatus.DEGRADED,
                    latency_ms=0,
                    last_check=datetime.now(),
                    message=f"Analytics issue: {str(e)}"
                )
            return HealthCheckResult(
                component_name="analytics",
                status=HealthStatus.DEGRADED,
                latency_ms=0,
                last_check=datetime.now(),
                message="Analytics not fully configured"
            )
        
        self.register_health_check("environment", check_environment)
        self.register_health_check("ai_system", check_ai_system)
        self.register_health_check("analytics", check_analytics)
    
    def register_metric(self, metric: MetricDefinition):
        """Register a new metric for monitoring."""
        self.metrics[metric.metric_id] = metric
        self.metric_data[metric.metric_id] = deque(maxlen=self.max_data_points)
        self.baseline_windows[metric.metric_id] = deque(maxlen=100)  # For anomaly detection
        logger.debug(f"Registered metric: {metric.metric_id}")
    
    def record_metric(
        self,
        metric_id: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Record a metric data point.
        
        Args:
            metric_id: Metric identifier
            value: Metric value
            labels: Optional labels
            metadata: Optional metadata
        """
        if metric_id not in self.metrics:
            logger.warning(f"Unknown metric: {metric_id}")
            return
        
        data_point = MetricDataPoint(
            timestamp=datetime.now(),
            value=value,
            labels=labels or {},
            metadata=metadata or {}
        )
        
        self.metric_data[metric_id].append(data_point)
        self.baseline_windows[metric_id].append(value)
        self.analytics["metrics_collected"] += 1
        
        # Check thresholds and trigger alerts
        self._check_thresholds(metric_id, value)
        
        # Check for anomalies
        anomaly = self._detect_anomaly(metric_id, value)
        if anomaly and anomaly.is_anomaly:
            self.anomaly_history.append(anomaly)
            self.analytics["anomalies_detected"] += 1
            self._trigger_anomaly_alert(anomaly)
    
    def _check_thresholds(self, metric_id: str, value: float):
        """Check metric value against thresholds."""
        metric = self.metrics.get(metric_id)
        if not metric or not metric.enabled:
            return
        
        severity = None
        threshold = None
        
        # Determine severity based on thresholds
        if metric.comparison == "greater_than":
            if metric.critical_threshold and value >= metric.critical_threshold:
                severity = AlertSeverity.CRITICAL
                threshold = metric.critical_threshold
            elif metric.warning_threshold and value >= metric.warning_threshold:
                severity = AlertSeverity.HIGH
                threshold = metric.warning_threshold
        elif metric.comparison == "less_than":
            if metric.critical_threshold and value <= metric.critical_threshold:
                severity = AlertSeverity.CRITICAL
                threshold = metric.critical_threshold
            elif metric.warning_threshold and value <= metric.warning_threshold:
                severity = AlertSeverity.HIGH
                threshold = metric.warning_threshold
        
        if severity:
            self._trigger_alert(metric_id, severity, value, threshold)
    
    def _detect_anomaly(self, metric_id: str, value: float) -> Optional[AnomalyDetection]:
        """
        Detect anomalies using statistical methods.
        
        Uses Z-score based detection with adaptive thresholds.
        """
        baseline = list(self.baseline_windows.get(metric_id, []))
        
        if len(baseline) < 10:
            return None  # Not enough data
        
        # Calculate statistics
        mean = statistics.mean(baseline)
        stdev = statistics.stdev(baseline) if len(baseline) > 1 else 0
        
        if stdev == 0:
            return None
        
        # Calculate Z-score
        z_score = abs(value - mean) / stdev
        
        # Determine anomaly type
        anomaly_type = "normal"
        is_anomaly = False
        
        if z_score > 3.0:  # More than 3 standard deviations
            is_anomaly = True
            if value > mean:
                anomaly_type = "spike"
            else:
                anomaly_type = "drop"
        elif z_score > 2.0:
            # Check for trend change
            recent = baseline[-5:] if len(baseline) >= 5 else baseline
            if len(recent) >= 3:
                trend = recent[-1] - recent[0]
                if abs(trend) > stdev:
                    is_anomaly = True
                    anomaly_type = "trend_change"
        
        return AnomalyDetection(
            metric_id=metric_id,
            timestamp=datetime.now(),
            expected_value=mean,
            actual_value=value,
            deviation_score=z_score,
            is_anomaly=is_anomaly,
            anomaly_type=anomaly_type,
            confidence=min(1.0, z_score / 5.0),  # Normalize confidence
            context={
                "baseline_mean": mean,
                "baseline_stdev": stdev,
                "baseline_size": len(baseline)
            }
        )
    
    def _trigger_alert(
        self,
        metric_id: str,
        severity: AlertSeverity,
        value: float,
        threshold: float
    ):
        """Trigger an alert."""
        # Check suppression rules
        if self._is_suppressed(metric_id):
            return
        
        # Check if similar alert is already active
        active_key = f"{metric_id}_{severity.value}"
        if active_key in self.alerts and self.alerts[active_key].status == AlertStatus.ACTIVE:
            return  # Don't duplicate
        
        metric = self.metrics[metric_id]
        
        alert = Alert(
            alert_id=f"alert_{secrets.token_hex(8)}",
            metric_id=metric_id,
            severity=severity,
            status=AlertStatus.ACTIVE,
            title=f"{severity.value.upper()}: {metric.name} threshold exceeded",
            description=f"{metric.name} is {value} {metric.unit}, threshold is {threshold} {metric.unit}",
            triggered_at=datetime.now(),
            value=value,
            threshold=threshold,
            remediation_actions=self._get_remediation_actions(metric_id, severity)
        )
        
        self.alerts[active_key] = alert
        self.analytics["alerts_triggered"] += 1
        
        # Call alert handlers
        for handler in self.alert_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")
        
        logger.warning(f"Alert triggered: {alert.title}")
    
    def _trigger_anomaly_alert(self, anomaly: AnomalyDetection):
        """Trigger an alert for an anomaly."""
        severity = AlertSeverity.HIGH if anomaly.confidence > 0.8 else AlertSeverity.MEDIUM
        
        alert = Alert(
            alert_id=f"anomaly_{secrets.token_hex(8)}",
            metric_id=anomaly.metric_id,
            severity=severity,
            status=AlertStatus.ACTIVE,
            title=f"Anomaly Detected: {anomaly.anomaly_type} in {anomaly.metric_id}",
            description=(
                f"Value {anomaly.actual_value} deviates {anomaly.deviation_score:.2f} "
                f"standard deviations from expected {anomaly.expected_value:.2f}"
            ),
            triggered_at=anomaly.timestamp,
            value=anomaly.actual_value,
            labels={"anomaly_type": anomaly.anomaly_type},
            remediation_actions=[
                "Investigate recent changes",
                "Check for external factors",
                "Review system logs"
            ]
        )
        
        self.alerts[alert.alert_id] = alert
        logger.warning(f"Anomaly alert: {alert.title}")
    
    def _get_remediation_actions(self, metric_id: str, severity: AlertSeverity) -> List[str]:
        """Get recommended remediation actions."""
        actions = []
        
        if metric_id == "response_time_ms":
            actions = [
                "Check system load and resource utilization",
                "Review recent deployments",
                "Scale horizontally if needed"
            ]
        elif metric_id == "error_rate":
            actions = [
                "Check application logs for errors",
                "Verify external dependencies",
                "Review recent code changes"
            ]
        elif metric_id == "ai_accuracy":
            actions = [
                "Review AI model performance",
                "Check training data quality",
                "Consider model retraining"
            ]
        elif metric_id == "security_threats":
            actions = [
                "Review security logs immediately",
                "Check for unauthorized access",
                "Consider enabling additional security measures"
            ]
        elif metric_id in ["cpu_usage", "memory_usage"]:
            actions = [
                "Identify resource-intensive processes",
                "Consider scaling resources",
                "Optimize application performance"
            ]
        
        if severity == AlertSeverity.CRITICAL:
            actions.insert(0, "IMMEDIATE ACTION REQUIRED")
        
        return actions
    
    def _is_suppressed(self, metric_id: str) -> bool:
        """Check if alerts for this metric are suppressed."""
        now = datetime.now()
        for rule in self.suppression_rules:
            if rule.get("metric_id") == metric_id or rule.get("metric_id") == "*":
                if rule.get("until") and now < rule["until"]:
                    return True
        return False
    
    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert."""
        for key, alert in self.alerts.items():
            if alert.alert_id == alert_id and alert.status == AlertStatus.ACTIVE:
                alert.status = AlertStatus.ACKNOWLEDGED
                alert.acknowledged_at = datetime.now()
                alert.acknowledged_by = acknowledged_by
                logger.info(f"Alert acknowledged: {alert_id} by {acknowledged_by}")
                return True
        return False
    
    def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """Resolve an alert."""
        for key, alert in self.alerts.items():
            if alert.alert_id == alert_id and alert.status in [AlertStatus.ACTIVE, AlertStatus.ACKNOWLEDGED]:
                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.now()
                alert.resolved_by = resolved_by
                self.analytics["alerts_resolved"] += 1
                logger.info(f"Alert resolved: {alert_id} by {resolved_by}")
                return True
        return False
    
    def suppress_alerts(
        self,
        metric_id: str,
        duration_minutes: int,
        reason: str
    ) -> Dict[str, Any]:
        """Temporarily suppress alerts for a metric."""
        until = datetime.now() + timedelta(minutes=duration_minutes)
        rule = {
            "metric_id": metric_id,
            "until": until,
            "reason": reason,
            "created_at": datetime.now().isoformat()
        }
        self.suppression_rules.append(rule)
        
        logger.info(f"Suppressed alerts for {metric_id} until {until}")
        return {"suppression_until": until.isoformat(), "metric_id": metric_id}
    
    def register_sla(self, sla: SLADefinition):
        """Register an SLA for monitoring."""
        self.slas[sla.sla_id] = sla
        logger.debug(f"Registered SLA: {sla.sla_id}")
    
    def check_sla_compliance(self, sla_id: str) -> SLAReport:
        """Check compliance for a specific SLA."""
        sla = self.slas.get(sla_id)
        if not sla:
            raise ValueError(f"Unknown SLA: {sla_id}")
        
        now = datetime.now()
        period_start = now - sla.measurement_window
        
        # Get metric data for the period
        metric_data = self.metric_data.get(sla.metric_id, deque())
        period_data = [
            dp.value for dp in metric_data
            if dp.timestamp >= period_start
        ]
        
        if not period_data:
            return SLAReport(
                sla_id=sla_id,
                period_start=period_start,
                period_end=now,
                target_value=sla.target_value,
                actual_value=0,
                compliance_percentage=100.0,
                is_compliant=True,
                violations=[],
                generated_at=now
            )
        
        # Calculate compliance
        if sla.comparison == "less_than":
            compliant_count = sum(1 for v in period_data if v < sla.target_value)
        else:
            compliant_count = sum(1 for v in period_data if v > sla.target_value)
        
        compliance_percentage = (compliant_count / len(period_data)) * 100
        is_compliant = compliance_percentage >= sla.penalty_threshold
        
        # Identify violations
        violations = []
        for dp in metric_data:
            if dp.timestamp >= period_start:
                if sla.comparison == "less_than" and dp.value >= sla.target_value:
                    violations.append({
                        "timestamp": dp.timestamp.isoformat(),
                        "value": dp.value,
                        "threshold": sla.target_value
                    })
                elif sla.comparison == "greater_than" and dp.value <= sla.target_value:
                    violations.append({
                        "timestamp": dp.timestamp.isoformat(),
                        "value": dp.value,
                        "threshold": sla.target_value
                    })
        
        if not is_compliant:
            self.analytics["sla_violations"] += 1
        
        report = SLAReport(
            sla_id=sla_id,
            period_start=period_start,
            period_end=now,
            target_value=sla.target_value,
            actual_value=statistics.mean(period_data) if period_data else 0,
            compliance_percentage=compliance_percentage,
            is_compliant=is_compliant,
            violations=violations[:50],  # Limit
            generated_at=now
        )
        
        self.sla_reports.append(report)
        return report
    
    def register_health_check(self, component_name: str, check_func: Callable):
        """Register a health check function."""
        self.health_checks[component_name] = check_func
    
    def perform_health_checks(self) -> Dict[str, HealthCheckResult]:
        """Perform all registered health checks."""
        results = {}
        
        for component_name, check_func in self.health_checks.items():
            try:
                start_time = time.time()
                result = check_func()
                result.latency_ms = (time.time() - start_time) * 1000
                results[component_name] = result
            except Exception as e:
                results[component_name] = HealthCheckResult(
                    component_name=component_name,
                    status=HealthStatus.UNHEALTHY,
                    latency_ms=0,
                    last_check=datetime.now(),
                    message=f"Health check failed: {str(e)}"
                )
            
            self.health_results[component_name] = results[component_name]
            self.analytics["health_checks_performed"] += 1
        
        # Record health history
        self.health_history.append({
            "timestamp": datetime.now().isoformat(),
            "results": {k: v.status.value for k, v in results.items()}
        })
        
        return results
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health status."""
        results = self.perform_health_checks()
        
        # Determine overall status
        statuses = [r.status for r in results.values()]
        
        if HealthStatus.CRITICAL in statuses:
            overall_status = HealthStatus.CRITICAL
        elif HealthStatus.UNHEALTHY in statuses:
            overall_status = HealthStatus.UNHEALTHY
        elif HealthStatus.DEGRADED in statuses:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
        
        return {
            "overall_status": overall_status.value,
            "components": {
                name: {
                    "status": result.status.value,
                    "latency_ms": result.latency_ms,
                    "message": result.message,
                    "details": result.details
                }
                for name, result in results.items()
            },
            "checked_at": datetime.now().isoformat()
        }
    
    def start_profile(self, profile_name: str) -> str:
        """Start a performance profile."""
        profile_id = f"profile_{secrets.token_hex(4)}_{profile_name}"
        self.active_profiles[profile_id] = {
            "name": profile_name,
            "started_at": time.time(),
            "checkpoints": []
        }
        return profile_id
    
    def checkpoint_profile(self, profile_id: str, checkpoint_name: str):
        """Add a checkpoint to a profile."""
        if profile_id in self.active_profiles:
            elapsed = time.time() - self.active_profiles[profile_id]["started_at"]
            self.active_profiles[profile_id]["checkpoints"].append({
                "name": checkpoint_name,
                "elapsed_ms": elapsed * 1000
            })
    
    def end_profile(self, profile_id: str) -> Dict[str, Any]:
        """End a performance profile and get results."""
        if profile_id not in self.active_profiles:
            return {}
        
        profile = self.active_profiles.pop(profile_id)
        total_time = (time.time() - profile["started_at"]) * 1000
        
        result = {
            "profile_id": profile_id,
            "name": profile["name"],
            "total_time_ms": total_time,
            "checkpoints": profile["checkpoints"]
        }
        
        # Store in history
        if profile["name"] not in self.profile_data:
            self.profile_data[profile["name"]] = deque(maxlen=100)
        self.profile_data[profile["name"]].append(result)
        
        return result
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active alerts."""
        return [
            alert for alert in self.alerts.values()
            if alert.status in [AlertStatus.ACTIVE, AlertStatus.ACKNOWLEDGED]
        ]
    
    def get_metric_summary(self, metric_id: str, period_minutes: int = 60) -> Dict[str, Any]:
        """Get summary statistics for a metric."""
        if metric_id not in self.metric_data:
            return {"error": f"Unknown metric: {metric_id}"}
        
        cutoff = datetime.now() - timedelta(minutes=period_minutes)
        values = [
            dp.value for dp in self.metric_data[metric_id]
            if dp.timestamp >= cutoff
        ]
        
        if not values:
            return {"metric_id": metric_id, "data_points": 0}
        
        return {
            "metric_id": metric_id,
            "period_minutes": period_minutes,
            "data_points": len(values),
            "current_value": values[-1],
            "min": min(values),
            "max": max(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "stdev": statistics.stdev(values) if len(values) > 1 else 0,
            "percentile_95": sorted(values)[int(len(values) * 0.95)] if values else 0
        }
    
    def get_analytics(self) -> Dict[str, Any]:
        """Get monitoring system analytics."""
        return {
            "metrics": {
                "registered_metrics": len(self.metrics),
                "total_data_points": sum(len(d) for d in self.metric_data.values()),
                **self.analytics
            },
            "alerts": {
                "active_alerts": len(self.get_active_alerts()),
                "total_alerts": len(self.alerts),
                "suppression_rules": len(self.suppression_rules)
            },
            "slas": {
                "registered_slas": len(self.slas),
                "recent_reports": len(self.sla_reports)
            },
            "health": {
                "registered_checks": len(self.health_checks),
                "history_size": len(self.health_history)
            },
            "anomalies": {
                "detected": self.analytics["anomalies_detected"],
                "recent": len([a for a in self.anomaly_history if a.timestamp > datetime.now() - timedelta(hours=1)])
            },
            "profiles": {
                "active": len(self.active_profiles),
                "stored": sum(len(p) for p in self.profile_data.values())
            }
        }


# Factory function
def create_monitoring_system(environment_ref=None) -> AdvancedMonitoringSystem:
    """Create monitoring system instance."""
    return AdvancedMonitoringSystem(environment_ref)


# Global instance
monitoring_system = AdvancedMonitoringSystem()
