"""Predictive Analytics Engine.

Advanced analytics system that predicts email trends, workload patterns, and provides
intelligent insights for proactive email management. This engine goes beyond reactive
processing to enable predictive email triage and strategic insights.

Features:
- Time series forecasting for email volume and patterns
- Sender behavior prediction and anomaly detection
- Workload prediction and capacity planning
- Intelligent trend analysis and insights generation
- Seasonal pattern recognition and adaptation
- Risk prediction and early warning systems
"""

import json
import time
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
from enum import Enum
import logging
import math
from statistics import mean, median, stdev
import re

from models import Email, EmailCategory, EmailPriority, SenderInfo


class TrendType(str, Enum):
    """Types of trends that can be detected."""
    INCREASING = "increasing"
    DECREASING = "decreasing" 
    STABLE = "stable"
    CYCLICAL = "cyclical"
    ANOMALOUS = "anomalous"


class AlertSeverity(str, Enum):
    """Severity levels for predictive alerts."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class TimeSeriesPoint:
    """Single point in a time series."""
    timestamp: str
    value: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TrendAnalysis:
    """Analysis of a trend in data."""
    trend_type: TrendType
    confidence: float  # 0.0 to 1.0
    strength: float   # How strong the trend is
    direction: str    # "up", "down", "stable"
    change_rate: float  # Rate of change per unit time
    r_squared: float   # Statistical fit quality
    forecast_points: List[TimeSeriesPoint]
    detected_at: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PredictiveAlert:
    """Predictive alert for potential issues."""
    alert_id: str
    alert_type: str  # "volume_spike", "sender_anomaly", "sla_risk", etc.
    severity: AlertSeverity
    title: str
    description: str
    predicted_time: str  # When the event is predicted to occur
    confidence: float
    recommended_actions: List[str]
    triggers: List[str]  # What triggered this prediction
    created_at: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkloadForecast:
    """Forecast for email processing workload."""
    forecast_period: str  # "next_hour", "next_4_hours", "next_day"
    predicted_volume: int
    confidence_interval: Tuple[int, int]  # (min, max)
    peak_times: List[str]  # Predicted peak periods
    recommended_staffing: int
    difficulty_score: float  # Predicted average difficulty
    category_breakdown: Dict[str, int]
    priority_breakdown: Dict[str, int]
    generated_at: str


@dataclass
class SenderBehaviorProfile:
    """Profile of sender behavior patterns."""
    sender: str
    sender_domain: str
    total_emails: int
    avg_emails_per_day: float
    peak_sending_hours: List[int]  # Hours of day (0-23)
    category_distribution: Dict[str, float]
    priority_distribution: Dict[str, float]
    response_urgency: float  # How urgently they typically need responses
    behavioral_change_score: float  # Recent change in behavior
    anomaly_score: float  # Current anomaly level
    trust_evolution: List[float]  # Trust score over time
    last_updated: str


class PredictiveAnalyticsEngine:
    """Advanced predictive analytics for email management."""
    
    def __init__(self):
        # Time series data storage
        self.email_volume_series: List[TimeSeriesPoint] = []
        self.category_series: Dict[str, List[TimeSeriesPoint]] = defaultdict(list)
        self.priority_series: Dict[str, List[TimeSeriesPoint]] = defaultdict(list)
        self.response_time_series: List[TimeSeriesPoint] = []
        self.sla_performance_series: List[TimeSeriesPoint] = []
        
        # Sender tracking
        self.sender_profiles: Dict[str, SenderBehaviorProfile] = {}
        self.sender_activity_series: Dict[str, List[TimeSeriesPoint]] = defaultdict(list)
        
        # Prediction models state
        self.trend_analyses: Dict[str, TrendAnalysis] = {}
        self.active_alerts: List[PredictiveAlert] = []
        self.workload_forecasts: List[WorkloadForecast] = []
        
        # Configuration
        self.min_data_points = 5  # Minimum points needed for analysis
        self.forecast_horizon_hours = 24
        self.alert_threshold = 0.7  # Confidence threshold for alerts
        self.anomaly_threshold = 2.0  # Standard deviations for anomaly detection
        
        # Rolling windows for different analysis periods
        self.hourly_window = deque(maxlen=168)  # 1 week of hourly data
        self.daily_window = deque(maxlen=30)    # 30 days of daily data
        self.weekly_window = deque(maxlen=12)   # 12 weeks of weekly data
        
        # Analytics state
        self.last_analysis_time = datetime.now().isoformat()
        self.prediction_accuracy_history: List[float] = []
        self.model_performance_metrics = {
            'volume_prediction_accuracy': 0.0,
            'trend_detection_accuracy': 0.0,
            'anomaly_detection_precision': 0.0,
            'alert_false_positive_rate': 0.0
        }
        
        # Seasonal patterns
        self.daily_patterns: Dict[int, float] = {}  # Hour -> volume multiplier
        self.weekly_patterns: Dict[int, float] = {} # Weekday -> volume multiplier
        self.monthly_patterns: Dict[int, float] = {} # Day of month -> multiplier
        
        logger = logging.getLogger(__name__)
        logger.info("Predictive Analytics Engine initialized")
    
    def add_email_data_point(self, email: Email, processing_context: Dict[str, Any] = None):
        """Add a new email data point for analysis."""
        timestamp = datetime.now().isoformat()
        
        # Add to volume series
        self.email_volume_series.append(TimeSeriesPoint(
            timestamp=timestamp,
            value=1.0,
            metadata={'email_id': email.id, 'category': email.category.value if email.category else None}
        ))
        
        # Add to category series if categorized
        if email.category:
            self.category_series[email.category.value].append(TimeSeriesPoint(
                timestamp=timestamp,
                value=1.0,
                metadata={'email_id': email.id}
            ))
        
        # Add to priority series
        if email.priority:
            self.priority_series[email.priority.value].append(TimeSeriesPoint(
                timestamp=timestamp,
                value=1.0,
                metadata={'email_id': email.id}
            ))
        
        # Update sender profile
        self._update_sender_profile(email)
        
        # Add sender activity
        self.sender_activity_series[email.sender].append(TimeSeriesPoint(
            timestamp=timestamp,
            value=1.0,
            metadata={'email_id': email.id, 'subject_length': len(email.subject)}
        ))
        
        # Add response time data if available
        if processing_context and 'response_time_ms' in processing_context:
            self.response_time_series.append(TimeSeriesPoint(
                timestamp=timestamp,
                value=processing_context['response_time_ms'],
                metadata={'email_id': email.id}
            ))
        
        # Update rolling windows
        self._update_rolling_windows()
        
        # Trigger analysis if enough data points accumulated
        if len(self.email_volume_series) % 10 == 0:  # Analyze every 10 emails
            self._run_predictive_analysis()
    
    def _update_sender_profile(self, email: Email):
        """Update or create sender behavior profile."""
        sender = email.sender
        
        if sender not in self.sender_profiles:
            self.sender_profiles[sender] = SenderBehaviorProfile(
                sender=sender,
                sender_domain=sender.split('@')[-1] if '@' in sender else '',
                total_emails=0,
                avg_emails_per_day=0.0,
                peak_sending_hours=[],
                category_distribution={},
                priority_distribution={},
                response_urgency=0.5,
                behavioral_change_score=0.0,
                anomaly_score=0.0,
                trust_evolution=[],
                last_updated=datetime.now().isoformat()
            )
        
        profile = self.sender_profiles[sender]
        profile.total_emails += 1
        
        # Update category distribution
        if email.category:
            cat_key = email.category.value
            if cat_key not in profile.category_distribution:
                profile.category_distribution[cat_key] = 0
            profile.category_distribution[cat_key] += 1
        
        # Update priority distribution  
        if email.priority:
            pri_key = email.priority.value
            if pri_key not in profile.priority_distribution:
                profile.priority_distribution[pri_key] = 0
            profile.priority_distribution[pri_key] += 1
        
        # Update sending hour pattern
        current_hour = datetime.now().hour
        profile.peak_sending_hours.append(current_hour)
        if len(profile.peak_sending_hours) > 100:  # Keep last 100 send times
            profile.peak_sending_hours.pop(0)
        
        # Update trust evolution
        if email.sender_info:
            profile.trust_evolution.append(email.sender_info.trust_score)
            if len(profile.trust_evolution) > 50:  # Keep last 50 trust scores
                profile.trust_evolution.pop(0)
        
        profile.last_updated = datetime.now().isoformat()
        
        # Calculate behavioral change score
        if len(profile.trust_evolution) > 5:
            recent_trust = mean(profile.trust_evolution[-5:])
            historical_trust = mean(profile.trust_evolution[:-5])
            profile.behavioral_change_score = abs(recent_trust - historical_trust)
    
    def _update_rolling_windows(self):
        """Update rolling time windows for different analysis periods."""
        current_time = datetime.now()
        
        # Update hourly window
        hourly_emails = self._count_emails_in_period(current_time - timedelta(hours=1), current_time)
        self.hourly_window.append((current_time.isoformat(), hourly_emails))
        
        # Update daily patterns
        hour = current_time.hour
        if hour not in self.daily_patterns:
            self.daily_patterns[hour] = 0
        self.daily_patterns[hour] = (self.daily_patterns[hour] * 0.9) + (hourly_emails * 0.1)  # Exponential smoothing
        
        # Update weekly patterns
        weekday = current_time.weekday()
        if weekday not in self.weekly_patterns:
            self.weekly_patterns[weekday] = 0
        daily_emails = self._count_emails_in_period(current_time - timedelta(days=1), current_time)
        self.weekly_patterns[weekday] = (self.weekly_patterns[weekday] * 0.9) + (daily_emails * 0.1)
    
    def _count_emails_in_period(self, start_time: datetime, end_time: datetime) -> int:
        """Count emails received in a specific time period."""
        count = 0
        for point in self.email_volume_series:
            point_time = datetime.fromisoformat(point.timestamp)
            if start_time <= point_time <= end_time:
                count += 1
        return count
    
    def _run_predictive_analysis(self):
        """Run comprehensive predictive analysis."""
        current_time = datetime.now()
        
        # Volume trend analysis
        self._analyze_volume_trends()
        
        # Sender behavior analysis
        self._analyze_sender_behaviors()
        
        # Generate workload forecasts
        self._generate_workload_forecasts()
        
        # Check for predictive alerts
        self._check_predictive_alerts()
        
        # Update seasonal patterns
        self._update_seasonal_patterns()
        
        self.last_analysis_time = current_time.isoformat()
    
    def _analyze_volume_trends(self):
        """Analyze email volume trends and patterns."""
        if len(self.email_volume_series) < self.min_data_points:
            return
        
        # Get recent data points
        recent_points = self.email_volume_series[-50:]  # Last 50 emails
        
        # Group by hour for hourly volume analysis
        hourly_volumes = defaultdict(int)
        for point in recent_points:
            hour_key = point.timestamp[:13]  # YYYY-MM-DDTHH
            hourly_volumes[hour_key] += 1
        
        if len(hourly_volumes) < 3:
            return
        
        # Convert to time series for trend analysis
        sorted_hours = sorted(hourly_volumes.keys())
        values = [hourly_volumes[hour] for hour in sorted_hours]
        
        # Simple linear trend analysis
        n = len(values)
        x = list(range(n))
        
        # Calculate linear regression
        sum_x = sum(x)
        sum_y = sum(values)
        sum_xy = sum(x[i] * values[i] for i in range(n))
        sum_x2 = sum(x[i] ** 2 for i in range(n))
        
        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
        intercept = (sum_y - slope * sum_x) / n
        
        # Calculate R-squared
        y_mean = mean(values)
        ss_tot = sum((y - y_mean) ** 2 for y in values)
        ss_res = sum((values[i] - (slope * x[i] + intercept)) ** 2 for i in range(n))
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        # Determine trend type
        if abs(slope) < 0.1:
            trend_type = TrendType.STABLE
            direction = "stable"
        elif slope > 0:
            trend_type = TrendType.INCREASING
            direction = "up"
        else:
            trend_type = TrendType.DECREASING
            direction = "down"
        
        # Generate forecast points
        forecast_points = []
        forecast_hours = min(24, n * 2)  # Forecast next 24 hours or 2x current data
        
        for i in range(forecast_hours):
            future_x = n + i
            predicted_value = max(0, slope * future_x + intercept)
            future_time = datetime.now() + timedelta(hours=i)
            
            forecast_points.append(TimeSeriesPoint(
                timestamp=future_time.isoformat(),
                value=predicted_value,
                metadata={'forecast': True, 'confidence': r_squared}
            ))
        
        # Store trend analysis
        self.trend_analyses['volume_trend'] = TrendAnalysis(
            trend_type=trend_type,
            confidence=r_squared,
            strength=abs(slope),
            direction=direction,
            change_rate=slope,
            r_squared=r_squared,
            forecast_points=forecast_points,
            detected_at=datetime.now().isoformat(),
            metadata={
                'data_points': n,
                'analysis_period_hours': len(hourly_volumes)
            }
        )
    
    def _analyze_sender_behaviors(self):
        """Analyze sender behavior patterns and detect anomalies."""
        current_time = datetime.now()
        
        for sender, profile in self.sender_profiles.items():
            if profile.total_emails < 3:  # Need minimum data
                continue
            
            # Calculate daily sending rate
            profile.avg_emails_per_day = profile.total_emails / max(1, 
                (current_time - datetime.fromisoformat(profile.last_updated)).days or 1)
            
            # Detect sending hour patterns
            if profile.peak_sending_hours:
                hour_counts = defaultdict(int)
                for hour in profile.peak_sending_hours:
                    hour_counts[hour] += 1
                
                # Find top 3 sending hours
                top_hours = sorted(hour_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                profile.peak_sending_hours = [hour for hour, _ in top_hours]
            
            # Calculate anomaly score based on recent activity
            recent_activity = self.sender_activity_series[sender][-10:]  # Last 10 emails
            
            if len(recent_activity) >= 3:
                # Check for unusual timing patterns
                recent_hours = []
                for point in recent_activity:
                    point_time = datetime.fromisoformat(point.timestamp)
                    recent_hours.append(point_time.hour)
                
                # Calculate deviation from normal sending pattern
                normal_hours = set(profile.peak_sending_hours)
                unusual_hours = [h for h in recent_hours if h not in normal_hours]
                
                profile.anomaly_score = len(unusual_hours) / len(recent_hours)
                
                # Check for volume anomalies
                if len(recent_activity) >= 5:
                    recent_volumes = [1 for _ in recent_activity]  # Simple count
                    if len(recent_activity) > profile.avg_emails_per_day * 2:
                        profile.anomaly_score = max(profile.anomaly_score, 0.7)
    
    def _generate_workload_forecasts(self):
        """Generate workload forecasts for different time horizons."""
        current_time = datetime.now()
        
        # Clear old forecasts
        self.workload_forecasts = []
        
        # Generate forecasts for different periods
        periods = [
            ("next_hour", 1),
            ("next_4_hours", 4), 
            ("next_day", 24)
        ]
        
        for period_name, hours in periods:
            forecast = self._forecast_period(period_name, hours)
            if forecast:
                self.workload_forecasts.append(forecast)
    
    def _forecast_period(self, period_name: str, hours: int) -> Optional[WorkloadForecast]:
        """Generate forecast for a specific time period."""
        if len(self.email_volume_series) < self.min_data_points:
            return None
        
        # Base prediction on recent hourly averages
        recent_hours = min(hours * 3, len(self.hourly_window))
        if recent_hours == 0:
            return None
        
        recent_volumes = [volume for _, volume in list(self.hourly_window)[-recent_hours:]]
        base_hourly_rate = mean(recent_volumes) if recent_volumes else 1.0
        
        # Apply seasonal adjustments
        predicted_volume = 0
        peak_times = []
        
        for hour_offset in range(hours):
            future_time = datetime.now() + timedelta(hours=hour_offset)
            hour_of_day = future_time.hour
            day_of_week = future_time.weekday()
            
            # Seasonal multipliers
            daily_multiplier = self.daily_patterns.get(hour_of_day, 1.0)
            weekly_multiplier = self.weekly_patterns.get(day_of_week, 1.0)
            
            hour_prediction = base_hourly_rate * daily_multiplier * weekly_multiplier
            predicted_volume += hour_prediction
            
            # Track peak times (above average)
            if hour_prediction > base_hourly_rate * 1.2:
                peak_times.append(future_time.strftime("%H:00"))
        
        predicted_volume = int(predicted_volume)
        
        # Calculate confidence interval (±20% based on historical variance)
        confidence_range = max(1, int(predicted_volume * 0.2))
        confidence_interval = (
            max(0, predicted_volume - confidence_range),
            predicted_volume + confidence_range
        )
        
        # Estimate category and priority breakdown based on recent patterns
        category_breakdown = {}
        priority_breakdown = {}
        
        # Use recent distribution
        recent_categories = defaultdict(int)
        recent_priorities = defaultdict(int)
        
        for point in self.email_volume_series[-100:]:  # Last 100 emails
            if point.metadata.get('category'):
                recent_categories[point.metadata['category']] += 1
        
        total_recent = sum(recent_categories.values())
        if total_recent > 0:
            for category, count in recent_categories.items():
                category_breakdown[category] = int((count / total_recent) * predicted_volume)
        
        # Estimate difficulty score (higher for more complex categories)
        difficulty_weights = {
            'technical': 0.8,
            'customer_support': 0.7,
            'billing': 0.6,
            'sales': 0.5,
            'internal': 0.4,
            'newsletter': 0.2,
            'spam': 0.1
        }
        
        weighted_difficulty = 0.0
        total_weight = 0.0
        
        for category, volume in category_breakdown.items():
            weight = difficulty_weights.get(category, 0.5)
            weighted_difficulty += weight * volume
            total_weight += volume
        
        difficulty_score = weighted_difficulty / max(1, total_weight)
        
        # Recommend staffing based on volume and difficulty
        base_capacity = 10  # emails per hour per person
        adjusted_capacity = base_capacity * (2 - difficulty_score)  # Harder emails reduce capacity
        recommended_staffing = max(1, int(predicted_volume / adjusted_capacity))
        
        return WorkloadForecast(
            forecast_period=period_name,
            predicted_volume=predicted_volume,
            confidence_interval=confidence_interval,
            peak_times=peak_times,
            recommended_staffing=recommended_staffing,
            difficulty_score=difficulty_score,
            category_breakdown=category_breakdown,
            priority_breakdown=priority_breakdown,
            generated_at=datetime.now().isoformat()
        )
    
    def _check_predictive_alerts(self):
        """Check for conditions that warrant predictive alerts."""
        current_time = datetime.now()
        
        # Clear old alerts (older than 24 hours)
        self.active_alerts = [
            alert for alert in self.active_alerts
            if (current_time - datetime.fromisoformat(alert.created_at)).total_seconds() < 24 * 3600
        ]
        
        # Volume spike prediction
        self._check_volume_spike_alert()
        
        # Sender anomaly alerts
        self._check_sender_anomaly_alerts()
        
        # SLA risk alerts
        self._check_sla_risk_alerts()
        
        # Capacity overload alerts
        self._check_capacity_alerts()
    
    def _check_volume_spike_alert(self):
        """Check for predicted volume spikes."""
        if 'volume_trend' not in self.trend_analyses:
            return
        
        trend = self.trend_analyses['volume_trend']
        
        if (trend.trend_type == TrendType.INCREASING and 
            trend.confidence > 0.7 and 
            trend.strength > 0.5):
            
            # Predict spike magnitude
            current_volume = len(self.email_volume_series[-10:])  # Last 10 emails
            predicted_peak = trend.forecast_points[5].value if len(trend.forecast_points) > 5 else current_volume * 2
            
            spike_magnitude = predicted_peak / max(1, current_volume)
            
            if spike_magnitude > 1.5:  # 50% increase
                severity = AlertSeverity.HIGH if spike_magnitude > 2.0 else AlertSeverity.MEDIUM
                
                alert = PredictiveAlert(
                    alert_id=f"volume_spike_{int(time.time())}",
                    alert_type="volume_spike",
                    severity=severity,
                    title="Email Volume Spike Predicted",
                    description=f"Email volume is trending upward and may increase by {spike_magnitude:.1f}x in the next few hours.",
                    predicted_time=(datetime.now() + timedelta(hours=2)).isoformat(),
                    confidence=trend.confidence,
                    recommended_actions=[
                        "Consider increasing staffing",
                        "Prepare for higher workload",
                        "Review automation rules",
                        "Monitor system performance"
                    ],
                    triggers=[
                        f"Increasing volume trend detected (slope: {trend.change_rate:.2f})",
                        f"High trend confidence ({trend.confidence:.1%})"
                    ],
                    created_at=datetime.now().isoformat(),
                    metadata={
                        'current_volume': current_volume,
                        'predicted_volume': predicted_peak,
                        'spike_magnitude': spike_magnitude
                    }
                )
                
                self.active_alerts.append(alert)
    
    def _check_sender_anomaly_alerts(self):
        """Check for sender behavior anomalies."""
        for sender, profile in self.sender_profiles.items():
            if profile.anomaly_score > 0.7 and profile.total_emails > 5:
                
                severity = AlertSeverity.HIGH if profile.anomaly_score > 0.9 else AlertSeverity.MEDIUM
                
                alert = PredictiveAlert(
                    alert_id=f"sender_anomaly_{hash(sender) % 10000}",
                    alert_type="sender_anomaly", 
                    severity=severity,
                    title=f"Unusual Activity from {sender}",
                    description=f"Sender {sender} is exhibiting unusual behavior patterns.",
                    predicted_time=datetime.now().isoformat(),
                    confidence=profile.anomaly_score,
                    recommended_actions=[
                        "Review recent emails from this sender",
                        "Check for potential security issues",
                        "Verify sender identity if needed",
                        "Consider adjusting trust score"
                    ],
                    triggers=[
                        f"Anomaly score: {profile.anomaly_score:.2f}",
                        f"Behavioral change detected"
                    ],
                    created_at=datetime.now().isoformat(),
                    metadata={
                        'sender': sender,
                        'total_emails': profile.total_emails,
                        'behavioral_change_score': profile.behavioral_change_score
                    }
                )
                
                self.active_alerts.append(alert)
    
    def _check_sla_risk_alerts(self):
        """Check for SLA breach risk based on current workload."""
        # This would integrate with SLA tracking from the main environment
        pass
    
    def _check_capacity_alerts(self):
        """Check for potential capacity/workload issues."""
        if not self.workload_forecasts:
            return
        
        for forecast in self.workload_forecasts:
            if forecast.predicted_volume > 100:  # Threshold for high volume
                
                severity = AlertSeverity.CRITICAL if forecast.predicted_volume > 200 else AlertSeverity.HIGH
                
                alert = PredictiveAlert(
                    alert_id=f"capacity_risk_{forecast.forecast_period}",
                    alert_type="capacity_overload",
                    severity=severity,
                    title=f"High Workload Predicted: {forecast.forecast_period}",
                    description=f"Predicted {forecast.predicted_volume} emails in {forecast.forecast_period} "
                              f"requiring {forecast.recommended_staffing} staff members.",
                    predicted_time=forecast.generated_at,
                    confidence=0.8,
                    recommended_actions=[
                        f"Scale staffing to {forecast.recommended_staffing} people",
                        "Activate additional automation rules",
                        "Prioritize urgent emails",
                        "Consider deferring non-urgent tasks"
                    ],
                    triggers=[
                        f"Volume prediction: {forecast.predicted_volume} emails",
                        f"Difficulty score: {forecast.difficulty_score:.2f}"
                    ],
                    created_at=datetime.now().isoformat(),
                    metadata=forecast.__dict__
                )
                
                self.active_alerts.append(alert)
    
    def _update_seasonal_patterns(self):
        """Update seasonal patterns based on recent data."""
        current_time = datetime.now()
        
        # Update monthly patterns
        day_of_month = current_time.day
        if day_of_month not in self.monthly_patterns:
            self.monthly_patterns[day_of_month] = 1.0
        
        # Use recent daily volume to update monthly pattern
        daily_volume = self._count_emails_in_period(
            current_time - timedelta(days=1), 
            current_time
        )
        
        # Exponential smoothing
        self.monthly_patterns[day_of_month] = (
            self.monthly_patterns[day_of_month] * 0.8 + 
            (daily_volume / max(1, mean(self.daily_patterns.values()) or 1)) * 0.2
        )
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get comprehensive analytics summary."""
        current_time = datetime.now()
        
        summary = {
            "data_collection": {
                "total_data_points": len(self.email_volume_series),
                "sender_profiles": len(self.sender_profiles),
                "time_series_count": len(self.category_series) + len(self.priority_series),
                "data_span_hours": (
                    (current_time - datetime.fromisoformat(self.email_volume_series[0].timestamp)).total_seconds() / 3600
                    if self.email_volume_series else 0
                )
            },
            "trend_analysis": {
                "active_trends": len(self.trend_analyses),
                "trend_details": {
                    trend_name: {
                        "type": trend.trend_type.value,
                        "confidence": trend.confidence,
                        "direction": trend.direction,
                        "strength": trend.strength
                    }
                    for trend_name, trend in self.trend_analyses.items()
                }
            },
            "predictive_alerts": {
                "active_alerts": len(self.active_alerts),
                "alert_breakdown": {
                    alert.alert_type: len([a for a in self.active_alerts if a.alert_type == alert.alert_type])
                    for alert in self.active_alerts
                },
                "severity_distribution": {
                    severity.value: len([a for a in self.active_alerts if a.severity == severity])
                    for severity in AlertSeverity
                }
            },
            "workload_forecasts": {
                "forecast_count": len(self.workload_forecasts),
                "forecasts": [
                    {
                        "period": f.forecast_period,
                        "predicted_volume": f.predicted_volume,
                        "confidence_range": f.confidence_interval,
                        "difficulty": f.difficulty_score,
                        "recommended_staff": f.recommended_staffing
                    }
                    for f in self.workload_forecasts
                ]
            },
            "seasonal_patterns": {
                "daily_pattern_hours": len(self.daily_patterns),
                "peak_daily_hours": sorted(self.daily_patterns.keys(), 
                                         key=lambda h: self.daily_patterns[h], reverse=True)[:3],
                "weekly_pattern_days": len(self.weekly_patterns),
                "monthly_pattern_days": len(self.monthly_patterns)
            },
            "sender_insights": {
                "total_senders": len(self.sender_profiles),
                "anomalous_senders": len([p for p in self.sender_profiles.values() if p.anomaly_score > 0.5]),
                "high_volume_senders": len([p for p in self.sender_profiles.values() if p.total_emails > 10])
            },
            "performance_metrics": self.model_performance_metrics,
            "last_analysis": self.last_analysis_time
        }
        
        return summary
    
    def get_forecasts(self, period: Optional[str] = None) -> List[WorkloadForecast]:
        """Get workload forecasts, optionally filtered by period."""
        if period:
            return [f for f in self.workload_forecasts if f.forecast_period == period]
        return self.workload_forecasts
    
    def get_alerts(self, severity: Optional[AlertSeverity] = None) -> List[PredictiveAlert]:
        """Get active alerts, optionally filtered by severity."""
        if severity:
            return [a for a in self.active_alerts if a.severity == severity]
        return self.active_alerts
    
    def get_trend_analysis(self, trend_type: Optional[str] = None) -> Dict[str, TrendAnalysis]:
        """Get trend analyses, optionally filtered by type."""
        if trend_type:
            return {k: v for k, v in self.trend_analyses.items() if k == trend_type}
        return self.trend_analyses
    
    def get_sender_insights(self, min_emails: int = 1) -> List[SenderBehaviorProfile]:
        """Get sender behavior profiles with minimum email threshold."""
        return [
            profile for profile in self.sender_profiles.values()
            if profile.total_emails >= min_emails
        ]


# Global predictive analytics engine instance
predictive_engine = PredictiveAnalyticsEngine()