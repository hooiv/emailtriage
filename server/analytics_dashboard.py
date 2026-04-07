"""Advanced Analytics Dashboard & Business Intelligence Engine.

This module provides enterprise-grade analytics, real-time dashboards, and business
intelligence capabilities for the email triage system. It transforms raw operational
data into actionable insights with advanced visualizations and predictive analytics.

Features:
- Real-time dashboard with interactive charts and metrics
- Advanced business intelligence with trend analysis 
- Performance analytics and KPI tracking
- Custom dashboard creation and sharing
- Automated reporting and insights generation
- Data export and visualization API
- Advanced filtering and drill-down capabilities
- Historical data analysis and comparison
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
import uuid

from models import Email, EmailCategory, EmailPriority


class ChartType(str, Enum):
    """Types of charts available in the dashboard."""
    LINE = "line"
    BAR = "bar"
    PIE = "pie"
    AREA = "area"
    SCATTER = "scatter"
    HISTOGRAM = "histogram"
    HEATMAP = "heatmap"
    GAUGE = "gauge"
    TABLE = "table"


class MetricType(str, Enum):
    """Types of metrics that can be tracked."""
    EMAIL_VOLUME = "email_volume"
    PROCESSING_SPEED = "processing_speed"
    ACCURACY_RATE = "accuracy_rate"
    RESPONSE_TIME = "response_time"
    CATEGORY_DISTRIBUTION = "category_distribution"
    PRIORITY_DISTRIBUTION = "priority_distribution"
    SLA_PERFORMANCE = "sla_performance"
    AI_CONFIDENCE = "ai_confidence"
    SECURITY_ALERTS = "security_alerts"
    AUTONOMOUS_ACTIONS = "autonomous_actions"
    USER_PRODUCTIVITY = "user_productivity"
    SYSTEM_HEALTH = "system_health"


class AggregationMethod(str, Enum):
    """Methods for aggregating time-series data."""
    SUM = "sum"
    AVERAGE = "average"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    PERCENTILE_95 = "percentile_95"


@dataclass
class ChartData:
    """Data structure for chart visualization."""
    chart_type: ChartType
    title: str
    labels: List[str]  # X-axis labels
    datasets: List[Dict[str, Any]]  # Chart datasets
    options: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class KPIMetric:
    """Key Performance Indicator metric."""
    metric_id: str
    name: str
    current_value: float
    target_value: Optional[float]
    previous_value: Optional[float]
    unit: str
    format_type: str  # "number", "percentage", "duration", "currency"
    trend: str  # "up", "down", "stable"
    change_percentage: Optional[float]
    status: str  # "good", "warning", "critical"
    description: str
    last_updated: str


@dataclass
class DashboardWidget:
    """Individual dashboard widget configuration."""
    widget_id: str
    widget_type: str  # "chart", "kpi", "table", "text"
    title: str
    position: Dict[str, int]  # {"x": 0, "y": 0, "width": 6, "height": 4}
    data_source: str
    configuration: Dict[str, Any]
    refresh_interval: int = 30  # seconds
    is_visible: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class Dashboard:
    """Complete dashboard configuration."""
    dashboard_id: str
    name: str
    description: str
    widgets: List[DashboardWidget]
    layout: Dict[str, Any]
    filters: Dict[str, Any] = field(default_factory=dict)
    auto_refresh: bool = True
    refresh_interval: int = 30
    is_public: bool = False
    created_by: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_modified: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class AnalyticsQuery:
    """Query configuration for analytics data."""
    metric_type: MetricType
    time_range: Dict[str, str]  # {"start": "iso_date", "end": "iso_date"}
    aggregation: AggregationMethod
    group_by: Optional[str] = None  # Field to group by
    filters: Dict[str, Any] = field(default_factory=dict)
    limit: Optional[int] = None


class AdvancedAnalyticsEngine:
    """Advanced analytics and business intelligence engine."""
    
    def __init__(self, environment_ref):
        self.environment = environment_ref
        
        # Data storage for analytics
        self.metrics_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.kpi_cache: Dict[str, KPIMetric] = {}
        
        # Dashboard management
        self.dashboards: Dict[str, Dashboard] = {}
        self.default_dashboard: Optional[Dashboard] = None
        
        # Chart configurations
        self.chart_templates: Dict[str, Dict[str, Any]] = {}
        self.color_palettes: Dict[str, List[str]] = {
            'default': ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c'],
            'professional': ['#2c3e50', '#34495e', '#7f8c8d', '#95a5a6', '#bdc3c7', '#ecf0f1'],
            'vibrant': ['#ff6b6b', '#4ecdc4', '#45b7d1', '#f9ca24', '#f0932b', '#eb4d4b']
        }
        
        # Analytics cache
        self.analytics_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl_seconds = 300  # 5 minutes
        
        # Performance tracking
        self.analytics_performance = {
            'queries_executed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'avg_query_time_ms': 0.0,
            'last_updated': datetime.now().isoformat()
        }
        
        # Initialize default dashboard and templates
        self._initialize_chart_templates()
        self._create_default_dashboard()
        
        logger = logging.getLogger(__name__)
        logger.info("Advanced Analytics Engine initialized")
    
    def collect_metrics(self):
        """Collect current metrics from the environment."""
        current_time = datetime.now().isoformat()
        
        if not hasattr(self.environment, 'emails'):
            return
        
        # Email volume metrics
        total_emails = len(self.environment.emails)
        unread_emails = len([e for e in self.environment.emails if not e.is_read])
        processed_emails = len([e for e in self.environment.emails if e.category is not None])
        
        self._store_metric('email_volume', {
            'timestamp': current_time,
            'total_emails': total_emails,
            'unread_emails': unread_emails,
            'processed_emails': processed_emails,
            'processing_rate': processed_emails / max(1, total_emails) * 100
        })
        
        # Category distribution
        category_dist = defaultdict(int)
        for email in self.environment.emails:
            if email.category:
                category_dist[email.category.value] += 1
        
        self._store_metric('category_distribution', {
            'timestamp': current_time,
            **dict(category_dist)
        })
        
        # Priority distribution
        priority_dist = defaultdict(int)
        for email in self.environment.emails:
            if email.priority:
                priority_dist[email.priority.value] += 1
        
        self._store_metric('priority_distribution', {
            'timestamp': current_time,
            **dict(priority_dist)
        })
        
        # AI/ML performance metrics
        if hasattr(self.environment, 'agent_consensus'):
            consensus_count = len(self.environment.agent_consensus)
            if consensus_count > 0:
                # Calculate average confidence from recent consensuses
                recent_confidences = []
                for consensus_data in list(self.environment.agent_consensus.values())[-50:]:
                    if isinstance(consensus_data, dict) and 'consensus_results' in consensus_data:
                        consensus_results = consensus_data['consensus_results']
                        for result in consensus_results.values():
                            if hasattr(result, 'confidence'):
                                recent_confidences.append(result.confidence)
                
                avg_confidence = mean(recent_confidences) if recent_confidences else 0.0
                
                self._store_metric('ai_confidence', {
                    'timestamp': current_time,
                    'average_confidence': avg_confidence,
                    'consensus_count': consensus_count,
                    'confidence_distribution': self._calculate_confidence_distribution(recent_confidences)
                })
        
        # Security metrics
        if hasattr(self.environment, 'security_scans'):
            security_scans = len(self.environment.security_scans)
            high_risk_emails = len([
                scan for scan in self.environment.security_scans.values()
                if hasattr(scan, 'risk_score') and scan.risk_score > 0.7
            ])
            
            self._store_metric('security_alerts', {
                'timestamp': current_time,
                'total_scans': security_scans,
                'high_risk_emails': high_risk_emails,
                'risk_rate': high_risk_emails / max(1, security_scans) * 100
            })
        
        # System performance metrics
        if hasattr(self.environment, 'metrics'):
            env_metrics = self.environment.metrics
            response_times = getattr(self.environment, '_request_times', [])
            avg_response_time = mean(response_times[-100:]) if response_times else 0.0
            
            self._store_metric('system_performance', {
                'timestamp': current_time,
                'total_requests': env_metrics.total_requests,
                'emails_processed': env_metrics.emails_processed,
                'actions_taken': env_metrics.actions_taken,
                'avg_response_time_ms': avg_response_time * 1000,
                'throughput_per_minute': self._calculate_throughput()
            })
        
        # Autonomous system metrics
        if hasattr(self.environment, 'autonomous_manager'):
            auto_mgr = self.environment.autonomous_manager
            self._store_metric('autonomous_performance', {
                'timestamp': current_time,
                'autonomous_decisions': len(auto_mgr.autonomous_decisions),
                'emails_processed_autonomous': auto_mgr.system_metrics.emails_processed_autonomous,
                'average_confidence': auto_mgr.system_metrics.average_confidence,
                'accuracy_rate': auto_mgr.system_metrics.accuracy_rate,
                'decisions_per_minute': auto_mgr.system_metrics.decisions_per_minute,
                'autonomous_fixes': auto_mgr.system_metrics.autonomous_fixes_applied
            })
        
        # Update KPIs
        self._update_kpis()
    
    def _store_metric(self, metric_name: str, data: Dict[str, Any]):
        """Store a metric data point."""
        self.metrics_history[metric_name].append(data)
    
    def _calculate_confidence_distribution(self, confidences: List[float]) -> Dict[str, int]:
        """Calculate distribution of confidence scores."""
        if not confidences:
            return {}
        
        distribution = {
            'very_low': len([c for c in confidences if c < 0.3]),
            'low': len([c for c in confidences if 0.3 <= c < 0.5]),
            'medium': len([c for c in confidences if 0.5 <= c < 0.7]),
            'high': len([c for c in confidences if 0.7 <= c < 0.9]),
            'very_high': len([c for c in confidences if c >= 0.9])
        }
        
        return distribution
    
    def _calculate_throughput(self) -> float:
        """Calculate emails processed per minute."""
        if not hasattr(self.environment, 'emails'):
            return 0.0
        
        # Get emails processed in the last hour
        current_time = datetime.now()
        one_hour_ago = current_time - timedelta(hours=1)
        
        processed_in_hour = len([
            email for email in self.environment.emails
            if email.category is not None  # Processed indicator
        ])
        
        return processed_in_hour / 60.0  # Per minute
    
    def _update_kpis(self):
        """Update Key Performance Indicators."""
        current_time = datetime.now().isoformat()
        
        # Email Processing Rate KPI
        if 'email_volume' in self.metrics_history:
            recent_metrics = list(self.metrics_history['email_volume'])[-10:]
            if recent_metrics:
                current_rate = recent_metrics[-1]['processing_rate']
                previous_rate = recent_metrics[-2]['processing_rate'] if len(recent_metrics) > 1 else current_rate
                
                trend = "up" if current_rate > previous_rate else "down" if current_rate < previous_rate else "stable"
                change_pct = ((current_rate - previous_rate) / max(1, previous_rate)) * 100 if previous_rate > 0 else 0
                
                status = "good" if current_rate >= 80 else "warning" if current_rate >= 60 else "critical"
                
                self.kpi_cache['email_processing_rate'] = KPIMetric(
                    metric_id='email_processing_rate',
                    name='Email Processing Rate',
                    current_value=current_rate,
                    target_value=85.0,
                    previous_value=previous_rate,
                    unit='%',
                    format_type='percentage',
                    trend=trend,
                    change_percentage=change_pct,
                    status=status,
                    description='Percentage of emails that have been processed',
                    last_updated=current_time
                )
        
        # AI Confidence KPI
        if 'ai_confidence' in self.metrics_history:
            recent_ai_metrics = list(self.metrics_history['ai_confidence'])[-5:]
            if recent_ai_metrics:
                current_confidence = recent_ai_metrics[-1]['average_confidence'] * 100
                previous_confidence = recent_ai_metrics[-2]['average_confidence'] * 100 if len(recent_ai_metrics) > 1 else current_confidence
                
                trend = "up" if current_confidence > previous_confidence else "down" if current_confidence < previous_confidence else "stable"
                change_pct = ((current_confidence - previous_confidence) / max(1, previous_confidence)) * 100 if previous_confidence > 0 else 0
                
                status = "good" if current_confidence >= 75 else "warning" if current_confidence >= 60 else "critical"
                
                self.kpi_cache['ai_confidence'] = KPIMetric(
                    metric_id='ai_confidence',
                    name='AI Confidence Score',
                    current_value=current_confidence,
                    target_value=80.0,
                    previous_value=previous_confidence,
                    unit='%',
                    format_type='percentage',
                    trend=trend,
                    change_percentage=change_pct,
                    status=status,
                    description='Average confidence level of AI decisions',
                    last_updated=current_time
                )
        
        # System Response Time KPI
        if 'system_performance' in self.metrics_history:
            recent_perf = list(self.metrics_history['system_performance'])[-5:]
            if recent_perf:
                current_response = recent_perf[-1]['avg_response_time_ms']
                previous_response = recent_perf[-2]['avg_response_time_ms'] if len(recent_perf) > 1 else current_response
                
                trend = "down" if current_response < previous_response else "up" if current_response > previous_response else "stable"
                change_pct = ((current_response - previous_response) / max(1, previous_response)) * 100 if previous_response > 0 else 0
                
                status = "good" if current_response <= 100 else "warning" if current_response <= 500 else "critical"
                
                self.kpi_cache['response_time'] = KPIMetric(
                    metric_id='response_time',
                    name='Avg Response Time',
                    current_value=current_response,
                    target_value=100.0,
                    previous_value=previous_response,
                    unit='ms',
                    format_type='duration',
                    trend=trend,
                    change_percentage=abs(change_pct),  # Positive change is bad for response time
                    status=status,
                    description='Average API response time',
                    last_updated=current_time
                )
        
        # Autonomous Processing KPI
        if hasattr(self.environment, 'autonomous_manager'):
            auto_decisions = len(self.environment.autonomous_manager.autonomous_decisions)
            total_emails = len(self.environment.emails) if hasattr(self.environment, 'emails') else 0
            autonomy_rate = (auto_decisions / max(1, total_emails)) * 100
            
            status = "good" if autonomy_rate >= 70 else "warning" if autonomy_rate >= 50 else "critical"
            
            self.kpi_cache['autonomy_rate'] = KPIMetric(
                metric_id='autonomy_rate',
                name='Autonomous Processing',
                current_value=autonomy_rate,
                target_value=75.0,
                previous_value=None,  # Would need historical tracking
                unit='%',
                format_type='percentage',
                trend='stable',  # Would need historical data
                change_percentage=None,
                status=status,
                description='Percentage of emails processed autonomously',
                last_updated=current_time
            )
    
    def execute_query(self, query: AnalyticsQuery) -> Dict[str, Any]:
        """Execute an analytics query and return results."""
        start_time = time.time()
        
        # Check cache first
        cache_key = self._generate_cache_key(query)
        if cache_key in self.analytics_cache:
            cache_entry = self.analytics_cache[cache_key]
            if time.time() - cache_entry['timestamp'] < self.cache_ttl_seconds:
                self.analytics_performance['cache_hits'] += 1
                return cache_entry['data']
        
        # Execute query
        self.analytics_performance['cache_misses'] += 1
        result = self._execute_query_internal(query)
        
        # Cache result
        self.analytics_cache[cache_key] = {
            'data': result,
            'timestamp': time.time()
        }
        
        # Update performance metrics
        query_time = (time.time() - start_time) * 1000
        self.analytics_performance['queries_executed'] += 1
        
        total_time = self.analytics_performance['avg_query_time_ms'] * (self.analytics_performance['queries_executed'] - 1)
        self.analytics_performance['avg_query_time_ms'] = (total_time + query_time) / self.analytics_performance['queries_executed']
        self.analytics_performance['last_updated'] = datetime.now().isoformat()
        
        return result
    
    def _execute_query_internal(self, query: AnalyticsQuery) -> Dict[str, Any]:
        """Internal query execution logic."""
        metric_type = query.metric_type.value
        
        if metric_type not in self.metrics_history:
            return {'error': f'No data available for metric type: {metric_type}'}
        
        # Get data within time range
        start_time = datetime.fromisoformat(query.time_range['start'])
        end_time = datetime.fromisoformat(query.time_range['end'])
        
        relevant_data = []
        for data_point in self.metrics_history[metric_type]:
            point_time = datetime.fromisoformat(data_point['timestamp'])
            if start_time <= point_time <= end_time:
                relevant_data.append(data_point)
        
        if not relevant_data:
            return {'error': 'No data found in specified time range'}
        
        # Apply filters
        if query.filters:
            relevant_data = self._apply_filters(relevant_data, query.filters)
        
        # Apply aggregation
        result = self._apply_aggregation(relevant_data, query.aggregation, query.group_by)
        
        # Apply limit
        if query.limit and isinstance(result, list):
            result = result[:query.limit]
        
        return {
            'metric_type': metric_type,
            'time_range': query.time_range,
            'data_points': len(relevant_data),
            'result': result
        }
    
    def _apply_filters(self, data: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply filters to data."""
        filtered_data = []
        
        for item in data:
            include_item = True
            
            for filter_key, filter_value in filters.items():
                if filter_key in item:
                    if isinstance(filter_value, dict):
                        # Range filter
                        if 'min' in filter_value and item[filter_key] < filter_value['min']:
                            include_item = False
                            break
                        if 'max' in filter_value and item[filter_key] > filter_value['max']:
                            include_item = False
                            break
                    elif item[filter_key] != filter_value:
                        include_item = False
                        break
            
            if include_item:
                filtered_data.append(item)
        
        return filtered_data
    
    def _apply_aggregation(self, data: List[Dict[str, Any]], aggregation: AggregationMethod, group_by: Optional[str]) -> Any:
        """Apply aggregation to data."""
        if not data:
            return []
        
        if group_by:
            # Group data by field
            groups = defaultdict(list)
            for item in data:
                if group_by in item:
                    groups[item[group_by]].append(item)
            
            # Apply aggregation to each group
            result = {}
            for group_key, group_data in groups.items():
                result[group_key] = self._aggregate_group(group_data, aggregation)
            
            return result
        else:
            # Aggregate all data
            return self._aggregate_group(data, aggregation)
    
    def _aggregate_group(self, data: List[Dict[str, Any]], aggregation: AggregationMethod) -> Any:
        """Aggregate a group of data points."""
        if not data:
            return None
        
        # Find numeric fields to aggregate
        numeric_fields = set()
        for item in data:
            for key, value in item.items():
                if isinstance(value, (int, float)) and key != 'timestamp':
                    numeric_fields.add(key)
        
        result = {}
        
        for field in numeric_fields:
            values = [item[field] for item in data if field in item and isinstance(item[field], (int, float))]
            
            if not values:
                continue
            
            if aggregation == AggregationMethod.SUM:
                result[field] = sum(values)
            elif aggregation == AggregationMethod.AVERAGE:
                result[field] = mean(values)
            elif aggregation == AggregationMethod.MEDIAN:
                result[field] = median(values)
            elif aggregation == AggregationMethod.MIN:
                result[field] = min(values)
            elif aggregation == AggregationMethod.MAX:
                result[field] = max(values)
            elif aggregation == AggregationMethod.COUNT:
                result[field] = len(values)
            elif aggregation == AggregationMethod.PERCENTILE_95:
                result[field] = np.percentile(values, 95)
        
        return result
    
    def _generate_cache_key(self, query: AnalyticsQuery) -> str:
        """Generate cache key for analytics query."""
        key_data = {
            'metric_type': query.metric_type.value,
            'time_range': query.time_range,
            'aggregation': query.aggregation.value,
            'group_by': query.group_by,
            'filters': query.filters,
            'limit': query.limit
        }
        return f"analytics_{hash(json.dumps(key_data, sort_keys=True))}"
    
    def generate_chart_data(self, chart_type: ChartType, metric_type: MetricType, 
                           time_range: Dict[str, str], **kwargs) -> ChartData:
        """Generate chart data for visualization."""
        # Create analytics query
        query = AnalyticsQuery(
            metric_type=metric_type,
            time_range=time_range,
            aggregation=kwargs.get('aggregation', AggregationMethod.AVERAGE),
            group_by=kwargs.get('group_by'),
            filters=kwargs.get('filters', {}),
            limit=kwargs.get('limit')
        )
        
        # Execute query
        query_result = self.execute_query(query)
        
        if 'error' in query_result:
            return ChartData(
                chart_type=chart_type,
                title=f"Error: {query_result['error']}",
                labels=[],
                datasets=[]
            )
        
        # Generate chart based on type
        if chart_type == ChartType.LINE:
            return self._generate_line_chart(query_result, metric_type, **kwargs)
        elif chart_type == ChartType.BAR:
            return self._generate_bar_chart(query_result, metric_type, **kwargs)
        elif chart_type == ChartType.PIE:
            return self._generate_pie_chart(query_result, metric_type, **kwargs)
        elif chart_type == ChartType.AREA:
            return self._generate_area_chart(query_result, metric_type, **kwargs)
        else:
            return ChartData(
                chart_type=chart_type,
                title=f"{metric_type.value.replace('_', ' ').title()}",
                labels=[],
                datasets=[]
            )
    
    def _generate_line_chart(self, query_result: Dict[str, Any], metric_type: MetricType, **kwargs) -> ChartData:
        """Generate line chart data."""
        # For time series data
        metric_data = self.metrics_history[metric_type.value]
        recent_data = list(metric_data)[-50:]  # Last 50 points
        
        labels = []
        datasets = []
        
        if recent_data:
            # Extract timestamps for labels
            labels = [
                datetime.fromisoformat(item['timestamp']).strftime('%H:%M')
                for item in recent_data
            ]
            
            # Find numeric fields to plot
            numeric_fields = set()
            for item in recent_data:
                for key, value in item.items():
                    if isinstance(value, (int, float)) and key != 'timestamp':
                        numeric_fields.add(key)
            
            colors = self.color_palettes['default']
            
            for i, field in enumerate(numeric_fields):
                if i >= len(colors):
                    break
                
                values = [item.get(field, 0) for item in recent_data]
                
                datasets.append({
                    'label': field.replace('_', ' ').title(),
                    'data': values,
                    'borderColor': colors[i],
                    'backgroundColor': colors[i] + '20',  # Semi-transparent
                    'fill': False,
                    'tension': 0.1
                })
        
        return ChartData(
            chart_type=ChartType.LINE,
            title=f"{metric_type.value.replace('_', ' ').title()} Over Time",
            labels=labels,
            datasets=datasets,
            options={
                'responsive': True,
                'scales': {
                    'y': {
                        'beginAtZero': True
                    }
                },
                'plugins': {
                    'legend': {
                        'position': 'top'
                    },
                    'title': {
                        'display': True,
                        'text': f"{metric_type.value.replace('_', ' ').title()} Over Time"
                    }
                }
            }
        )
    
    def _generate_pie_chart(self, query_result: Dict[str, Any], metric_type: MetricType, **kwargs) -> ChartData:
        """Generate pie chart data.""" 
        if metric_type == MetricType.CATEGORY_DISTRIBUTION:
            # Get latest category distribution
            recent_data = list(self.metrics_history['category_distribution'])
            if recent_data:
                latest = recent_data[-1]
                
                labels = []
                values = []
                
                for key, value in latest.items():
                    if key != 'timestamp' and value > 0:
                        labels.append(key.replace('_', ' ').title())
                        values.append(value)
                
                return ChartData(
                    chart_type=ChartType.PIE,
                    title="Email Category Distribution",
                    labels=labels,
                    datasets=[{
                        'data': values,
                        'backgroundColor': self.color_palettes['vibrant'][:len(values)],
                        'borderWidth': 2,
                        'borderColor': '#ffffff'
                    }],
                    options={
                        'responsive': True,
                        'plugins': {
                            'legend': {
                                'position': 'right'
                            }
                        }
                    }
                )
        
        return ChartData(
            chart_type=ChartType.PIE,
            title="No Data Available",
            labels=[],
            datasets=[]
        )
    
    def _generate_bar_chart(self, query_result: Dict[str, Any], metric_type: MetricType, **kwargs) -> ChartData:
        """Generate bar chart data."""
        # Similar implementation to line chart but with bar styling
        chart_data = self._generate_line_chart(query_result, metric_type, **kwargs)
        chart_data.chart_type = ChartType.BAR
        
        # Update styling for bar chart
        for dataset in chart_data.datasets:
            dataset['type'] = 'bar'
            dataset.pop('fill', None)
            dataset.pop('tension', None)
        
        return chart_data
    
    def _generate_area_chart(self, query_result: Dict[str, Any], metric_type: MetricType, **kwargs) -> ChartData:
        """Generate area chart data."""
        chart_data = self._generate_line_chart(query_result, metric_type, **kwargs)
        chart_data.chart_type = ChartType.AREA
        
        # Update styling for area chart
        for dataset in chart_data.datasets:
            dataset['fill'] = True
        
        return chart_data
    
    def create_dashboard(self, dashboard_config: Dict[str, Any]) -> Dashboard:
        """Create a new dashboard."""
        dashboard_id = f"dashboard_{uuid.uuid4().hex[:8]}"
        
        dashboard = Dashboard(
            dashboard_id=dashboard_id,
            name=dashboard_config['name'],
            description=dashboard_config.get('description', ''),
            widgets=[],
            layout=dashboard_config.get('layout', {}),
            filters=dashboard_config.get('filters', {}),
            auto_refresh=dashboard_config.get('auto_refresh', True),
            refresh_interval=dashboard_config.get('refresh_interval', 30),
            is_public=dashboard_config.get('is_public', False),
            created_by=dashboard_config.get('created_by')
        )
        
        # Create widgets
        for widget_config in dashboard_config.get('widgets', []):
            widget = DashboardWidget(
                widget_id=f"widget_{uuid.uuid4().hex[:8]}",
                widget_type=widget_config['widget_type'],
                title=widget_config['title'],
                position=widget_config['position'],
                data_source=widget_config['data_source'],
                configuration=widget_config.get('configuration', {}),
                refresh_interval=widget_config.get('refresh_interval', 30)
            )
            dashboard.widgets.append(widget)
        
        self.dashboards[dashboard_id] = dashboard
        return dashboard
    
    def _create_default_dashboard(self):
        """Create the default system dashboard."""
        default_config = {
            'name': 'Email Triage System Dashboard',
            'description': 'Comprehensive overview of system performance and metrics',
            'auto_refresh': True,
            'refresh_interval': 30,
            'widgets': [
                {
                    'widget_type': 'kpi',
                    'title': 'Processing Rate',
                    'position': {'x': 0, 'y': 0, 'width': 3, 'height': 2},
                    'data_source': 'email_processing_rate',
                    'configuration': {'format': 'percentage'}
                },
                {
                    'widget_type': 'kpi', 
                    'title': 'AI Confidence',
                    'position': {'x': 3, 'y': 0, 'width': 3, 'height': 2},
                    'data_source': 'ai_confidence',
                    'configuration': {'format': 'percentage'}
                },
                {
                    'widget_type': 'kpi',
                    'title': 'Response Time',
                    'position': {'x': 6, 'y': 0, 'width': 3, 'height': 2},
                    'data_source': 'response_time',
                    'configuration': {'format': 'duration'}
                },
                {
                    'widget_type': 'chart',
                    'title': 'Email Volume Trend',
                    'position': {'x': 0, 'y': 2, 'width': 6, 'height': 4},
                    'data_source': 'email_volume',
                    'configuration': {'chart_type': 'line', 'time_range': 'last_hour'}
                },
                {
                    'widget_type': 'chart',
                    'title': 'Category Distribution',
                    'position': {'x': 6, 'y': 2, 'width': 6, 'height': 4},
                    'data_source': 'category_distribution',
                    'configuration': {'chart_type': 'pie'}
                },
                {
                    'widget_type': 'chart',
                    'title': 'System Performance',
                    'position': {'x': 0, 'y': 6, 'width': 12, 'height': 4},
                    'data_source': 'system_performance',
                    'configuration': {'chart_type': 'area', 'time_range': 'last_hour'}
                }
            ]
        }
        
        self.default_dashboard = self.create_dashboard(default_config)
    
    def _initialize_chart_templates(self):
        """Initialize chart templates for common visualizations."""
        self.chart_templates = {
            'email_volume_line': {
                'chart_type': 'line',
                'title': 'Email Volume Over Time',
                'y_axis_label': 'Number of Emails',
                'colors': self.color_palettes['professional']
            },
            'category_pie': {
                'chart_type': 'pie',
                'title': 'Email Category Distribution',
                'colors': self.color_palettes['vibrant']
            },
            'performance_gauge': {
                'chart_type': 'gauge',
                'title': 'System Performance',
                'min_value': 0,
                'max_value': 100,
                'thresholds': [{'value': 70, 'color': 'red'}, {'value': 85, 'color': 'yellow'}, {'value': 100, 'color': 'green'}]
            }
        }
    
    def get_dashboard_data(self, dashboard_id: str) -> Dict[str, Any]:
        """Get complete dashboard data ready for frontend rendering."""
        if dashboard_id not in self.dashboards:
            return {'error': 'Dashboard not found'}
        
        dashboard = self.dashboards[dashboard_id]
        
        # Generate data for each widget
        widget_data = {}
        
        for widget in dashboard.widgets:
            if widget.widget_type == 'kpi':
                # Get KPI data
                kpi_data = self.kpi_cache.get(widget.data_source)
                if kpi_data:
                    widget_data[widget.widget_id] = {
                        'type': 'kpi',
                        'data': {
                            'name': kpi_data.name,
                            'current_value': kpi_data.current_value,
                            'target_value': kpi_data.target_value,
                            'previous_value': kpi_data.previous_value,
                            'unit': kpi_data.unit,
                            'format_type': kpi_data.format_type,
                            'trend': kpi_data.trend,
                            'change_percentage': kpi_data.change_percentage,
                            'status': kpi_data.status,
                            'description': kpi_data.description,
                            'last_updated': kpi_data.last_updated
                        }
                    }
                
            elif widget.widget_type == 'chart':
                # Generate chart data
                config = widget.configuration
                chart_type = ChartType(config.get('chart_type', 'line'))
                
                # Determine metric type from data source
                if widget.data_source in [mt.value for mt in MetricType]:
                    metric_type = MetricType(widget.data_source)
                else:
                    metric_type = MetricType.EMAIL_VOLUME  # Default
                
                # Generate time range
                time_range = self._get_time_range(config.get('time_range', 'last_hour'))
                
                chart_data = self.generate_chart_data(
                    chart_type=chart_type,
                    metric_type=metric_type,
                    time_range=time_range
                )
                
                widget_data[widget.widget_id] = {
                    'type': 'chart',
                    'data': {
                        'chart_type': chart_data.chart_type.value,
                        'title': chart_data.title,
                        'labels': chart_data.labels,
                        'datasets': chart_data.datasets,
                        'options': chart_data.options
                    }
                }
        
        return {
            'dashboard_id': dashboard_id,
            'name': dashboard.name,
            'description': dashboard.description,
            'layout': dashboard.layout,
            'auto_refresh': dashboard.auto_refresh,
            'refresh_interval': dashboard.refresh_interval,
            'widgets': widget_data,
            'last_updated': datetime.now().isoformat()
        }
    
    def _get_time_range(self, time_range_str: str) -> Dict[str, str]:
        """Convert time range string to actual datetime range."""
        end_time = datetime.now()
        
        if time_range_str == 'last_hour':
            start_time = end_time - timedelta(hours=1)
        elif time_range_str == 'last_4_hours':
            start_time = end_time - timedelta(hours=4)
        elif time_range_str == 'last_day':
            start_time = end_time - timedelta(days=1)
        elif time_range_str == 'last_week':
            start_time = end_time - timedelta(weeks=1)
        else:
            start_time = end_time - timedelta(hours=1)  # Default
        
        return {
            'start': start_time.isoformat(),
            'end': end_time.isoformat()
        }
    
    def get_analytics_overview(self) -> Dict[str, Any]:
        """Get comprehensive analytics overview."""
        return {
            'kpis': {kpi_id: {
                'name': kpi.name,
                'current_value': kpi.current_value,
                'unit': kpi.unit,
                'status': kpi.status,
                'trend': kpi.trend,
                'change_percentage': kpi.change_percentage
            } for kpi_id, kpi in self.kpi_cache.items()},
            
            'data_summary': {
                'total_metrics_tracked': len(self.metrics_history),
                'total_data_points': sum(len(history) for history in self.metrics_history.values()),
                'available_metrics': list(self.metrics_history.keys()),
                'last_collection': max([
                    list(history)[-1]['timestamp'] for history in self.metrics_history.values() if history
                ], default=None)
            },
            
            'performance': self.analytics_performance,
            
            'dashboards': {
                'total_dashboards': len(self.dashboards),
                'default_dashboard': self.default_dashboard.dashboard_id if self.default_dashboard else None,
                'dashboard_list': [
                    {'id': dashboard_id, 'name': dashboard.name, 'widgets': len(dashboard.widgets)}
                    for dashboard_id, dashboard in self.dashboards.items()
                ]
            }
        }


# Create analytics engine factory
def create_analytics_engine(environment_ref) -> AdvancedAnalyticsEngine:
    """Factory function to create analytics engine."""
    return AdvancedAnalyticsEngine(environment_ref)