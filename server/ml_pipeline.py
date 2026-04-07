"""Real-time Machine Learning Pipeline for Email Triage.

This module implements an adaptive ML system that learns from user actions
to improve email categorization, priority scoring, and recommendations.
"""

import json
import numpy as np
import time
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import pickle
import logging

from models import Email, EmailCategory, EmailPriority, ActionType


@dataclass
class MLFeature:
    """Feature extracted from email for ML model."""
    name: str
    value: float
    category: str  # "text", "metadata", "behavioral"


@dataclass
class TrainingExample:
    """Training example for ML model."""
    features: List[MLFeature]
    label: str  # Category, priority, or action
    confidence: float
    timestamp: str
    user_feedback: Optional[float] = None  # User satisfaction score


class EmailFeatureExtractor:
    """Extract features from emails for ML models."""
    
    def __init__(self):
        self.spam_keywords = {
            'urgent', 'winner', 'congratulations', 'prize', 'claim', 'suspend',
            'limited', 'offer', 'free', 'click', 'now', 'money', 'cash'
        }
        self.business_keywords = {
            'meeting', 'deadline', 'project', 'budget', 'quarterly', 'report',
            'client', 'customer', 'proposal', 'contract', 'schedule'
        }
        self.support_keywords = {
            'help', 'issue', 'problem', 'bug', 'error', 'support', 'assistance',
            'question', 'unable', 'not working', 'broken'
        }
        
    def extract_features(self, email: Email) -> List[MLFeature]:
        """Extract comprehensive features from email."""
        features = []
        
        # Text features
        features.extend(self._extract_text_features(email))
        
        # Metadata features
        features.extend(self._extract_metadata_features(email))
        
        # Sender features
        features.extend(self._extract_sender_features(email))
        
        # Temporal features
        features.extend(self._extract_temporal_features(email))
        
        # Attachment features
        features.extend(self._extract_attachment_features(email))
        
        return features
    
    def _extract_text_features(self, email: Email) -> List[MLFeature]:
        """Extract features from email text content."""
        features = []
        subject = email.subject.lower()
        body = email.body.lower()
        combined_text = subject + " " + body
        
        # Basic statistics
        features.append(MLFeature("subject_length", len(email.subject), "text"))
        features.append(MLFeature("body_length", len(email.body), "text"))
        features.append(MLFeature("word_count", len(combined_text.split()), "text"))
        features.append(MLFeature("exclamation_count", combined_text.count('!'), "text"))
        features.append(MLFeature("question_count", combined_text.count('?'), "text"))
        features.append(MLFeature("caps_ratio", sum(1 for c in email.subject if c.isupper()) / max(1, len(email.subject)), "text"))
        
        # Keyword presence
        spam_score = sum(1 for kw in self.spam_keywords if kw in combined_text) / len(self.spam_keywords)
        features.append(MLFeature("spam_keywords_ratio", spam_score, "text"))
        
        business_score = sum(1 for kw in self.business_keywords if kw in combined_text) / len(self.business_keywords)
        features.append(MLFeature("business_keywords_ratio", business_score, "text"))
        
        support_score = sum(1 for kw in self.support_keywords if kw in combined_text) / len(self.support_keywords)
        features.append(MLFeature("support_keywords_ratio", support_score, "text"))
        
        # Advanced text features
        features.append(MLFeature("has_url", float("http" in combined_text), "text"))
        features.append(MLFeature("has_email", float("@" in combined_text), "text"))
        features.append(MLFeature("has_phone", float(any(c.isdigit() for c in combined_text)), "text"))
        features.append(MLFeature("currency_mentions", combined_text.count('$') + combined_text.count('€'), "text"))
        
        return features
    
    def _extract_metadata_features(self, email: Email) -> List[MLFeature]:
        """Extract metadata-based features."""
        features = []
        
        # Email metadata
        features.append(MLFeature("has_attachments", float(email.has_attachments), "metadata"))
        features.append(MLFeature("attachment_count", len(email.attachments), "metadata"))
        features.append(MLFeature("is_reply", float(email.in_reply_to is not None), "metadata"))
        features.append(MLFeature("thread_position", email.thread_position, "metadata"))
        features.append(MLFeature("thread_size", email.thread_size, "metadata"))
        
        # Current state
        features.append(MLFeature("is_read", float(email.is_read), "metadata"))
        features.append(MLFeature("is_flagged", float(email.is_flagged), "metadata"))
        features.append(MLFeature("is_archived", float(email.is_archived), "metadata"))
        features.append(MLFeature("tag_count", len(email.tags), "metadata"))
        
        return features
    
    def _extract_sender_features(self, email: Email) -> List[MLFeature]:
        """Extract sender-based features."""
        features = []
        
        if email.sender_info:
            features.append(MLFeature("sender_is_vip", float(email.sender_info.sender_type.value == "vip"), "sender"))
            features.append(MLFeature("sender_is_known", float(email.sender_info.sender_type.value == "known"), "sender"))
            features.append(MLFeature("sender_is_suspicious", float(email.sender_info.sender_type.value == "suspicious"), "sender"))
            features.append(MLFeature("sender_trust_score", email.sender_info.trust_score, "sender"))
            features.append(MLFeature("sender_previous_emails", min(100, email.sender_info.previous_emails) / 100.0, "sender"))
            features.append(MLFeature("sender_is_internal", float(email.sender_info.is_internal), "sender"))
        else:
            # Default values for unknown senders
            features.extend([
                MLFeature("sender_is_vip", 0.0, "sender"),
                MLFeature("sender_is_known", 0.0, "sender"),
                MLFeature("sender_is_suspicious", 0.0, "sender"),
                MLFeature("sender_trust_score", 0.5, "sender"),
                MLFeature("sender_previous_emails", 0.0, "sender"),
                MLFeature("sender_is_internal", 0.0, "sender")
            ])
        
        # Domain analysis
        domain = email.sender.split('@')[-1] if '@' in email.sender else ""
        features.append(MLFeature("domain_is_common", float(domain.endswith(('.com', '.org', '.net', '.edu', '.gov'))), "sender"))
        features.append(MLFeature("domain_is_suspicious", float(domain.endswith(('.tk', '.ml', '.ga', '.cf'))), "sender"))
        
        return features
    
    def _extract_temporal_features(self, email: Email) -> List[MLFeature]:
        """Extract time-based features."""
        features = []
        
        try:
            received_time = datetime.fromisoformat(email.received_at.replace('Z', '+00:00'))
            now = datetime.now(received_time.tzinfo)
            
            # Time since received
            hours_old = (now - received_time).total_seconds() / 3600
            features.append(MLFeature("hours_since_received", min(168, hours_old) / 168.0, "temporal"))  # Cap at 1 week
            
            # Time of day features
            hour = received_time.hour
            features.append(MLFeature("received_hour", hour / 23.0, "temporal"))
            features.append(MLFeature("is_business_hours", float(9 <= hour <= 17), "temporal"))
            features.append(MLFeature("is_weekend", float(received_time.weekday() >= 5), "temporal"))
            
            # SLA features
            if email.sla_deadline:
                deadline = datetime.fromisoformat(email.sla_deadline.replace('Z', '+00:00'))
                hours_to_deadline = (deadline - now).total_seconds() / 3600
                features.append(MLFeature("sla_urgency", max(0, min(1, (24 - hours_to_deadline) / 24)), "temporal"))
            else:
                features.append(MLFeature("sla_urgency", 0.0, "temporal"))
                
        except (ValueError, TypeError, AttributeError):
            # Default temporal features if parsing fails
            features.extend([
                MLFeature("hours_since_received", 0.0, "temporal"),
                MLFeature("received_hour", 0.5, "temporal"),
                MLFeature("is_business_hours", 1.0, "temporal"),
                MLFeature("is_weekend", 0.0, "temporal"),
                MLFeature("sla_urgency", 0.0, "temporal")
            ])
        
        return features
    
    def _extract_attachment_features(self, email: Email) -> List[MLFeature]:
        """Extract attachment-based features."""
        features = []
        
        if not email.has_attachments:
            features.extend([
                MLFeature("has_image_attachment", 0.0, "attachment"),
                MLFeature("has_pdf_attachment", 0.0, "attachment"),
                MLFeature("has_document_attachment", 0.0, "attachment"),
                MLFeature("has_log_attachment", 0.0, "attachment")
            ])
        else:
            attachment_types = [att.attachment_type.value for att in email.attachments]
            features.append(MLFeature("has_image_attachment", float("image" in attachment_types), "attachment"))
            features.append(MLFeature("has_pdf_attachment", float("pdf" in attachment_types), "attachment"))
            features.append(MLFeature("has_document_attachment", float("document" in attachment_types), "attachment"))
            features.append(MLFeature("has_log_attachment", float("log" in attachment_types), "attachment"))
        
        return features


class AdaptiveCategoryClassifier:
    """ML classifier that adapts based on user feedback."""
    
    def __init__(self, learning_rate: float = 0.01):
        self.learning_rate = learning_rate
        self.feature_weights: Dict[str, float] = defaultdict(float)
        self.category_priors: Dict[str, float] = defaultdict(float)
        self.training_examples: List[TrainingExample] = []
        self.feature_stats = defaultdict(lambda: {'count': 0, 'sum': 0, 'sum_sq': 0})
        self.model_version = 1
        self.last_training = datetime.now()
        
    def add_training_example(self, features: List[MLFeature], category: str, confidence: float = 1.0, feedback: Optional[float] = None):
        """Add a training example and incrementally update model."""
        example = TrainingExample(
            features=features,
            label=category,
            confidence=confidence,
            timestamp=datetime.now().isoformat(),
            user_feedback=feedback
        )
        
        self.training_examples.append(example)
        
        # Update feature statistics for normalization
        for feature in features:
            stats = self.feature_stats[feature.name]
            stats['count'] += 1
            stats['sum'] += feature.value
            stats['sum_sq'] += feature.value * feature.value
        
        # Incremental learning
        self._incremental_update(example)
        
        # Keep only recent examples (sliding window)
        if len(self.training_examples) > 10000:
            self.training_examples = self.training_examples[-8000:]  # Keep 8000 most recent
    
    def _incremental_update(self, example: TrainingExample):
        """Update model weights incrementally."""
        # Calculate feature importance based on correlation with label
        feature_vector = self._normalize_features(example.features)
        
        # Update category prior
        self.category_priors[example.label] += self.learning_rate * example.confidence
        
        # Update feature weights using gradient-like approach
        for feature in feature_vector:
            # Positive update for correct category
            self.feature_weights[f"{feature.name}_{example.label}"] += (
                self.learning_rate * feature.value * example.confidence
            )
            
            # Small negative update for other categories to maintain discrimination
            for other_category in ['spam', 'internal', 'customer_support', 'billing', 'sales', 'technical', 'newsletter']:
                if other_category != example.label:
                    self.feature_weights[f"{feature.name}_{other_category}"] -= (
                        self.learning_rate * 0.1 * feature.value * example.confidence
                    )
        
        # Apply user feedback if available
        if example.user_feedback is not None:
            feedback_multiplier = example.user_feedback * 2.0 - 1.0  # Convert 0-1 to -1,1
            for feature in feature_vector:
                self.feature_weights[f"{feature.name}_{example.label}"] += (
                    self.learning_rate * 0.5 * feedback_multiplier * feature.value
                )
    
    def _normalize_features(self, features: List[MLFeature]) -> List[MLFeature]:
        """Normalize features using running statistics."""
        normalized = []
        
        for feature in features:
            stats = self.feature_stats[feature.name]
            if stats['count'] > 1:
                mean = stats['sum'] / stats['count']
                var = (stats['sum_sq'] / stats['count']) - (mean * mean)
                std = max(0.001, var ** 0.5)  # Avoid division by zero
                normalized_value = (feature.value - mean) / std
            else:
                normalized_value = feature.value
            
            normalized.append(MLFeature(
                name=feature.name,
                value=normalized_value,
                category=feature.category
            ))
        
        return normalized
    
    def predict_category(self, features: List[MLFeature]) -> Tuple[EmailCategory, float]:
        """Predict email category with confidence score."""
        if not self.feature_weights:
            # Fallback for untrained model
            return EmailCategory.INTERNAL, 0.5
        
        normalized_features = self._normalize_features(features)
        category_scores = defaultdict(float)
        
        # Calculate scores for each category
        for category in EmailCategory:
            score = self.category_priors[category.value]
            
            for feature in normalized_features:
                weight_key = f"{feature.name}_{category.value}"
                score += self.feature_weights[weight_key] * feature.value
            
            category_scores[category.value] = score
        
        # Get best category and confidence
        best_category = max(category_scores.keys(), key=lambda c: category_scores[c])
        best_score = category_scores[best_category]
        
        # Calculate confidence as softmax-like normalized score
        exp_scores = {cat: np.exp(score - best_score) for cat, score in category_scores.items()}
        total_exp = sum(exp_scores.values())
        confidence = exp_scores[best_category] / total_exp if total_exp > 0 else 0.5
        
        return EmailCategory(best_category), confidence
    
    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance scores."""
        importance = defaultdict(float)
        
        for weight_key, weight in self.feature_weights.items():
            if '_' in weight_key:
                feature_name = weight_key.rsplit('_', 1)[0]
                importance[feature_name] += abs(weight)
        
        return dict(importance)
    
    def get_model_stats(self) -> Dict[str, Any]:
        """Get model performance statistics."""
        return {
            'training_examples': len(self.training_examples),
            'feature_count': len(self.feature_weights),
            'model_version': self.model_version,
            'last_training': self.last_training.isoformat(),
            'category_distribution': dict(self.category_priors),
            'top_features': dict(sorted(
                self.get_feature_importance().items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10])
        }


class MLPipeline:
    """Main ML Pipeline for adaptive email processing."""
    
    def __init__(self):
        self.feature_extractor = EmailFeatureExtractor()
        self.category_classifier = AdaptiveCategoryClassifier()
        self.priority_classifier = AdaptiveCategoryClassifier()
        self.action_recommender = AdaptiveCategoryClassifier()
        
        self.performance_metrics = {
            'predictions_made': 0,
            'feedback_received': 0,
            'accuracy_estimates': defaultdict(list),
            'drift_detection': defaultdict(list)
        }
        
        self.model_cache = {}
        self.last_retrain = datetime.now()
        
    def process_email(self, email: Email) -> Dict[str, Any]:
        """Process email through ML pipeline."""
        features = self.feature_extractor.extract_features(email)
        
        # Predict category
        predicted_category, cat_confidence = self.category_classifier.predict_category(features)
        
        # Predict priority based on category and features
        priority_features = features + [
            MLFeature("predicted_category_spam", float(predicted_category == EmailCategory.SPAM), "derived"),
            MLFeature("predicted_category_urgent", float(predicted_category in [EmailCategory.CUSTOMER_SUPPORT, EmailCategory.BILLING]), "derived")
        ]
        predicted_priority, pri_confidence = self._predict_priority(priority_features)
        
        # Recommend actions
        recommended_actions = self._recommend_actions(features, predicted_category, predicted_priority)
        
        self.performance_metrics['predictions_made'] += 1
        
        return {
            'category_prediction': {
                'category': predicted_category,
                'confidence': cat_confidence
            },
            'priority_prediction': {
                'priority': predicted_priority,
                'confidence': pri_confidence
            },
            'recommended_actions': recommended_actions,
            'feature_count': len(features),
            'model_version': self.category_classifier.model_version
        }
    
    def _predict_priority(self, features: List[MLFeature]) -> Tuple[EmailPriority, float]:
        """Predict email priority."""
        # Simple heuristic-based priority prediction for now
        spam_score = next((f.value for f in features if f.name == "spam_keywords_ratio"), 0)
        sla_urgency = next((f.value for f in features if f.name == "sla_urgency"), 0)
        sender_vip = next((f.value for f in features if f.name == "sender_is_vip"), 0)
        
        if spam_score > 0.3:
            return EmailPriority.LOW, 0.8
        elif sla_urgency > 0.7 or sender_vip > 0.5:
            return EmailPriority.URGENT, 0.8
        elif sla_urgency > 0.3:
            return EmailPriority.HIGH, 0.7
        else:
            return EmailPriority.NORMAL, 0.6
    
    def _recommend_actions(self, features: List[MLFeature], category: EmailCategory, priority: EmailPriority) -> List[Dict[str, Any]]:
        """Recommend next actions based on ML analysis."""
        actions = []
        
        # Category-based recommendations
        if category == EmailCategory.SPAM:
            actions.append({
                'action': 'mark_spam',
                'confidence': 0.9,
                'reason': 'High probability spam email detected'
            })
        elif category == EmailCategory.CUSTOMER_SUPPORT:
            actions.append({
                'action': 'prioritize',
                'priority': 'high',
                'confidence': 0.8,
                'reason': 'Customer support email requires prompt attention'
            })
            actions.append({
                'action': 'reply',
                'template': 'customer_support',
                'confidence': 0.7,
                'reason': 'Use customer support template for consistent response'
            })
        elif category == EmailCategory.NEWSLETTER:
            actions.append({
                'action': 'archive',
                'confidence': 0.8,
                'reason': 'Newsletter can be archived for later reading'
            })
        
        # Priority-based recommendations
        if priority == EmailPriority.URGENT:
            actions.append({
                'action': 'flag',
                'confidence': 0.9,
                'reason': 'Urgent priority email should be flagged'
            })
        
        # Feature-based recommendations
        attachment_count = next((f.value for f in features if f.name == "attachment_count"), 0)
        if attachment_count > 0:
            actions.append({
                'action': 'review_attachments',
                'confidence': 0.7,
                'reason': f'Email has {int(attachment_count)} attachment(s) to review'
            })
        
        return actions[:3]  # Return top 3 recommendations
    
    def learn_from_feedback(self, email: Email, user_action: Dict[str, Any], satisfaction: float):
        """Learn from user feedback."""
        features = self.feature_extractor.extract_features(email)
        
        # Extract user's actual choice
        if user_action.get('action_type') == 'categorize':
            actual_category = user_action.get('category')
            if actual_category:
                self.category_classifier.add_training_example(
                    features, actual_category, confidence=1.0, feedback=satisfaction
                )
        
        if user_action.get('action_type') == 'prioritize':
            actual_priority = user_action.get('priority')
            if actual_priority:
                priority_features = features + [
                    MLFeature("user_selected_priority", 1.0, "feedback")
                ]
                self.priority_classifier.add_training_example(
                    priority_features, actual_priority, confidence=1.0, feedback=satisfaction
                )
        
        self.performance_metrics['feedback_received'] += 1
        
        # Update accuracy estimates
        if satisfaction is not None:
            self.performance_metrics['accuracy_estimates']['overall'].append(satisfaction)
            if len(self.performance_metrics['accuracy_estimates']['overall']) > 1000:
                self.performance_metrics['accuracy_estimates']['overall'] = (
                    self.performance_metrics['accuracy_estimates']['overall'][-500:]
                )
    
    def detect_concept_drift(self) -> Dict[str, Any]:
        """Detect if email patterns are changing (concept drift)."""
        recent_accuracy = self.performance_metrics['accuracy_estimates']['overall'][-100:] if len(self.performance_metrics['accuracy_estimates']['overall']) >= 100 else []
        older_accuracy = self.performance_metrics['accuracy_estimates']['overall'][-200:-100] if len(self.performance_metrics['accuracy_estimates']['overall']) >= 200 else []
        
        drift_detected = False
        drift_magnitude = 0.0
        
        if recent_accuracy and older_accuracy:
            recent_avg = np.mean(recent_accuracy)
            older_avg = np.mean(older_accuracy)
            drift_magnitude = abs(recent_avg - older_avg)
            drift_detected = drift_magnitude > 0.1  # 10% change threshold
        
        return {
            'drift_detected': drift_detected,
            'drift_magnitude': drift_magnitude,
            'recent_accuracy': np.mean(recent_accuracy) if recent_accuracy else None,
            'baseline_accuracy': np.mean(older_accuracy) if older_accuracy else None,
            'recommendation': 'Retrain model' if drift_detected else 'Continue monitoring'
        }
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get comprehensive pipeline statistics."""
        return {
            'performance': self.performance_metrics,
            'category_model': self.category_classifier.get_model_stats(),
            'priority_model': self.priority_classifier.get_model_stats(),
            'drift_analysis': self.detect_concept_drift(),
            'last_retrain': self.last_retrain.isoformat(),
            'feature_extraction_time_ms': 0.0,  # Placeholder for timing metrics
            'prediction_time_ms': 0.0
        }
    
    def save_models(self, filepath: str):
        """Save trained models to disk."""
        models_data = {
            'category_classifier': {
                'feature_weights': dict(self.category_classifier.feature_weights),
                'category_priors': dict(self.category_classifier.category_priors),
                'feature_stats': dict(self.category_classifier.feature_stats),
                'model_version': self.category_classifier.model_version
            },
            'priority_classifier': {
                'feature_weights': dict(self.priority_classifier.feature_weights),
                'category_priors': dict(self.priority_classifier.category_priors),
                'feature_stats': dict(self.priority_classifier.feature_stats),
                'model_version': self.priority_classifier.model_version
            },
            'performance_metrics': self.performance_metrics,
            'save_timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(models_data, f)
            logging.info(f"Models saved successfully to {filepath}")
            return True
        except Exception as e:
            logging.error(f"Failed to save models: {e}")
            return False
    
    def load_models(self, filepath: str):
        """Load trained models from disk."""
        try:
            with open(filepath, 'rb') as f:
                models_data = pickle.load(f)
            
            # Restore category classifier
            cat_data = models_data.get('category_classifier', {})
            self.category_classifier.feature_weights.update(cat_data.get('feature_weights', {}))
            self.category_classifier.category_priors.update(cat_data.get('category_priors', {}))
            self.category_classifier.feature_stats.update(cat_data.get('feature_stats', {}))
            self.category_classifier.model_version = cat_data.get('model_version', 1)
            
            # Restore priority classifier
            pri_data = models_data.get('priority_classifier', {})
            self.priority_classifier.feature_weights.update(pri_data.get('feature_weights', {}))
            self.priority_classifier.category_priors.update(pri_data.get('category_priors', {}))
            self.priority_classifier.feature_stats.update(pri_data.get('feature_stats', {}))
            self.priority_classifier.model_version = pri_data.get('model_version', 1)
            
            # Restore metrics
            self.performance_metrics.update(models_data.get('performance_metrics', {}))
            
            logging.info(f"Models loaded successfully from {filepath}")
            return True
        except Exception as e:
            logging.error(f"Failed to load models: {e}")
            return False


# Global ML pipeline instance
ml_pipeline = MLPipeline()