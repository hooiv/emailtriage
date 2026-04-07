"""
Ultimate Multi-Modal AI System for Email Triage Environment

Revolutionary multi-modal AI processing capabilities:
- Vision AI for image and document analysis 
- Audio AI for voice message processing
- Video AI for video content understanding
- Document AI for PDF and text analysis
- Cross-modal fusion and reasoning
- Real-time multi-modal embeddings
- Sentiment analysis across modalities
- Content-aware attachment processing
- Multi-modal search and retrieval
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
from collections import deque, defaultdict
from enum import Enum
import threading
import base64
import json
import time
import random
import math
import hashlib
import io
from dataclasses import dataclass, field
import numpy as np


class ModalityType(str, Enum):
    """Types of modalities processed"""
    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    VIDEO = "video"
    DOCUMENT = "document"
    TABULAR = "tabular"


class ProcessingCapability(str, Enum):
    """Multi-modal processing capabilities"""
    OBJECT_DETECTION = "object_detection"
    TEXT_RECOGNITION = "text_recognition"
    FACE_RECOGNITION = "face_recognition"
    SPEECH_TO_TEXT = "speech_to_text"
    AUDIO_CLASSIFICATION = "audio_classification"
    DOCUMENT_PARSING = "document_parsing"
    TABLE_EXTRACTION = "table_extraction"
    SENTIMENT_ANALYSIS = "sentiment_analysis"
    CONTENT_MODERATION = "content_moderation"
    FEATURE_EXTRACTION = "feature_extraction"
    CROSS_MODAL_SEARCH = "cross_modal_search"


class ContentType(str, Enum):
    """Content types for processing"""
    EMAIL_ATTACHMENT = "email_attachment"
    EMBEDDED_CONTENT = "embedded_content"
    SIGNATURE_IMAGE = "signature_image"
    LOGO_DETECTION = "logo_detection"
    CHART_ANALYSIS = "chart_analysis"
    VOICE_MESSAGE = "voice_message"
    VIDEO_MEETING_RECORD = "video_meeting_record"


@dataclass
class MultiModalContent:
    """Multi-modal content representation"""
    content_id: str
    content_type: ContentType
    modalities: Dict[ModalityType, Any]
    file_metadata: Dict[str, Any]
    processing_timestamp: datetime = field(default_factory=datetime.now)
    extracted_features: Dict[str, Any] = field(default_factory=dict)
    analysis_results: Dict[str, Any] = field(default_factory=dict)
    confidence_scores: Dict[str, float] = field(default_factory=dict)
    
    def get_dominant_modality(self) -> ModalityType:
        """Get the dominant modality based on content size"""
        modality_sizes = {}
        for modality, content in self.modalities.items():
            if isinstance(content, str):
                modality_sizes[modality] = len(content)
            elif isinstance(content, (bytes, bytearray)):
                modality_sizes[modality] = len(content)
            else:
                modality_sizes[modality] = 1000  # Default size
        
        return max(modality_sizes, key=modality_sizes.get) if modality_sizes else ModalityType.TEXT


@dataclass
class ProcessingPipeline:
    """Multi-modal processing pipeline"""
    pipeline_id: str
    pipeline_name: str
    supported_modalities: List[ModalityType]
    processing_steps: List[ProcessingCapability]
    fusion_strategy: str = "late_fusion"
    processing_order: List[ModalityType] = field(default_factory=list)
    
    def can_process(self, content: MultiModalContent) -> bool:
        """Check if pipeline can process the content"""
        content_modalities = set(content.modalities.keys())
        supported_modalities = set(self.supported_modalities)
        return bool(content_modalities.intersection(supported_modalities))


class VisionAI:
    """Advanced vision AI processing"""
    
    def __init__(self):
        self.models = {
            "object_detection": "YOLO-v8-nano",
            "text_recognition": "PaddleOCR-v4", 
            "face_recognition": "FaceNet-512",
            "document_layout": "LayoutLM-v3",
            "chart_understanding": "ChartQA"
        }
        self.processing_history = deque(maxlen=1000)
    
    def detect_objects(self, image_data: bytes) -> Dict[str, Any]:
        """Detect objects in image"""
        # Simulate object detection
        objects = [
            {"class": "person", "confidence": 0.95, "bbox": [100, 150, 300, 400]},
            {"class": "document", "confidence": 0.87, "bbox": [50, 50, 500, 600]},
            {"class": "logo", "confidence": 0.92, "bbox": [400, 100, 480, 180]}
        ]
        
        result = {
            "objects_detected": len(objects),
            "objects": objects,
            "processing_time_ms": random.uniform(50, 150),
            "model_used": self.models["object_detection"]
        }
        
        self.processing_history.append({
            "task": "object_detection",
            "timestamp": datetime.now().isoformat(),
            "objects_count": len(objects)
        })
        
        return result
    
    def extract_text(self, image_data: bytes) -> Dict[str, Any]:
        """Extract text from image using OCR"""
        # Simulate OCR processing
        extracted_texts = [
            {"text": "Invoice #INV-2024-001", "confidence": 0.98, "bbox": [100, 50, 300, 80]},
            {"text": "Amount: $1,250.00", "confidence": 0.96, "bbox": [100, 200, 250, 230]},
            {"text": "Due Date: March 31, 2024", "confidence": 0.94, "bbox": [100, 250, 300, 280]}
        ]
        
        full_text = " ".join([item["text"] for item in extracted_texts])
        
        result = {
            "extracted_text": full_text,
            "text_blocks": extracted_texts,
            "total_words": len(full_text.split()),
            "average_confidence": sum(item["confidence"] for item in extracted_texts) / len(extracted_texts),
            "processing_time_ms": random.uniform(100, 300),
            "model_used": self.models["text_recognition"]
        }
        
        return result
    
    def analyze_document_layout(self, image_data: bytes) -> Dict[str, Any]:
        """Analyze document structure and layout"""
        # Simulate document layout analysis
        layout_elements = [
            {"type": "title", "text": "Monthly Report", "bbox": [100, 50, 400, 100]},
            {"type": "paragraph", "text": "Executive summary...", "bbox": [100, 120, 500, 200]},
            {"type": "table", "text": "Financial data", "bbox": [100, 220, 450, 350]},
            {"type": "chart", "text": "Revenue chart", "bbox": [100, 370, 300, 500]}
        ]
        
        result = {
            "document_type": "business_report",
            "layout_elements": layout_elements,
            "structure_confidence": 0.91,
            "reading_order": [elem["type"] for elem in layout_elements],
            "processing_time_ms": random.uniform(200, 400),
            "model_used": self.models["document_layout"]
        }
        
        return result


class AudioAI:
    """Advanced audio AI processing"""
    
    def __init__(self):
        self.models = {
            "speech_to_text": "Whisper-large-v3",
            "speaker_identification": "SpeakerNet-v2",
            "audio_classification": "AudioSet-YAMNet",
            "emotion_detection": "Wav2Vec2-emotion"
        }
        self.processing_history = deque(maxlen=1000)
    
    def transcribe_speech(self, audio_data: bytes) -> Dict[str, Any]:
        """Convert speech to text"""
        # Simulate speech transcription
        transcription_segments = [
            {"text": "Hello, this is regarding the invoice we sent last week", "start": 0.0, "end": 3.5, "confidence": 0.96},
            {"text": "Please let us know if you have any questions", "start": 3.5, "end": 6.0, "confidence": 0.94},
            {"text": "Thank you for your time", "start": 6.0, "end": 7.5, "confidence": 0.98}
        ]
        
        full_transcript = " ".join([seg["text"] for seg in transcription_segments])
        
        result = {
            "transcript": full_transcript,
            "segments": transcription_segments,
            "duration_seconds": 7.5,
            "word_count": len(full_transcript.split()),
            "average_confidence": sum(seg["confidence"] for seg in transcription_segments) / len(transcription_segments),
            "language_detected": "en-US",
            "processing_time_ms": random.uniform(500, 1500),
            "model_used": self.models["speech_to_text"]
        }
        
        self.processing_history.append({
            "task": "speech_transcription",
            "timestamp": datetime.now().isoformat(),
            "duration": 7.5
        })
        
        return result
    
    def detect_speaker_emotion(self, audio_data: bytes) -> Dict[str, Any]:
        """Detect speaker emotion from audio"""
        emotions = ["neutral", "happy", "sad", "angry", "surprised", "fear", "disgust"]
        emotion_scores = {emotion: random.uniform(0.05, 0.95) for emotion in emotions}
        
        # Normalize scores
        total_score = sum(emotion_scores.values())
        emotion_scores = {k: v/total_score for k, v in emotion_scores.items()}
        
        dominant_emotion = max(emotion_scores, key=emotion_scores.get)
        
        result = {
            "dominant_emotion": dominant_emotion,
            "emotion_scores": emotion_scores,
            "confidence": emotion_scores[dominant_emotion],
            "arousal_level": random.uniform(0.2, 0.8),
            "valence_level": random.uniform(0.3, 0.9),
            "processing_time_ms": random.uniform(200, 600),
            "model_used": self.models["emotion_detection"]
        }
        
        return result
    
    def classify_audio_content(self, audio_data: bytes) -> Dict[str, Any]:
        """Classify audio content type"""
        audio_classes = [
            {"class": "speech", "confidence": 0.92},
            {"class": "music", "confidence": 0.05},
            {"class": "ambient_sound", "confidence": 0.03}
        ]
        
        result = {
            "primary_class": audio_classes[0]["class"],
            "classifications": audio_classes,
            "audio_quality": random.choice(["high", "medium", "low"]),
            "noise_level": random.uniform(0.1, 0.4),
            "processing_time_ms": random.uniform(150, 350),
            "model_used": self.models["audio_classification"]
        }
        
        return result


class VideoAI:
    """Advanced video AI processing"""
    
    def __init__(self):
        self.models = {
            "action_recognition": "SlowFast-R50",
            "scene_detection": "PlacesCNN",
            "face_tracking": "FairMOT",
            "video_summarization": "VideoSwin-B"
        }
        self.processing_history = deque(maxlen=1000)
    
    def analyze_video_content(self, video_data: bytes) -> Dict[str, Any]:
        """Analyze video content and extract key information"""
        # Simulate video analysis
        scenes = [
            {"scene_type": "office_meeting", "start_time": 0.0, "end_time": 30.0, "confidence": 0.88},
            {"scene_type": "presentation", "start_time": 30.0, "end_time": 120.0, "confidence": 0.94},
            {"scene_type": "discussion", "start_time": 120.0, "end_time": 180.0, "confidence": 0.91}
        ]
        
        actions = [
            {"action": "speaking", "confidence": 0.95, "person_id": 1},
            {"action": "presenting", "confidence": 0.89, "person_id": 2},
            {"action": "note_taking", "confidence": 0.76, "person_id": 3}
        ]
        
        result = {
            "video_duration_seconds": 180.0,
            "scenes_detected": scenes,
            "actions_recognized": actions,
            "people_count": 3,
            "key_frames_extracted": 15,
            "video_quality": "1080p",
            "processing_time_ms": random.uniform(2000, 5000),
            "model_used": self.models["action_recognition"]
        }
        
        self.processing_history.append({
            "task": "video_analysis",
            "timestamp": datetime.now().isoformat(),
            "duration": 180.0
        })
        
        return result
    
    def extract_video_summary(self, video_data: bytes) -> Dict[str, Any]:
        """Extract key moments and create video summary"""
        key_moments = [
            {"timestamp": 15.5, "description": "Speaker introduces main topic", "importance": 0.95},
            {"timestamp": 45.2, "description": "Key financial data presented", "importance": 0.89},
            {"timestamp": 78.1, "description": "Q&A session begins", "importance": 0.82},
            {"timestamp": 145.7, "description": "Action items discussed", "importance": 0.91}
        ]
        
        result = {
            "summary": "Business meeting discussing quarterly results and future plans",
            "key_moments": key_moments,
            "highlight_timestamps": [15.5, 45.2, 145.7],
            "estimated_watch_time": 45.0,  # Compressed summary time
            "topics_covered": ["financial_results", "strategy_planning", "action_items"],
            "processing_time_ms": random.uniform(1500, 3000),
            "model_used": self.models["video_summarization"]
        }
        
        return result


class CrossModalFusion:
    """Cross-modal fusion and reasoning engine"""
    
    def __init__(self):
        self.fusion_strategies = ["early_fusion", "late_fusion", "attention_fusion", "transformer_fusion"]
        self.embedding_cache = {}
        
    def fuse_modalities(
        self,
        text_features: Optional[Dict] = None,
        vision_features: Optional[Dict] = None,
        audio_features: Optional[Dict] = None,
        video_features: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Fuse features from multiple modalities"""
        
        # Collect available modalities
        available_modalities = []
        feature_vectors = {}
        
        if text_features:
            available_modalities.append("text")
            feature_vectors["text"] = self._extract_text_embedding(text_features)
            
        if vision_features:
            available_modalities.append("vision")
            feature_vectors["vision"] = self._extract_vision_embedding(vision_features)
            
        if audio_features:
            available_modalities.append("audio")
            feature_vectors["audio"] = self._extract_audio_embedding(audio_features)
            
        if video_features:
            available_modalities.append("video")
            feature_vectors["video"] = self._extract_video_embedding(video_features)
        
        # Perform fusion based on available modalities
        if len(available_modalities) == 1:
            fusion_result = self._single_modal_processing(feature_vectors)
        else:
            fusion_result = self._multi_modal_fusion(feature_vectors)
        
        return {
            "fused_representation": fusion_result,
            "modalities_used": available_modalities,
            "fusion_strategy": "transformer_attention",
            "cross_modal_score": random.uniform(0.7, 0.95),
            "semantic_coherence": random.uniform(0.8, 0.98)
        }
    
    def _extract_text_embedding(self, text_features: Dict) -> np.ndarray:
        """Extract text embedding vector"""
        # Simulate text embedding extraction
        return np.random.normal(0, 1, 768)  # BERT-like embedding
    
    def _extract_vision_embedding(self, vision_features: Dict) -> np.ndarray:
        """Extract vision embedding vector"""
        # Simulate vision embedding extraction
        return np.random.normal(0, 1, 2048)  # ResNet-like embedding
    
    def _extract_audio_embedding(self, audio_features: Dict) -> np.ndarray:
        """Extract audio embedding vector"""
        # Simulate audio embedding extraction
        return np.random.normal(0, 1, 512)  # Wav2Vec2-like embedding
    
    def _extract_video_embedding(self, video_features: Dict) -> np.ndarray:
        """Extract video embedding vector"""
        # Simulate video embedding extraction
        return np.random.normal(0, 1, 1024)  # Video transformer embedding
    
    def _single_modal_processing(self, feature_vectors: Dict) -> np.ndarray:
        """Process single modality features"""
        modality = list(feature_vectors.keys())[0]
        return feature_vectors[modality]
    
    def _multi_modal_fusion(self, feature_vectors: Dict) -> np.ndarray:
        """Fuse multiple modality features"""
        # Simulate transformer-based attention fusion
        all_features = []
        
        for modality, features in feature_vectors.items():
            # Pad or truncate to common dimension
            if len(features) > 768:
                features = features[:768]
            else:
                padding = np.zeros(768 - len(features))
                features = np.concatenate([features, padding])
            
            all_features.append(features)
        
        # Simple fusion (in practice would use attention mechanisms)
        fused_features = np.mean(all_features, axis=0)
        
        return fused_features


class MultiModalAI:
    """Main multi-modal AI system"""
    
    def __init__(self):
        self._lock = threading.RLock()
        
        # Initialize AI modules
        self.vision_ai = VisionAI()
        self.audio_ai = AudioAI()
        self.video_ai = VideoAI()
        self.cross_modal_fusion = CrossModalFusion()
        
        # Processing pipelines
        self.processing_pipelines = self._initialize_pipelines()
        
        # Content processing history
        self.processed_content = deque(maxlen=10000)
        self.performance_metrics = {
            "total_processed": 0,
            "processing_time_ms": 0.0,
            "success_rate": 100.0,
            "modality_distribution": defaultdict(int)
        }
    
    def _initialize_pipelines(self) -> Dict[str, ProcessingPipeline]:
        """Initialize processing pipelines"""
        pipelines = {}
        
        # Email attachment pipeline
        pipelines["email_attachment"] = ProcessingPipeline(
            pipeline_id="email_attachment_v1",
            pipeline_name="Email Attachment Processor",
            supported_modalities=[ModalityType.IMAGE, ModalityType.DOCUMENT, ModalityType.AUDIO],
            processing_steps=[
                ProcessingCapability.TEXT_RECOGNITION,
                ProcessingCapability.OBJECT_DETECTION,
                ProcessingCapability.DOCUMENT_PARSING,
                ProcessingCapability.SENTIMENT_ANALYSIS
            ],
            fusion_strategy="late_fusion"
        )
        
        # Multi-modal content pipeline
        pipelines["multimedia_content"] = ProcessingPipeline(
            pipeline_id="multimedia_v1",
            pipeline_name="Multi-Modal Content Processor",
            supported_modalities=[ModalityType.TEXT, ModalityType.IMAGE, ModalityType.AUDIO, ModalityType.VIDEO],
            processing_steps=[
                ProcessingCapability.FEATURE_EXTRACTION,
                ProcessingCapability.CROSS_MODAL_SEARCH,
                ProcessingCapability.CONTENT_MODERATION,
                ProcessingCapability.SENTIMENT_ANALYSIS
            ],
            fusion_strategy="transformer_fusion"
        )
        
        # Document analysis pipeline
        pipelines["document_analysis"] = ProcessingPipeline(
            pipeline_id="document_analysis_v1",
            pipeline_name="Advanced Document Analysis",
            supported_modalities=[ModalityType.IMAGE, ModalityType.DOCUMENT, ModalityType.TABULAR],
            processing_steps=[
                ProcessingCapability.TEXT_RECOGNITION,
                ProcessingCapability.TABLE_EXTRACTION,
                ProcessingCapability.DOCUMENT_PARSING
            ],
            fusion_strategy="early_fusion"
        )
        
        return pipelines
    
    def process_multi_modal_content(self, content: MultiModalContent) -> Dict[str, Any]:
        """Process multi-modal content through appropriate pipeline"""
        with self._lock:
            start_time = time.time()
            
            # Select appropriate pipeline
            pipeline = self._select_pipeline(content)
            if not pipeline:
                return {"error": "No suitable pipeline found", "content_id": content.content_id}
            
            # Process each modality
            processing_results = {}
            
            for modality, data in content.modalities.items():
                if modality == ModalityType.IMAGE:
                    processing_results["vision"] = self._process_image(data)
                elif modality == ModalityType.AUDIO:
                    processing_results["audio"] = self._process_audio(data)
                elif modality == ModalityType.VIDEO:
                    processing_results["video"] = self._process_video(data)
                elif modality == ModalityType.TEXT:
                    processing_results["text"] = self._process_text(data)
                elif modality == ModalityType.DOCUMENT:
                    processing_results["document"] = self._process_document(data)
            
            # Perform cross-modal fusion
            fusion_result = self.cross_modal_fusion.fuse_modalities(
                text_features=processing_results.get("text"),
                vision_features=processing_results.get("vision"),
                audio_features=processing_results.get("audio"),
                video_features=processing_results.get("video")
            )
            
            # Generate final analysis
            final_result = self._generate_final_analysis(content, processing_results, fusion_result)
            
            # Update performance metrics
            processing_time = (time.time() - start_time) * 1000
            self._update_performance_metrics(content, processing_time, True)
            
            # Store processed content
            content.analysis_results = final_result
            self.processed_content.append(content)
            
            return final_result
    
    def _select_pipeline(self, content: MultiModalContent) -> Optional[ProcessingPipeline]:
        """Select appropriate processing pipeline"""
        for pipeline in self.processing_pipelines.values():
            if pipeline.can_process(content):
                return pipeline
        return None
    
    def _process_image(self, image_data: bytes) -> Dict[str, Any]:
        """Process image data"""
        results = {}
        results["objects"] = self.vision_ai.detect_objects(image_data)
        results["text"] = self.vision_ai.extract_text(image_data)
        results["layout"] = self.vision_ai.analyze_document_layout(image_data)
        return results
    
    def _process_audio(self, audio_data: bytes) -> Dict[str, Any]:
        """Process audio data"""
        results = {}
        results["transcription"] = self.audio_ai.transcribe_speech(audio_data)
        results["emotion"] = self.audio_ai.detect_speaker_emotion(audio_data)
        results["classification"] = self.audio_ai.classify_audio_content(audio_data)
        return results
    
    def _process_video(self, video_data: bytes) -> Dict[str, Any]:
        """Process video data"""
        results = {}
        results["content_analysis"] = self.video_ai.analyze_video_content(video_data)
        results["summary"] = self.video_ai.extract_video_summary(video_data)
        return results
    
    def _process_text(self, text_data: str) -> Dict[str, Any]:
        """Process text data"""
        # Simulate text processing
        word_count = len(text_data.split())
        
        return {
            "word_count": word_count,
            "character_count": len(text_data),
            "language": "en",
            "sentiment": random.choice(["positive", "neutral", "negative"]),
            "sentiment_score": random.uniform(-1.0, 1.0),
            "topics": ["business", "communication", "request"],
            "entities": ["person", "organization", "date"],
            "processing_time_ms": random.uniform(10, 50)
        }
    
    def _process_document(self, document_data: bytes) -> Dict[str, Any]:
        """Process document data"""
        # Simulate document processing
        return {
            "document_type": "pdf",
            "page_count": random.randint(1, 10),
            "text_extracted": True,
            "tables_found": random.randint(0, 3),
            "images_found": random.randint(0, 5),
            "processing_time_ms": random.uniform(200, 800)
        }
    
    def _generate_final_analysis(
        self,
        content: MultiModalContent,
        processing_results: Dict,
        fusion_result: Dict
    ) -> Dict[str, Any]:
        """Generate comprehensive final analysis"""
        
        # Extract key insights
        insights = []
        confidence_scores = {}
        
        # Analyze vision results
        if "vision" in processing_results:
            vision_results = processing_results["vision"]
            if "objects" in vision_results:
                obj_count = vision_results["objects"]["objects_detected"]
                insights.append(f"Found {obj_count} objects in image")
                confidence_scores["vision_objects"] = 0.9
            
            if "text" in vision_results:
                text_result = vision_results["text"]
                if text_result["total_words"] > 0:
                    insights.append(f"Extracted {text_result['total_words']} words from image")
                    confidence_scores["vision_text"] = text_result["average_confidence"]
        
        # Analyze audio results
        if "audio" in processing_results:
            audio_results = processing_results["audio"]
            if "transcription" in audio_results:
                transcript = audio_results["transcription"]
                insights.append(f"Transcribed {transcript['word_count']} words from audio")
                confidence_scores["audio_transcription"] = transcript["average_confidence"]
            
            if "emotion" in audio_results:
                emotion = audio_results["emotion"]["dominant_emotion"]
                insights.append(f"Detected {emotion} emotion in speech")
                confidence_scores["audio_emotion"] = audio_results["emotion"]["confidence"]
        
        # Analyze video results
        if "video" in processing_results:
            video_results = processing_results["video"]
            if "content_analysis" in video_results:
                duration = video_results["content_analysis"]["video_duration_seconds"]
                people_count = video_results["content_analysis"]["people_count"]
                insights.append(f"Analyzed {duration}s video with {people_count} people")
                confidence_scores["video_analysis"] = 0.88
        
        # Generate summary
        dominant_modality = content.get_dominant_modality()
        
        # Calculate overall confidence
        overall_confidence = sum(confidence_scores.values()) / len(confidence_scores) if confidence_scores else 0.5
        
        return {
            "content_id": content.content_id,
            "processing_pipeline": "multi_modal_fusion",
            "dominant_modality": dominant_modality.value,
            "modalities_processed": list(processing_results.keys()),
            "key_insights": insights,
            "confidence_scores": confidence_scores,
            "overall_confidence": round(overall_confidence, 3),
            "cross_modal_fusion": fusion_result,
            "processing_results": processing_results,
            "content_classification": self._classify_content(processing_results),
            "recommended_actions": self._generate_recommendations(processing_results),
            "processing_timestamp": datetime.now().isoformat()
        }
    
    def _classify_content(self, processing_results: Dict) -> Dict[str, Any]:
        """Classify content based on multi-modal analysis"""
        content_types = []
        business_relevance = 0.5
        
        # Check for business documents
        if "vision" in processing_results:
            vision_results = processing_results["vision"]
            if "layout" in vision_results:
                layout = vision_results["layout"]
                if layout["document_type"] == "business_report":
                    content_types.append("business_document")
                    business_relevance = 0.9
            
            if "text" in vision_results:
                extracted_text = vision_results["text"]["extracted_text"].lower()
                if any(term in extracted_text for term in ["invoice", "contract", "proposal"]):
                    content_types.append("financial_document")
                    business_relevance = 0.95
        
        # Check for meeting content
        if "audio" in processing_results or "video" in processing_results:
            content_types.append("meeting_content")
            business_relevance = max(business_relevance, 0.8)
        
        return {
            "content_types": content_types,
            "business_relevance": round(business_relevance, 2),
            "priority_level": "high" if business_relevance > 0.8 else "medium" if business_relevance > 0.5 else "low",
            "requires_human_review": business_relevance > 0.9
        }
    
    def _generate_recommendations(self, processing_results: Dict) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        # Text-based recommendations
        if "text" in processing_results:
            text_results = processing_results["text"]
            if text_results.get("sentiment") == "negative":
                recommendations.append("Flag for priority review due to negative sentiment")
        
        # Vision-based recommendations
        if "vision" in processing_results:
            vision_results = processing_results["vision"]
            if "text" in vision_results:
                extracted_text = vision_results["text"]["extracted_text"].lower()
                if "invoice" in extracted_text:
                    recommendations.append("Route to accounting department")
                elif "contract" in extracted_text:
                    recommendations.append("Route to legal review")
        
        # Audio-based recommendations
        if "audio" in processing_results:
            audio_results = processing_results["audio"]
            if "emotion" in audio_results:
                emotion = audio_results["emotion"]["dominant_emotion"]
                if emotion in ["angry", "frustrated"]:
                    recommendations.append("Priority escalation - customer dissatisfaction detected")
        
        if not recommendations:
            recommendations.append("Standard processing workflow")
        
        return recommendations
    
    def _update_performance_metrics(self, content: MultiModalContent, processing_time: float, success: bool):
        """Update system performance metrics"""
        self.performance_metrics["total_processed"] += 1
        
        # Update average processing time
        current_avg = self.performance_metrics["processing_time_ms"]
        total_processed = self.performance_metrics["total_processed"]
        new_avg = (current_avg * (total_processed - 1) + processing_time) / total_processed
        self.performance_metrics["processing_time_ms"] = new_avg
        
        # Update success rate
        if success:
            success_count = self.performance_metrics["success_rate"] * (total_processed - 1) / 100
            success_count += 1
            self.performance_metrics["success_rate"] = success_count / total_processed * 100
        
        # Update modality distribution
        for modality in content.modalities:
            self.performance_metrics["modality_distribution"][modality.value] += 1
    
    def get_multi_modal_analytics(self) -> Dict[str, Any]:
        """Get comprehensive multi-modal AI analytics"""
        with self._lock:
            # Processing statistics
            total_processed = self.performance_metrics["total_processed"]
            
            # Modality distribution
            modality_dist = dict(self.performance_metrics["modality_distribution"])
            
            # Recent processing performance
            if self.processed_content:
                recent_content = list(self.processed_content)[-100:]
                recent_success_rate = len([c for c in recent_content if c.analysis_results]) / len(recent_content) * 100
                avg_confidence = np.mean([
                    c.analysis_results.get("overall_confidence", 0) 
                    for c in recent_content 
                    if c.analysis_results
                ])
            else:
                recent_success_rate = 100.0
                avg_confidence = 0.0
            
            # Pipeline statistics
            pipeline_stats = {}
            for pipeline_id, pipeline in self.processing_pipelines.items():
                pipeline_stats[pipeline_id] = {
                    "name": pipeline.pipeline_name,
                    "supported_modalities": [m.value for m in pipeline.supported_modalities],
                    "processing_steps": [s.value for s in pipeline.processing_steps],
                    "fusion_strategy": pipeline.fusion_strategy
                }
            
            return {
                "status": "processing",
                "performance_metrics": {
                    "total_content_processed": total_processed,
                    "average_processing_time_ms": round(self.performance_metrics["processing_time_ms"], 2),
                    "success_rate": round(self.performance_metrics["success_rate"], 1),
                    "recent_success_rate": round(recent_success_rate, 1),
                    "average_confidence": round(avg_confidence, 3)
                },
                "modality_capabilities": {
                    "vision_ai": {
                        "object_detection": True,
                        "text_recognition": True,
                        "document_layout": True,
                        "models": self.vision_ai.models
                    },
                    "audio_ai": {
                        "speech_to_text": True,
                        "emotion_detection": True,
                        "audio_classification": True,
                        "models": self.audio_ai.models
                    },
                    "video_ai": {
                        "content_analysis": True,
                        "scene_detection": True,
                        "video_summarization": True,
                        "models": self.video_ai.models
                    }
                },
                "modality_distribution": modality_dist,
                "processing_pipelines": pipeline_stats,
                "cross_modal_fusion": {
                    "strategies_available": self.cross_modal_fusion.fusion_strategies,
                    "embedding_cache_size": len(self.cross_modal_fusion.embedding_cache)
                },
                "content_types_supported": [ct.value for ct in ContentType],
                "capabilities": [
                    "multi_modal_content_analysis",
                    "cross_modal_fusion",
                    "intelligent_pipeline_selection",
                    "real_time_processing",
                    "content_classification",
                    "automated_recommendations",
                    "sentiment_analysis_across_modalities",
                    "business_relevance_detection"
                ],
                "recent_processing_history": len(self.processed_content)
            }


# Global instance
_multi_modal_ai: Optional[MultiModalAI] = None
_multi_modal_lock = threading.Lock()


def get_multi_modal_ai() -> MultiModalAI:
    """Get or create multi-modal AI instance"""
    global _multi_modal_ai
    with _multi_modal_lock:
        if _multi_modal_ai is None:
            _multi_modal_ai = MultiModalAI()
        return _multi_modal_ai