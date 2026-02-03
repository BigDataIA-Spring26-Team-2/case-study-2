"""Signal models for external evidence."""
from pydantic import BaseModel, Field, model_validator
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional
from enum import Enum


class SignalCategory(str, Enum):
    """Signal category types."""
    HIRING = "hiring"
    PATENT = "patent"
    GITHUB = "github"
    LEADERSHIP = "leadership"


class SignalCreate(BaseModel):
    """Data required to create a signal."""
    company_id: UUID
    category: SignalCategory
    
    # Core signal output
    score: float = Field(ge=0, le=100)
    confidence: float = Field(ge=0, le=1)
    
    # Supporting data
    metadata: dict = Field(default_factory=dict)
    
    @model_validator(mode='after')
    def validate_metadata(self) -> 'SignalCreate':
        """Ensure metadata has required fields based on category."""
        if self.category == SignalCategory.HIRING:
            # Hiring signals should document their source
            if 'source' not in self.metadata:
                self.metadata['source'] = 'unknown'
        
        # Add collection timestamp if not present
        if 'collected_at' not in self.metadata:
            self.metadata['collected_at'] = datetime.now(timezone.utc).isoformat()
        
        return self


class SignalResponse(SignalCreate):
    """Signal with generated fields."""
    id: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        from_attributes = True


class CompanyEvidenceSummary(BaseModel):
    """
    Pre-aggregated evidence summary for a company.
    This is the primary model for dashboard queries.
    """
    company_id: UUID
    ticker: str
    
    # Document aggregates
    total_documents: int = 0
    total_chunks: int = 0
    latest_filing_date: Optional[datetime] = None
    has_10k: bool = False
    has_10q: bool = False
    has_8k: bool = False
    
    # Signal scores (one per category)
    hiring_score: Optional[float] = Field(None, ge=0, le=100)
    hiring_metadata: Optional[dict] = None
    
    patent_score: Optional[float] = Field(None, ge=0, le=100)
    patent_metadata: Optional[dict] = None
    
    github_score: Optional[float] = Field(None, ge=0, le=100)
    github_metadata: Optional[dict] = None
    
    leadership_score: Optional[float] = Field(None, ge=0, le=100)
    leadership_metadata: Optional[dict] = None
    
    # Composite score (calculated)
    composite_score: Optional[float] = Field(None, ge=0, le=100)
    
    # Quality metric
    evidence_quality: Optional[float] = Field(None, ge=0, le=1)
    
    # Timestamps
    last_updated: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    @model_validator(mode='after')
    def calculate_composite_score(self) -> 'CompanyEvidenceSummary':
        """
        Calculate weighted composite score from signal categories.
        
        Weights from PE Org-AI-R framework:
        - Hiring: 30%
        - Patent: 25%
        - GitHub: 20%
        - Leadership: 25%
        """
        scores = []
        weights = []
        
        if self.hiring_score is not None:
            scores.append(self.hiring_score)
            weights.append(0.30)
        
        if self.patent_score is not None:
            scores.append(self.patent_score)
            weights.append(0.25)
        
        if self.github_score is not None:
            scores.append(self.github_score)
            weights.append(0.20)
        
        if self.leadership_score is not None:
            scores.append(self.leadership_score)
            weights.append(0.25)
        
        if scores:
            # Normalize weights to sum to 1
            total_weight = sum(weights)
            normalized_weights = [w / total_weight for w in weights]
            
            # Weighted average
            self.composite_score = round(
                sum(s * w for s, w in zip(scores, normalized_weights)),
                2
            )
        
        return self
    
    @model_validator(mode='after')
    def calculate_evidence_quality(self) -> 'CompanyEvidenceSummary':
        """
        Calculate evidence quality score based on completeness and confidence.
        
        Quality = (data_completeness × 0.5) + (avg_confidence × 0.5)
        """
        # Data completeness: how many signal categories do we have?
        signals_present = sum([
            self.hiring_score is not None,
            self.patent_score is not None,
            self.github_score is not None,
            self.leadership_score is not None
        ])
        completeness = signals_present / 4  # 4 signal categories
        
        # Average confidence from metadata
        confidences = []
        for metadata in [self.hiring_metadata, self.patent_metadata, 
                        self.github_metadata, self.leadership_metadata]:
            if metadata and 'confidence' in metadata:
                confidences.append(metadata['confidence'])
        
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0.5
        
        self.evidence_quality = round(
            (completeness * 0.5) + (avg_confidence * 0.5),
            3
        )
        
        return self
    
    class Config:
        from_attributes = True