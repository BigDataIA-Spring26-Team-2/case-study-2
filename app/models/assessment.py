"""Assessment Model for PE Org-AI-R Platform."""
from pydantic import BaseModel, Field, model_validator, ConfigDict
from uuid import UUID
from datetime import datetime, timezone
from typing import Optional, TYPE_CHECKING
from app.models.enums import AssessmentType, AssessmentStatus

if TYPE_CHECKING:
    from app.models.dimension import DimensionScoreResponse


class AssessmentBase(BaseModel):
    """Base assessment attributes."""
    
    company_id: UUID
    assessment_type: AssessmentType
    assessment_date: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    primary_assessor: Optional[str] = None
    secondary_assessor: Optional[str] = None


class AssessmentCreate(AssessmentBase):
    """Schema for creating a new assessment."""
    pass


class AssessmentUpdate(BaseModel):
    """Schema for updating an assessment."""
    
    assessment_type: Optional[AssessmentType] = None
    assessment_date: Optional[datetime] = None
    primary_assessor: Optional[str] = None
    secondary_assessor: Optional[str] = None
    status: Optional[AssessmentStatus] = None


class AssessmentResponse(AssessmentBase):
    """Schema for assessment responses."""
    
    id: UUID
    status: AssessmentStatus = AssessmentStatus.DRAFT
    v_r_score: Optional[float] = Field(None, ge=0, le=100)
    confidence_lower: Optional[float] = Field(None, ge=0, le=100)
    confidence_upper: Optional[float] = Field(None, ge=0, le=100)
    created_at: datetime
    
    @model_validator(mode='after')
    def validate_confidence_interval(self) -> 'AssessmentResponse':
        if (self.confidence_upper is not None and 
            self.confidence_lower is not None and 
            self.confidence_upper < self.confidence_lower):
            raise ValueError('confidence_upper must be >= confidence_lower')
        return self
    
    model_config = ConfigDict(from_attributes=True)


class AssessmentWithScores(AssessmentResponse):
    """Assessment response with dimension scores."""
    
    dimension_scores: list['DimensionScoreResponse'] = []