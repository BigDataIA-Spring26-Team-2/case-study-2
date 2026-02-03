"""Dimension Score Model for PE Org-AI-R Platform."""
from pydantic import BaseModel, Field, model_validator, ConfigDict
from uuid import UUID
from datetime import datetime
from typing import Optional
from app.models.enums import Dimension, DIMENSION_WEIGHTS


class DimensionScoreBase(BaseModel):
    """Base dimension score attributes."""
    
    assessment_id: UUID
    dimension: Dimension
    score: float = Field(..., ge=0, le=100)
    weight: Optional[float] = Field(default=None, ge=0, le=1)
    confidence: float = Field(default=0.8, ge=0, le=1)
    evidence_count: int = Field(default=0, ge=0)
    
    @model_validator(mode='after')
    def set_default_weight(self) -> 'DimensionScoreBase':
        if self.weight is None:
            self.weight = DIMENSION_WEIGHTS.get(self.dimension, 0.1)
        return self


class DimensionScoreCreate(DimensionScoreBase):
    """Schema for creating a dimension score."""
    pass


class DimensionScoreUpdate(BaseModel):
    """Schema for updating a dimension score."""
    
    score: Optional[float] = Field(None, ge=0, le=100)
    weight: Optional[float] = Field(None, ge=0, le=1)
    confidence: Optional[float] = Field(None, ge=0, le=1)
    evidence_count: Optional[int] = Field(None, ge=0)


class DimensionScoreResponse(DimensionScoreBase):
    """Schema for dimension score responses."""
    
    id: UUID
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)