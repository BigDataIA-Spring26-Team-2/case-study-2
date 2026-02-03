"""Industry Model for PE Org-AI-R Platform."""
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID
from datetime import datetime
from typing import Optional


class IndustryBase(BaseModel):
    """Base industry attributes."""
    
    name: str = Field(..., min_length=1, max_length=255)
    sector: str = Field(..., min_length=1, max_length=100)
    h_r_base: float = Field(..., ge=0, le=100)


class IndustryCreate(IndustryBase):
    """Schema for creating a new industry."""
    pass


class IndustryResponse(IndustryBase):
    """Schema for industry responses."""
    
    id: UUID
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)