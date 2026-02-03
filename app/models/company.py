"""Company Model for PE Org-AI-R Platform."""
from pydantic import BaseModel, Field, field_validator, ConfigDict
from uuid import UUID
from datetime import datetime
from typing import Optional


class CompanyBase(BaseModel):
    """Base company attributes."""
    
    name: str = Field(..., min_length=1, max_length=255)
    ticker: Optional[str] = Field(None, max_length=10)
    industry_id: UUID
    position_factor: float = Field(default=0.0, ge=-1.0, le=1.0)
    
    @field_validator('ticker')
    @classmethod
    def uppercase_ticker(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else None
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('Company name cannot be empty or whitespace')
        return v.strip()


class CompanyCreate(CompanyBase):
    """Schema for creating a new company."""
    pass


class CompanyUpdate(BaseModel):
    """Schema for updating an existing company."""
    
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    ticker: Optional[str] = Field(None, max_length=10)
    industry_id: Optional[UUID] = None
    position_factor: Optional[float] = Field(None, ge=-1.0, le=1.0)
    
    @field_validator('ticker')
    @classmethod
    def uppercase_ticker(cls, v: Optional[str]) -> Optional[str]:
        return v.upper() if v else None
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError('Company name cannot be empty or whitespace')
        return v.strip() if v else None


class CompanyResponse(CompanyBase):
    """Schema for company responses."""
    
    id: UUID
    is_deleted: bool = False
    created_at: datetime
    updated_at: datetime
    
    model_config = ConfigDict(from_attributes=True)


class CompanyWithIndustry(CompanyResponse):
    """Company response with industry details."""
    
    industry_name: str
    industry_sector: str