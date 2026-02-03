"""Evidence document and chunk models for CS2."""
from pydantic import BaseModel, Field, field_validator
from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import Optional
from enum import Enum


class DocumentStatus(str, Enum):
    """Document processing status."""
    PARSED = "parsed"
    FAILED = "failed"


class EvidenceDocumentCreate(BaseModel):
    """Data required to create a new evidence document."""
    company_id: UUID
    ticker: str = Field(min_length=1, max_length=10)
    filing_type: str = Field(min_length=1, max_length=20)
    filing_date: datetime
    accession_number: str = Field(min_length=1, max_length=50)
    
    # Storage references
    content_hash: str = Field(min_length=64, max_length=64)  # SHA256
    s3_key: Optional[str] = Field(None, max_length=500)
    
    # Pre-computed statistics
    total_chunks: int = Field(ge=0)
    total_words: int = Field(ge=0)
    section_count: int = Field(ge=0)
    table_count: int = Field(ge=0)
    
    # Section-level aggregates (small JSON)
    sections_summary: dict = Field(default_factory=dict)
    
    @field_validator('ticker')
    @classmethod
    def uppercase_ticker(cls, v: str) -> str:
        """Ensure ticker is uppercase."""
        return v.upper()
    
    @field_validator('filing_type')
    @classmethod
    def validate_filing_type(cls, v: str) -> str:
        """Validate filing type is known format."""
        valid_types = ['10-K', '10-Q', '8-K', 'DEF-14A', '20-F']
        if v.upper() not in valid_types:
            # Allow it but log warning
            pass
        return v.upper()


class EvidenceDocumentResponse(EvidenceDocumentCreate):
    """Evidence document with generated fields."""
    id: UUID = Field(default_factory=uuid4)
    status: DocumentStatus = DocumentStatus.PARSED
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        from_attributes = True


class ChunkCreate(BaseModel):
    """Data required to create a document chunk."""
    document_id: UUID
    chunk_index: int = Field(ge=0)
    
    # Section context
    section_id: Optional[str] = Field(None, max_length=50)
    section_title: Optional[str] = Field(None, max_length=255)
    
    # Content
    content: str = Field(min_length=1)
    
    # Metadata
    word_count: int = Field(ge=0)
    has_table: bool = False
    page: int = Field(ge=1)


class ChunkResponse(ChunkCreate):
    """Chunk with generated fields."""
    id: UUID = Field(default_factory=uuid4)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    class Config:
        from_attributes = True


class DocumentSummary(BaseModel):
    """Lightweight document summary for list views."""
    id: UUID
    ticker: str
    filing_type: str
    filing_date: datetime
    total_chunks: int
    section_count: int
    status: DocumentStatus
    
    class Config:
        from_attributes = True