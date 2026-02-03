"""Test evidence and signal models."""
import pytest
from uuid import uuid4
from datetime import datetime, timezone
from app.models.evidence import (
    EvidenceDocumentCreate,
    EvidenceDocumentResponse,
    ChunkCreate,
    ChunkResponse,
    DocumentStatus
)
from app.models.signal import (
    SignalCreate,
    SignalResponse,
    SignalCategory,
    CompanyEvidenceSummary
)


class TestEvidenceDocumentModels:
    """Test document models."""
    
    def test_document_create_valid(self):
        """Test creating valid document."""
        doc = EvidenceDocumentCreate(
            company_id=uuid4(),
            ticker="cat",  # lowercase
            filing_type="10-k",  # lowercase
            filing_date=datetime(2024, 12, 31, tzinfo=timezone.utc),
            accession_number="0000950123-24-012345",
            content_hash="a" * 64,  # 64-char SHA256
            total_chunks=487,
            total_words=195340,
            section_count=7,
            table_count=23,
            sections_summary={
                "item_1": {"chunk_count": 45, "word_count": 18540}
            }
        )
        
        assert doc.ticker == "CAT"  # Auto-uppercased
        assert doc.filing_type == "10-K"  # Auto-uppercased
        assert doc.total_chunks == 487
    
    def test_document_create_invalid_ticker(self):
        """Test ticker validation."""
        with pytest.raises(ValueError):
            EvidenceDocumentCreate(
                company_id=uuid4(),
                ticker="",  # Empty ticker
                filing_type="10-K",
                filing_date=datetime.now(timezone.utc),
                accession_number="0000950123-24-012345",
                content_hash="a" * 64,
                total_chunks=100,
                total_words=50000,
                section_count=5,
                table_count=10
            )
    
    def test_document_response_has_id(self):
        """Test response model generates ID."""
        doc = EvidenceDocumentResponse(
            company_id=uuid4(),
            ticker="JPM",
            filing_type="10-Q",
            filing_date=datetime.now(timezone.utc),
            accession_number="0001193125-24-056789",
            content_hash="b" * 64,
            total_chunks=250,
            total_words=98000,
            section_count=4,
            table_count=15
        )
        
        assert doc.id is not None
        assert doc.status == DocumentStatus.PARSED
        assert doc.created_at is not None
    
    def test_chunk_create_valid(self):
        """Test creating valid chunk."""
        chunk = ChunkCreate(
            document_id=uuid4(),
            chunk_index=0,
            section_id="item_1",
            section_title="Business",
            content="Caterpillar Inc. operates through three primary business segments...",
            word_count=412,
            has_table=False,
            page=12
        )
        
        assert chunk.chunk_index == 0
        assert chunk.word_count == 412
        assert chunk.has_table is False
    
    def test_chunk_create_invalid_word_count(self):
        """Test negative word count validation."""
        with pytest.raises(ValueError):
            ChunkCreate(
                document_id=uuid4(),
                chunk_index=0,
                content="Test content",
                word_count=-10,  # Invalid
                page=1
            )


class TestSignalModels:
    """Test signal models."""
    
    def test_signal_create_valid(self):
        """Test creating valid signal."""
        signal = SignalCreate(
            company_id=uuid4(),
            category=SignalCategory.HIRING,
            score=75.5,
            confidence=0.85,
            metadata={
                "source": "indeed",
                "ai_job_count": 42,
                "total_tech_jobs": 87
            }
        )
        
        assert signal.score == 75.5
        assert signal.confidence == 0.85
        assert signal.category == SignalCategory.HIRING
    
    def test_signal_score_validation(self):
        """Test score must be 0-100."""
        with pytest.raises(ValueError):
            SignalCreate(
                company_id=uuid4(),
                category=SignalCategory.PATENT,
                score=150,  # Invalid - over 100
                confidence=0.9
            )
    
    def test_signal_confidence_validation(self):
        """Test confidence must be 0-1."""
        with pytest.raises(ValueError):
            SignalCreate(
                company_id=uuid4(),
                category=SignalCategory.GITHUB,
                score=68.0,
                confidence=1.5  # Invalid - over 1
            )
    
    def test_signal_metadata_auto_timestamp(self):
        """Test metadata gets auto-timestamp."""
        signal = SignalCreate(
            company_id=uuid4(),
            category=SignalCategory.LEADERSHIP,
            score=80.0,
            confidence=0.95,
            metadata={"source": "sec_def14a"}
        )
        
        assert 'collected_at' in signal.metadata


class TestCompanyEvidenceSummary:
    """Test company evidence summary model."""
    
    def test_composite_score_calculation(self):
        """Test composite score auto-calculates."""
        summary = CompanyEvidenceSummary(
            company_id=uuid4(),
            ticker="CAT",
            hiring_score=75.0,
            patent_score=89.0,
            github_score=68.0,
            leadership_score=80.0
        )
        
        # Weighted average: 75*0.3 + 89*0.25 + 68*0.2 + 80*0.25
        # = 22.5 + 22.25 + 13.6 + 20 = 78.35
        assert summary.composite_score == pytest.approx(78.35, abs=0.1)
    
    def test_composite_score_partial_signals(self):
        """Test composite with some missing signals."""
        summary = CompanyEvidenceSummary(
            company_id=uuid4(),
            ticker="HCA",
            hiring_score=45.0,
            patent_score=None,  # Missing
            github_score=None,  # Missing
            leadership_score=60.0
        )
        
        #45*0.545 + 60*0.455 = 24.55 + 27.27 = 51.82
        assert summary.composite_score == pytest.approx(51.82, abs=0.1)

    def test_evidence_quality_calculation(self):
        """Test evidence quality score."""
        summary = CompanyEvidenceSummary(
            company_id=uuid4(),
            ticker="JPM",
            hiring_score=85.0,
            hiring_metadata={"confidence": 0.90},
            patent_score=92.0,
            patent_metadata={"confidence": 0.95},
            github_score=88.0,
            github_metadata={"confidence": 0.85},
            leadership_score=80.0,
            leadership_metadata={"confidence": 0.95}
        )
        
        # Completeness: 4/4 = 1.0
        # Avg confidence: (0.90 + 0.95 + 0.85 + 0.95) / 4 = 0.9125
        # Quality: 1.0*0.5 + 0.9125*0.5 = 0.956
        assert summary.evidence_quality == pytest.approx(0.956, abs=0.01)
    
    def test_no_signals_defaults(self):
        """Test summary with no signals."""
        summary = CompanyEvidenceSummary(
            company_id=uuid4(),
            ticker="UNKNOWN"
        )
        
        assert summary.composite_score is None
        assert summary.evidence_quality == pytest.approx(0.25, abs=0.01)  # 0*0.5 + 0.5*0.5