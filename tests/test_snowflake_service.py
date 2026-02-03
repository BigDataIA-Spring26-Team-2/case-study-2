"""Tests for Snowflake database service."""
import pytest
import pytest_asyncio
from uuid import uuid4

from app.services.snowflake import SnowflakeService
from app.models import (
    CompanyCreate, CompanyUpdate,
    AssessmentCreate, AssessmentType,
    DimensionScoreCreate, Dimension
)
from app.config import get_settings


@pytest_asyncio.fixture(scope="module")
async def db():
    """Snowflake service instance."""
    settings = get_settings()
    service = SnowflakeService(
        account=settings.snowflake.account,
        user=settings.snowflake.user,
        password=settings.snowflake.password.get_secret_value(),
        warehouse=settings.snowflake.warehouse,
        database=settings.snowflake.database,
        schema=settings.snowflake.schema,
        role=settings.snowflake.role
    )
    yield service
    service.close()


@pytest_asyncio.fixture(scope="module")
async def industry_id(db):
    """Get first industry ID for tests."""
    industries = await db.list_industries()
    assert len(industries) > 0, "No industries seeded"
    return industries[0].id


@pytest_asyncio.fixture
async def test_company(db, industry_id):
    """Create test company, cleanup after test."""
    company = await db.create_company(CompanyCreate(
        name=f"Test Company {uuid4().hex[:8]}",
        ticker=f"T{uuid4().hex[:4].upper()}",
        industry_id=industry_id,
        position_factor=0.5
    ))
    yield company
    await db.delete_company(company.id)


@pytest_asyncio.fixture
async def test_assessment(db, test_company):
    """Create test assessment, cleanup after test."""
    assessment = await db.create_assessment(AssessmentCreate(
        company_id=test_company.id,
        assessment_type=AssessmentType.DUE_DILIGENCE
    ))
    yield assessment


# Industries

@pytest.mark.asyncio
async def test_list_industries(db):
    industries = await db.list_industries()
    assert len(industries) == 5
    assert all(ind.name for ind in industries)


# Companies

@pytest.mark.asyncio
async def test_create_company(test_company):
    assert test_company.id is not None
    assert test_company.is_deleted is False


@pytest.mark.asyncio
async def test_get_company(db, test_company):
    retrieved = await db.get_company(test_company.id)
    assert retrieved is not None
    assert retrieved.id == test_company.id


@pytest.mark.asyncio
async def test_get_nonexistent_company(db):
    result = await db.get_company(uuid4())
    assert result is None


@pytest.mark.asyncio
async def test_list_companies(db, test_company):
    companies, total = await db.list_companies(skip=0, limit=10)
    assert total >= 1
    assert any(c.id == test_company.id for c in companies)


@pytest.mark.asyncio
async def test_update_company(db, test_company):
    updated = await db.update_company(
        test_company.id,
        CompanyUpdate(name="Updated Name")
    )
    assert updated.name == "Updated Name"


@pytest.mark.asyncio
async def test_delete_company(db, industry_id):
    company = await db.create_company(CompanyCreate(
        name="Delete Test",
        ticker="DEL",
        industry_id=industry_id
    ))
    
    deleted = await db.delete_company(company.id)
    assert deleted is True
    
    retrieved = await db.get_company(company.id)
    assert retrieved is None


# Assessments

@pytest.mark.asyncio
async def test_create_assessment(test_assessment, test_company):
    assert test_assessment.id is not None
    assert test_assessment.company_id == test_company.id


@pytest.mark.asyncio
async def test_get_assessment(db, test_assessment):
    retrieved = await db.get_assessment(test_assessment.id)
    assert retrieved is not None
    assert retrieved.id == test_assessment.id


# Dimension Scores

@pytest.mark.asyncio
async def test_create_dimension_score(db, test_assessment):
    score = await db.create_dimension_score(DimensionScoreCreate(
        assessment_id=test_assessment.id,
        dimension=Dimension.DATA_INFRASTRUCTURE,
        score=75.5
    ))
    
    assert score.id is not None
    assert score.weight == 0.25


@pytest.mark.asyncio
async def test_get_dimension_scores(db, test_assessment):
    await db.create_dimension_score(DimensionScoreCreate(
        assessment_id=test_assessment.id,
        dimension=Dimension.DATA_INFRASTRUCTURE,
        score=80.0
    ))
    
    scores = await db.get_dimension_scores(test_assessment.id)
    assert len(scores) >= 1


# Integration

@pytest.mark.asyncio
async def test_full_workflow(db, industry_id):
    """Complete workflow with cleanup."""
    company = await db.create_company(CompanyCreate(
        name="Integration Test",
        ticker="INT",
        industry_id=industry_id
    ))
    
    try:
        assessment = await db.create_assessment(AssessmentCreate(
            company_id=company.id,
            assessment_type=AssessmentType.SCREENING
        ))
        
        for dimension in Dimension:
            await db.create_dimension_score(DimensionScoreCreate(
                assessment_id=assessment.id,
                dimension=dimension,
                score=75.0
            ))
        
        scores = await db.get_dimension_scores(assessment.id)
        assert len(scores) == 7
        
    finally:
        await db.delete_company(company.id)