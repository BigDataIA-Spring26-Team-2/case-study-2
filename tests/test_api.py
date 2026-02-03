"""Integration tests for API endpoints."""
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.services.snowflake import SnowflakeService
from app.config import get_settings


@pytest_asyncio.fixture(scope="module")
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


@pytest_asyncio.fixture(scope="module")
async def industry_id():
    settings = get_settings()
    db = SnowflakeService(
        settings.snowflake.account, settings.snowflake.user,
        settings.snowflake.password.get_secret_value(),
        settings.snowflake.warehouse, settings.snowflake.database,
        settings.snowflake.schema, settings.snowflake.role
    )
    industries = await db.list_industries()
    db.close()
    return str(industries[0].id)


@pytest_asyncio.fixture
async def test_company(client, industry_id):
    """Create company, cleanup after test."""
    r = await client.post("/api/v1/companies", json={
        "name": "Test Company", "ticker": "TST", "industry_id": industry_id
    })
    company_id = r.json()["id"]
    yield company_id
    await client.delete(f"/api/v1/companies/{company_id}")


@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_create_company(test_company):
    assert test_company is not None


@pytest.mark.asyncio
async def test_list_companies(client):
    r = await client.get("/api/v1/companies")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_company(client, test_company):
    r = await client.get(f"/api/v1/companies/{test_company}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_update_company(client, test_company):
    r = await client.put(f"/api/v1/companies/{test_company}", json={"name": "Updated"})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_delete_company(client, industry_id):
    r = await client.post("/api/v1/companies", json={
        "name": "Delete Test", "ticker": "DEL", "industry_id": industry_id
    })
    company_id = r.json()["id"]
    r = await client.delete(f"/api/v1/companies/{company_id}")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_create_assessment(client, test_company):
    r = await client.post("/api/v1/assessments", json={
        "company_id": test_company,
        "assessment_type": "due_diligence",
        "assessment_date": "2026-01-31T00:00:00Z"
    })
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_list_assessments(client):
    r = await client.get("/api/v1/assessments")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_get_assessment(client, test_company):
    assessment = await client.post("/api/v1/assessments", json={
        "company_id": test_company,
        "assessment_type": "screening",
        "assessment_date": "2026-01-31T00:00:00Z"
    })
    r = await client.get(f"/api/v1/assessments/{assessment.json()['id']}")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_create_dimension_score(client, test_company):
    assessment = await client.post("/api/v1/assessments", json={
        "company_id": test_company,
        "assessment_type": "quarterly",
        "assessment_date": "2026-01-31T00:00:00Z"
    })
    assessment_id = assessment.json()["id"]
    
    r = await client.post(f"/api/v1/assessments/{assessment_id}/scores", json={
        "assessment_id": assessment_id,
        "dimension": "data_infrastructure",
        "score": 75.0
    })
    assert r.status_code == 201
    assert r.json()["weight"] == 0.25


@pytest.mark.asyncio
async def test_get_dimension_scores(client, test_company):
    assessment = await client.post("/api/v1/assessments", json={
        "company_id": test_company,
        "assessment_type": "exit_prep",
        "assessment_date": "2026-01-31T00:00:00Z"
    })
    r = await client.get(f"/api/v1/assessments/{assessment.json()['id']}/scores")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_update_dimension_score(client, test_company):
    assessment = await client.post("/api/v1/assessments", json={
        "company_id": test_company,
        "assessment_type": "screening",
        "assessment_date": "2026-01-31T00:00:00Z"
    })
    score = await client.post(f"/api/v1/assessments/{assessment.json()['id']}/scores", json={
        "assessment_id": assessment.json()["id"],
        "dimension": "ai_governance",
        "score": 70.0
    })
    
    r = await client.put(f"/api/v1/scores/{score.json()['id']}", json={"score": 85.0})
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_status_validation(client, test_company):
    assessment = await client.post("/api/v1/assessments", json={
        "company_id": test_company,
        "assessment_type": "due_diligence",
        "assessment_date": "2026-01-31T00:00:00Z"
    })
    
    r = await client.patch(f"/api/v1/assessments/{assessment.json()['id']}/status?status=submitted")
    assert r.status_code == 400