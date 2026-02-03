"""Assessments router"""
from fastapi import APIRouter, HTTPException, Query
from uuid import UUID
from typing import Optional, List

from app.models import AssessmentCreate, AssessmentResponse, PaginatedResponse
from app.services.snowflake import SnowflakeService
from app.config import get_settings

router = APIRouter(prefix="/api/v1/assessments", tags=["assessments"])


def get_db():
    settings = get_settings()
    return SnowflakeService(
        settings.snowflake.account, settings.snowflake.user,
        settings.snowflake.password.get_secret_value(),
        settings.snowflake.warehouse, settings.snowflake.database,
        settings.snowflake.schema, settings.snowflake.role
    )


@router.post("", response_model=AssessmentResponse, status_code=201)
async def create_assessment(assessment: AssessmentCreate):
    db = get_db()
    try:
        return await db.create_assessment(assessment)
    finally:
        db.close()


@router.get("", response_model=PaginatedResponse[AssessmentResponse])
async def list_assessments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    company_id: Optional[UUID] = None,
    status: Optional[str] = None
):
    db = get_db()
    try:
        skip = (page - 1) * page_size
        assessments, total = await db.list_assessments(skip, page_size, company_id, status)
        return PaginatedResponse.create(assessments, total, page, page_size)
    finally:
        db.close()


@router.get("/{assessment_id}", response_model=AssessmentResponse)
async def get_assessment(assessment_id: UUID):
    db = get_db()
    try:
        assessment = await db.get_assessment(assessment_id)
        if not assessment:
            raise HTTPException(404, "Assessment not found")
        return assessment
    finally:
        db.close()


@router.patch("/{assessment_id}/status")
async def update_status(assessment_id: UUID, status: str = Query(...)):
    db = get_db()
    try:
        updated = await db.update_assessment_status(assessment_id, status)
        if not updated:
            raise HTTPException(404, "Assessment not found")
        return updated
    except ValueError as e:
        raise HTTPException(400, str(e))
    finally:
        db.close()