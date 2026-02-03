"""Dimension scores router."""
from fastapi import APIRouter, HTTPException
from uuid import UUID
from typing import List

from app.models import DimensionScoreCreate, DimensionScoreUpdate, DimensionScoreResponse
from app.services.snowflake import SnowflakeService
from app.config import get_settings

router = APIRouter(prefix="/api/v1", tags=["dimension_scores"])


def get_db():
    settings = get_settings()
    return SnowflakeService(
        settings.snowflake.account, settings.snowflake.user,
        settings.snowflake.password.get_secret_value(),
        settings.snowflake.warehouse, settings.snowflake.database,
        settings.snowflake.schema, settings.snowflake.role
    )


@router.post("/assessments/{assessment_id}/scores", response_model=DimensionScoreResponse, status_code=201)
async def create_dimension_score(assessment_id: UUID, score: DimensionScoreCreate):
    db = get_db()
    try:
        return await db.create_dimension_score(score)
    finally:
        db.close()


@router.get("/assessments/{assessment_id}/scores", response_model=List[DimensionScoreResponse])
async def get_dimension_scores(assessment_id: UUID):
    db = get_db()
    try:
        return await db.get_dimension_scores(assessment_id)
    finally:
        db.close()


@router.put("/scores/{score_id}", response_model=DimensionScoreResponse)
async def update_dimension_score(score_id: UUID, score: DimensionScoreUpdate):
    db = get_db()
    try:
        updated = await db.update_dimension_score(score_id, score)
        if not updated:
            raise HTTPException(404, "Dimension score not found")
        return updated
    finally:
        db.close()