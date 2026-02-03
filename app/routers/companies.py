"""Companies router."""
from fastapi import APIRouter, HTTPException, Query
from uuid import UUID
from typing import Optional

from app.models import CompanyCreate, CompanyUpdate, CompanyResponse, PaginatedResponse
from app.services.snowflake import SnowflakeService
from app.config import get_settings

router = APIRouter(prefix="/api/v1/companies", tags=["companies"])


def get_db():
    settings = get_settings()
    return SnowflakeService(
        settings.snowflake.account,
        settings.snowflake.user,
        settings.snowflake.password.get_secret_value(),
        settings.snowflake.warehouse,
        settings.snowflake.database,
        settings.snowflake.schema,
        settings.snowflake.role
    )


@router.post("", response_model=CompanyResponse, status_code=201)
async def create_company(company: CompanyCreate):
    db = get_db()
    try:
        return await db.create_company(company)
    finally:
        db.close()


@router.get("", response_model=PaginatedResponse[CompanyResponse])
async def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    industry_id: Optional[UUID] = None
):
    db = get_db()
    try:
        skip = (page - 1) * page_size
        companies, total = await db.list_companies(skip, page_size, industry_id)
        return PaginatedResponse.create(companies, total, page, page_size)
    finally:
        db.close()


@router.get("/{company_id}", response_model=CompanyResponse)
async def get_company(company_id: UUID):
    db = get_db()
    try:
        company = await db.get_company(company_id)
        if not company:
            raise HTTPException(status_code=404, detail="Company not found")
        return company
    finally:
        db.close()


@router.put("/{company_id}", response_model=CompanyResponse)
async def update_company(company_id: UUID, company: CompanyUpdate):
    db = get_db()
    try:
        updated = await db.update_company(company_id, company)
        if not updated:
            raise HTTPException(status_code=404, detail="Company not found")
        return updated
    finally:
        db.close()


@router.delete("/{company_id}", status_code=204)
async def delete_company(company_id: UUID):
    db = get_db()
    try:
        deleted = await db.delete_company(company_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Company not found")
    finally:
        db.close()