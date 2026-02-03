"""Health check router."""
from fastapi import APIRouter, status
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Dict

router = APIRouter()


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str
    dependencies: Dict[str, str]


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Check health of all dependencies."""
    
    dependencies = {
        "snowflake": "healthy",
        "redis": "not_implemented",
        "s3": "not_implemented"
    }
    
    all_healthy = all(v == "healthy" for v in dependencies.values())
    
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        timestamp=datetime.now(timezone.utc),
        version="1.0.0",
        dependencies=dependencies
    )