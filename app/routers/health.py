"""Health check router with Redis, Snowflake, and S3 status."""
from fastapi import APIRouter
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
    
    # Check Redis
    try:
        from app.services.redis_cache import redis_service
        redis_alive = await redis_service.ping()
        redis_status = "healthy" if redis_alive else "unhealthy"
    except Exception as e:
        redis_status = "unhealthy"
    
    # Check Snowflake
    try:
        from app.services.snowflake import SnowflakeService
        from app.config import get_settings
        
        settings = get_settings()
        db = SnowflakeService(
            account=settings.snowflake.account,
            user=settings.snowflake.user,
            password=settings.snowflake.password.get_secret_value(),
            warehouse=settings.snowflake.warehouse,
            database=settings.snowflake.database,
            schema=settings.snowflake.schema,
            role=settings.snowflake.role
        )
        
        conn = db.connect()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        db.close()
        snowflake_status = "healthy"
    except Exception as e:
        snowflake_status = "unhealthy"
    
    # Check S3
    try:
        import boto3
        from botocore.exceptions import ClientError
        from app.config import get_settings
        
        settings = get_settings()
        
        # Check if S3 is configured
        if not settings.s3.bucket or not settings.s3.access_key_id:
            s3_status = "not_configured"
        else:
            # S3 is configured, test the connection
            s3_client = boto3.client(
                's3',
                aws_access_key_id=settings.s3.access_key_id,
                aws_secret_access_key=settings.s3.secret_access_key.get_secret_value(),
                region_name=settings.s3.region
            )
            
            # Test by checking if bucket exists
            s3_client.head_bucket(Bucket=settings.s3.bucket)
            s3_status = "healthy"
            
    except ClientError:
        s3_status = "unhealthy"
    except Exception:
        s3_status = "not_configured"
    
    dependencies = {
        "snowflake": snowflake_status,
        "redis": redis_status,
        "s3": s3_status
    }
    
    # Only require Redis and Snowflake for overall health
    all_healthy = (redis_status == "healthy" and snowflake_status == "healthy")
    
    return HealthResponse(
        status="healthy" if all_healthy else "degraded",
        timestamp=datetime.now(timezone.utc),
        version="1.0.0",
        dependencies=dependencies
    )