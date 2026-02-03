"""Database initialization for application runtime."""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import asyncio
import subprocess
import structlog

logger = structlog.get_logger()


async def init_database():
    """Initialize database schema and seed data."""
    try:
        logger.info("Running Alembic migrations")
        subprocess.run(['alembic', 'upgrade', 'head'], check=True, cwd='.')
        logger.info("Migrations complete")
        
        logger.info("Seeding reference data")
        from app.database.seed import seed_all
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
        
        async with db.cursor() as cur:
            await seed_all(cur)
        
        db.close()
        logger.info("Seed data complete")
        
    except Exception as e:
        logger.error("Database initialization failed", error=str(e))
        raise


if __name__ == "__main__":
    asyncio.run(init_database())