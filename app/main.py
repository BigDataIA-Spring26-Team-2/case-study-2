"""FastAPI application entry point."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import structlog

from app.routers import health, companies, assessments, dimensions
from app.routers import documents, signals, evidence
from app.services.redis_cache import redis_service

logger = structlog.get_logger()

app = FastAPI(
    title="PE Org-AI-R Platform",
    description="AI-readiness assessment platform for private equity",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(companies.router)
app.include_router(assessments.router)
app.include_router(dimensions.router)
app.include_router(evidence.router)
app.include_router(signals.router)
app.include_router(documents.router)


@app.on_event("startup")
async def startup():
    logger.info("PE Org-AI-R Platform starting")
    
    # Run database initialization from database/init.py
    try:
        from app.databasey.init import init_database
        await init_database()
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise
    
    # Connect to Redis
    await redis_service.connect()  
    logger.info("Redis connected")


@app.on_event("shutdown")
async def shutdown():
    logger.info("PE Org-AI-R Platform shutting down")
    await redis_service.disconnect()  
    logger.info("Redis disconnected")


@app.get("/")
async def root():
    return {
        "message": "PE Org-AI-R Platform API",
        "version": "1.0.0",
        "docs": "/docs"
    }