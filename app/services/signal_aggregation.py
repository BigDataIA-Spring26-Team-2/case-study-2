"""Signal aggregation and composite score calculation."""
import snowflake.connector
from app.config import get_settings


def calculate_composite_scores(ticker: str) -> None:
    """Calculate composite score for a company after signal collection."""
    
    settings = get_settings()
    
    conn = snowflake.connector.connect(
        account=settings.snowflake.account,
        user=settings.snowflake.user,
        password=settings.snowflake.password.get_secret_value(),
        warehouse=settings.snowflake.warehouse,
        database=settings.snowflake.database,
        schema=settings.snowflake.schema,
        role=settings.snowflake.role
    )
    
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            UPDATE company_evidence_summary
            SET 
                composite_score = (
                    COALESCE(hiring_score * 0.30, 0) +
                    COALESCE(patent_score * 0.25, 0) +
                    COALESCE(github_score * 0.20, 0) +
                    COALESCE(leadership_score * 0.25, 0)
                ) / NULLIF(
                    (CASE WHEN hiring_score IS NOT NULL THEN 0.30 ELSE 0 END) +
                    (CASE WHEN patent_score IS NOT NULL THEN 0.25 ELSE 0 END) +
                    (CASE WHEN github_score IS NOT NULL THEN 0.20 ELSE 0 END) +
                    (CASE WHEN leadership_score IS NOT NULL THEN 0.25 ELSE 0 END),
                    0
                ),
                last_updated = CURRENT_TIMESTAMP()
            WHERE ticker = %s
        """, (ticker,))
        
        conn.commit()
        
    finally:
        cursor.close()
        conn.close()