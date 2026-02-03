"""
Idempotent database seeding for reference data.
"""
from typing import List, Dict, Any
import structlog

logger = structlog.get_logger()


INDUSTRIES_SEED_DATA: List[Dict[str, Any]] = [
    {'id': '550e8400-e29b-41d4-a716-446655440001', 'name': 'Manufacturing', 
     'sector': 'Industrials', 'h_r_base': 72.0},
    {'id': '550e8400-e29b-41d4-a716-446655440002', 'name': 'Healthcare Services', 
     'sector': 'Healthcare', 'h_r_base': 78.0},
    {'id': '550e8400-e29b-41d4-a716-446655440003', 'name': 'Business Services', 
     'sector': 'Services', 'h_r_base': 75.0},
    {'id': '550e8400-e29b-41d4-a716-446655440004', 'name': 'Retail', 
     'sector': 'Consumer', 'h_r_base': 70.0},
    {'id': '550e8400-e29b-41d4-a716-446655440005', 'name': 'Financial Services', 
     'sector': 'Financial', 'h_r_base': 80.0},
]


async def seed_industries(cursor) -> int:
    """
    Seed industries table with reference data.
    Idempotent - uses MERGE for upsert.
    
    Returns:
        Number of industries seeded
    """
    merge_sql = """
    MERGE INTO industries AS target
    USING (
        SELECT 
            %(id)s AS id,
            %(name)s AS name,
            %(sector)s AS sector,
            %(h_r_base)s AS h_r_base
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN
        UPDATE SET
            name = source.name,
            sector = source.sector,
            h_r_base = source.h_r_base
    WHEN NOT MATCHED THEN
        INSERT (id, name, sector, h_r_base)
        VALUES (source.id, source.name, source.sector, source.h_r_base)
    """
    
    count = 0
    for industry in INDUSTRIES_SEED_DATA:
        cursor.execute(merge_sql, industry)
        count += 1
    
    logger.info("Industries seeded", count=count)
    return count


async def seed_all(cursor) -> Dict[str, int]:
    """
    Seed all reference data tables.
    
    Returns:
        Dictionary with counts per table
    """
    results = {
        'industries': await seed_industries(cursor)
    }
    
    logger.info("Seeding complete", **results)
    return results