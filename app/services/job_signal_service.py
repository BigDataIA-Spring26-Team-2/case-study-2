"""Snowflake service for job signal insertion - CLEAN SLATE VERSION."""
import snowflake.connector
from typing import List
import json
import logging

logger = logging.getLogger(__name__)


class JobSignalService:
    """Handle job signal insertion into Snowflake."""
    
    def __init__(self, connection):
        self.conn = connection
    
    def insert_job_signals(self, signals: List[dict]) -> int:
        """Insert job signals - DELETES old data first for clean slate."""
        if not signals:
            return 0
        
        cursor = self.conn.cursor()
        company_id = signals[0]["company_id"]
        
        # CLEAN SLATE: Delete all old hiring signals for this company
        logger.info(f"Deleting old hiring_signal data for company {company_id}")
        cursor.execute(
            "DELETE FROM external_signals WHERE company_id = %s AND category = 'hiring_signal'",
            (company_id,)
        )
        self.conn.commit()
        logger.info(f"Deleted old signals, now inserting {len(signals)} fresh signals")
        
        # Insert fresh signals
        inserted_count = 0
        
        for signal in signals:
            try:
                metadata_json = json.dumps(signal["metadata"], ensure_ascii=False)
                
                cursor.execute(
                    """
                    INSERT INTO external_signals (
                        id, company_id, category, source, score, confidence,
                        metadata, s3_full_data_key, collected_at
                    )
                    SELECT 
                        %(id)s, %(company_id)s, %(category)s, %(source)s, 
                        %(score)s, %(confidence)s,
                        TO_VARIANT(PARSE_JSON(%(metadata)s)),
                        %(s3_key)s, %(collected_at)s
                    """,
                    {
                        'id': signal["id"],
                        'company_id': signal["company_id"],
                        'category': signal["category"],
                        'source': signal["source"],
                        'score': signal["score"],
                        'confidence': signal["confidence"],
                        'metadata': metadata_json,
                        's3_key': signal.get("s3_full_data_key"),
                        'collected_at': signal["collected_at"]
                    }
                )
                inserted_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert signal: {e}")
                continue
        
        self.conn.commit()
        cursor.close()
        logger.info(f"Successfully inserted {inserted_count}/{len(signals)} signals")
        return inserted_count
    
    def update_company_summary(self, company_id: str, ticker: str, stats: dict = None):
        """Update company_evidence_summary with pre-calculated stats from pipeline."""
        cursor = self.conn.cursor()
        
        if stats:
            # USE PRE-CALCULATED STATS FROM PIPELINE (matches terminal output exactly)
            total_scraped = stats['total_jobs_scraped']
            ai_jobs = stats['ai_jobs']
            ai_verified = stats['ai_verified']
            multi_source = stats['multi_source']
            with_salary = stats['with_salary']
            seniority = stats['seniority']
            ai_ratio = stats['ai_ratio']
            
            # Calculate ratios
            leadership = seniority.get('leadership', 0)
            senior = seniority.get('senior', 0)
            mid = seniority.get('mid', 0)
            entry = seniority.get('entry', 0)
            
            lead_pct = leadership / total_scraped if total_scraped > 0 else 0
            sr_lower = senior / (mid + entry) if (mid + entry) > 0 else float(senior)
            entry_pct = entry / total_scraped if total_scraped > 0 else 0
            
            # Calculate score (same formula as before)
            import math
            score = min(100,
                30 * math.log1p(total_scraped) / math.log1p(100) +
                min(30, lead_pct * 150) +
                min(25, sr_lower * 10) +
                (min(15, entry_pct * 50) if entry_pct > 0 and senior > 0 else 0)
            )
            
            # Determine phase
            if lead_pct >= 0.15:
                phase = 'STRATEGIC'
            elif sr_lower >= 1.5 and lead_pct >= 0.05:
                phase = 'EXPERTISE'
            elif entry_pct >= 0.25:
                phase = 'SCALING'
            elif total_scraped > 0 and mid / total_scraped >= 0.4:
                phase = 'BUILDING'
            else:
                phase = 'EXPLORING'
            
            # Build metadata object
            metadata = {
                'total_jobs': total_scraped,  
                'ai_related_count': ai_jobs,   
                'ai_verified': ai_verified,
                'multi_source': multi_source,
                'with_salary': with_salary,
                'ai_ratio': round(ai_ratio, 3),
                'seniority': {
                    'leadership': leadership,
                    'senior': senior,
                    'mid': mid,
                    'entry': entry
                },
                'phase': phase,
                'ratios': {
                    'leadership_pct': round(lead_pct * 100, 1),
                    'senior_lower': round(sr_lower, 2),
                    'entry_pct': round(entry_pct * 100, 1)
                }
            }
            
            metadata_json = json.dumps(metadata, ensure_ascii=False)
            
            # Merge into summary table
            cursor.execute("""
                MERGE INTO company_evidence_summary AS target
                USING (
                    SELECT 
                        %(company_id)s as company_id,
                        %(ticker)s as ticker,
                        %(score)s as hiring_score,
                        TO_VARIANT(PARSE_JSON(%(metadata)s)) as hiring_metadata,
                        CURRENT_TIMESTAMP() as hiring_collected_at
                ) AS source
                ON target.company_id = source.company_id
                WHEN MATCHED THEN
                    UPDATE SET
                        hiring_score = source.hiring_score,
                        hiring_metadata = source.hiring_metadata,
                        hiring_collected_at = source.hiring_collected_at,
                        last_updated = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (company_id, ticker, hiring_score, hiring_metadata, hiring_collected_at, last_updated)
                    VALUES (source.company_id, source.ticker, source.hiring_score, source.hiring_metadata, source.hiring_collected_at, CURRENT_TIMESTAMP())
            """, {
                'company_id': company_id,
                'ticker': ticker,
                'score': round(score, 2),
                'metadata': metadata_json
            })
            
            self.conn.commit()
            cursor.close()
            
        else:
            # FALLBACK: Aggregate from external_signals (existing logic)
            # This is the OLD way - only used if stats not provided
            merge_query = """
                MERGE INTO company_evidence_summary AS target
                USING (
                    WITH counts AS (
                        SELECT 
                            COUNT(*) as total,
                            SUM(CASE WHEN metadata:seniority_label::string = 'leadership' THEN 1 ELSE 0 END) as leadership,
                            SUM(CASE WHEN metadata:seniority_label::string = 'senior' THEN 1 ELSE 0 END) as senior,
                            SUM(CASE WHEN metadata:seniority_label::string = 'mid' THEN 1 ELSE 0 END) as mid,
                            SUM(CASE WHEN metadata:seniority_label::string = 'entry' THEN 1 ELSE 0 END) as entry,
                            SUM(CASE WHEN metadata:multi_source::boolean = true THEN 1 ELSE 0 END) as multi_source,
                            SUM(CASE WHEN metadata:ai_score::number >= 50 THEN 1 ELSE 0 END) as ai_verified,
                            SUM(CASE WHEN metadata:salary_min::number IS NOT NULL THEN 1 ELSE 0 END) as with_salary,
                            SUM(CASE WHEN metadata:is_ai_related::boolean = true THEN 1 ELSE 0 END) as ai_related_count
                        FROM external_signals
                        WHERE company_id = %(company_id)s AND category = 'hiring_signal'
                    ),
                    ratios AS (
                        SELECT *,
                            CASE WHEN total > 0 THEN leadership::float / total ELSE 0 END as lead_pct,
                            CASE WHEN mid + entry > 0 THEN senior::float / (mid + entry) ELSE senior::float END as sr_lower,
                            CASE WHEN total > 0 THEN entry::float / total ELSE 0 END as entry_pct,
                            1.0 as ai_ratio
                        FROM counts
                    ),
                    scored AS (
                        SELECT *,
                            LEAST(100, 
                                30 * LN(1 + total) / LN(100) +
                                LEAST(30, lead_pct * 150) +
                                LEAST(25, sr_lower * 10) +
                                CASE WHEN entry_pct > 0 AND senior > 0 THEN LEAST(15, entry_pct * 50) ELSE 0 END
                            ) as score,
                            CASE 
                                WHEN lead_pct >= 0.15 THEN 'STRATEGIC'
                                WHEN sr_lower >= 1.5 AND lead_pct >= 0.05 THEN 'EXPERTISE'
                                WHEN entry_pct >= 0.25 THEN 'SCALING'
                                WHEN total > 0 AND mid::float / total >= 0.4 THEN 'BUILDING'
                                ELSE 'EXPLORING'
                            END as phase
                        FROM ratios
                    )
                    SELECT 
                        %(company_id)s as company_id,
                        %(ticker)s as ticker,
                        score as hiring_score,
                        OBJECT_CONSTRUCT(
                            'total_jobs', total,
                            'ai_related_count', ai_related_count,
                            'ai_verified', ai_verified,
                            'multi_source', multi_source,
                            'with_salary', with_salary,
                            'ai_ratio', ROUND(ai_ratio, 3),
                            'seniority', OBJECT_CONSTRUCT(
                                'leadership', leadership,
                                'senior', senior,
                                'mid', mid,
                                'entry', entry
                            ),
                            'phase', phase,
                            'ratios', OBJECT_CONSTRUCT(
                                'leadership_pct', ROUND(lead_pct * 100, 1),
                                'senior_lower', ROUND(sr_lower, 2),
                                'entry_pct', ROUND(entry_pct * 100, 1)
                            )
                        ) as hiring_metadata,
                        CURRENT_TIMESTAMP() as hiring_collected_at
                    FROM scored
                ) AS source
                ON target.company_id = source.company_id
                WHEN MATCHED THEN
                    UPDATE SET
                        hiring_score = source.hiring_score,
                        hiring_metadata = source.hiring_metadata,
                        hiring_collected_at = source.hiring_collected_at,
                        last_updated = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (company_id, ticker, hiring_score, hiring_metadata, hiring_collected_at, last_updated)
                    VALUES (source.company_id, source.ticker, source.hiring_score, source.hiring_metadata, source.hiring_collected_at, CURRENT_TIMESTAMP())
            """
            
            cursor.execute(merge_query, {'company_id': company_id, 'ticker': ticker})
            self.conn.commit()
            cursor.close()

"""
Hiring Score Formula (0-100):
  30 * LN(1 + total_jobs) / LN(100)           # Logarithmic volume (max 30 pts)
  + LEAST(30, leadership_pct * 150)           # Leadership ratio (max 30 pts)
  + LEAST(25, senior/(mid+entry) * 10)        # Senior/lower ratio (max 25 pts)
  + LEAST(15, entry_pct * 50)                 # Entry diversity if senior>0 (max 15 pts)

Ratios Calculated:
  - leadership_pct: leadership / total
  - senior_lower: senior / (mid + entry)
  - entry_pct: entry / total

Phase Classification:
  - STRATEGIC: leadership_pct >= 15%
  - EXPERTISE: senior_lower >= 1.5 AND leadership_pct >= 5%
  - SCALING: entry_pct >= 25%
  - BUILDING: mid / total >= 40%
  - EXPLORING: default
"""