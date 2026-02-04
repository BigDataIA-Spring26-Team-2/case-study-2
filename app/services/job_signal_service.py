"""Snowflake service for job signal insertion."""
import snowflake.connector
from typing import List
import json


class JobSignalService:
    """Handle job signal insertion into Snowflake."""
    
    def __init__(self, connection):
        self.conn = connection
    
    def insert_job_signals(self, signals: List[dict]) -> int:
        if not signals:
            return 0
        
        cursor = self.conn.cursor()
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
            except:
                continue
        
        self.conn.commit()
        cursor.close()
        return inserted_count
    
    def update_company_summary(self, company_id: str, ticker: str):
        """Log Based Ratio-based scoring."""
        cursor = self.conn.cursor()
        
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
                        SUM(CASE WHEN metadata:multi_source::boolean = true THEN 1 ELSE 0 END) as multi_source
                    FROM external_signals
                    WHERE company_id = %s AND category = 'hiring_signal'
                ),
                ratios AS (
                    SELECT *,
                        CASE WHEN total > 0 THEN leadership::float / total ELSE 0 END as lead_pct,
                        CASE WHEN mid + entry > 0 THEN senior::float / (mid + entry) ELSE senior::float END as sr_lower,
                        CASE WHEN total > 0 THEN entry::float / total ELSE 0 END as entry_pct
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
                    %s as company_id,
                    %s as ticker,
                    score as hiring_score,
                    OBJECT_CONSTRUCT(
                        'total', total,
                        'leadership', leadership,
                        'senior', senior,
                        'mid', mid,
                        'entry', entry,
                        'phase', phase,
                        'ratios', OBJECT_CONSTRUCT(
                            'leadership_pct', ROUND(lead_pct * 100, 1),
                            'senior_lower', ROUND(sr_lower, 2),
                            'entry_pct', ROUND(entry_pct * 100, 1)
                        )
                    ) as hiring_metadata
                FROM scored
            ) AS source
            ON target.company_id = source.company_id
            WHEN MATCHED THEN
                UPDATE SET
                    hiring_score = source.hiring_score,
                    hiring_metadata = source.hiring_metadata,
                    hiring_collected_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (company_id, ticker, hiring_score, hiring_metadata, hiring_collected_at)
                VALUES (source.company_id, source.ticker, source.hiring_score, source.hiring_metadata, CURRENT_TIMESTAMP())
        """
        
        cursor.execute(merge_query, (company_id, company_id, ticker))
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