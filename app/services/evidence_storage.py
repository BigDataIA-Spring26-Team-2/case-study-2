"""Evidence storage service for Snowflake operations."""
import json
import logging
from uuid import uuid4

logger = logging.getLogger(__name__)


class EvidenceStorage:
    """Handle Snowflake operations for evidence data."""
    
    def __init__(self, snowflake_conn):
        self.conn = snowflake_conn
    
    def _update_company_summary(self, company_id: str, ticker: str):
        """Update company_evidence_summary after document insert."""
        cursor = self.conn.cursor()
        
        # Check if exists
        cursor.execute(f"SELECT COUNT(*) FROM company_evidence_summary WHERE company_id = '{company_id}'")
        exists = cursor.fetchone()[0] > 0
        
        # Aggregate stats
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_docs,
                COALESCE(SUM(total_chunks), 0) as total_chunks,
                MAX(filing_date) as latest_date,
                MAX(CASE WHEN filing_type = '10-K' THEN 1 ELSE 0 END) as has_10k,
                MAX(CASE WHEN filing_type = '10-Q' THEN 1 ELSE 0 END) as has_10q,
                MAX(CASE WHEN filing_type = '8-K' THEN 1 ELSE 0 END) as has_8k
            FROM evidence_documents
            WHERE company_id = '{company_id}'
        """)
        
        stats = cursor.fetchone()
        
        if exists:
            cursor.execute(f"""
                UPDATE company_evidence_summary
                SET total_documents = {stats[0]},
                    total_chunks = {stats[1]},
                    latest_filing_date = '{stats[2]}',
                    has_10k = {stats[3] == 1},
                    has_10q = {stats[4] == 1},
                    has_8k = {stats[5] == 1},
                    last_updated = CURRENT_TIMESTAMP()
                WHERE company_id = '{company_id}'
            """)
        else:
            cursor.execute(f"""
                INSERT INTO company_evidence_summary (
                    company_id, ticker, total_documents, total_chunks,
                    latest_filing_date, has_10k, has_10q, has_8k, last_updated
                ) VALUES (
                    '{company_id}', '{ticker}', {stats[0]}, {stats[1]},
                    '{stats[2]}', {stats[3] == 1}, {stats[4] == 1}, {stats[5] == 1},
                    CURRENT_TIMESTAMP()
                )
            """)
        
        self.conn.commit()
        logger.debug(f"Updated summary: {ticker} - {stats[0]} docs, {stats[1]} chunks")