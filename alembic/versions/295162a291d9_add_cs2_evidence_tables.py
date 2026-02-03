"""add_cs2_evidence_tables

Revision ID: XXXX  # Keep the auto-generated ID
Revises: YYYY      # Keep the previous revision ID
Create Date: ZZZZ  # Keep the auto-generated timestamp

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers
revision = '295162a291d9'  
down_revision = 'f88633c59ce0'  
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create CS2 evidence tables."""
    
    # Table 1: evidence_documents (filing metadata + pre-computed stats)
    op.execute("""
        CREATE TABLE IF NOT EXISTS evidence_documents (
            id VARCHAR(36) PRIMARY KEY,
            company_id VARCHAR(36) NOT NULL,
            ticker VARCHAR(10) NOT NULL,
            filing_type VARCHAR(20) NOT NULL,
            filing_date DATE NOT NULL,
            accession_number VARCHAR(50) UNIQUE,
            
            -- Storage references
            content_hash VARCHAR(64) UNIQUE,
            s3_key VARCHAR(500),
            s3_bucket VARCHAR(100) DEFAULT 'pe-org-air-dev',
            
            -- Pre-computed statistics (avoid COUNT queries)
            total_chunks INT DEFAULT 0,
            total_words INT DEFAULT 0,
            section_count INT DEFAULT 0,
            table_count INT DEFAULT 0,
            
            -- Section-level aggregates (small JSON)
            sections_summary VARIANT,
            
            -- Processing metadata
            status VARCHAR(20) DEFAULT 'parsed',
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            
            -- Foreign key
            CONSTRAINT fk_evidence_doc_company 
                FOREIGN KEY (company_id) REFERENCES companies(id)
        )
    """)
    
    
    # Table 2: document_chunks (actual chunk content, partitioned)
    op.execute("""
        CREATE TABLE IF NOT EXISTS document_chunks (
            id VARCHAR(36) PRIMARY KEY,
            document_id VARCHAR(36) NOT NULL,
            chunk_index INT NOT NULL,
            
            -- Section context
            section_id VARCHAR(50),
            section_title VARCHAR(255),
            
            -- Content
            content TEXT NOT NULL,
            
            -- Metadata
            word_count INT NOT NULL,
            has_table BOOLEAN DEFAULT FALSE,
            page INT DEFAULT 1,
            
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            
            -- Unique constraint
            CONSTRAINT uk_chunk_document_index UNIQUE (document_id, chunk_index),
            
            -- Foreign key
            CONSTRAINT fk_chunk_document 
                FOREIGN KEY (document_id) REFERENCES evidence_documents(id)
        )
    """)
     
    # Clustering key for partition optimization (Snowflake-specific)
    op.execute("ALTER TABLE document_chunks CLUSTER BY (document_id)")
    
    # Table 3: company_evidence_summary (single row per company, read-optimized)
    op.execute("""
        CREATE TABLE IF NOT EXISTS company_evidence_summary (
            company_id VARCHAR(36) PRIMARY KEY,
            ticker VARCHAR(10) NOT NULL,
            
            -- Document aggregates
            total_documents INT DEFAULT 0,
            total_chunks INT DEFAULT 0,
            latest_filing_date DATE,
            has_10k BOOLEAN DEFAULT FALSE,
            has_10q BOOLEAN DEFAULT FALSE,
            has_8k BOOLEAN DEFAULT FALSE,
            
            -- Signal scores (flat structure for fast queries)
            hiring_score DECIMAL(5,2),
            hiring_metadata VARIANT,
            
            patent_score DECIMAL(5,2),
            patent_metadata VARIANT,
            
            github_score DECIMAL(5,2),
            github_metadata VARIANT,
            
            leadership_score DECIMAL(5,2),
            leadership_metadata VARIANT,
            
            -- Composite metrics
            composite_score DECIMAL(5,2),
            evidence_quality DECIMAL(4,3),
            
            -- Timestamps
            last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            
            -- Foreign key
            CONSTRAINT fk_summary_company 
                FOREIGN KEY (company_id) REFERENCES companies(id)
        )
    """)

def downgrade() -> None:
    """Drop CS2 evidence tables."""
    # Drop in reverse order (children before parents)
    op.execute("DROP TABLE IF EXISTS company_evidence_summary")
    op.execute("DROP TABLE IF EXISTS document_chunks")
    op.execute("DROP TABLE IF EXISTS evidence_documents")