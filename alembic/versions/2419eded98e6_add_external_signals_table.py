"""add_external_signals_table

Revision ID: 2419eded98e6
Revises: 295162a291d9
Create Date: ZZZZ

"""
from alembic import op
import sqlalchemy as sa

revision = '2419eded98e6'
down_revision = '295162a291d9'
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add external_signals table for time-series signal storage."""
    
    # External signals table - time-series storage with rich metadata
    op.execute("""
        CREATE TABLE IF NOT EXISTS external_signals (
            id VARCHAR(36) PRIMARY KEY,
            company_id VARCHAR(36) NOT NULL,
            category VARCHAR(30) NOT NULL,
            source VARCHAR(50) NOT NULL,
            score DECIMAL(5,2) NOT NULL,
            confidence DECIMAL(4,3) NOT NULL,
            metadata VARIANT NOT NULL,
            s3_full_data_key VARCHAR(500),
            collected_at TIMESTAMP_NTZ NOT NULL,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            FOREIGN KEY (company_id) REFERENCES companies(id)
        )
    """)
    
    # Update company_evidence_summary - add collection timestamps
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN hiring_collected_at TIMESTAMP_NTZ")
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN patent_collected_at TIMESTAMP_NTZ")
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN github_collected_at TIMESTAMP_NTZ")
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN leadership_collected_at TIMESTAMP_NTZ")
    
    # Add S3 keys for full datasets
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN hiring_s3_key VARCHAR(500)")
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN patent_s3_key VARCHAR(500)")
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN github_s3_key VARCHAR(500)")
    op.execute("ALTER TABLE company_evidence_summary ADD COLUMN leadership_s3_key VARCHAR(500)")


def downgrade() -> None:
    """Remove external_signals table and related columns."""
    
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS hiring_collected_at")
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS patent_collected_at")
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS github_collected_at")
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS leadership_collected_at")
    
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS hiring_s3_key")
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS patent_s3_key")
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS github_s3_key")
    op.execute("ALTER TABLE company_evidence_summary DROP COLUMN IF EXISTS leadership_s3_key")
    
    op.execute("DROP TABLE IF EXISTS external_signals")