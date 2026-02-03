"""Initial schema - CS1 tables."""
from alembic import op
import sqlalchemy as sa

revision = 'f88633c59ce0'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
        CREATE TABLE industries (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL UNIQUE,
            sector VARCHAR(100) NOT NULL,
            h_r_base DECIMAL(5,2),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    op.execute("""
        CREATE TABLE companies (
            id VARCHAR(36) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            ticker VARCHAR(10),
            industry_id VARCHAR(36) NOT NULL REFERENCES industries(id),
            position_factor DECIMAL(4,3) DEFAULT 0.0,
            is_deleted BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    op.execute("""
        CREATE TABLE assessments (
            id VARCHAR(36) PRIMARY KEY,
            company_id VARCHAR(36) NOT NULL REFERENCES companies(id),
            assessment_type VARCHAR(20) NOT NULL,
            assessment_date DATE NOT NULL,
            status VARCHAR(20) DEFAULT 'draft',
            primary_assessor VARCHAR(255),
            secondary_assessor VARCHAR(255),
            v_r_score DECIMAL(5,2),
            confidence_lower DECIMAL(5,2),
            confidence_upper DECIMAL(5,2),
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    
    op.execute("""
        CREATE TABLE dimension_scores (
            id VARCHAR(36) PRIMARY KEY,
            assessment_id VARCHAR(36) NOT NULL REFERENCES assessments(id),
            dimension VARCHAR(30) NOT NULL,
            score DECIMAL(5,2) NOT NULL,
            weight DECIMAL(4,3),
            confidence DECIMAL(4,3) DEFAULT 0.8,
            evidence_count INT DEFAULT 0,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            UNIQUE (assessment_id, dimension)
        )
    """)


def downgrade():
    op.execute("DROP TABLE IF EXISTS dimension_scores")
    op.execute("DROP TABLE IF EXISTS assessments")
    op.execute("DROP TABLE IF EXISTS companies")
    op.execute("DROP TABLE IF EXISTS industries")