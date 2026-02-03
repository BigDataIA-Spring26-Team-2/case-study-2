"""Simplified CS2 schema tests."""
import pytest
import json
from uuid import uuid4
from datetime import datetime, timezone, timedelta
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture(scope="module")
def conn():
    """Snowflake connection."""
    c = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='PUBLIC',
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    yield c
    c.close()


def test_all_tables_exist(conn):
    """Verify all 9 tables exist."""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = {row[1] for row in cursor.fetchall()}
    
    expected = {
        'ALEMBIC_VERSION', 'INDUSTRIES', 'COMPANIES', 'ASSESSMENTS',
        'DIMENSION_SCORES', 'EVIDENCE_DOCUMENTS', 'DOCUMENT_CHUNKS',
        'COMPANY_EVIDENCE_SUMMARY', 'EXTERNAL_SIGNALS'
    }
    
    assert expected.issubset(tables)


def test_insert_document_with_json(conn):
    """Test inserting document with sections_summary JSON."""
    cursor = conn.cursor()
    
    industry_id = str(uuid4())
    company_id = str(uuid4())
    doc_id = str(uuid4())
    
    cursor.execute(f"INSERT INTO industries (id, name, sector, h_r_base) VALUES ('{industry_id}', 'Test', 'Tech', 75.0)")
    cursor.execute(f"INSERT INTO companies (id, name, ticker, industry_id, created_at, updated_at) VALUES ('{company_id}', 'Test Co', 'TST', '{industry_id}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())")
    
    sections = {"item_1": {"chunks": 45, "words": 18540}}
    
    # Use parameterized query with PARSE_JSON on the JSON string directly
    cursor.execute("""
        INSERT INTO evidence_documents (
            id, company_id, ticker, filing_type, filing_date,
            accession_number, content_hash, total_chunks,
            total_words, section_count, table_count, sections_summary
        ) SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, PARSE_JSON(%s)
    """, (doc_id, company_id, 'TST', '10-K', '2024-12-31',
          f'acc-{doc_id}', f'hash-{doc_id}', 45, 18540, 1, 0,
          json.dumps(sections)))
    
    conn.commit()
    
    # Verify
    cursor.execute(f"SELECT sections_summary FROM evidence_documents WHERE id = '{doc_id}'")
    result = cursor.fetchone()[0]
    retrieved = json.loads(result) if isinstance(result, str) else result
    
    assert retrieved["item_1"]["chunks"] == 45
    
    # Cleanup
    cursor.execute(f"DELETE FROM evidence_documents WHERE id = '{doc_id}'")
    cursor.execute(f"DELETE FROM companies WHERE id = '{company_id}'")
    cursor.execute(f"DELETE FROM industries WHERE id = '{industry_id}'")
    conn.commit()


def test_insert_signal_with_rich_metadata(conn):
    """Test inserting signal with complex nested JSON."""
    cursor = conn.cursor()
    
    industry_id = str(uuid4())
    company_id = str(uuid4())
    signal_id = str(uuid4())
    
    cursor.execute(f"INSERT INTO industries (id, name, sector, h_r_base) VALUES ('{industry_id}', 'Test', 'Tech', 75.0)")
    cursor.execute(f"INSERT INTO companies (id, name, ticker, industry_id, created_at, updated_at) VALUES ('{company_id}', 'Test Co', 'TST', '{industry_id}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())")
    
    metadata = {
        "source": "indeed",
        "ai_jobs": 18,
        "job_breakdown": {"by_role": {"ml_engineer": 18}},
        "top_skills": [{"skill": "pytorch", "count": 28}]
    }
    
    # Use SELECT with PARSE_JSON for parameterized queries
    cursor.execute("""
        INSERT INTO external_signals (
            id, company_id, category, source, score, confidence,
            metadata, collected_at
        ) SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), CURRENT_TIMESTAMP()
    """, (signal_id, company_id, 'hiring', 'indeed', 75.5, 0.85, json.dumps(metadata)))
    
    conn.commit()
    
    # Verify
    cursor.execute(f"SELECT metadata FROM external_signals WHERE id = '{signal_id}'")
    result = cursor.fetchone()[0]
    retrieved = json.loads(result) if isinstance(result, str) else result
    
    assert retrieved["ai_jobs"] == 18
    
    # Cleanup
    cursor.execute(f"DELETE FROM external_signals WHERE id = '{signal_id}'")
    cursor.execute(f"DELETE FROM companies WHERE id = '{company_id}'")
    cursor.execute(f"DELETE FROM industries WHERE id = '{industry_id}'")
    conn.commit()


def test_time_series_query(conn):
    """Test querying signal history."""
    cursor = conn.cursor()
    
    industry_id = str(uuid4())
    company_id = str(uuid4())
    
    cursor.execute(f"INSERT INTO industries (id, name, sector, h_r_base) VALUES ('{industry_id}', 'Test', 'Tech', 75.0)")
    cursor.execute(f"INSERT INTO companies (id, name, ticker, industry_id, created_at, updated_at) VALUES ('{company_id}', 'Test Co', 'TST', '{industry_id}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())")
    
    # Insert 3 signals with different timestamps
    base_date = datetime.now(timezone.utc)
    for i in range(3):
        signal_id = str(uuid4())
        collected_at = (base_date - timedelta(days=30 * i)).isoformat()
        score = 60 + (i * 5)
        
        cursor.execute(f"""
            INSERT INTO external_signals (
                id, company_id, category, source, score, confidence,
                metadata, collected_at
            ) SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), %s
        """, (signal_id, company_id, 'hiring', 'test', score, 0.85,
              json.dumps({"ai_jobs": 10 + i}), collected_at))
    
    conn.commit()
    
    # Query
    cursor.execute(f"""
        SELECT score FROM external_signals
        WHERE company_id = '{company_id}' AND category = 'hiring'
        ORDER BY collected_at DESC
    """)
    
    results = cursor.fetchall()
    assert len(results) == 3
    assert results[0][0] == 60  # Most recent
    
    # Cleanup
    cursor.execute(f"DELETE FROM external_signals WHERE company_id = '{company_id}'")
    cursor.execute(f"DELETE FROM companies WHERE id = '{company_id}'")
    cursor.execute(f"DELETE FROM industries WHERE id = '{industry_id}'")
    conn.commit()