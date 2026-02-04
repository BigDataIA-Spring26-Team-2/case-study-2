"""GitHub pipeline runner."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import os
import json
import snowflake.connector
from app.config import get_settings
from app.core.config_loader import get_target_companies
from app.pipelines.github_scanner import scan_company
from dotenv import load_dotenv
load_dotenv()


def insert_signal(conn, signal):
    """Insert into external_signals."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO external_signals (
                id, company_id, category, source, score, confidence,
                metadata, s3_full_data_key, collected_at
            ) SELECT 
                %(id)s, %(company_id)s, %(category)s, %(source)s,
                %(score)s, %(confidence)s,
                TO_VARIANT(PARSE_JSON(%(metadata)s)),
                %(s3_key)s, %(collected_at)s
        """, {
            'id': signal["id"],
            'company_id': signal["company_id"],
            'category': signal["category"],
            'source': signal["source"],
            'score': signal["score"],
            'confidence': signal["confidence"],
            'metadata': json.dumps(signal["metadata"]),
            's3_key': signal.get("s3_full_data_key"),
            'collected_at': signal["collected_at"]
        })
        conn.commit()
        return True
    except Exception as e:
        print(f"    Insert failed: {str(e)[:100]}")
        return False
    finally:
        cursor.close()


def update_summary(conn, company_id, ticker, signal):
    """Update company_evidence_summary."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            MERGE INTO company_evidence_summary AS t
            USING (SELECT 
                %(company_id)s as company_id,
                %(ticker)s as ticker,
                %(score)s as github_score,
                TO_VARIANT(PARSE_JSON(%(metadata)s)) as github_metadata
            ) AS s
            ON t.company_id = s.company_id
            WHEN MATCHED THEN UPDATE SET
                github_score = s.github_score,
                github_metadata = s.github_metadata,
                github_collected_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (company_id, ticker, github_score, github_metadata, github_collected_at)
                VALUES (s.company_id, s.ticker, s.github_score, s.github_metadata, CURRENT_TIMESTAMP())
        """, {
            'company_id': company_id,
            'ticker': ticker,
            'score': signal['score'],
            'metadata': json.dumps(signal['metadata'])
        })
        conn.commit()
        return True
    except Exception as e:
        print(f"    Summary update failed: {str(e)[:100]}")
        return False
    finally:
        cursor.close()


async def collect(conn, ticker, company_name, company_id, token):
    """Collect for one company."""
    print(f"\n{'='*70}\n{ticker}: {company_name}\n{'='*70}")
    
    signal = await scan_company(ticker, company_id, token)
    
    if signal['score'] == 0:
        print(f"  No GitHub activity")
        return {"status": "no_activity", "score": 0}
    
    print(f"  Found: {signal['metadata']['ai_repos']} AI repos, {signal['metadata']['ai_stars']} stars")
    
    if insert_signal(conn, signal) and update_summary(conn, company_id, ticker, signal):
        print(f"  Score: {signal['score']:.1f}/100")
        return {"status": "success", "score": signal['score']}
    
    return {"status": "error"}


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--ticker')
    group.add_argument('--all', action='store_true')
    args = parser.parse_args()
    
    # Load all tokens
    tokens = [
        os.getenv('GITHUB_TOKEN'),
        os.getenv('GITHUB_TOKEN_2'),
        os.getenv('GITHUB_TOKEN_3')
    ]
    tokens = [t for t in tokens if t]  
    
    if not tokens:
        print("No GitHub tokens found")
        sys.exit(1)
    
    print(f"Using {len(tokens)} GitHub tokens")
    
    settings = get_settings()
    companies = get_target_companies()
    
    conn = snowflake.connector.connect(
        account=settings.snowflake.account,
        user=settings.snowflake.user,
        password=settings.snowflake.password.get_secret_value(),
        warehouse=settings.snowflake.warehouse,
        database=settings.snowflake.database,
        schema=settings.snowflake.schema,
        role=settings.snowflake.role
    )
    
    to_process = list(companies.items() if args.all else [(args.ticker, companies[args.ticker])])
    
    results = {}
    for idx, (ticker, info) in enumerate(to_process):
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM companies WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            token = tokens[idx % len(tokens)]  # Rotate tokens
            r = asyncio.run(collect(conn, ticker, info['name'], result[0], token))
            results[ticker] = r
    
    conn.close()
    
    print(f"\n{'='*70}\nSUMMARY\n{'='*70}")
    for ticker, data in results.items():
        print(f"{ticker}: {data.get('status')} - Score {data.get('score', 0):.1f}")


if __name__ == "__main__":
    main()