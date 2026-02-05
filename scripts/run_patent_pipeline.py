"""Patent signal collection pipeline."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

import asyncio
import argparse
import os
import json
import snowflake.connector
import boto3

from app.config import get_settings
from app.core.config_loader import get_target_companies
from app.pipelines.patent_scanner import scan_company


def insert_signal(conn, signal):
    cursor = conn.cursor()
    company_id = signal["company_id"]  

    try:
        cursor.execute(
            "DELETE FROM external_signals WHERE company_id = %s AND category = 'patent'", (company_id,)
        )
        conn.commit()

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
    cursor = conn.cursor()
    try:
        cursor.execute("""
            MERGE INTO company_evidence_summary AS t
            USING (SELECT 
                %(company_id)s as company_id,
                %(ticker)s as ticker,
                %(score)s as patent_score,
                TO_VARIANT(PARSE_JSON(%(metadata)s)) as patent_metadata
            ) AS s
            ON t.company_id = s.company_id
            WHEN MATCHED THEN UPDATE SET
                patent_score = s.patent_score,
                patent_metadata = s.patent_metadata,
                patent_collected_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (
                company_id, ticker, patent_score, patent_metadata, patent_collected_at
            ) VALUES (
                s.company_id, s.ticker, s.patent_score, s.patent_metadata, CURRENT_TIMESTAMP()
            )
        """, {
            'company_id': company_id,
            'ticker': ticker,
            'score': signal['score'],
            'metadata': json.dumps(signal['metadata'])
        })
        conn.commit()
        return True
    except Exception as e:
        print(f"    Update failed: {str(e)[:100]}")
        return False
    finally:
        cursor.close()


def upload_to_s3(signal, s3_client, bucket):
    if not signal.get('s3_full_data_key'):
        return True
    
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=signal['s3_full_data_key'],
            Body=json.dumps(signal['metadata'], indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        return True
    except Exception:
        return False


async def collect(conn, s3_client, bucket, ticker, company_name, company_id, api_key, year_from):
    print(f"\n{'='*70}\n{ticker}: {company_name}\n{'='*70}")
    
    signal = await scan_company(company_name, company_id, ticker, api_key, year_from)
    
    if signal['score'] == 0:
        print(f"  No AI patents")
        return {"status": "no_activity", "score": 0}
    
    print(f"\n  Uploading to S3...")
    upload_to_s3(signal, s3_client, bucket)
    
    print(f"  Inserting into Snowflake...")
    if insert_signal(conn, signal) and update_summary(conn, company_id, ticker, signal):
        return {
            "status": "success",
            "score": signal['score'],
            "ai_patents": signal['metadata']['ai_patents'],
            "total": signal['metadata']['total_patents']
        }
    
    return {"status": "error"}


def main():
    parser = argparse.ArgumentParser(
        description="Patent Signal Collection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_patent_pipeline.py --ticker CAT
  python scripts/run_patent_pipeline.py --ticker JPM --year 2022
  python scripts/run_patent_pipeline.py --all --year 2020
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--ticker', type=str)
    group.add_argument('--all', action='store_true')
    
    parser.add_argument('--year', type=int, default=2020, help='Patents granted since year (default: 2020)')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("PATENT SIGNAL COLLECTION PIPELINE")
    print("="*70)
    print(f"Year filter: Patents granted >= {args.year}")
    
    api_key = os.getenv('PATENT_API_KEY') or os.getenv('PATENT_API')
    if not api_key:
        print("\nPATENT_API_KEY not found")
        print("  Add to .env: PATENT_API_KEY=your_key")
        sys.exit(1)
    
    print(f"API key found")
    
    settings = get_settings()
    companies = get_target_companies()
    
    print(f"\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=settings.snowflake.account,
        user=settings.snowflake.user,
        password=settings.snowflake.password.get_secret_value(),
        warehouse=settings.snowflake.warehouse,
        database=settings.snowflake.database,
        schema=settings.snowflake.schema,
        role=settings.snowflake.role
    )
    print(f"Connected")
    
    s3_client = boto3.client('s3')
    bucket = settings.s3.bucket or "pe-org-air-dev"
    
    to_process = companies.items() if args.all else [(args.ticker, companies[args.ticker])]
    
    results = {}
    for ticker, info in to_process:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM companies WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            r = asyncio.run(collect(conn, s3_client, bucket, ticker, info.get('patent_search', info['name']), result[0], api_key, args.year))
            results[ticker] = r
    
    conn.close()
    
    print(f"\n{'='*70}\nSUMMARY\n{'='*70}")
    for ticker, data in results.items():
        if data.get("status") == "success":
            print(f"{ticker}: Score {data['score']:.1f} | {data['ai_patents']}/{data['total']} AI patents")
        else:
            print(f"{ticker}: {data.get('status', 'no_activity')}")
    
    print(f"\nComplete!")


if __name__ == "__main__":
    main()
