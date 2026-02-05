""" SEC pipeline"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import logging
from uuid import uuid4
import argparse
from dotenv import load_dotenv
import snowflake.connector

from app.pipelines.sec_integration import SECIntegration
from app.core.config_loader import get_target_companies, get_industries, get_filing_config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)

logger = logging.getLogger(__name__)
load_dotenv()


def get_or_create_industry(conn, industry_name: str) -> str:
    industries = get_industries()
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT id FROM industries WHERE name = '{industry_name}'")
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    industry_config = industries[industry_name]
    industry_id = str(uuid4())
    
    cursor.execute(f"""
        INSERT INTO industries (id, name, sector, h_r_base)
        VALUES ('{industry_id}', '{industry_name}', '{industry_config['sector']}', {industry_config['h_r_base']})
    """)
    conn.commit()
    
    return industry_id


def get_or_create_company(conn, ticker: str) -> str:
    companies = get_target_companies()
    
    if ticker not in companies:
        raise ValueError(f"Unknown ticker: {ticker}. Add to config/companies.yml")
    
    cursor = conn.cursor()
    cursor.execute(f"SELECT id FROM companies WHERE ticker = '{ticker}'")
    result = cursor.fetchone()
    
    if result:
        return result[0]
    
    company_data = companies[ticker]
    industry_id = get_or_create_industry(conn, company_data['industry'])
    company_id = str(uuid4())
    
    cursor.execute(f"""
        INSERT INTO companies (id, name, ticker, industry_id, created_at, updated_at)
        VALUES ('{company_id}', '{company_data['name']}', '{ticker}', '{industry_id}',
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """)
    conn.commit()
    logger.info(f"Created {ticker}: {company_data['name']}")
    
    return company_id


def process_company(pipeline, conn, ticker: str, filing_types: list, limit: int) -> dict:
    companies = get_target_companies()
    
    logger.info("="*60)
    logger.info(f"{ticker} - {companies[ticker]['name']}")
    logger.info("="*60)
    
    company_id = get_or_create_company(conn, ticker)
    
    stats = pipeline.process_company(
        ticker=ticker,
        company_id=company_id,
        filing_types=filing_types,
        limit=limit
    )
    
    logger.info(f"{ticker} COMPLETE: docs={stats['documents']}, chunks={stats['chunks']}, errors={stats['errors']}")
    
    return stats


def main():
    parser = argparse.ArgumentParser(description='SEC Evidence Pipeline')
    parser.add_argument('--companies', default='CAT', help='Ticker(s): CAT or CAT,JPM or "all"')
    parser.add_argument('--types', default=None, help='Override filing types (default: from config)')
    parser.add_argument('--limit', type=int, default=None, help='Override limit (default: from config)')
    args = parser.parse_args()
    
    filing_config = get_filing_config()
    filing_types = args.types.split(',') if args.types else filing_config['types']
    limit = args.limit if args.limit else filing_config['default_limit']
    
    if args.companies.lower() == 'all':
        tickers = list(get_target_companies().keys())
    else:
        tickers = [t.strip().upper() for t in args.companies.split(',')]
    
    logger.info("="*60)
    logger.info("SEC EVIDENCE COLLECTION")
    logger.info(f"Companies: {', '.join(tickers)}")
    logger.info(f"Types: {', '.join(filing_types)}")
    logger.info(f"Limit: {limit} per type")
    logger.info("="*60)
    
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='PUBLIC',
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    
    pipeline = SECIntegration(
        snowflake_conn=conn,
        s3_bucket=os.getenv('S3_BUCKET'),
        email="student@university.edu"
    )
    
    aggregate_stats = {'documents': 0, 'chunks': 0, 'skipped_db': 0, 'from_s3': 0, 'from_sec': 0, 'errors': 0, 'companies': 0}
    
    for ticker in tickers:
        try:
            stats = process_company(pipeline, conn, ticker, filing_types, limit)
            
            aggregate_stats['documents'] += stats['documents']
            aggregate_stats['chunks'] += stats['chunks']
            aggregate_stats['skipped_db'] += stats['skipped_db']
            aggregate_stats['from_s3'] += stats['from_s3']
            aggregate_stats['from_sec'] += stats['from_sec']
            aggregate_stats['errors'] += stats['errors']
            aggregate_stats['companies'] += 1
            
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {e}", exc_info=True)
            aggregate_stats['errors'] += 1
    
    logger.info("="*60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"  Companies Processed: {aggregate_stats['companies']}/{len(tickers)}")
    logger.info(f"  Total Documents: {aggregate_stats['documents']}")
    logger.info(f"  Total Chunks: {aggregate_stats['chunks']}")
    logger.info(f"  Skipped (DB): {aggregate_stats['skipped_db']}")
    logger.info(f"  From S3: {aggregate_stats['from_s3']}")
    logger.info(f"  From SEC: {aggregate_stats['from_sec']}")
    logger.info(f"  Errors: {aggregate_stats['errors']}")
    logger.info("="*60)
    
    conn.close()


if __name__ == "__main__":
    main()