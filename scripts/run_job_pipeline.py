"""Job Signal Collection Pipeline - CORRECTED to track total_scraped."""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import argparse
import snowflake.connector
from datetime import datetime

from app.config import get_settings
from app.core.config_loader import get_target_companies
from app.pipelines.job_signal_collector import scrape_ai_jobs, prepare_for_snowflake
from app.services.job_signal_service import JobSignalService


def collect_for_company(
    conn,
    service: JobSignalService,
    ticker: str,
    company_name: str,
    company_id: str,
    max_jobs: int = 50,
    hours_old: int = 240
) -> dict:
    """Collect job signals for a single company."""
    print(f"\n{'='*70}")
    print(f"Processing: {ticker} - {company_name}")
    print(f"{'='*70}")
    print(f"  Company ID: {company_id}")
    print(f"  Max jobs per query: {max_jobs}")
    print(f"  Search window: {hours_old} hours ({hours_old // 24} days)")
    
    # Scrape
    print(f"\n  [1/3] Scraping jobs...")
    df = scrape_ai_jobs(ticker, company_name, max_jobs=max_jobs, hours_old=hours_old)
    
    # EXTRACT total_scraped from DataFrame attrs (set in scrape_ai_jobs)
    total_jobs_scraped = df.attrs.get('total_scraped', len(df))
    
    if df.empty:
        print(f" No jobs found")
        return {"status": "no_jobs", "count": 0}
    
    print(f" Found {len(df)} AI jobs (from {total_jobs_scraped} total scraped)")
    
    # Stats
    ai_verified = df['ai_score'].ge(50).sum()
    multi_source = df['multi_source'].sum()
    with_salary = df['min_amount'].notna().sum()
    
    print(f"     AI verified: {ai_verified}/{len(df)}")
    print(f"     Multi-source: {multi_source}")
    print(f"     With salary: {with_salary}")
    
    # Seniority breakdown
    sen_counts = df['seniority_label'].value_counts().to_dict()
    print(f"     Seniority: L={sen_counts.get('leadership',0)} " +
          f"Sr={sen_counts.get('senior',0)} " +
          f"Mid={sen_counts.get('mid',0)} " +
          f"Jr={sen_counts.get('entry',0)}")
    
    # Top jobs
    print(f"\n  Top jobs by AI score:")
    for i, (_, job) in enumerate(df.nlargest(3, 'ai_score').iterrows(), 1):
        loc = str(job.get('location', 'NULL') or 'NULL')[:20]
        print(f"    {i}. {job['title'][:40]:40} [{job['seniority_label']:10}] {loc:20} (score: {job['ai_score']:.0f})")
    
    # Insert
    print(f"\n  [2/3] Inserting into Snowflake...")
    signals = prepare_for_snowflake(df, company_id, ticker)
    inserted = service.insert_job_signals(signals)
    
    print(f"  Inserted {inserted}/{len(signals)} signals")
    
    if inserted < len(signals):
        print(f"  {len(signals) - inserted} signals failed")
    
    # Update summary with pre-calculated stats
    print(f"\n  [3/3] Updating company summary...")
    
    summary_stats = {
        'total_jobs_scraped': total_jobs_scraped,   # TOTAL before AI filter (e.g., 312)
        'ai_jobs': len(df),                          # AI jobs only (e.g., 238)
        'ai_verified': int(ai_verified),
        'multi_source': int(multi_source),
        'with_salary': int(with_salary),
        'seniority': sen_counts,
        'ai_ratio': len(df) / total_jobs_scraped if total_jobs_scraped > 0 else 1.0
    }
    
    service.update_company_summary(company_id, ticker, summary_stats)
    
    # Verify
    cursor = conn.cursor()
    cursor.execute(
        "SELECT hiring_score FROM company_evidence_summary WHERE company_id = %s",
        (company_id,)
    )
    result = cursor.fetchone()
    hiring_score = result[0] if result else 0.0
    cursor.close()
    
    print(f"  Hiring score: {hiring_score:.1f}/100")
    
    return {
        "status": "success",
        "total_jobs": len(df),
        "inserted": inserted,
        "ai_verified": int(ai_verified),
        "hiring_score": hiring_score
    }


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Job Signal Collection Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/run_job_pipeline.py --ticker CAT
  python scripts/run_job_pipeline.py --ticker JPM --max-jobs 200
  python scripts/run_job_pipeline.py --all
        """
    )
    
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--ticker', type=str, help='Company ticker')
    group.add_argument('--all', action='store_true', help='All companies from YAML')
    
    parser.add_argument('--max-jobs', type=int, default=50, help='Max jobs per query (default: 50)')
    parser.add_argument('--days', type=int, default=10, help='Days of data to search (default: 10)')
    
    args = parser.parse_args()
    
    print("\n" + "="*70)
    print("JOB SIGNAL COLLECTION PIPELINE")
    print("="*70)
    print(f"Collection window: Last {args.days} days ({args.days * 24} hours)")
    print(f"Max jobs per query: {args.max_jobs}")
    
    # Load config
    settings = get_settings()
    companies_config = get_target_companies()
    
    # Connect
    print(f"\nConnecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(
            account=settings.snowflake.account,
            user=settings.snowflake.user,
            password=settings.snowflake.password.get_secret_value(),
            warehouse=settings.snowflake.warehouse,
            database=settings.snowflake.database,
            schema=settings.snowflake.schema,
            role=settings.snowflake.role
        )
        print(f"Connected to {settings.snowflake.database}.{settings.snowflake.schema}")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)
    
    service = JobSignalService(conn)
    results = {}
    
    # Determine companies to process
    if args.all:
        companies_to_process = companies_config.items()
        print(f"\nProcessing ALL {len(companies_config)} companies from companies.yml")
    else:
        if args.ticker not in companies_config:
            print(f"\nTicker '{args.ticker}' not found in companies.yml")
            print(f"Available: {', '.join(companies_config.keys())}")
            conn.close()
            sys.exit(1)
        
        companies_to_process = [(args.ticker, companies_config[args.ticker])]
        print(f"\nProcessing single company: {args.ticker}")
    
    # Process
    for ticker, company_info in companies_to_process:
        company_name = company_info['name']
        
        # Get company_id from DB
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM companies WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        cursor.close()
        
        if not result:
            print(f"\n{ticker} not in database - skipping")
            results[ticker] = {"status": "not_in_db"}
            continue
        
        company_id = result[0]
        
        # Collect
        try:
            result = collect_for_company(conn, service, ticker, company_name, company_id, max_jobs=args.max_jobs, hours_old=args.days * 24)
            results[ticker] = result
        except Exception as e:
            print(f"\n Error: {str(e)[:150]}")
            results[ticker] = {"status": "error", "error": str(e)[:200]}
    
    conn.close()
    
    # Summary
    print(f"\n{'='*70}")
    print("PIPELINE SUMMARY")
    print(f"{'='*70}")
    
    for ticker, data in results.items():
        if data.get("status") == "success":
            print(f"{ticker}: {data['total_jobs']} jobs | {data['ai_verified']} AI-verified | Score: {data['hiring_score']:.1f}")
        elif data.get("status") == "no_jobs":
            print(f"{ticker}: No jobs found")
        else:
            print(f"{ticker}: {data.get('status', 'unknown')}")
    
    print(f"\n Pipeline complete!")


if __name__ == "__main__":
    main()