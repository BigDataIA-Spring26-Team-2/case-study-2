"""
Evidence Collection Orchestrator
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import subprocess
import argparse
import sys
from datetime import datetime

COMPANIES = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]


def run_command(cmd: list, description: str) -> bool:
    """Run command and return success status."""
    print(f"\n{'='*70}")
    print(f"{description}")
    print(f"{'='*70}")
    
    start = datetime.now()
    result = subprocess.run(cmd)
    duration = (datetime.now() - start).total_seconds()
    
    success = result.returncode == 0
    status = "✓ Complete" if success else "✗ Failed"
    print(f"\n{status} in {duration:.1f}s")
    
    return success


def show_pipeline_menu() -> list:
    """Interactive menu to select pipelines."""
    print("\n" + "="*70)
    print("SELECT PIPELINES TO RUN")
    print("="*70)
    print("1. SEC only")
    print("2. Job only")
    print("3. Patent only")
    print("4. GitHub only")
    print("5. All pipelines (SEC → Job → Patent → GitHub)")
    print("6. Signals only (Job + Patent + GitHub)")
    print("7. Custom selection")
    print("="*70)
    
    choice = input("\nEnter choice (1-7): ").strip()
    
    pipelines = {
        '1': ['sec'],
        '2': ['job'],
        '3': ['patent'],
        '4': ['github'],
        '5': ['sec', 'job', 'patent', 'github'],
        '6': ['job', 'patent', 'github'],
    }
    
    if choice in pipelines:
        return pipelines[choice]
    elif choice == '7':
        selected = []
        if input("Run SEC? (y/n): ").lower() == 'y':
            selected.append('sec')
        if input("Run Job? (y/n): ").lower() == 'y':
            selected.append('job')
        if input("Run Patent? (y/n): ").lower() == 'y':
            selected.append('patent')
        if input("Run GitHub? (y/n): ").lower() == 'y':
            selected.append('github')
        return selected
    else:
        print("Invalid choice, running all pipelines")
        return ['sec', 'job', 'patent', 'github']


def collect_for_company(ticker: str, pipelines: list, sec_types: str, sec_limit: int, 
                        job_days: int, patent_year: int) -> dict:
    """Run selected pipelines for a single company."""
    
    print(f"\n\n{'#'*70}")
    print(f"# {ticker}")
    print(f"{'#'*70}")
    
    results = {
        'ticker': ticker,
        'sec': None,
        'job': None,
        'patent': None,
        'github': None
    }
    
    pipeline_count = len(pipelines)
    current = 0
    
    # SEC Pipeline
    if 'sec' in pipelines:
        current += 1
        sec_cmd = [
            "poetry", "run", "python", "scripts/run_sec_pipeline.py",
            "--companies", ticker,
            "--types", sec_types,
            "--limit", str(sec_limit)
        ]
        results['sec'] = run_command(sec_cmd, f"[{current}/{pipeline_count}] SEC Pipeline - {ticker}")
        
        # If SEC fails and it's required, stop
        if not results['sec'] and 'sec' in pipelines:
            print(f"\n⚠ SEC pipeline failed for {ticker}")
            if len(pipelines) > 1:
                print("Skipping remaining pipelines (SEC must succeed first)")
                return results
    
    # Job Pipeline
    if 'job' in pipelines:
        current += 1
        job_cmd = [
            "poetry", "run", "python", "scripts/run_job_pipeline.py",
            "--ticker", ticker
        ]
        results['job'] = run_command(job_cmd, f"[{current}/{pipeline_count}] Job Pipeline - {ticker}")
    
    # Patent Pipeline
    if 'patent' in pipelines:
        current += 1
        patent_cmd = [
            "poetry", "run", "python", "scripts/run_patent_pipeline.py",
            "--ticker", ticker,
            "--year", str(patent_year)
        ]
        results['patent'] = run_command(patent_cmd, f"[{current}/{pipeline_count}] Patent Pipeline - {ticker}")
    
    # GitHub Pipeline
    if 'github' in pipelines:
        current += 1
        github_cmd = [
            "poetry", "run", "python", "scripts/run_github_pipeline.py",
            "--ticker", ticker
        ]
        results['github'] = run_command(github_cmd, f"[{current}/{pipeline_count}] GitHub Pipeline - {ticker}")
    
    # Calculate composite score if any signal pipeline succeeded
    signal_pipelines = ['job', 'patent', 'github']
    ran_signals = [p for p in signal_pipelines if p in pipelines]
    succeeded_signals = [p for p in ran_signals if results.get(p)]
    
    if succeeded_signals:
        print(f"\n{'='*70}")
        print(f"Calculating Composite Score - {ticker}")
        print(f"{'='*70}")
        print(f"Signals collected: {', '.join(succeeded_signals)}")
        
        try:
            from app.services.signal_aggregation import calculate_composite_scores
            calculate_composite_scores(ticker)
            print(f"Composite score calculated for {ticker}\n")
            
        except Exception as e:
            print(f"Failed to calculate composite score: {e}\n")


    return results


def main():
    parser = argparse.ArgumentParser(
        description='Evidence Collection Orchestrator',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (choose pipelines)
  python collect_evidence.py
  
  # All companies, all pipelines (batch mode)
  python collect_evidence.py --all
  
  # Single company, all pipelines
  python collect_evidence.py --ticker CAT
  
  # Multiple companies, specific pipelines
  python collect_evidence.py --companies CAT,JPM --pipelines sec,job
  
  # Custom parameters
  python collect_evidence.py --ticker CAT --sec-limit 5 --patent-year 2022
"""
    )
    
    # Company selection
    parser.add_argument('--ticker', help='Single company ticker')
    parser.add_argument('--companies', help='Comma-separated tickers')
    parser.add_argument('--all', action='store_true', help='All 10 companies')
    
    # Pipeline selection
    parser.add_argument('--pipelines', help='Pipelines to run: sec,job,patent,github (default: all)')
    
    # SEC parameters
    parser.add_argument('--sec-types', default='10-K', help='Filing types (default: 10-K)')
    parser.add_argument('--sec-limit', type=int, default=1, help='Docs per type (default: 1)')
    
    # Job parameters  
    parser.add_argument('--job-days', type=int, default=10, help='Job window days (default: 10)')
    
    # Patent parameters
    parser.add_argument('--patent-year', type=int, default=2024, help='Patents since year (default: 2024)')
    
    args = parser.parse_args()
    
    # Determine companies
    if args.all:
        tickers = COMPANIES
    elif args.companies:
        tickers = [t.strip().upper() for t in args.companies.split(',')]
    elif args.ticker:
        tickers = [args.ticker.upper()]
    else:
        # Interactive mode
        print("\n" + "="*70)
        print("COMPANY SELECTION")
        print("="*70)
        print("1. Single company")
        print("2. Multiple companies")
        print("3. All 10 companies")
        print("="*70)
        
        choice = input("\nEnter choice (1-3): ").strip()
        
        if choice == '1':
            ticker = input("Enter ticker: ").strip().upper()
            tickers = [ticker]
        elif choice == '2':
            companies_input = input("Enter tickers (comma-separated): ").strip().upper()
            tickers = [t.strip() for t in companies_input.split(',')]
        elif choice == '3':
            tickers = COMPANIES
        else:
            print("Invalid choice, using CAT")
            tickers = ['CAT']
    
    # Determine pipelines
    if args.pipelines:
        pipelines = [p.strip() for p in args.pipelines.split(',')]
    elif args.ticker or args.companies or args.all:
        # Batch mode: run all pipelines
        pipelines = ['sec', 'job', 'patent', 'github']
    else:
        # Interactive mode: show menu
        pipelines = show_pipeline_menu()
    
    # Summary
    print("\n" + "="*70)
    print("EVIDENCE COLLECTION ORCHESTRATOR")
    print("="*70)
    print(f"Companies: {', '.join(tickers)}")
    print(f"Pipelines: {', '.join(pipelines)}")
    print(f"SEC: types={args.sec_types}, limit={args.sec_limit}")
    print(f"Job: window={args.job_days} days")
    print(f"Patent: since {args.patent_year}")
    print("="*70)
    
    if not args.all and not args.ticker and not args.companies:
        confirm = input("\nProceed? (y/n): ")
        if confirm.lower() != 'y':
            print("Cancelled")
            sys.exit(0)
    
    overall_start = datetime.now()
    all_results = []
    
    # Process each company
    for ticker in tickers:
        results = collect_for_company(
            ticker=ticker,
            pipelines=pipelines,
            sec_types=args.sec_types,
            sec_limit=args.sec_limit,
            job_days=args.job_days,
            patent_year=args.patent_year
        )
        all_results.append(results)
    
    overall_duration = (datetime.now() - overall_start).total_seconds()
    
    # Summary
    print(f"\n\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"Total Duration: {overall_duration:.1f}s ({overall_duration/60:.1f}m)")
    print(f"\n{'Ticker':<8} {'SEC':<6} {'Job':<6} {'Patent':<8} {'GitHub':<8}")
    print("-"*70)
    
    for r in all_results:
        sec_icon = '✓' if r['sec'] else ('✗' if r['sec'] is False else '-')
        job_icon = '✓' if r['job'] else ('✗' if r['job'] is False else '-')
        patent_icon = '✓' if r['patent'] else ('✗' if r['patent'] is False else '-')
        github_icon = '✓' if r['github'] else ('✗' if r['github'] is False else '-')
        
        print(f"{r['ticker']:<8} {sec_icon:<6} {job_icon:<6} {patent_icon:<8} {github_icon:<8}")
    
    print("="*70)
    
    # Exit code based on requested pipelines
    requested_succeeded = all(
        r.get(p) for r in all_results for p in pipelines if r.get(p) is not None
    )
    sys.exit(0 if requested_succeeded else 1)


if __name__ == "__main__":
    main()