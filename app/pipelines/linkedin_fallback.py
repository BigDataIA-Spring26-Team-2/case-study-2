"""Lean LinkedIn Playwright Fallback - Fast execution, activates on JobSpy failure."""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from playwright.sync_api import sync_playwright
import pandas as pd
import re
import time
import random
from datetime import datetime, timedelta

from app.core.keywords import get_seniority_keywords


def parse_relative_date(text: str) -> datetime:
    """Convert '2 days ago' to datetime."""
    text = text.lower().strip()
    now = datetime.now()
    
    if 'just now' in text or 'today' in text:
        return now
    
    match = re.search(r'(\d+)\s*(hour|day|week|month)', text)
    if not match:
        return now
    
    num = int(match.group(1))
    unit = match.group(2)
    
    if 'hour' in unit:
        return now - timedelta(hours=num)
    elif 'day' in unit:
        return now - timedelta(days=num)
    elif 'week' in unit:
        return now - timedelta(weeks=num)
    elif 'month' in unit:
        return now - timedelta(days=num * 30)
    
    return now


def scrape_linkedin_fast(ticker: str, company_name: str) -> pd.DataFrame:
    """Fast LinkedIn scrape - uses top job titles from config."""
    
    from app.core.keywords import get_all_job_titles
    
    # Get ALL job titles from config
    job_titles = get_all_job_titles()
    
    seniority_kw = get_seniority_keywords()
    
    # Company variations
    variations = [
        company_name,
        re.sub(r'\s*(Inc\.?|LLC\.?|Corp\.?|Company|,)\.?', '', company_name).strip(),
        company_name.split()[0]
    ]
    
    all_jobs = []
    seen = set()
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        for search in job_titles[:10]:
            url = f"https://www.linkedin.com/jobs/search?keywords={search}%20{company_name.replace(' ', '%20')}&location=United%20States&f_TPR=r2592000&sortBy=DD"
            
            try:
                page.goto(url, timeout=15000)
                page.wait_for_timeout(2000)
                
                # Quick scroll (5x max)
                for _ in range(5):
                    page.keyboard.press("End")
                    page.wait_for_timeout(800)
                
                cards = page.query_selector_all(".base-card, .job-search-card")
                
                for card in cards:
                    try:
                        title_el = card.query_selector(".base-search-card__title, .job-search-card__title")
                        comp_el = card.query_selector(".base-search-card__subtitle, .job-search-card__company")
                        
                        if not title_el or not comp_el:
                            continue
                        
                        title = title_el.inner_text().strip()
                        company = comp_el.inner_text().strip()
                        
                        # Verify company
                        if not any(v.lower() in company.lower() for v in variations):
                            continue
                        
                        loc_el = card.query_selector(".job-search-card__location")
                        location = loc_el.inner_text().strip() if loc_el else ""
                        
                        # Fingerprint dedupe
                        fp = f"{title.lower()}|{location.lower()}"
                        if fp in seen:
                            continue
                        seen.add(fp)
                        
                        # Date
                        date_posted = None
                        time_el = card.query_selector("time")
                        if time_el:
                            date_iso = time_el.get_attribute("datetime")
                            date_text = time_el.inner_text().strip()
                            if date_iso:
                                try:
                                    date_posted = pd.to_datetime(date_iso)
                                except:
                                    date_posted = parse_relative_date(date_text)
                            else:
                                date_posted = parse_relative_date(date_text)
                        
                        # URL
                        link_el = card.query_selector("a")
                        job_url = link_el.get_attribute("href").split('?')[0] if link_el else ""
                        
                        # Seniority
                        t = title.lower()
                        if any(k in t for k in seniority_kw.get("leadership", [])):
                            seniority = "leadership"
                        elif any(k in t for k in seniority_kw.get("senior", [])):
                            seniority = "senior"
                        elif any(k in t for k in seniority_kw.get("entry", [])):
                            seniority = "entry"
                        else:
                            seniority = "mid"
                        
                        all_jobs.append({
                            "title": title,
                            "company": company,
                            "location": location,
                            "job_url": job_url,
                            "date_posted": date_posted,
                            "description": "",
                            "site": "linkedin",
                            "sources": "linkedin",
                            "multi_source": False,
                            "dupe_count": 1,
                            "is_remote": "remote" in location.lower(),
                            "min_amount": None,
                            "max_amount": None,
                            "seniority_label": seniority
                        })
                    
                    except:
                        continue
                
                time.sleep(random.uniform(2, 4))  # Short delay
            
            except:
                continue
        
        browser.close()
    
    if not all_jobs:
        return pd.DataFrame()
    
    return pd.DataFrame(all_jobs)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', default='CAT')
    parser.add_argument('--company', default='Caterpillar Inc')
    args = parser.parse_args()
    
    print(f"Testing for {args.ticker}...")
    df = scrape_linkedin_fast(args.ticker, args.company)
    
    if df.empty:
        print("No jobs")
    else:
        print(f"\n{len(df)} jobs found")
        print(f"Seniority: L={len(df[df['seniority_label']=='leadership'])} "
              f"Sr={len(df[df['seniority_label']=='senior'])} "
              f"Mid={len(df[df['seniority_label']=='mid'])} "
              f"Jr={len(df[df['seniority_label']=='entry'])}")
        df.to_csv(f"linkedin_{args.ticker}_fast.csv", index=False)
        print(f"Saved to linkedin_{args.ticker}_fast.csv")