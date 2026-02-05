"""Lean AI job scraper - config from YAML."""
from jobspy import scrape_jobs
import pandas as pd
import re
from datetime import datetime
from difflib import SequenceMatcher
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor, as_completed
from sentence_transformers import SentenceTransformer, util

# Check if Playwright available
try:
    from app.pipelines.linkedin_fallback import scrape_linkedin_fast
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

from app.core.keywords import (
    get_all_job_titles, get_ai_references, get_similarity_threshold,
    get_acronyms, get_seniority_keywords
)

_model, _ref_emb = None, None

def _load_model():
    global _model, _ref_emb
    if _model is None:
        _model = SentenceTransformer('all-mpnet-base-v2')
        _ref_emb = _model.encode(get_ai_references(), convert_to_tensor=True)
    return _model, _ref_emb


def score_job(title: str, desc: str = "") -> dict:
    model, ref_emb = _load_model()
    title = str(title or "").strip()
    desc = str(desc or "").strip()[:1500]
    
    if not title and not desc:
        return {"score": 0, "similarity": 0, "is_ai": False}
    
    text = f"{title}. {title}. {desc}" if desc else f"{title}. {title}. {title}"
    emb = model.encode(text, convert_to_tensor=True)
    sim = util.cos_sim(emb, ref_emb)[0].max().item()
    
    base = min(60, sim * 100)
    sen = get_seniority_keywords()
    t_low = title.lower()
    bonus = 20 if any(k in t_low for k in sen.get("leadership", [])) else (
            10 if any(k in t_low for k in sen.get("senior", [])) else 0)
    desc_bonus = 10 if len(desc) > 500 else (5 if len(desc) > 100 else 0)
    
    return {
        "score": round(min(100, base + bonus + desc_bonus), 1),
        "similarity": round(sim, 3),
        "is_ai": sim >= get_similarity_threshold()
    }


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """Simple: 85% similarity on 'title | location' combined string."""
    if df.empty:
        return pd.DataFrame()
    
    acronyms = get_acronyms()
    
    def norm_title(t):
        t = str(t).lower().strip()
        t = re.sub(r'\bsr\.?\s+', 'senior ', t)
        t = re.sub(r'\bjr\.?\s+', 'junior ', t)
        for k, v in sorted(acronyms.items(), key=lambda x: -len(x[0])):
            t = re.sub(rf'\b{re.escape(k)}\b', v, t)
        return re.sub(r'\s+', ' ', t).strip()
    
    def norm_loc(loc):
        loc = str(loc).lower().strip()
        loc = re.sub(r'[,\s]+(il|us|usa|united states)$', '', loc)
        return re.sub(r'\s+', ' ', loc).strip() or 'null'
    
    df = df.copy()
    df['_keep'] = True
    df['_sources'] = df.get('site', 'unknown')
    
    for i in range(len(df)):
        if not df.at[i, '_keep']:
            continue
        
        title_i = norm_title(df.at[i, 'title'])
        loc_i = norm_loc(df.at[i, 'location'])
        combined_i = f"{title_i} | {loc_i}"
        
        for j in range(i + 1, len(df)):
            if not df.at[j, '_keep']:
                continue
            
            title_j = norm_title(df.at[j, 'title'])
            loc_j = norm_loc(df.at[j, 'location'])
            combined_j = f"{title_j} | {loc_j}"
            
            sim = SequenceMatcher(None, combined_i, combined_j).ratio()
            
            if sim >= 0.9:
                if loc_i == 'null' and loc_j != 'null':
                    df.at[i, '_keep'] = False
                    break
                elif loc_j == 'null' and loc_i != 'null':
                    df.at[j, '_keep'] = False
                else:
                    # Merge sources
                    src_i = set(str(df.at[i, '_sources']).split(','))
                    src_j = set(str(df.at[j, 'site']).split(','))
                    df.at[i, '_sources'] = ','.join(sorted(src_i | src_j))
                    df.at[j, '_keep'] = False
    
    result = df[df['_keep']].copy()
    result['sources'] = result['_sources']
    result['multi_source'] = result['_sources'].str.contains(',')
    result['dupe_count'] = 1
    
    return result.drop(columns=['_keep', '_sources'])

def scrape_ai_jobs(ticker: str, company_name: str, max_jobs: int = 100, hours_old: int = 240) -> pd.DataFrame:
    """Scrape jobs with Playwright fallback if JobSpy fails/returns 0."""
    
    job_titles = get_all_job_titles()
    all_jobs = []
    
    variations = list(dict.fromkeys([
        company_name,
        re.sub(r'\s*(Inc|LLC|Corp|Company|,)\.?', '', company_name).strip(),
        company_name.split()[0]
    ]))
    
    def scrape_one(title):
        try:
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "glassdoor", "zip_recruiter", "google"],  
                search_term=f"{title} {company_name}",
                location="USA", 
                results_wanted=max_jobs, 
                hours_old=hours_old, 
                country_indeed="USA"
            )
            if jobs.empty:
                return pd.DataFrame()
            mask = pd.Series([False] * len(jobs))
            for v in variations:
                mask |= jobs["company"].str.lower().str.contains(v.lower(), na=False, regex=False)
            return jobs[mask]
        except Exception:
            return pd.DataFrame()
    
    with ThreadPoolExecutor(max_workers=min(10, len(job_titles))) as ex:
        for f in as_completed([ex.submit(scrape_one, t) for t in job_titles]):
            r = f.result()
            if not r.empty:
                all_jobs.append(r)
    
    # Filter empty dataframes before concat
    all_jobs = [j for j in all_jobs if not j.empty]
    print(f"  [SCRAPE] Raw jobs from JobSpy: {sum(len(j) for j in all_jobs)}")  
    
    if not all_jobs:
        df = pd.DataFrame()
    else:
        df = dedupe(pd.concat(all_jobs, ignore_index=True))
        print(f"  [DEDUPE] After deduplication: {len(df)} unique jobs")  

    
    # ACTIVATE FALLBACK if JobSpy returned 0 jobs
    if df.empty and PLAYWRIGHT_AVAILABLE:
        print(f"  JobSpy returned 0 jobs → Activating Playwright fallback...")
        try:
            df = scrape_linkedin_fast(ticker, company_name)
        except Exception as e:
            print(f"  Playwright failed: {str(e)[:80]}")
            return pd.DataFrame()
    
    if df.empty:
        return pd.DataFrame()
    
    # Score all jobs
    scores = df.apply(lambda r: score_job(r.get("title", ""), r.get("description", "")), axis=1)
    df["ai_score"] = scores.apply(lambda x: x["score"])
    df["ai_similarity"] = scores.apply(lambda x: x["similarity"])
    df["is_ai"] = scores.apply(lambda x: x["is_ai"])
    print(f"  [FILTER] AI jobs (similarity >= {get_similarity_threshold()}): {df['is_ai'].sum()}/{len(df)}") 
    
    # Seniority (if not already present from Playwright)
    if "seniority_label" not in df.columns:
        sen = get_seniority_keywords()
        def get_seniority_label(title):
            t = str(title).lower()
            if any(k in t for k in sen.get("leadership", [])):
                return "leadership"
            if any(k in t for k in sen.get("senior", [])):
                return "senior"
            if any(k in t for k in sen.get("entry", [])):
                return "entry"
            return "mid"
        df["seniority_label"] = df["title"].apply(get_seniority_label)
    
    return df[df["is_ai"]].drop(columns=["is_ai"])


def prepare_for_snowflake(df: pd.DataFrame, company_id: str, ticker: str) -> list[dict]:
    signals = []
    for _, job in df.iterrows():
        signals.append({
            "id": str(uuid4()),
            "company_id": company_id,
            "category": "hiring_signal",
            "source": str(job.get("sources", "unknown")).split(",")[0],
            "score": float(job.get("ai_score", 0)),
            "confidence": round(float(job.get("ai_similarity", 0)) * 0.8, 2),
            "metadata": {
                "title": str(job.get("title", "")),
                "seniority_label": str(job.get("seniority_label", "mid")),
                "multi_source": bool(job.get("multi_source", False)),
                "location": str(job.get("location", "")) if pd.notna(job.get("location")) else "",
                "is_remote": bool(job.get("is_remote", False)),
                "date_posted": str(job["date_posted"]) if pd.notna(job.get("date_posted")) else None,
                "salary_min": int(job["min_amount"]) if pd.notna(job.get("min_amount")) else None,
                "salary_max": int(job["max_amount"]) if pd.notna(job.get("max_amount")) else None,
                "job_url": str(job.get("job_url", "")) if pd.notna(job.get("job_url")) else "",
                "is_ai_related": True, 
                "ai_score": float(job.get("ai_score", 0))
            },
            "s3_full_data_key": None,
            "collected_at": datetime.now()
        })
    return signals