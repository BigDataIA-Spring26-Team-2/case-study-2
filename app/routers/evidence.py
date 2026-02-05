"""Evidence endpoints."""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from uuid import UUID, uuid4
from typing import Optional, List
import subprocess

from app.database import get_db

router = APIRouter(prefix="/api/v1/evidence", tags=["evidence"])


# ========== ADD THESE MODELS ==========
class BackfillRequest(BaseModel):
    tickers: Optional[List[str]] = None
    pipelines: List[str] = ["sec", "job", "patent", "github"]


class BackfillResponse(BaseModel):
    task_id: str
    message: str
    tickers: List[str]
    pipelines: List[str]


COMPANIES = {
    "CAT": {"name": "Caterpillar Inc", "industry": "Manufacturing"},
    "DE": {"name": "Deere & Company", "industry": "Manufacturing"},
    "UNH": {"name": "UnitedHealth Group", "industry": "Healthcare Services"},
    "HCA": {"name": "HCA Healthcare", "industry": "Healthcare Services"},
    "ADP": {"name": "Automatic Data Processing", "industry": "Business Services"},
    "PAYX": {"name": "Paychex Inc", "industry": "Business Services"},
    "WMT": {"name": "Walmart Inc", "industry": "Retail"},
    "TGT": {"name": "Target Corporation", "industry": "Retail"},
    "JPM": {"name": "JPMorgan Chase", "industry": "Financial Services"},
    "GS": {"name": "Goldman Sachs", "industry": "Financial Services"}
}


def ensure_companies_exist(tickers: List[str], conn):
    """Create companies if they don't exist."""
    cursor = conn.cursor()
    
    for ticker in tickers:
        if ticker not in COMPANIES:
            continue
        
        company_id = str(uuid4())
        
        cursor.execute("""
            MERGE INTO companies t
            USING (
                SELECT 
                    %(id)s as id,
                    %(ticker)s as ticker,
                    %(name)s as name,
                    (SELECT id FROM industries WHERE name = %(industry)s LIMIT 1) as industry_id
            ) s
            ON t.ticker = s.ticker
            WHEN NOT MATCHED THEN INSERT (id, ticker, name, industry_id, created_at)
                VALUES (s.id, s.ticker, s.name, s.industry_id, CURRENT_TIMESTAMP())
        """, {
            'id': company_id,
            'ticker': ticker,
            'name': COMPANIES[ticker]['name'],
            'industry': COMPANIES[ticker]['industry']
        })
    
    conn.commit()


def run_collection_task(task_id: str, tickers: List[str], pipelines: List[str]):
    """Background task that runs collect_evidence.py"""
    try:
        cmd = [
            "poetry", "run", "python", "scripts/collect_evidence.py",
            "--companies", ",".join(tickers),
            "--pipelines", ",".join(pipelines)
        ]
        subprocess.run(cmd, capture_output=True, text=True)
    except Exception as e:
        print(f"Task {task_id} failed: {e}")


# ========== ADD THIS ENDPOINT ==========
@router.post("/backfill", response_model=BackfillResponse)
async def backfill_evidence(
    request: BackfillRequest, 
    background_tasks: BackgroundTasks,
    conn = Depends(get_db)
):
    """Trigger evidence collection. If tickers=null, runs for all 10 companies."""
    
    tickers = request.tickers or list(COMPANIES.keys())
    
    # Ensure companies exist
    ensure_companies_exist(tickers, conn)
    
    # Queue collection
    task_id = str(uuid4())
    background_tasks.add_task(run_collection_task, task_id, tickers, request.pipelines)
    
    return BackfillResponse(
        task_id=task_id,
        message=f"Collection started for {len(tickers)} companies. Check back in 20-30 minutes.",
        tickers=tickers,
        pipelines=request.pipelines
    )


@router.get("/stats")
async def get_evidence_stats(conn = Depends(get_db)):
    """Get overall evidence statistics."""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT company_id) as companies,
            COALESCE(SUM(total_documents), 0) as docs,
            COALESCE(SUM(total_chunks), 0) as chunks,
            AVG(composite_score) as avg_score
        FROM company_evidence_summary
    """)
    
    overall = cursor.fetchone()
    
    cursor.execute("""
        SELECT ticker, total_documents, hiring_score, patent_score, 
               github_score, composite_score
        FROM company_evidence_summary
        ORDER BY ticker
    """)
    
    companies = cursor.fetchall()
    
    return {
        "overall": {
            "companies_processed": overall[0] or 0,
            "total_documents": overall[1] or 0,
            "total_chunks": overall[2] or 0,
            "avg_composite_score": round(float(overall[3]), 2) if overall[3] else None
        },
        "by_company": [
            {
                "ticker": c[0],
                "documents": c[1] or 0,
                "hiring_score": float(c[2]) if c[2] else None,
                "patent_score": float(c[3]) if c[3] else None,
                "github_score": float(c[4]) if c[4] else None,
                "composite_score": float(c[5]) if c[5] else None
            }
            for c in companies
        ]
    }


@router.get("/companies/{company_id}/evidence")
async def get_company_evidence(company_id: UUID, conn = Depends(get_db)):
    """Get all evidence for a company."""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT ticker, total_documents, total_chunks,
               hiring_score, patent_score, github_score, leadership_score,
               composite_score, last_updated
        FROM company_evidence_summary
        WHERE company_id = %(id)s
    """, {'id': str(company_id)})
    
    summary = cursor.fetchone()
    
    if not summary:
        raise HTTPException(status_code=404, detail="Company not found")
    
    cursor.execute("""
        SELECT id, filing_type, filing_date, status, total_words
        FROM evidence_documents
        WHERE company_id = %(id)s
        ORDER BY filing_date DESC
        LIMIT 10
    """, {'id': str(company_id)})
    
    docs = cursor.fetchall()
    
    cursor.execute("""
        SELECT category, score, confidence, collected_at
        FROM external_signals
        WHERE company_id = %(id)s
        ORDER BY collected_at DESC
    """, {'id': str(company_id)})
    
    signals = cursor.fetchall()
    
    return {
        "ticker": summary[0],
        "summary": {
            "total_documents": summary[1] or 0,
            "total_chunks": summary[2] or 0,
            "hiring_score": float(summary[3]) if summary[3] else None,
            "patent_score": float(summary[4]) if summary[4] else None,
            "github_score": float(summary[5]) if summary[5] else None,
            "leadership_score": float(summary[6]) if summary[6] else None,
            "composite_score": float(summary[7]) if summary[7] else None,
            "last_updated": summary[8]
        },
        "recent_documents": [
            {"id": d[0], "filing_type": d[1], "filing_date": d[2], "status": d[3], "word_count": d[4]}
            for d in docs
        ],
        "recent_signals": [
            {"category": s[0], "score": float(s[1]), "confidence": float(s[2]), "collected_at": s[3]}
            for s in signals
        ]
    }