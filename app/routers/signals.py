"""Signal collection and retrieval endpoints."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from uuid import UUID
from typing import List
import subprocess
import json

from app.database import get_db

router = APIRouter(prefix="/api/v1/signals", tags=["signals"])


class SignalCollectionRequest(BaseModel):
    company_id: UUID
    pipelines: List[str] = ["job", "patent", "github"]


class SignalCollectionResponse(BaseModel):
    message: str
    company_id: UUID
    pipelines: List[str]


def get_ticker_from_company_id(company_id: UUID, conn) -> str:
    """Helper to get ticker from company_id."""
    cursor = conn.cursor()
    cursor.execute("SELECT ticker FROM companies WHERE id = %(id)s", {'id': str(company_id)})
    result = cursor.fetchone()
    
    if not result:
        raise HTTPException(status_code=404, detail="Company not found")
    
    return result[0]


def run_signal_pipelines(ticker: str, pipelines: List[str]):
    """Run signal collection pipelines."""
    pipeline_map = {
        "job": "run_job_pipeline.py",
        "patent": "run_patent_pipeline.py",
        "github": "run_github_pipeline.py"
    }
    
    for pipeline in pipelines:
        if pipeline in pipeline_map:
            cmd = ["poetry", "run", "python", f"scripts/{pipeline_map[pipeline]}", "--ticker", ticker]
            subprocess.run(cmd)


@router.post("/collect", response_model=SignalCollectionResponse)
async def collect_signals(request: SignalCollectionRequest, background_tasks: BackgroundTasks, conn = Depends(get_db)):
    """Trigger signal collection for a company."""
    ticker = get_ticker_from_company_id(request.company_id, conn)
    background_tasks.add_task(run_signal_pipelines, ticker, request.pipelines)
    
    return SignalCollectionResponse(
        message=f"Signal collection started for {ticker}. Check back in 5-10 minutes.",
        company_id=request.company_id,
        pipelines=request.pipelines
    )


@router.post("/collect/hiring")
async def collect_hiring_signal(company_id: UUID, background_tasks: BackgroundTasks, conn = Depends(get_db)):
    """Trigger hiring signal collection only."""
    ticker = get_ticker_from_company_id(company_id, conn)
    background_tasks.add_task(run_signal_pipelines, ticker, ["job"])
    return {"message": f"Hiring signal collection started for {ticker}"}


@router.post("/collect/patent")
async def collect_patent_signal(company_id: UUID, background_tasks: BackgroundTasks, conn = Depends(get_db)):
    """Trigger patent signal collection only."""
    ticker = get_ticker_from_company_id(company_id, conn)
    background_tasks.add_task(run_signal_pipelines, ticker, ["patent"])
    return {"message": f"Patent signal collection started for {ticker}"}


@router.post("/collect/github")
async def collect_github_signal(company_id: UUID, background_tasks: BackgroundTasks, conn = Depends(get_db)):
    """Trigger GitHub signal collection only."""
    ticker = get_ticker_from_company_id(company_id, conn)
    background_tasks.add_task(run_signal_pipelines, ticker, ["github"])
    return {"message": f"GitHub signal collection started for {ticker}"}


@router.get("/companies/{company_id}/signals")
async def get_company_signals(company_id: UUID, conn = Depends(get_db)):
    """Get signal summary for a company."""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT ticker, hiring_score, patent_score, github_score, 
               leadership_score, composite_score, last_updated
        FROM company_evidence_summary
        WHERE company_id = %(id)s
    """, {'id': str(company_id)})
    
    row = cursor.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    
    return {
        "ticker": row[0],
        "hiring_score": float(row[1]) if row[1] else None,
        "patent_score": float(row[2]) if row[2] else None,
        "github_score": float(row[3]) if row[3] else None,
        "leadership_score": float(row[4]) if row[4] else None,
        "composite_score": float(row[5]) if row[5] else None,
        "last_updated": row[6]
    }


@router.get("/companies/{company_id}/signals/{category}")
async def get_company_signals_by_category(company_id: UUID, category: str, conn = Depends(get_db)):
    """Get detailed signal data with FULL metadata parsing for dashboard."""
    cursor = conn.cursor()
    
    # Map category to summary table columns
    column_map = {
        "hiring_signal": ("hiring_score", "hiring_metadata", "hiring_collected_at"),
        "patent": ("patent_score", "patent_metadata", "patent_collected_at"),
        "github": ("github_score", "github_metadata", "github_collected_at")
    }
    
    if category not in column_map:
        raise HTTPException(status_code=400, detail="Invalid category")
    
    score_col, meta_col, time_col = column_map[category]
    
    # Get from summary table
    cursor.execute(f"""
        SELECT {score_col}, {meta_col}, {time_col}
        FROM company_evidence_summary
        WHERE company_id = %(id)s
    """, {'id': str(company_id)})
    
    row = cursor.fetchone()
    
    if not row or row[0] is None:
        raise HTTPException(status_code=404, detail=f"No {category} data found")
    
    score = float(row[0])
    metadata = row[1] if isinstance(row[1], dict) else json.loads(row[1]) if row[1] else {}
    collected_at = row[2]
    
    # Get confidence from latest external signal
    cursor.execute("""
        SELECT confidence
        FROM external_signals
        WHERE company_id = %(id)s AND category = %(cat)s
        ORDER BY collected_at DESC
        LIMIT 1
    """, {'id': str(company_id), 'cat': category})
    
    conf_row = cursor.fetchone()
    confidence = float(conf_row[0]) if conf_row else 0.8
    
    # Parse metadata based on category
    if category == "hiring_signal":
        seniority = metadata.get('seniority', {})
        ratios = metadata.get('ratios', {})
        
        return {
            "category": category,
            "score": score,
            "confidence": confidence,
            "collected_at": collected_at,
            "summary": {
                "total_jobs": metadata.get('total_jobs', 0),
                "ai_jobs": metadata.get('ai_related_count', 0),
                "ai_ratio": metadata.get('ai_ratio', 0.0)
            },
            "seniority_breakdown": {
                "leadership": seniority.get('leadership', 0),
                "senior": seniority.get('senior', 0),
                "mid": seniority.get('mid', 0),
                "entry": seniority.get('entry', 0)
            },
            "ratios": ratios,
            "jobs": [],  # ADD THIS - empty list since jobs aren't in summary metadata
            "metadata": metadata
        }
    
    elif category == "patent":
        by_year = metadata.get('by_year', {})
        top_patents = metadata.get('top_patents', [])
        
        return {
            "category": category,
            "score": score,
            "confidence": confidence,
            "collected_at": collected_at,
            "summary": {
                "total_patents": metadata.get('total_patents', 0),
                "ai_patents": metadata.get('ai_patents', 0),
                "recent_ai_count": metadata.get('recent_ai_count', 0),
                "ai_ratio": metadata.get('ai_ratio', 0.0)
            },
            "by_year": by_year,
            "patents": top_patents,  
            "metadata": metadata
        }
    
    elif category == "github":
        top_repos = metadata.get('top_repos', [])
        orgs = metadata.get('orgs', [])
        
        return {
            "category": category,
            "score": score,
            "confidence": confidence,
            "collected_at": collected_at,
            "summary": {
                "total_repos": metadata.get('total_repos', 0),
                "ai_repos": metadata.get('ai_repos', 0),
                "total_stars": metadata.get('ai_stars', 0),
                "organizations": len(orgs)
            },
            "top_repos": top_repos,
            "organizations": orgs,
            "metadata": metadata
        }
    
    return {
        "category": category,
        "score": score,
        "confidence": confidence,
        "collected_at": collected_at,
        "metadata": metadata
    }
