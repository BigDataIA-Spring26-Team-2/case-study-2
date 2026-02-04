"""Patent scanner with transformer classification."""
import asyncio
import httpx
import math
import json
from datetime import datetime
from uuid import uuid4
from collections import defaultdict
from sentence_transformers import SentenceTransformer, util

from app.core.patent_config import (
    get_ai_references, get_ai_cpc_codes,
    get_similarity_threshold, get_scoring_config
)

_model = None
_ref_emb = None

def _load_model():
    global _model, _ref_emb
    if _model is None:
        _model = SentenceTransformer('all-mpnet-base-v2')
        _ref_emb = _model.encode(get_ai_references(), convert_to_tensor=True)
    return _model, _ref_emb


class PatentScanner:
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://search.patentsview.org/api/v1"
        self.headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}
        self.threshold = get_similarity_threshold()
        self.scoring = get_scoring_config()
        self.ai_cpc = get_ai_cpc_codes()
    
    async def scan_company(self, company_name: str, company_id: str, ticker: str, year_from: int) -> dict:
        print(f"  Fetching patents for {company_name} (granted >= {year_from})...")
        
        patents = await self._fetch_patents([company_name], year_from)

        if not patents:
            print(f"  → No patents found")
            return self._empty_signal(company_id)
        
        print(f"  → {len(patents)} patents fetched")
        print(f"\n  Classifying with transformer...")
        
        ai_patents = self._classify_patents(patents)
        
        print(f"  → {len(ai_patents)} AI patents ({len(ai_patents)/len(patents)*100:.1f}%)")
        
        if not ai_patents:
            return self._empty_signal(company_id)
        
        print(f"\n  Top AI patents:")
        for i, p in enumerate(sorted(ai_patents, key=lambda x: -x['ai_score'])[:3], 1):
            cpc = " [CPC]" if p.get('has_ai_cpc') else ""
            print(f"    {i}. {p['title'][:45]:45} {p['ai_score']:.2f}{cpc}")
        
        by_year = self._calculate_by_year(patents, ai_patents)
        score = self._calculate_score(len(patents), ai_patents, by_year)
        
        current_year = datetime.now().year
        recent_years = list(range(current_year - self.scoring['recency_years'], current_year + 1))
        recent_ai = sum(by_year.get(str(y), {}).get('ai', 0) for y in recent_years)
        
        return {
            "id": str(uuid4()),
            "company_id": company_id,
            "category": "patent",
            "source": "uspto",
            "score": score,
            "confidence": self._calculate_confidence(len(patents), len(ai_patents)),
            "metadata": {
                "total_patents": len(patents),
                "ai_patents": len(ai_patents),
                "ai_ratio": round(len(ai_patents) / len(patents), 3),
                "by_year": by_year,
                "recent_ai_count": recent_ai,
                "top_patents": [
                    {
                        "id": p['patent_id'],
                        "title": p['title'],
                        "grant_date": p['grant_date'],
                        "filing_date": p['filing_date'],
                        "score": round(p['ai_score'], 3),
                        "has_ai_cpc": p.get('has_ai_cpc', False)
                    }
                    for p in sorted(ai_patents, key=lambda x: -x['ai_score'])[:10]
                ]
            },
            "s3_full_data_key": f"patents/{ticker}_ai_patents_{datetime.now():%Y%m%d}.json",
            "collected_at": datetime.now()
        }
    
    async def _fetch_patents(self, patent_names: list, year_from: int) -> list:
        if not patent_names:
            return []
        
        all_patents = []
        
        for name in patent_names:
            query = {"_and": [{"_begins": {"assignees.assignee_organization": name}},{"_gte": {"patent_date": f"{year_from}-01-01"}}]}
            fields = ["patent_id", "patent_title", "patent_date", "application.filing_date", "patent_abstract", "cpc_current"]
            
            after = None
            async with httpx.AsyncClient(timeout=60.0, headers=self.headers) as client:
                while len(all_patents) < 10000:
                    body = {"q": query, "f": fields, "o": {"size": 1000}}
                    if after:
                        body["o"]["after"] = after
                    
                    print(f"    {len(all_patents)} fetched...", end="\r", flush=True)
                    
                    r = await client.post(f"{self.base_url}/patent/", json=body)
                    if r.status_code != 200:
                        break
                    
                    data = r.json()
                    batch = data.get("patents", [])
                    if not batch:
                        break
                    
                    all_patents.extend(batch)
                    if len(batch) < 1000:
                        break
                    
                    after = batch[-1]["patent_id"]
                    await asyncio.sleep(1.5)
        
        print(f"\n    Total: {len(all_patents)} patents")
        return all_patents
    
    def _classify_patents(self, patents: list) -> list:
        """Classify in parallel batches (this is the slow part)."""
        from concurrent.futures import ThreadPoolExecutor
        import numpy as np
        
        model, ref_emb = _load_model()
        ai_patents = []
        
        batch_size = 100
        batches = [patents[i:i + batch_size] for i in range(0, len(patents), batch_size)]
        
        def classify_batch(batch):
            """Classify one batch."""
            texts = [f"{p.get('patent_title', '')}. {p.get('patent_abstract', '')}" for p in batch]
            text_embeddings = model.encode(texts, convert_to_tensor=True, show_progress_bar=False)
            similarities = util.cos_sim(text_embeddings, ref_emb)
            max_sims = similarities.max(dim=1).values
            
            batch_ai = []
            for j, sim in enumerate(max_sims):
                patent = batch[j]
                
                has_ai_cpc = False
                if patent.get('cpc_current'):
                    cpcs = patent['cpc_current']
                    if isinstance(cpcs, list):
                        patent_cpcs = set(c.get('cpc_section_id', '')[:4] for c in cpcs if isinstance(c, dict))
                        has_ai_cpc = bool(patent_cpcs & self.ai_cpc)
                
                is_ai = sim >= self.threshold or has_ai_cpc
                
                if is_ai:
                    grant_date = patent.get('patent_date', '')
                    filing_date = None
                    
                    if patent.get('application'):
                        app = patent['application']
                        if isinstance(app, list) and app:
                            filing_date = app[0].get('filing_date')
                        elif isinstance(app, dict):
                            filing_date = app.get('filing_date')
                    
                    batch_ai.append({
                        'patent_id': patent.get('patent_id', ''),
                        'title': patent.get('patent_title', ''),
                        'grant_date': grant_date,
                        'filing_date': filing_date or grant_date,
                        'ai_score': float(sim),
                        'has_ai_cpc': has_ai_cpc,
                        'abstract': patent.get('patent_abstract', '')
                    })
            
            return batch_ai
        
        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            results = list(executor.map(classify_batch, batches))
        
        # Flatten results
        for batch_result in results:
            ai_patents.extend(batch_result)
        
        return ai_patents
    
    def _calculate_by_year(self, all_patents: list, ai_patents: list) -> dict:
        """Calculate year-by-year breakdown."""
        year_total = defaultdict(int)
        year_ai = defaultdict(int)
        
        for p in all_patents:
            year = p.get('patent_date', '')[:4]
            if year:
                year_total[year] += 1
        
        for p in ai_patents:
            year = p.get('grant_date', '')[:4]
            if year:
                year_ai[year] += 1
        
        by_year = {}
        for year in sorted(year_total.keys()):
            total = year_total[year]
            ai = year_ai.get(year, 0)
            by_year[year] = {
                "total": total,
                "ai": ai,
                "ratio": round(ai / total, 3) if total > 0 else 0.0
            }
        
        return by_year
    
    def _calculate_score(self, total_patents: int, ai_patents: list, by_year: dict) -> float:
        """Calculate composite patent score."""
        ai_count = len(ai_patents)
        if ai_count == 0:
            return 0.0
        
        max_vol = self.scoring['max_volume_patents']
        volume_score = min(40, 40 * math.log1p(ai_count) / math.log1p(max_vol))
        
        current_year = datetime.now().year
        recent_years = list(range(current_year - self.scoring['recency_years'], current_year + 1))
        recent_count = sum(by_year.get(str(y), {}).get('ai', 0) for y in recent_years)
        recent_ratio = recent_count / ai_count if ai_count > 0 else 0
        recency_score = min(30, (recent_ratio / self.scoring['max_recency_ratio']) * 30)
        
        intensity = ai_count / total_patents if total_patents > 0 else 0
        intensity_score = min(30, (intensity / self.scoring['max_intensity_ratio']) * 30)
        
        final = volume_score + recency_score + intensity_score
        
        print(f"\n  Scoring:")
        print(f"    Volume:    {ai_count} AI patents → {volume_score:.1f} pts")
        print(f"    Recency:   {recent_count}/{ai_count} recent → {recency_score:.1f} pts")
        print(f"    Intensity: {ai_count}/{total_patents} ({intensity*100:.1f}%) → {intensity_score:.1f} pts")
        print(f"    TOTAL:     {final:.1f}/100")
        
        return round(final, 1)
    
    def _calculate_confidence(self, total: int, ai_count: int) -> float:
        """Calculate confidence based on sample size."""
        sample_conf = min(0.4, total / 500 * 0.4)
        ai_conf = min(0.5, ai_count / 50 * 0.5)
        return round(0.1 + sample_conf + ai_conf, 2)
    
    def _empty_signal(self, company_id: str) -> dict:
        """Empty signal when no patents."""
        return {
            "id": str(uuid4()),
            "company_id": company_id,
            "category": "patent",
            "source": "uspto",
            "score": 0.0,
            "confidence": 0.0,
            "metadata": {
                "total_patents": 0,
                "ai_patents": 0,
                "ai_ratio": 0.0,
                "by_year": {},
                "recent_ai_count": 0,
                "top_patents": []
            },
            "s3_full_data_key": None,
            "collected_at": datetime.now()
        }


async def scan_company(company_name: str, company_id: str, ticker: str, api_key: str, year_from: int) -> dict:
    """Entry point for patent scanning."""
    scanner = PatentScanner(api_key)
    return await scanner.scan_company(company_name, company_id, ticker, year_from)