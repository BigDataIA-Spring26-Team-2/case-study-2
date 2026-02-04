"""GitHub scanner with TRUE diversity scoring."""
import asyncio
import httpx
import math
import base64
from datetime import datetime
from uuid import uuid4
from sentence_transformers import SentenceTransformer, util
from app.core.github_config import (
    get_github_orgs, get_ai_topics, get_ai_languages, 
    get_ai_references, get_similarity_threshold, get_scoring_config
)

_model = None
_ref_emb = None

def _load_model():
    global _model, _ref_emb
    if _model is None:
        _model = SentenceTransformer('all-mpnet-base-v2')
        _ref_emb = _model.encode(get_ai_references(), convert_to_tensor=True)
    return _model, _ref_emb


async def scan_company(ticker: str, company_id: str, token: str = None) -> dict:
    """Scan GitHub for one company."""
    orgs = get_github_orgs(ticker)
    if not orgs:
        return _empty(company_id)
    
    print(f"  Orgs: {', '.join(orgs)}")
    
    # Setup
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    
    # Fetch repos
    all_repos = []
    async with httpx.AsyncClient(timeout=30, headers=headers) as client:
        for org in orgs:
            print(f"  Fetching repos from {org}...", end=" ")
            for page in range(1, 4):
                try:
                    r = await client.get(
                        f"https://api.github.com/orgs/{org}/repos",
                        params={"per_page": 100, "page": page}
                    )
                    if r.status_code != 200:
                        print(f"Error {r.status_code}: {r.text[:100]}")
                        break
                    data = r.json()
                    if not data:
                        break
                    all_repos.extend(data)
                except:
                    break
            print(f"{len(all_repos)} total")
            await asyncio.sleep(0.5)
        
        # Fetch READMEs for repos without topics
        print(f"\n  Fetching READMEs for repos without topics...")
        readme_fetched = 0
        for r in all_repos:
            if not r.get('topics') and not r.get('fork'):
                readme = await _fetch_readme(client, r['full_name'])
                if readme:
                    r['readme_text'] = readme
                    readme_fetched += 1
                await asyncio.sleep(0.3)
        print(f"  → Fetched {readme_fetched} READMEs")
    
    # Classify
    ai_topics = get_ai_topics()
    ai_langs = get_ai_languages()
    threshold = get_similarity_threshold()
    scoring = get_scoring_config()
    
    ai_repos = []
    all_matched_topics = set()  # Track UNIQUE topics across all repos
    
    print(f"\n  Classifying {len(all_repos)} repos...")
    
    for r in all_repos:
        if r.get('fork'):
            continue
        
        score = 0
        why = []
        matched_topics = set()
        
        # Topics
        topics = set(r.get('topics', []))
        matches = topics & ai_topics
        if matches:
            score += scoring['topic_match_weight']
            matched_topics.update(matches)
            why.append(f"topics:{list(matches)[0]}")
        
        # Language
        if r.get('language') in ai_langs:
            score += scoring['language_weight']
            why.append(f"lang:{r['language']}")
        
        # Description semantic
        if score < scoring['min_score_threshold']:
            if r.get('description'):
                sim = _semantic_score(r['description'])
                if sim > threshold:
                    score += scoring['semantic_weight']
                    why.append(f"desc:{sim:.2f}")
            
            # README semantic
            if score < scoring['min_score_threshold'] and r.get('readme_text'):
                sim = _semantic_score(r['readme_text'][:2000])
                if sim > threshold:
                    score += scoring['semantic_weight']
                    why.append(f"readme:{sim:.2f}")
        
        if score >= scoring['min_score_threshold']:
            ai_repos.append({
                'name': r['name'],
                'stars': r.get('stargazers_count', 0),
                'score': score,
                'why': why,
                'topics': list(matched_topics)
            })
            all_matched_topics.update(matched_topics)  # Aggregate unique topics
            print(f"    ✓ {r['name'][:35]:35} {score:2.0f}pts ({', '.join(why)})")
    
    # Calculate scores
    total = len([r for r in all_repos if not r.get('fork')])
    ai_count = len(ai_repos)
    stars = sum(r['stars'] for r in ai_repos)
    
    if ai_count == 0:
        print(f"  → No AI repos found")
        return _empty(company_id)
    
    # Component 1: Ratio (0-40 points)
    ratio = ai_count / max(total, 1)
    ratio_score = min(40, ratio * 100)
    
    # Component 2: Stars (0-30 points, logarithmic)
    star_score = min(30, 30 * math.log1p(stars) / math.log1p(50000))
    
    # Component 3: DIVERSITY (0-30 points)
    # How many UNIQUE AI topics are covered across all repos
    total_possible_topics = len(ai_topics)
    unique_topics_count = len(all_matched_topics)
    diversity_score = min(30, 30 * math.log1p(unique_topics_count) / math.log1p(20))    
    final = ratio_score + star_score + diversity_score
    
    print(f"\n  Scoring:")
    print(f"    Ratio:     {ai_count}/{total} ({ratio*100:.1f}%) → {ratio_score:.1f} pts")
    print(f"    Stars:     {stars:,} (log scale) → {star_score:.1f} pts")
    print(f"    Diversity: {unique_topics_count}/{total_possible_topics} unique topics → {diversity_score:.1f} pts")
    print(f"    Topics covered: {sorted(list(all_matched_topics))[:5]}...")
    print(f"    TOTAL:     {final:.1f}/100")
    
    return {
        "id": str(uuid4()),
        "company_id": company_id,
        "category": "github",
        "source": "github_api",
        "score": round(final, 1),
        "confidence": round(min(0.9, 0.5 + total/100*0.3 + ai_count/20*0.2), 2),
        "metadata": {
            "orgs": orgs,
            "total_repos": total,
            "ai_repos": ai_count,
            "ai_stars": stars,
            "unique_ai_topics": list(all_matched_topics),  # NEW
            "topic_coverage": f"{unique_topics_count}/{total_possible_topics}",  # NEW
            "top_repos": sorted(ai_repos, key=lambda x: -x['stars'])[:5]
        },
        "s3_full_data_key": None,
        "collected_at": datetime.now()
    }


async def _fetch_readme(client, repo_full_name):
    """Fetch README.md content."""
    try:
        r = await client.get(f"https://api.github.com/repos/{repo_full_name}/readme")
        if r.status_code == 200:
            content = r.json().get('content', '')
            return base64.b64decode(content).decode('utf-8', errors='ignore')
    except:
        pass
    return None


def _semantic_score(text):
    """Semantic similarity score."""
    if len(text) < 20:
        return 0
    model, ref_emb = _load_model()
    emb = model.encode(text[:500], convert_to_tensor=True)
    return float(util.cos_sim(emb, ref_emb)[0].max())


def _empty(company_id):
    """Empty signal when no orgs."""
    return {
        "id": str(uuid4()),
        "company_id": company_id,
        "category": "github",
        "source": "github_api",
        "score": 0.0,
        "confidence": 0.0,
        "metadata": {
            "orgs": [],
            "total_repos": 0,
            "ai_repos": 0,
            "ai_stars": 0,
            "unique_ai_topics": [],
            "topic_coverage": "0/46",
            "top_repos": []
        },
        "s3_full_data_key": None,
        "collected_at": datetime.now()
    }