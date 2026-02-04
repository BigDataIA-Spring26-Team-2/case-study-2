"""Keyword configuration loader for job signal pipeline."""
import yaml
from pathlib import Path
from functools import lru_cache
from typing import Dict, List


@lru_cache()
def load_keywords() -> dict:
    """Load keywords config (cached for performance)."""
    config_path = Path(__file__).parent.parent.parent / "config" / "keywords.yml"
    
    if not config_path.exists():
        raise FileNotFoundError(f"Keywords config not found: {config_path}")
    
    with open(config_path) as f:
        return yaml.safe_load(f)


def _get_talent_skills() -> dict:
    """Get talent_skills dimension config."""
    return load_keywords()['dimensions']['talent_skills']


# ============================================================
# Job Signal Pipeline Functions (all actually used)
# ============================================================

def get_all_job_titles() -> List[str]:
    """Get all AI job titles for scraping."""
    return _get_talent_skills().get('job_titles', [])


def get_ai_references() -> List[str]:
    """Get AI reference descriptions for semantic similarity."""
    return _get_talent_skills().get('ai_references', [])


def get_similarity_threshold() -> float:
    """Get threshold for classifying a job as AI-related."""
    return _get_talent_skills().get('ai_similarity_threshold', 0.40)


def get_acronyms() -> Dict[str, str]:
    """Get acronym expansion map for deduplication."""
    return _get_talent_skills().get('acronyms', {})


def get_seniority_keywords() -> Dict[str, List[str]]:
    """Get seniority level keywords for job classification."""
    return _get_talent_skills().get('seniority_keywords', {})