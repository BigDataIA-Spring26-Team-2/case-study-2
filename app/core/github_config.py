"""GitHub configuration loader - NO hardcoding."""
import yaml
from pathlib import Path
from typing import List, Set

_config = None

def _load_config() -> dict:
    """Load GitHub config from YAML."""
    global _config
    if _config is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "github_orgs.yml"
        with open(config_path, 'r') as f:
            _config = yaml.safe_load(f)
    return _config


def get_github_orgs(ticker: str) -> List[str]:
    """Get GitHub orgs for company."""
    config = _load_config()
    return config['github_orgs'].get(ticker, [])


def get_ai_topics() -> Set[str]:
    """Get AI topic tags from config."""
    config = _load_config()
    return set(config['detection']['ai_topics'])


def get_ml_libraries() -> Set[str]:
    """Get ML library names from config."""
    config = _load_config()
    return set(config['detection']['ml_libraries'])


def get_ai_languages() -> Set[str]:
    """Get AI-related programming languages."""
    config = _load_config()
    return set(config['detection']['ai_languages'])


def get_ai_references() -> List[str]:
    """Get semantic reference phrases."""
    config = _load_config()
    return config['detection']['ai_references']


def get_similarity_threshold() -> float:
    """Get semantic similarity threshold."""
    config = _load_config()
    return config['detection']['min_similarity']


def get_scoring_config() -> dict:
    """Get scoring weights and thresholds."""
    config = _load_config()
    return config['scoring']