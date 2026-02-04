"""Patent configuration loader."""
import yaml
from pathlib import Path
from typing import List, Set

_config = None

def _load_config() -> dict:
    global _config
    if _config is None:
        config_path = Path(__file__).parent.parent.parent / "config" / "patent_config.yml"
        with open(config_path, 'r') as f:
            _config = yaml.safe_load(f)
    return _config

def get_ai_references() -> List[str]:
    return _load_config()['detection']['ai_references']

def get_ai_cpc_codes() -> Set[str]:
    return set(_load_config()['detection']['ai_cpc_codes'])

def get_similarity_threshold() -> float:
    return _load_config()['detection']['min_similarity']

def get_scoring_config() -> dict:
    return _load_config()['scoring']