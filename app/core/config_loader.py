"""Configuration loaders for companies and settings."""
import yaml
from pathlib import Path
from functools import lru_cache


@lru_cache()
def load_companies_config() -> dict:
    config_path = Path(__file__).parent.parent.parent / "config" / "companies.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_target_companies() -> dict:
    config = load_companies_config()
    return config['target_companies']


def get_industries() -> dict:
    config = load_companies_config()
    return config['industries']


def get_filing_config() -> dict:
    config = load_companies_config()
    return config['filing_config']

def get_patent_search_term(ticker: str) -> str:
    """Get patent search term for company."""
    companies = get_target_companies()
    return companies[ticker].get('patent_search', companies[ticker]['name'])