"""
Enumerations for PE Org-AI-R Platform
"""
from enum import Enum


class AssessmentType(str, Enum):
    """Types of AI-readiness assessments conducted by PE firms."""
    
    SCREENING = "screening"              # Quick external assessment
    DUE_DILIGENCE = "due_diligence"      # Deep dive with internal access
    QUARTERLY = "quarterly"              # Regular portfolio monitoring
    EXIT_PREP = "exit_prep"              # Pre-exit assessment


class AssessmentStatus(str, Enum):
    """Assessment lifecycle states with defined progression."""
    
    DRAFT = "draft"                      # Initial creation
    IN_PROGRESS = "in_progress"          # Active assessment
    SUBMITTED = "submitted"              # Awaiting approval
    APPROVED = "approved"                # Finalized
    SUPERSEDED = "superseded"            # Replaced by newer assessment


class Dimension(str, Enum):
    """Seven dimensions of AI-readiness in the PE Org-AI-R framework.
    
    Each dimension has an associated default weight for VR score calculation:
    - DATA_INFRASTRUCTURE: 0.25
    - AI_GOVERNANCE: 0.20
    - TECHNOLOGY_STACK: 0.15
    - TALENT_SKILLS: 0.15
    - LEADERSHIP_VISION: 0.10
    - USE_CASE_PORTFOLIO: 0.10
    - CULTURE_CHANGE: 0.05
    """
    
    DATA_INFRASTRUCTURE = "data_infrastructure"
    AI_GOVERNANCE = "ai_governance"
    TECHNOLOGY_STACK = "technology_stack"
    TALENT_SKILLS = "talent_skills"
    LEADERSHIP_VISION = "leadership_vision"
    USE_CASE_PORTFOLIO = "use_case_portfolio"
    CULTURE_CHANGE = "culture_change"


# Default weights for dimension scoring (used in dimension.py)
DIMENSION_WEIGHTS = {
    Dimension.DATA_INFRASTRUCTURE: 0.25,
    Dimension.AI_GOVERNANCE: 0.20,
    Dimension.TECHNOLOGY_STACK: 0.15,
    Dimension.TALENT_SKILLS: 0.15,
    Dimension.LEADERSHIP_VISION: 0.10,
    Dimension.USE_CASE_PORTFOLIO: 0.10,
    Dimension.CULTURE_CHANGE: 0.05,
}


class SignalCategory(str, Enum):
    """Signal category types - match what pipeline uses."""
    HIRING = "hiring_signal"  
    PATENT = "patent"
    GITHUB = "github"
    LEADERSHIP = "leadership"