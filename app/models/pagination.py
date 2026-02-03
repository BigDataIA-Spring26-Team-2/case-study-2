"""
Pagination models for list endpoints.
"""
from pydantic import BaseModel, Field
from typing import Generic, TypeVar, List

T = TypeVar('T')


class PaginatedResponse(BaseModel, Generic[T]):
    """Generic paginated response wrapper."""
    
    items: List[T]
    total: int = Field(..., ge=0)
    page: int = Field(..., ge=1)
    page_size: int = Field(..., ge=1, le=100)
    total_pages: int = Field(..., ge=0)
    
    @staticmethod
    def create(items: List[T], total: int, page: int, page_size: int) -> 'PaginatedResponse[T]':
        """Factory method to create paginated response with automatic total_pages calculation."""
        total_pages = (total + page_size - 1) // page_size if page_size > 0 else 0
        return PaginatedResponse(
            items=items,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )