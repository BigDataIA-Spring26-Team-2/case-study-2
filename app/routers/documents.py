"""Document endpoints - ENHANCED with section data and content viewing."""
from fastapi import APIRouter, Depends, HTTPException
from uuid import UUID
from typing import Optional
from app.services.redis_cache import redis_service
import hashlib
import json

from app.database import get_db

router = APIRouter(prefix="/api/v1/documents", tags=["documents"])


@router.get("")
async def list_documents(
    company_id: Optional[UUID] = None,
    skip: int = 0,
    limit: int = 50,
    conn = Depends(get_db)
):
    """List documents with section metadata."""
    # Cache check
    cache_key = f"documents:list:{company_id}:{skip}:{limit}"
    cached = await redis_service.get(cache_key)
    if cached:
        return cached
    
    cursor = conn.cursor()
    
    query = """
        SELECT id, company_id, ticker, filing_type, filing_date, 
               status, total_words, total_chunks, section_count, sections_summary
        FROM evidence_documents
    """
    params = {}
    
    if company_id:
        query += " WHERE company_id = %(id)s"
        params['id'] = str(company_id)
    
    query += f" ORDER BY filing_date DESC LIMIT {limit} OFFSET {skip}"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    result= {
        "documents": [
            {
                "id": r[0],
                "company_id": r[1],
                "ticker": r[2],
                "filing_type": r[3],
                "filing_date": r[4],
                "status": r[5],
                "word_count": r[6],
                "total_chunks": r[7],
                "section_count": r[8],
                "sections_summary": r[9]
            }
            for r in rows
        ],
        "total": len(rows),
        "skip": skip,
        "limit": limit
    }
    await redis_service.set(cache_key, result, ttl=300)
    return result


@router.get("/{document_id}")
async def get_document(document_id: UUID, conn = Depends(get_db)):
    """Get single document with full metadata."""
    # Cache check
    cache_key = f"document:{document_id}"
    cached = await redis_service.get(cache_key)
    if cached:
        return cached
    
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, company_id, ticker, filing_type, filing_date, 
               status, total_words, total_chunks, s3_key, 
               section_count, sections_summary
        FROM evidence_documents
        WHERE id = %(id)s
    """, {'id': str(document_id)})
    
    row = cursor.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    
    result= {
        "id": row[0],
        "company_id": row[1],
        "ticker": row[2],
        "filing_type": row[3],
        "filing_date": row[4],
        "status": row[5],
        "word_count": row[6],
        "total_chunks": row[7],
        "s3_key": row[8],
        "section_count": row[9],
        "sections_summary": row[10]
    }
    await redis_service.set(cache_key, result, ttl=300)
    return result




@router.get("/{document_id}/chunks")
async def get_document_chunks(
    document_id: UUID, 
    section_id: Optional[str] = None,
    limit: int = 100, 
    conn = Depends(get_db)
):
    """Get chunks for a document, optionally filtered by section.
    
    Args:
        document_id: Document UUID
        section_id: Optional section filter (e.g., 'item_1a', 'item_7')
        limit: Max chunks to return
    """

    # Cache check
    cache_key = f"chunks:{document_id}:{section_id}:{limit}"
    cached = await redis_service.get(cache_key)
    if cached:
        return cached

    cursor = conn.cursor()
    
    query = """
        SELECT id, chunk_index, section_id, section_title, word_count, content
        FROM document_chunks
        WHERE document_id = %(id)s
    """
    params = {'id': str(document_id)}
    
    if section_id:
        query += " AND section_id = %(section)s"
        params['section'] = section_id
    
    query += f" ORDER BY chunk_index LIMIT {limit}"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    result= {
        "document_id": str(document_id),
        "section_id": section_id,
        "chunks": [
            {
                "id": r[0],
                "chunk_index": r[1],
                "section_id": r[2],
                "section_title": r[3],
                "word_count": r[4],
                "content": r[5]  # ADDED: actual parsed text
            }
            for r in rows
        ],
        "total_chunks": len(rows)
    }
    await redis_service.set(cache_key, result, ttl=300)
    return result




@router.get("/{document_id}/sections")
async def get_document_sections(document_id: UUID, conn = Depends(get_db)):
    """Get list of all sections in a document with summary stats."""
    # Cache check
    cache_key = f"sections:{document_id}"
    cached = await redis_service.get(cache_key)
    if cached:
        return cached
    
    import json

    cursor = conn.cursor()
    
    # Get document metadata
    cursor.execute("""
        SELECT ticker, filing_type, sections_summary
        FROM evidence_documents
        WHERE id = %(id)s
    """, {'id': str(document_id)})
    
    doc = cursor.fetchone()
    
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    
    sections_summary_raw = doc[2]
    if isinstance(sections_summary_raw, str):
        sections_summary = json.loads(sections_summary_raw)
    elif isinstance(sections_summary_raw, dict):
        sections_summary = sections_summary_raw
    else:
        sections_summary = {}
    
    # Get actual sections from chunks
    cursor.execute("""
    SELECT section_id, section_title, MIN(chunk_index) as first_chunk
    FROM document_chunks
    WHERE document_id = %(id)s
    GROUP BY section_id, section_title
    ORDER BY first_chunk
""", {'id': str(document_id)})
    
    sections = []
    for row in cursor.fetchall():
        section_id = row[0]
        section_title = row[1]
        
        # Get stats from summary if available
        section_stats = sections_summary.get(section_id, {})
        
        sections.append({
            "section_id": section_id,
            "section_title": section_title,
            "chunk_count": section_stats.get('chunk_count', 0),
            "word_count": section_stats.get('total_words', 0)
        })
    
    result= {
        "document_id": str(document_id),
        "ticker": doc[0],
        "filing_type": doc[1],
        "sections": sections
    }
    await redis_service.set(cache_key, result, ttl=300)
    return result


