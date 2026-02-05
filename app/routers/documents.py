"""Document endpoints."""
from fastapi import APIRouter, Depends, HTTPException
from uuid import UUID
from typing import Optional

from app.database import get_db

router = APIRouter(prefix="/api/v1/documents", tags=["documents"])


@router.get("")  # Changed from "/" to ""
async def list_documents(
    company_id: Optional[UUID] = None,
    skip: int = 0,
    limit: int = 50,
    conn = Depends(get_db)
):
    """List documents."""
    cursor = conn.cursor()
    
    query = """
        SELECT id, company_id, ticker, filing_type, filing_date, 
               status, total_words, total_chunks
        FROM evidence_documents
    """
    params = {}
    
    if company_id:
        query += " WHERE company_id = %(id)s"
        params['id'] = str(company_id)
    
    query += f" ORDER BY filing_date DESC LIMIT {limit} OFFSET {skip}"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    return {
        "documents": [
            {
                "id": r[0],
                "company_id": r[1],
                "ticker": r[2],
                "filing_type": r[3],
                "filing_date": r[4],
                "status": r[5],
                "word_count": r[6],
                "total_chunks": r[7]
            }
            for r in rows
        ],
        "total": len(rows),
        "skip": skip,
        "limit": limit
    }


@router.get("/{document_id}")
async def get_document(document_id: UUID, conn = Depends(get_db)):
    """Get single document."""
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, company_id, ticker, filing_type, filing_date, 
               status, total_words, total_chunks, s3_key
        FROM evidence_documents
        WHERE id = %(id)s
    """, {'id': str(document_id)})
    
    row = cursor.fetchone()
    
    if not row:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return {
        "id": row[0],
        "company_id": row[1],
        "ticker": row[2],
        "filing_type": row[3],
        "filing_date": row[4],
        "status": row[5],
        "word_count": row[6],
        "total_chunks": row[7],
        "s3_key": row[8]
    }


@router.get("/{document_id}/chunks")
async def get_document_chunks(document_id: UUID, limit: int = 100, conn = Depends(get_db)):
    """Get chunks for a document."""
    cursor = conn.cursor()
    
    cursor.execute(f"""
        SELECT id, chunk_index, section_id, section_title, word_count
        FROM document_chunks
        WHERE document_id = %(id)s
        ORDER BY chunk_index
        LIMIT {limit}
    """, {'id': str(document_id)})
    
    rows = cursor.fetchall()
    
    return {
        "document_id": str(document_id),
        "chunks": [
            {
                "id": r[0],
                "chunk_index": r[1],
                "section_id": r[2],
                "section_title": r[3],
                "word_count": r[4]
            }
            for r in rows
        ],
        "total_chunks": len(rows)
    }