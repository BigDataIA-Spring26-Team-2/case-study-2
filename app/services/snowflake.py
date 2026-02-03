"""Snowflake database service."""
from typing import Optional, List, Any, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timezone
from contextlib import asynccontextmanager
import snowflake.connector
from snowflake.connector import DictCursor
import structlog

from app.models import (
    CompanyCreate, CompanyUpdate, CompanyResponse,
    IndustryResponse,
    AssessmentCreate, AssessmentResponse, AssessmentStatus,
    DimensionScoreCreate, DimensionScoreUpdate, DimensionScoreResponse
)

logger = structlog.get_logger()


class SnowflakeService:
    
    def __init__(self, account: str, user: str, password: str, 
                 warehouse: str, database: str, schema: str, role: Optional[str] = None):
        self.config = {k: v for k, v in locals().items() if k != 'self' and v is not None}
        self._conn = None
    
    def connect(self):
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(**self.config)
        return self._conn
    
    def close(self):
        if self._conn and not self._conn.is_closed():
            self._conn.close()
    
    @asynccontextmanager
    async def cursor(self):
        conn = self.connect()
        cur = conn.cursor(DictCursor)
        try:
            yield cur
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            cur.close()
    
    async def create_company(self, company: CompanyCreate) -> CompanyResponse:
        company_id = uuid4()
        now = datetime.now(timezone.utc)
        
        async with self.cursor() as cur:
            cur.execute("""
                INSERT INTO companies 
                (id, name, ticker, industry_id, position_factor, is_deleted, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, FALSE, %s, %s)
            """, (str(company_id), company.name, company.ticker, str(company.industry_id),
                  company.position_factor, now, now))
        
        return CompanyResponse(
            id=company_id, name=company.name, ticker=company.ticker,
            industry_id=company.industry_id, position_factor=company.position_factor,
            is_deleted=False, created_at=now, updated_at=now
        )
    
    async def get_company(self, company_id: UUID) -> Optional[CompanyResponse]:
        async with self.cursor() as cur:
            cur.execute("""
                SELECT id, name, ticker, industry_id, position_factor, 
                       is_deleted, created_at, updated_at
                FROM companies WHERE id = %s AND is_deleted = FALSE
            """, (str(company_id),))
            row = cur.fetchone()
        
        if not row:
            return None
        
        return CompanyResponse(
            id=UUID(row['ID']), name=row['NAME'], ticker=row['TICKER'],
            industry_id=UUID(row['INDUSTRY_ID']), position_factor=float(row['POSITION_FACTOR']),
            is_deleted=row['IS_DELETED'], created_at=row['CREATED_AT'], 
            updated_at=row['UPDATED_AT']
        )
    
    async def list_companies(
        self, skip: int = 0, limit: int = 100, industry_id: Optional[UUID] = None
    ) -> Tuple[List[CompanyResponse], int]:
        where = "WHERE is_deleted = FALSE"
        params: List[Any] = []
        
        if industry_id:
            where += " AND industry_id = %s"
            params.append(str(industry_id))
        
        async with self.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as total FROM companies {where}", params)
            total = cur.fetchone()['TOTAL']
            
            cur.execute(f"""
                SELECT id, name, ticker, industry_id, position_factor,
                       is_deleted, created_at, updated_at
                FROM companies {where}
                ORDER BY created_at DESC LIMIT %s OFFSET %s
            """, params + [limit, skip])
            rows = cur.fetchall()
        
        companies = [
            CompanyResponse(
                id=UUID(r['ID']), name=r['NAME'], ticker=r['TICKER'],
                industry_id=UUID(r['INDUSTRY_ID']), position_factor=float(r['POSITION_FACTOR']),
                is_deleted=r['IS_DELETED'], created_at=r['CREATED_AT'], 
                updated_at=r['UPDATED_AT']
            ) for r in rows
        ]
        
        return companies, total
    
    async def update_company(
        self, company_id: UUID, company: CompanyUpdate
    ) -> Optional[CompanyResponse]:
        updates, params = [], []
        
        if company.name is not None:
            updates.append("name = %s")
            params.append(company.name)
        if company.ticker is not None:
            updates.append("ticker = %s")
            params.append(company.ticker)
        if company.industry_id is not None:
            updates.append("industry_id = %s")
            params.append(str(company.industry_id))
        if company.position_factor is not None:
            updates.append("position_factor = %s")
            params.append(company.position_factor)
        
        if not updates:
            return await self.get_company(company_id)
        
        now = datetime.now(timezone.utc)
        updates.append("updated_at = %s")
        params.extend([now, str(company_id)])
        
        async with self.cursor() as cur:
            cur.execute(f"""
                UPDATE companies SET {', '.join(updates)}
                WHERE id = %s AND is_deleted = FALSE
            """, params)
        
        return await self.get_company(company_id)
    
    async def delete_company(self, company_id: UUID) -> bool:
        async with self.cursor() as cur:
            cur.execute("""
                UPDATE companies SET is_deleted = TRUE, updated_at = %s
                WHERE id = %s AND is_deleted = FALSE
            """, (datetime.now(timezone.utc), str(company_id)))
            return cur.rowcount > 0
    
    async def list_industries(self) -> List[IndustryResponse]:
        async with self.cursor() as cur:
            cur.execute("SELECT id, name, sector, h_r_base, created_at FROM industries ORDER BY name")
            rows = cur.fetchall()
        
        return [
            IndustryResponse(
                id=UUID(r['ID']), name=r['NAME'], sector=r['SECTOR'],
                h_r_base=float(r['H_R_BASE']), created_at=r['CREATED_AT']
            ) for r in rows
        ]
    
    async def create_assessment(self, assessment: AssessmentCreate) -> AssessmentResponse:
        assessment_id = uuid4()
        now = datetime.now(timezone.utc)
        
        async with self.cursor() as cur:
            cur.execute("""
                INSERT INTO assessments 
                (id, company_id, assessment_type, assessment_date, status,
                 primary_assessor, secondary_assessor, created_at)
                VALUES (%s, %s, %s, %s, 'draft', %s, %s, %s)
            """, (str(assessment_id), str(assessment.company_id), assessment.assessment_type.value,
                  assessment.assessment_date, assessment.primary_assessor, 
                  assessment.secondary_assessor, now))
        
        return AssessmentResponse(
            id=assessment_id, company_id=assessment.company_id,
            assessment_type=assessment.assessment_type, assessment_date=assessment.assessment_date,
            status=AssessmentStatus.DRAFT, primary_assessor=assessment.primary_assessor,
            secondary_assessor=assessment.secondary_assessor, v_r_score=None,
            confidence_lower=None, confidence_upper=None, created_at=now
        )
    
    async def get_assessment(self, assessment_id: UUID) -> Optional[AssessmentResponse]:
        async with self.cursor() as cur:
            cur.execute("""
                SELECT id, company_id, assessment_type, assessment_date, status,
                       primary_assessor, secondary_assessor, v_r_score,
                       confidence_lower, confidence_upper, created_at
                FROM assessments WHERE id = %s
            """, (str(assessment_id),))
            row = cur.fetchone()
        
        if not row:
            return None
        
        return AssessmentResponse(
            id=UUID(row['ID']), company_id=UUID(row['COMPANY_ID']),
            assessment_type=row['ASSESSMENT_TYPE'], assessment_date=row['ASSESSMENT_DATE'],
            status=row['STATUS'], primary_assessor=row['PRIMARY_ASSESSOR'],
            secondary_assessor=row['SECONDARY_ASSESSOR'],
            v_r_score=float(row['V_R_SCORE']) if row['V_R_SCORE'] else None,
            confidence_lower=float(row['CONFIDENCE_LOWER']) if row['CONFIDENCE_LOWER'] else None,
            confidence_upper=float(row['CONFIDENCE_UPPER']) if row['CONFIDENCE_UPPER'] else None,
            created_at=row['CREATED_AT']
        )
    
    async def list_assessments(
        self, skip: int = 0, limit: int = 100, 
        company_id: Optional[UUID] = None, status: Optional[str] = None
    ) -> Tuple[List[AssessmentResponse], int]:
        where_clauses = []
        params: List[Any] = []
        
        if company_id:
            where_clauses.append("company_id = %s")
            params.append(str(company_id))
        if status:
            where_clauses.append("status = %s")
            params.append(status)
        
        where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        async with self.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as total FROM assessments {where}", params)
            total = cur.fetchone()['TOTAL']
            
            cur.execute(f"""
                SELECT id, company_id, assessment_type, assessment_date, status,
                       primary_assessor, secondary_assessor, v_r_score,
                       confidence_lower, confidence_upper, created_at
                FROM assessments {where}
                ORDER BY created_at DESC LIMIT %s OFFSET %s
            """, params + [limit, skip])
            rows = cur.fetchall()
        
        assessments = [
            AssessmentResponse(
                id=UUID(r['ID']), company_id=UUID(r['COMPANY_ID']),
                assessment_type=r['ASSESSMENT_TYPE'], assessment_date=r['ASSESSMENT_DATE'],
                status=r['STATUS'], primary_assessor=r['PRIMARY_ASSESSOR'],
                secondary_assessor=r['SECONDARY_ASSESSOR'],
                v_r_score=float(r['V_R_SCORE']) if r['V_R_SCORE'] else None,
                confidence_lower=float(r['CONFIDENCE_LOWER']) if r['CONFIDENCE_LOWER'] else None,
                confidence_upper=float(r['CONFIDENCE_UPPER']) if r['CONFIDENCE_UPPER'] else None,
                created_at=r['CREATED_AT']
            ) for r in rows
        ]
        
        return assessments, total
    
    async def validate_assessment_complete(self, assessment_id: UUID) -> Tuple[bool, List[str]]:
        from app.models.enums import Dimension
        
        errors = []
        scores = await self.get_dimension_scores(assessment_id)
        
        if len(scores) != 7:
            errors.append(f"Expected 7 dimensions, found {len(scores)}")
        
        dimensions_present = {s.dimension for s in scores}
        missing = set(Dimension) - dimensions_present
        if missing:
            errors.append(f"Missing: {[d.value for d in missing]}")
        
        total_weight = sum(s.weight for s in scores)
        if abs(total_weight - 1.0) > 0.001:
            errors.append(f"Weights sum to {total_weight:.3f}, expected 1.0")
        
        return (len(errors) == 0, errors)
    
    async def update_assessment_status(
        self, assessment_id: UUID, new_status: str
    ) -> Optional[AssessmentResponse]:
        status_enum = AssessmentStatus(new_status)
        
        if status_enum == AssessmentStatus.SUBMITTED:
            is_valid, errors = await self.validate_assessment_complete(assessment_id)
            if not is_valid:
                raise ValueError(f"Incomplete: {'; '.join(errors)}")
        
        async with self.cursor() as cur:
            cur.execute("""
                UPDATE assessments SET status = %s WHERE id = %s
            """, (new_status, str(assessment_id)))
            
            if cur.rowcount == 0:
                return None
        
        return await self.get_assessment(assessment_id)
    
    async def create_dimension_score(self, score: DimensionScoreCreate) -> DimensionScoreResponse:
        score_id = uuid4()
        now = datetime.now(timezone.utc)
        
        async with self.cursor() as cur:
            cur.execute("""
                INSERT INTO dimension_scores 
                (id, assessment_id, dimension, score, weight, confidence, evidence_count, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (str(score_id), str(score.assessment_id), score.dimension.value, score.score,
                  score.weight, score.confidence, score.evidence_count, now))
        
        return DimensionScoreResponse(
            id=score_id, assessment_id=score.assessment_id, dimension=score.dimension,
            score=score.score, weight=score.weight, confidence=score.confidence,
            evidence_count=score.evidence_count, created_at=now
        )
    
    async def get_dimension_scores(self, assessment_id: UUID) -> List[DimensionScoreResponse]:
        async with self.cursor() as cur:
            cur.execute("""
                SELECT id, assessment_id, dimension, score, weight, 
                       confidence, evidence_count, created_at
                FROM dimension_scores WHERE assessment_id = %s ORDER BY dimension
            """, (str(assessment_id),))
            rows = cur.fetchall()
        
        return [
            DimensionScoreResponse(
                id=UUID(r['ID']), assessment_id=UUID(r['ASSESSMENT_ID']),
                dimension=r['DIMENSION'], score=float(r['SCORE']), weight=float(r['WEIGHT']),
                confidence=float(r['CONFIDENCE']), evidence_count=r['EVIDENCE_COUNT'],
                created_at=r['CREATED_AT']
            ) for r in rows
        ]
    
    async def update_dimension_score(
        self, score_id: UUID, score_update: DimensionScoreUpdate
    ) -> Optional[DimensionScoreResponse]:
        updates, params = [], []
        
        if score_update.score is not None:
            updates.append("score = %s")
            params.append(score_update.score)
        if score_update.weight is not None:
            updates.append("weight = %s")
            params.append(score_update.weight)
        if score_update.confidence is not None:
            updates.append("confidence = %s")
            params.append(score_update.confidence)
        if score_update.evidence_count is not None:
            updates.append("evidence_count = %s")
            params.append(score_update.evidence_count)
        
        if not updates:
            async with self.cursor() as cur:
                cur.execute("SELECT * FROM dimension_scores WHERE id = %s", (str(score_id),))
                row = cur.fetchone()
            
            if not row:
                return None
            
            return DimensionScoreResponse(
                id=UUID(row['ID']), assessment_id=UUID(row['ASSESSMENT_ID']),
                dimension=row['DIMENSION'], score=float(row['SCORE']),
                weight=float(row['WEIGHT']), confidence=float(row['CONFIDENCE']),
                evidence_count=row['EVIDENCE_COUNT'], created_at=row['CREATED_AT']
            )
        
        params.append(str(score_id))
        
        async with self.cursor() as cur:
            cur.execute(f"""
                UPDATE dimension_scores SET {', '.join(updates)} WHERE id = %s
            """, params)
            
            if cur.rowcount == 0:
                return None
            
            cur.execute("SELECT * FROM dimension_scores WHERE id = %s", (str(score_id),))
            row = cur.fetchone()
        
        return DimensionScoreResponse(
            id=UUID(row['ID']), assessment_id=UUID(row['ASSESSMENT_ID']),
            dimension=row['DIMENSION'], score=float(row['SCORE']),
            weight=float(row['WEIGHT']), confidence=float(row['CONFIDENCE']),
            evidence_count=row['EVIDENCE_COUNT'], created_at=row['CREATED_AT']
        )
    
    async def list_industries(self) -> List[IndustryResponse]:
        async with self.cursor() as cur:
            cur.execute("SELECT id, name, sector, h_r_base, created_at FROM industries ORDER BY name")
            rows = cur.fetchall()
        
        return [
            IndustryResponse(
                id=UUID(r['ID']), name=r['NAME'], sector=r['SECTOR'],
                h_r_base=float(r['H_R_BASE']), created_at=r['CREATED_AT']
            ) for r in rows
        ]
    
    async def seed_data(self):
        from app.database.seed import seed_all
        async with self.cursor() as cur:
            await seed_all(cur)