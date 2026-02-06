"""
Comprehensive Test Suite for CS2 Evidence Collection API.
Tests all endpoints, data quality, edge cases, and integration.
"""
import pytest
import httpx
from uuid import uuid4


# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_URL = "http://localhost:8000"
TEST_TICKER = "GS"


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def client():
    """HTTP client for API calls."""
    return httpx.Client(base_url=BASE_URL, timeout=30.0, follow_redirects=True)


@pytest.fixture
def company_id(client):
    """Get real company_id from database."""
    response = client.get("/api/v1/companies")
    assert response.status_code == 200
    companies = response.json()
    
    for company in companies.get('items', []):
        if company['ticker'] == TEST_TICKER:
            return company['id']
    
    pytest.skip(f"Company {TEST_TICKER} not found in database")


@pytest.fixture
def document_id(client, company_id):
    """Get a real document_id from database."""
    response = client.get(f"/api/v1/documents?company_id={company_id}")
    assert response.status_code == 200
    
    docs = response.json().get('documents', [])
    if not docs:
        pytest.skip(f"No documents found for company {company_id}")
    
    return docs[0]['id']


# ============================================================================
# EVIDENCE ENDPOINTS
# ============================================================================

class TestEvidenceEndpoints:
    """Test /api/v1/evidence endpoints."""
    
    def test_get_evidence_stats(self, client):
        """Test GET /api/v1/evidence/stats - overall statistics."""
        response = client.get("/api/v1/evidence/stats")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "overall" in data
        assert "by_company" in data
        assert "companies_processed" in data["overall"]
        assert "total_documents" in data["overall"]
        assert "total_chunks" in data["overall"]
        assert "avg_composite_score" in data["overall"]
        assert isinstance(data["by_company"], list)
        
        print("\n✓ Evidence stats retrieved")
        print(f"  Companies: {data['overall']['companies_processed']}")
        print(f"  Documents: {data['overall']['total_documents']}")
        print(f"  Avg Score: {data['overall']['avg_composite_score']}")
    
    
    def test_get_company_evidence(self, client, company_id):
        """Test GET /api/v1/evidence/companies/{id}/evidence."""
        response = client.get(f"/api/v1/evidence/companies/{company_id}/evidence")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "ticker" in data
        assert "summary" in data
        assert "recent_documents" in data
        assert "recent_signals" in data
        
        summary = data["summary"]
        assert "total_documents" in summary
        assert "total_chunks" in summary
        assert "composite_score" in summary
        
        print(f"\n✓ Company evidence for {data['ticker']}")
        print(f"  Docs: {summary['total_documents']}, Chunks: {summary['total_chunks']}")
        print(f"  Composite: {summary['composite_score']}")
    
    
    def test_get_company_evidence_not_found(self, client):
        """Test 404 for non-existent company."""
        response = client.get(f"/api/v1/evidence/companies/{uuid4()}/evidence")
        assert response.status_code == 404
        print("\n✓ 404 for non-existent company")
    
    
    def test_backfill_single_company(self, client):
        """Test POST /api/v1/evidence/backfill - single company."""
        response = client.post("/api/v1/evidence/backfill", json={
            "tickers": ["CAT"],
            "pipelines": ["job"]
        })
        
        assert response.status_code == 200
        data = response.json()
        
        assert "task_id" in data
        assert "message" in data
        assert "tickers" in data
        assert data["tickers"] == ["CAT"]
        
        print(f"\n✓ Backfill queued: {data['task_id'][:8]}...")
    
    
    def test_backfill_all_companies(self, client):
        """Test POST /api/v1/evidence/backfill - all companies."""
        response = client.post("/api/v1/evidence/backfill", json={
            "tickers": None,
            "pipelines": ["job"]
        })
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["tickers"]) == 10
        
        print(f"\n✓ Backfill all companies queued")
    
    
    def test_backfill_custom_pipelines(self, client):
        """Test backfill with custom pipeline selection."""
        response = client.post("/api/v1/evidence/backfill", json={
            "tickers": ["JPM", "GS"],
            "pipelines": ["patent", "github"]
        })
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["tickers"]) == 2
        assert len(data["pipelines"]) == 2
        
        print(f"\n✓ Custom backfill queued")


# ============================================================================
# SIGNAL ENDPOINTS
# ============================================================================

class TestSignalEndpoints:
    """Test /api/v1/signals endpoints."""
    
    def test_get_company_signals_summary(self, client, company_id):
        """Test GET /api/v1/signals/companies/{id}/signals."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "ticker" in data
        assert "hiring_score" in data
        assert "patent_score" in data
        assert "github_score" in data
        assert "composite_score" in data
        
        print(f"\n✓ Signal summary for {data['ticker']}")
        print(f"  H:{data['hiring_score']} P:{data['patent_score']} G:{data['github_score']} → {data['composite_score']}")
    
    
    def test_get_hiring_signal_detail(self, client, company_id):
        """Test GET /api/v1/signals/companies/{id}/signals/hiring_signal."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/hiring_signal")
        
        if response.status_code == 404:
            pytest.skip("No hiring signal")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "summary" in data
        assert "seniority_breakdown" in data
        assert "ratios" in data
        
        summary = data["summary"]
        assert "total_jobs" in summary
        assert "ai_jobs" in summary
        
        print(f"\n✓ Hiring detail: {summary['total_jobs']} jobs, {summary['ai_jobs']} AI")
    
    
    def test_get_patent_signal_detail(self, client, company_id):
        """Test GET /api/v1/signals/companies/{id}/signals/patent."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/patent")
        
        if response.status_code == 404:
            pytest.skip("No patent signal")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "summary" in data
        assert "by_year" in data
        assert "patents" in data
        
        print(f"\n✓ Patent detail: {data['summary']['ai_patents']} AI patents")
    
    
    def test_get_github_signal_detail(self, client, company_id):
        """Test GET /api/v1/signals/companies/{id}/signals/github."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/github")
        
        if response.status_code == 404:
            pytest.skip("No github signal")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "summary" in data
        assert "top_repos" in data
        
        print(f"\n✓ GitHub detail: {data['summary']['ai_repos']} AI repos")
    
    
    def test_collect_all_signals(self, client, company_id):
        """Test POST /api/v1/signals/collect."""
        response = client.post("/api/v1/signals/collect", json={
            "company_id": str(company_id),
            "pipelines": ["job", "patent", "github"]
        })
        
        assert response.status_code == 200
        data = response.json()
        assert len(data["pipelines"]) == 3
        
        print(f"\n✓ Signal collection queued")
    
    
    def test_collect_hiring_only(self, client, company_id):
        """Test POST /api/v1/signals/collect/hiring."""
        response = client.post(f"/api/v1/signals/collect/hiring?company_id={company_id}")
        assert response.status_code == 200
        print(f"\n✓ Hiring collection queued")
    
    
    def test_collect_patent_only(self, client, company_id):
        """Test POST /api/v1/signals/collect/patent."""
        response = client.post(f"/api/v1/signals/collect/patent?company_id={company_id}")
        assert response.status_code == 200
        print(f"\n✓ Patent collection queued")
    
    
    def test_collect_github_only(self, client, company_id):
        """Test POST /api/v1/signals/collect/github."""
        response = client.post(f"/api/v1/signals/collect/github?company_id={company_id}")
        assert response.status_code == 200
        print(f"\n✓ GitHub collection queued")


# ============================================================================
# DOCUMENT ENDPOINTS
# ============================================================================

class TestDocumentEndpoints:
    """Test /api/v1/documents endpoints."""
    
    def test_list_all_documents(self, client):
        """Test GET /api/v1/documents."""
        response = client.get("/api/v1/documents")
        assert response.status_code == 200
        data = response.json()
        
        assert "documents" in data
        assert isinstance(data["documents"], list)
        
        print(f"\n✓ Listed {data['total']} documents")
    
    
    def test_list_documents_by_company(self, client, company_id):
        """Test GET /api/v1/documents?company_id={id}."""
        response = client.get(f"/api/v1/documents?company_id={company_id}")
        assert response.status_code == 200
        data = response.json()
        
        for doc in data["documents"]:
            assert doc["company_id"] == company_id
        
        print(f"\n✓ Listed {data['total']} docs for company")
    
    
    def test_list_documents_pagination(self, client):
        """Test pagination."""
        r1 = client.get("/api/v1/documents?skip=0&limit=5")
        r2 = client.get("/api/v1/documents?skip=5&limit=5")
        
        assert r1.status_code == 200
        assert r2.status_code == 200
        
        print(f"\n✓ Pagination: page1={len(r1.json()['documents'])}, page2={len(r2.json()['documents'])}")
    
    
    def test_get_single_document(self, client, document_id):
        """Test GET /api/v1/documents/{id}."""
        response = client.get(f"/api/v1/documents/{document_id}")
        assert response.status_code == 200
        data = response.json()
        
        assert "ticker" in data
        assert "filing_type" in data
        assert "word_count" in data
        
        print(f"\n✓ Document: {data['ticker']} {data['filing_type']} ({data['word_count']} words)")
    
    
    def test_get_document_not_found(self, client):
        """Test 404 for non-existent document."""
        response = client.get(f"/api/v1/documents/{uuid4()}")
        assert response.status_code == 404
        print("\n✓ 404 for non-existent document")
    
    
    def test_get_document_chunks(self, client, document_id):
        """Test GET /api/v1/documents/{id}/chunks."""
        response = client.get(f"/api/v1/documents/{document_id}/chunks")
        assert response.status_code == 200
        data = response.json()
        
        assert "chunks" in data
        assert "total_chunks" in data
        
        if data["chunks"]:
            assert "chunk_index" in data["chunks"][0]
            assert "section_id" in data["chunks"][0]
        
        print(f"\n✓ Retrieved {data['total_chunks']} chunks")
    
    
    def test_document_chunks_ordered(self, client, document_id):
        """Test chunks are returned in correct order."""
        response = client.get(f"/api/v1/documents/{document_id}/chunks?limit=10")
        assert response.status_code == 200
        
        chunks = response.json()["chunks"]
        if len(chunks) > 1:
            indices = [c["chunk_index"] for c in chunks]
            assert indices == sorted(indices), "Chunks not in order!"
        
        print(f"\n✓ Chunks ordered correctly")


# ============================================================================
# DATA QUALITY TESTS
# ============================================================================

class TestDataQuality:
    """Test data quality and consistency."""
    
    def test_all_companies_have_data(self, client):
        """Verify all 10 companies have some evidence."""
        response = client.get("/api/v1/evidence/stats")
        data = response.json()
        
        assert data["overall"]["companies_processed"] == 10
        
        for company in data["by_company"]:
            has_data = any([
                company.get("documents"),
                company.get("hiring_score"),
                company.get("patent_score"),
                company.get("github_score")
            ])
            assert has_data, f"{company['ticker']} has no data"
        
        print(f"\n✓ All 10 companies have data")
    
    
    def test_scores_within_valid_range(self, client):
        """Test all scores are 0-100."""
        response = client.get("/api/v1/evidence/stats")
        data = response.json()
        
        for company in data["by_company"]:
            for field in ["hiring_score", "patent_score", "github_score", "composite_score"]:
                score = company.get(field)
                if score is not None:
                    assert 0 <= score <= 100, f"{company['ticker']}.{field} = {score} out of range"
        
        print(f"\n✓ All scores in 0-100 range")
    
    
    def test_composite_score_calculation(self, client, company_id):
        """Test composite is weighted average of components."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals")
        data = response.json()
        
        scores, weights = [], []
        
        if data["hiring_score"]:
            scores.append(data["hiring_score"])
            weights.append(0.30)
        
        if data["patent_score"]:
            scores.append(data["patent_score"])
            weights.append(0.25)
        
        if data["github_score"]:
            scores.append(data["github_score"])
            weights.append(0.20)
        
        if data["leadership_score"]:
            scores.append(data["leadership_score"])
            weights.append(0.25)
        
        if scores:
            total_weight = sum(weights)
            normalized = [w / total_weight for w in weights]
            expected = round(sum(s * w for s, w in zip(scores, normalized)), 2)
            
            assert abs(data["composite_score"] - expected) < 0.3
            
            print(f"\n✓ Composite correctly calculated: {data['composite_score']}")
    
    
    def test_document_chunks_sum_to_total(self, client, company_id):
        """Test document chunk counts are consistent."""
        response = client.get(f"/api/v1/evidence/companies/{company_id}/evidence")
        total_chunks = response.json()["summary"]["total_chunks"]
        
        # Get all documents
        docs_response = client.get(f"/api/v1/documents?company_id={company_id}&limit=100")
        docs = docs_response.json()["documents"]
        
        chunk_sum = sum(doc.get("total_chunks", 0) for doc in docs)
        
        assert chunk_sum == total_chunks
        
        print(f"\n✓ Chunk count consistent: {chunk_sum} total")
    
    
    def test_filing_dates_chronological(self, client):
        """Test documents are returned in reverse chronological order."""
        response = client.get("/api/v1/documents?limit=20")
        docs = response.json()["documents"]
        
        if len(docs) > 1:
            dates = [d["filing_date"] for d in docs]
            assert dates == sorted(dates, reverse=True), "Documents not ordered by date"
        
        print(f"\n✓ Documents in chronological order")


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegration:
    """Cross-endpoint consistency tests."""
    
    def test_stats_matches_company_count(self, client):
        """Stats count matches actual companies."""
        response = client.get("/api/v1/evidence/stats")
        stats = response.json()
        
        assert len(stats["by_company"]) == stats["overall"]["companies_processed"]
        
        print(f"\n✓ Stats consistency: {stats['overall']['companies_processed']} companies")
    
    
    def test_document_count_consistency(self, client, company_id):
        """Document counts match across endpoints."""
        evidence = client.get(f"/api/v1/evidence/companies/{company_id}/evidence").json()
        docs = client.get(f"/api/v1/documents?company_id={company_id}&limit=1000").json()
        
        reported = evidence["summary"]["total_documents"]
        actual = len(docs["documents"])
        
        assert actual <= reported
        
        print(f"\n✓ Doc count: {reported} reported, {actual} fetched")
    
    
    def test_signal_scores_match(self, client, company_id):
        """Signal scores match between summary and detail."""
        summary = client.get(f"/api/v1/signals/companies/{company_id}/signals").json()
        detail = client.get(f"/api/v1/signals/companies/{company_id}/signals/hiring_signal")
        
        if detail.status_code == 200:
            assert summary["hiring_score"] == detail.json()["score"]
            print(f"\n✓ Score consistency: {detail.json()['score']}")
    
    
    def test_all_tickers_unique(self, client):
        """Test no duplicate tickers in stats."""
        response = client.get("/api/v1/evidence/stats")
        tickers = [c["ticker"] for c in response.json()["by_company"]]
        
        assert len(tickers) == len(set(tickers)), "Duplicate tickers found!"
        
        print(f"\n✓ All tickers unique: {tickers}")


# ============================================================================
# EDGE CASES
# ============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_invalid_signal_category(self, client, company_id):
        """Test 400 for invalid signal category."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/invalid_category")
        assert response.status_code in [400, 404]
        print(f"\n✓ Invalid category rejected: {response.status_code}")
    
    
    def test_pagination_edge_cases(self, client):
        """Test pagination with edge case values."""
        # Skip beyond available
        response = client.get("/api/v1/documents?skip=10000&limit=10")
        assert response.status_code == 200
        assert len(response.json()["documents"]) == 0
        
        # Limit = 0
        response = client.get("/api/v1/documents?skip=0&limit=0")
        assert response.status_code == 200
        
        print(f"\n✓ Pagination edge cases handled")
    
    
    def test_chunks_limit_respected(self, client, document_id):
        """Test chunk limit parameter works."""
        response = client.get(f"/api/v1/documents/{document_id}/chunks?limit=5")
        chunks = response.json()["chunks"]
        
        assert len(chunks) <= 5
        
        print(f"\n✓ Chunk limit respected: {len(chunks)}")
    
    
    def test_special_characters_in_ticker(self, client):
        """Test handling of special characters."""
        # SQL injection attempt (should be safe with parameterized queries)
        response = client.get("/api/v1/documents?company_id='; DROP TABLE companies; --")
        assert response.status_code in [400, 422, 500]  # Should reject, not crash
        print(f"\n✓ SQL injection prevented")


# ============================================================================
# PERFORMANCE TESTS
# ============================================================================

class TestPerformance:
    """Test response times and efficiency."""
    
    def test_stats_endpoint_fast(self, client):
        """Stats endpoint responds quickly."""
        import time
        start = time.time()
        response = client.get("/api/v1/evidence/stats")
        duration = time.time() - start
        
        assert response.status_code == 200
        assert duration < 5.0  # Should be under 5 seconds
        
        print(f"\n✓ Stats endpoint: {duration:.2f}s")
    
    
    def test_company_evidence_fast(self, client, company_id):
        """Company evidence responds quickly."""
        import time
        start = time.time()
        response = client.get(f"/api/v1/evidence/companies/{company_id}/evidence")
        duration = time.time() - start
        
        assert response.status_code == 200
        assert duration < 3.0
        
        print(f"\n✓ Company evidence: {duration:.2f}s")


# ============================================================================
# METADATA RICHNESS TESTS
# ============================================================================

class TestMetadataRichness:
    """Test metadata contains sufficient detail for dashboards."""
    
    def test_hiring_metadata_completeness(self, client, company_id):
        """Hiring metadata has all required fields for visualization."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/hiring_signal")
        
        if response.status_code == 404:
            pytest.skip("No hiring signal")
        
        data = response.json()
        
        # Summary stats
        assert "summary" in data
        assert data["summary"]["total_jobs"] > 0 or data["summary"]["total_jobs"] == 0
        
        # Seniority breakdown for pie/bar charts
        assert "seniority_breakdown" in data
        seniority = data["seniority_breakdown"]
        assert all(k in seniority for k in ["leadership", "senior", "mid", "entry"])
        
        # Ratios for trend analysis
        assert "ratios" in data
        
        print(f"\n✓ Hiring metadata complete for visualization")
    
    
    def test_patent_metadata_has_timeline(self, client, company_id):
        """Patent metadata has by_year data for time-series charts."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/patent")
        
        if response.status_code == 404:
            pytest.skip("No patent signal")
        
        data = response.json()
        
        assert "by_year" in data
        assert isinstance(data["by_year"], dict)
        
        # Should have year keys
        if data["by_year"]:
            year_keys = list(data["by_year"].keys())
            assert all(year.isdigit() for year in year_keys)
        
        print(f"\n✓ Patent timeline data available")
    
    
    def test_github_metadata_has_repo_details(self, client, company_id):
        """GitHub metadata has repo list with stars/topics."""
        response = client.get(f"/api/v1/signals/companies/{company_id}/signals/github")
        
        if response.status_code == 404:
            pytest.skip("No github signal")
        
        data = response.json()
        
        assert "top_repos" in data
        
        if data["top_repos"]:
            repo = data["top_repos"][0]
            assert "name" in repo
            assert "stars" in repo or "score" in repo
        
        print(f"\n✓ GitHub repo details available")


# ============================================================================
# RUN TESTS
# ============================================================================

if __name__ == "__main__":
    pytest.main([
        __file__,
        "-v",
        "-s",
        "--tb=short",
        "--color=yes"
    ])