# PE Org-AI-R Platform: Case Study 2
## Evidence Collection - "What Companies Say vs. What They Do"

**Course:** Big Data and Intelligent Analytics  

---

## Table of Contents
- [PE Org-AI-R Platform: Case Study 2](#pe-org-ai-r-platform-case-study-2)
  - [Evidence Collection - "What Companies Say vs. What They Do"](#evidence-collection---what-companies-say-vs-what-they-do)
  - [Table of Contents](#table-of-contents)
  - [Project Links](#project-links)
  - [Project Overview](#project-overview)
    - [Purpose](#purpose)
    - [Scope](#scope)
    - [Business Value](#business-value)
    - [Tech Stack](#tech-stack)
  - [Architecture](#architecture)
    - [System Architecture](#system-architecture)
    - [Data Flow](#data-flow)
  - [Directory Structure](#directory-structure)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
  - [Team Contributions](#team-contributions)
  - [AI Tools Usage](#ai-tools-usage)
  - [Academic Integrity](#academic-integrity)

---

## Project Links

| Resource | URL |
|----------|-----|
| Codelabs Document | https://docs.google.com/document/d/1lCN91g7-3JC3hcFBV3tNC85RoroQoVF6fdgQvhQdz9s/edit?usp=sharing |
| Video Presentation | https://northeastern.zoom.us/rec/share/o5VAgQeBBP9c_jdimYl_KlVIHy7jDZIiorEGH-wlTM8uJNllL3dlesM8DAgrJp5S.BQ90GkrEwcGVuNix?startTime=1770409186000 |
| Live Streamlit Dashboard | http://localhost:8501|


---

## Project Overview

### Purpose
Case Study 2 builds on the platform foundation from CS1 by adding evidence collection capabilities. We gather data from SEC filings and external sources to validate AI readiness claims with actual evidence.

### Scope
We implemented four main pipelines:
- SEC EDGAR document collection for 10 target companies
- Document parsing supporting both PDF and HTML formats
- External signal collection from job boards, GitHub, and USPTO
- Evidence scoring and storage integrated with existing platform

Our system processes 90+ SEC filings and generates normalized scores (0-100) for hiring activity, technology adoption, and patent innovation.

### Business Value
The gap between AI rhetoric and reality is measurable. Companies frequently mention AI in annual reports but lack corresponding evidence in hiring or R&D spending. This system quantifies that gap across four signal categories.

### Tech Stack
**Backend:** Python 3.11, FastAPI 0.109, Pydantic 2.0  
**Document Processing:** sec-edgar-downloader, pdfplumber 0.10, BeautifulSoup4  
**Storage:** Snowflake, Redis 7, AWS S3  
**API Clients:** httpx 0.26  
**Infrastructure:** Docker 24.0, Terraform 1.6  
**Testing:** Pytest 7.4

---

## Architecture

### System Architecture
```
┌──────────────────────────────────────────────────────────────────┐
│                     EXTERNAL DATA SOURCES                        │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌────────┐   │
│  │ SEC EDGAR   │  │ Job Boards  │  │  BuiltWith  │  │ USPTO  │   │
│  │  Filings    │  │  APIs       │  │    API      │  │Patents │   │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └───┬────┘   │
└─────────┼─────────────────┼─────────────────┼──────────────┼─────┘
          │                 │                 │              │
          ▼                 ▼                 ▼              ▼
┌──────────────────────────────────────────────────────────────────┐
│                      PIPELINE LAYER                              │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐            │
│  │ SEC Pipeline │  │ Document     │  │ Signal       │            │
│  │              │  │ Parser       │  │ Collectors   │            │
│  │ - Download   │  │ - PDF Parse  │  │ - Jobs       │            │
│  │ - Validate   │  │ - HTML Parse │  │ - GitHub     │            │
│  │ - Store S3   │  │ - Chunking   │  │ - Patents    │            │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘            │
└─────────┼──────────────────┼──────────────────┼──────────────────┘
          │                  │                  │
          ▼                  ▼                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                    APPLICATION LAYER                             │
│                                                                  │
│                 ┌─────────────────────┐                          │
│                 │   FastAPI           │                          │
│                 │   Application       │                          │
│                 └──────────┬──────────┘                          │
│                            │                                     │
│     ┌──────────┬───────────┼──────────┬──────────┐               │
│     │          │           │          │          │               │
│     ▼          ▼           ▼          ▼          ▼               │
│  ┌────────┐ ┌─────────┐ ┌────────┐ ┌────────┐ ┌────────┐         │
│  │Documents│ │Evidence│ │Signals │ │Companies││Health  │         │
│  │ Router │ │ Router  │ │ Router │ │ Router  ││ Router │         │
│  └────┬───┘ └────┬────┘ └────┬───┘ └────┬───┘ └────┬───┘         │
└───────┼──────────┼───────────┼──────────┼──────────┼───────────┘
        │          │           │          │          │
        └──────────┴───────────┴──────────┴──────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
                 ▼                         ▼
    ┌────────────────────────┐   ┌─────────────────────┐
    │   SERVICE LAYER        │   │   MODEL LAYER       │
    │                        │   │                     │
    │  ┌──────────────────┐  │   │  ┌───────────────┐  │
    │  │ Snowflake        │  │   │  │ Document      │  │
    │  │ Service          │  │   │  │ Evidence      │  │
    │  ├──────────────────┤  │   │  │ Signal        │  │
    │  │ Redis Cache      │  │   │  │ Company       │  │
    │  │ Service          │  │   │  │ Assessment    │  │
    │  ├──────────────────┤  │   │  └───────────────┘  │
    │  │ S3 Storage       │  │   │                     │
    │  │ Service          │  │   │                     │
    │  ├──────────────────┤  │   │                     │
    │  │ Evidence Storage │  │   │                     │
    │  │ Service          │  │   │                     │
    │  └──────────────────┘  │   │                     │
    └───────────┬────────────┘   └─────────────────────┘
                │
                ▼
┌──────────────────────────────────────────────────────────────────┐
│                        STORAGE LAYER                             │
│                                                                  │
│  ┌────────────────┐   ┌────────────────┐   ┌─────────────────┐   │
│  │  Snowflake     │   │     Redis      │   │     AWS S3      │   │
│  │                │   │     Cache      │   │                 │   │
│  │ - documents    │   │  (5m-24h TTL)  │   │ - Raw PDFs      │   │
│  │ - chunks       │   │                │   │ - HTML files    │   │
│  │ - signals      │   │                │   │ - Evidence      │   │
│  │ - evidence     │   │                │   │                 │   │
│  └────────────────┘   └────────────────┘   └─────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

### Data Flow
```
SEC Filing Collection:
1. Download from EDGAR → 2. Parse PDF/HTML → 3. Chunk semantically
   → 4. Store raw in S3 → 5. Store metadata in Snowflake

Signal Collection:
1. Query external APIs → 2. Normalize to 0-100 scale
   → 3. Store in Snowflake → 4. Cache in Redis

Evidence Aggregation:
1. Fetch documents + signals → 2. Calculate composite scores
   → 3. Return via API
```

---

## Directory Structure
```
CASE-STUDY-2/
│
├── alembic/
│   ├── versions/
│   ├── env.py
│   └── script.py.mako
│
├── app/
│   ├── core/
│   │   ├── config_loader.py
│   │   ├── github_config.py
│   │   ├── keywords.py
│   │   └── patent_config.py
│   │
│   ├── database/
│   │   ├── init.py
│   │   └── seed.py
│   │
│   ├── models/
│   │   ├── assessment.py
│   │   ├── company.py
│   │   ├── dimension.py
│   │   ├── enums.py
│   │   ├── evidence.py
│   │   ├── industry.py
│   │   ├── pagination.py
│   │   └── signal.py
│   │
│   ├── pipelines/
│   │   ├── github_scanner.py
│   │   ├── job_signal_collector.py
│   │   ├── linkedin_fallback.py
│   │   ├── patent_scanner.py
│   │   ├── sec_chunker.py
│   │   ├── sec_integration.py
│   │   └── sec_parser.py
│   │
│   ├── routers/
│   │   ├── assessments.py
│   │   ├── companies.py
│   │   ├── dimensions.py
│   │   ├── documents.py
│   │   ├── evidence.py
│   │   ├── health.py
│   │   └── signals.py
│   │
│   ├── services/
│   │   ├── evidence_storage.py
│   │   ├── job_signal_service.py
│   │   ├── redis_cache.py
│   │   ├── s3_storage.py
│   │   ├── signal_aggregation.py
│   │   └── snowflake.py
│   │
│   ├── config.py
│   ├── database.py
│   └── main.py
│
├── config/
│   ├── companies.yml
│   ├── github_orgs.yml
│   ├── keywords.yml
│   └── patent_config.yml
│
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
│
├── infra/terraform/snowflake/
│   ├── infrastructure.tf
│   ├── main.tf
│   ├── outputs.tf
│   └── variables.tf
│
├── scripts/
│   ├── collect_evidence.py
│   ├── run_github_pipeline.py
│   ├── run_job_pipeline.py
│   ├── run_patent_pipeline.py
│   └── run_sec_pipeline.py
│
├── tests/
│   ├── conftest.py
│   ├── test_api.py
│   ├── test_cs2_api.py
│   ├── test_cs2_schema.py
│   ├── test_evidence_models.py
│   └── test_github_scraper.py
│
├── prototyping/
│   ├── notebooks/
│   └── poc_scripts/
│
├── .env.example
├── .gitignore
├── alembic.ini
├── dashboard.py
├── poetry.lock
├── pyproject.toml
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Docker Desktop
- Git
- Snowflake account
- AWS account with S3 access
- Valid email for SEC EDGAR User-Agent

### Setup

**Clone and configure:**
```bash
git clone https://github.com/username/pe-org-air-cs2.git
cd pe-org-air-cs2
cp .env.example .env
```

**Edit .env:**
```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
S3_BUCKET=your_bucket
SEC_USER_AGENT_EMAIL=your_email@university.edu
```

**Install dependencies:**
```bash
poetry install && poetry shell
```

**Setup infrastructure:**
```bash
cd infra/terraform/snowflake
terraform init && terraform apply
cd ../../..
alembic upgrade head
```

**Start services:**
```bash
docker-compose up -d
```

**Run evidence collection:**
```bash
python scripts/collect_evidence.py --companies all
```

This downloads SEC filings, collects external signals, and populates the database. 

**Verify:**
```bash
curl http://localhost:8000/health
curl http://localhost:8000/api/v1/documents | jq
```



---

## Team Contributions

| Member | Work |
|--------|------------|
| [Anirudh] | SEC integration, document parsing, chunking |
| [Janhavi] | Signal collection, API endpoints, dashboard |
| [Minal] | Database design, Docker, deployment | 


## AI Tools Usage

| AI Tool | Usage Level | 
|---------|-------------|
| Claude 3.5 Sonnet (Anthropic) | High |
| GitHub Copilot | Medium | 


---

## Academic Integrity

This work is our own submission for Big Data and Intelligent Analytics, Spring 2026. We followed course guidelines on AI tool usage - tools assisted with code generation but all logic, algorithms, and design decisions are ours. Each team member can explain and defend any part of the implementation.

**Team:**
- Anirudh Raj
- Janhavi Patil
- Minal Naranje

---
