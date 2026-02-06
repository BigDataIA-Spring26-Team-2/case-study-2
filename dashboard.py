"""
PE Org-AI-R Platform Dashboard
Minimal | Dark | Retro | Professional
"""
import streamlit as st
import httpx
import plotly.graph_objects as go
import pandas as pd
from typing import Optional, Dict, Any
import json

# ============================================================================
# CONFIG
# ============================================================================

import os
API_BASE = os.getenv("API_BASE", "http://localhost:8000/api/v1")
COLORS = {
    'primary': "#45D009",
    'secondary': '#ff6b35',
    'accent': '#00d9ff',
    'warning': '#ffd23f',
    'bg_dark': '#0d0d0d',
    'bg_card': '#1a1a1a',
    'text': '#e0e6ed',
    'dim': '#6c757d'
}

# ============================================================================
# STYLING
# ============================================================================

st.set_page_config(
    page_title="PE Org-AI-R",
    layout="centered",
    initial_sidebar_state="collapsed",
    menu_items=None
)

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');
    
    * {{
        font-family: 'JetBrains Mono', monospace;
    }}
    
    .stApp {{
        background: {COLORS['bg_dark']};
        color: {COLORS['text']};
    }}
    
    .main .block-container {{
        max-width: 1400px;
        padding-top: 2rem;
    }}
    
    h1 {{
        color: {COLORS['text']};
        font-weight: 700;
        letter-spacing: -0.5px;
    }}
    
    h2, h3 {{
        color: {COLORS['primary']};
        font-weight: 700;
        letter-spacing: -0.5px;
    }}
    
    .stButton>button {{
        background: transparent;
        border: 1px solid {COLORS['primary']};
        color: {COLORS['primary']};
        border-radius: 2px;
        font-weight: 700;
        padding: 0.5rem 2rem;
        transition: all 0.2s;
    }}
    
    .stButton>button:hover {{
        background: {COLORS['primary']};
        color: {COLORS['bg_dark']};
        box-shadow: 0 0 20px {COLORS['primary']}40;
    }}
    
    div[data-testid="stMetricValue"] {{
        color: {COLORS['primary']};
        font-size: 2.5rem;
        font-weight: 700;
    }}
    
    div[data-testid="stMetricLabel"] {{
        color: {COLORS['dim']};
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 1px;
    }}
    
    .dataframe {{
        background: {COLORS['bg_card']};
        border: 1px solid {COLORS['primary']}40;
    }}
    
    .dataframe th {{
        background: {COLORS['bg_card']};
        color: {COLORS['primary']};
        border-bottom: 2px solid {COLORS['primary']};
    }}
    
    .dataframe td {{
        color: {COLORS['text']};
        border-bottom: 1px solid {COLORS['dim']}40;
    }}
    
    .stSelectbox label {{
        color: {COLORS['dim']};
        text-transform: uppercase;
    }}
    
    .stMultiSelect label {{
        color: {COLORS['dim']};
        text-transform: uppercase;
    }}
    
    .stTabs [data-baseweb="tab"] {{
        color: {COLORS['dim']};
    }}
    
    .stTabs [aria-selected="true"] {{
        color: {COLORS['primary']};
    }}
</style>
""", unsafe_allow_html=True)

# ============================================================================
# API
# ============================================================================

@st.cache_data(ttl=60)
def fetch_stats():
    try:
        return httpx.get(f"{API_BASE}/evidence/stats", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=60)
def fetch_company_evidence(company_id):
    try:
        return httpx.get(f"{API_BASE}/evidence/companies/{company_id}/evidence", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=60)
def fetch_signal_detail(company_id, category):
    try:
        return httpx.get(f"{API_BASE}/signals/companies/{company_id}/signals/{category}", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=60)
def fetch_companies():
    try:
        return httpx.get(f"{API_BASE}/companies?page=1&page_size=100", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=60)
def fetch_documents(company_id):
    try:
        return httpx.get(f"{API_BASE}/documents?company_id={company_id}&limit=100", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=300)
def fetch_document_sections(document_id: str):
    try:
        r = httpx.get(f"{API_BASE}/documents/{document_id}/sections", timeout=10)
        return r.json() if r.status_code == 200 else None
    except:
        return None

@st.cache_data(ttl=300)
def fetch_section_chunks(document_id: str, section_id: str):
    try:
        r = httpx.get(f"{API_BASE}/documents/{document_id}/chunks?section_id={section_id}&limit=20", timeout=10)
        return r.json() if r.status_code == 200 else None
    except:
        return None

def trigger_backfill(tickers, pipelines):
    try:
        r = httpx.post(
            f"{API_BASE}/evidence/backfill",
            json={"tickers": tickers, "pipelines": pipelines},
            timeout=30
        )
        return r.json() if r.status_code == 200 else None
    except Exception as e:
        st.error(f"Backfill failed: {str(e)}")
        return None

# ============================================================================
# COMPONENTS
# ============================================================================

def render_gauge(score, title, height=180):
    """Minimal gauge chart."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score or 0,
        title={'text': title, 'font': {'size': 11, 'color': COLORS['dim']}},
        number={'font': {'size': 32, 'color': COLORS['primary'], 'family': 'JetBrains Mono'}},
        gauge={
            'axis': {'range': [0, 100], 'tickcolor': COLORS['dim'], 'tickfont': {'size': 8}},
            'bar': {'color': COLORS['primary'], 'thickness': 0.7},
            'bgcolor': COLORS['bg_card'],
            'borderwidth': 0,
            'steps': [
                {'range': [0, 50], 'color': COLORS['bg_card']},
                {'range': [50, 75], 'color': 'rgba(0, 255, 65, 0.1)'},
                {'range': [75, 100], 'color': 'rgba(0, 255, 65, 0.2)'}
            ],
            'threshold': {
                'line': {'color': COLORS['secondary'], 'width': 2},
                'thickness': 0.75,
                'value': 75
            }
        }
    ))
    
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor=COLORS['bg_dark'],
        plot_bgcolor=COLORS['bg_dark'],
        font={'family': 'JetBrains Mono', 'color': COLORS['text']}
    )
    
    return fig


def render_bar(data, title):
    """Horizontal bar chart."""
    fig = go.Figure(go.Bar(
        y=list(data.keys()),
        x=list(data.values()),
        orientation='h',
        marker=dict(
            color=COLORS['primary'],
            line=dict(color=COLORS['primary'], width=1)
        )
    ))
    
    fig.update_layout(
        title={'text': title, 'font': {'size': 14, 'color': COLORS['dim']}},
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor=COLORS['bg_dark'],
        plot_bgcolor=COLORS['bg_dark'],
        font={'family': 'JetBrains Mono', 'color': COLORS['text']},
        xaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
        yaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'}
    )
    
    return fig


def render_line_chart(data, title):
    """Line chart for hiring trends."""
    if not data:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=list(data.keys()),
        y=list(data.values()),
        mode='lines+markers',
        line=dict(color=COLORS['primary'], width=3),
        marker=dict(size=8, color=COLORS['primary']),
        fill='tozeroy',
        fillcolor='rgba(0, 255, 65, 0.1)'
    ))
    
    fig.update_layout(
        title={'text': title, 'font': {'size': 14, 'color': COLORS['dim']}},
        height=300,
        margin=dict(l=20, r=20, t=40, b=40),
        paper_bgcolor=COLORS['bg_dark'],
        plot_bgcolor=COLORS['bg_dark'],
        font={'family': 'JetBrains Mono', 'color': COLORS['text']},
        xaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
        yaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
        hovermode='x unified'
    )
    
    return fig

# ============================================================================
# PAGES
# ============================================================================

def page_overview():
    """Main overview page."""
    st.markdown("# PE Org-AI-R Platform")
    st.markdown(f"<p style='color:{COLORS['dim']}'>AI Readiness Assessment • Private Equity Analytics</p>", unsafe_allow_html=True)
    
    stats = fetch_stats()
    
    if not stats:
        st.error("API not responding. Start server: poetry run uvicorn app.main:app")
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("COMPANIES", stats['overall']['companies_processed'])
    with col2:
        st.metric("DOCUMENTS", f"{stats['overall']['total_documents']:,}")
    with col3:
        st.metric("CHUNKS", f"{stats['overall']['total_chunks']:,}")
    with col4:
        avg = stats['overall']['avg_composite_score']
        st.metric("AVG SCORE", f"{avg:.1f}" if avg else "—")
    
    st.markdown("---")
    st.markdown("### Company Rankings")
    
    df = pd.DataFrame(stats['by_company'])
    df = df.sort_values('composite_score', ascending=False, na_position='last')
    
    df_display = df[['ticker', 'documents', 'hiring_score', 'patent_score', 'github_score', 'composite_score']].copy()
    
    for col in ['hiring_score', 'patent_score', 'github_score', 'composite_score']:
        df_display[col] = df_display[col].apply(lambda x: f"{x:.1f}" if pd.notna(x) else "—")
    
    df_display.columns = ['TICKER', 'DOCS', 'HIRING', 'PATENT', 'GITHUB', 'COMPOSITE']
    
    st.dataframe(df_display, width='stretch', hide_index=True, height=400)
    
    st.markdown("### Score Distribution")
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Hiring', x=df['ticker'], y=df['hiring_score'].fillna(0),
        marker_color=COLORS['primary'],
        text=df['hiring_score'].fillna(0).apply(lambda x: f"{x:.0f}"),
        textposition='outside', textfont=dict(size=10)
    ))
    
    fig.add_trace(go.Bar(
        name='Patent', x=df['ticker'], y=df['patent_score'].fillna(0),
        marker_color=COLORS['secondary'],
        text=df['patent_score'].fillna(0).apply(lambda x: f"{x:.0f}"),
        textposition='outside', textfont=dict(size=10)
    ))
    
    fig.add_trace(go.Bar(
        name='GitHub', x=df['ticker'], y=df['github_score'].fillna(0),
        marker_color=COLORS['accent'],
        text=df['github_score'].fillna(0).apply(lambda x: f"{x:.0f}"),
        textposition='outside', textfont=dict(size=10)
    ))
    
    fig.update_layout(
        title={'text': 'Signal Scores by Company', 'font': {'size': 14, 'color': COLORS['dim']}},
        barmode='group', height=400,
        margin=dict(l=20, r=20, t=40, b=60),
        paper_bgcolor=COLORS['bg_dark'], plot_bgcolor=COLORS['bg_dark'],
        font={'family': 'JetBrains Mono', 'color': COLORS['text']},
        xaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
        yaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)', 'range': [0, 110]},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, width='stretch')


def page_company_detail():
    """Company deep dive."""
    st.markdown("# Company Analysis")
    
    companies_resp = fetch_companies()
    if not companies_resp:
        st.error("Cannot load companies")
        return
    
    companies = companies_resp.get('items', [])
    ticker_to_company = {c['ticker']: c for c in companies}
    
    selected_ticker = st.selectbox("SELECT COMPANY", sorted(ticker_to_company.keys()), key="company_select")
    
    if not selected_ticker:
        return
    
    company = ticker_to_company[selected_ticker]
    company_id = company['id']
    company_name = company.get('name', selected_ticker)
    
    evidence = fetch_company_evidence(company_id)
    
    if not evidence:
        st.warning(f"No data for {selected_ticker}")
        return
    
    summary = evidence.get('summary', {})
    
    st.markdown(f"## {selected_ticker}")
    st.markdown(f"<p style='color:{COLORS['dim']}'>{company_name}</p>", unsafe_allow_html=True)
    st.markdown(f"<p style='color:{COLORS['dim']}'>Last updated: {summary.get('last_updated', 'Never')}</p>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns([1.2, 0.2, 1, 1, 1])
    
    with col1:
        composite = summary.get('composite_score')
        st.plotly_chart(render_gauge(composite, "COMPOSITE", height=180), width='stretch')
    
    with col3:
        st.markdown("<div style='padding-top:30px'></div>", unsafe_allow_html=True)
        h = summary.get('hiring_score')
        st.metric("HIRING", f"{h:.1f}" if h else "—", delta="30%")
    
    with col4:
        st.markdown("<div style='padding-top:30px'></div>", unsafe_allow_html=True)
        p = summary.get('patent_score')
        st.metric("PATENT", f"{p:.1f}" if p else "—", delta="25%")
    
    with col5:
        st.markdown("<div style='padding-top:30px'></div>", unsafe_allow_html=True)
        g = summary.get('github_score')
        st.metric("GITHUB", f"{g:.1f}" if g else "—", delta="20%")
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(["HIRING", "PATENT", "GITHUB", "DOCUMENTS"])
    
    with tab1:
        render_hiring_signal(company_id)
    with tab2:
        render_patent_signal(company_id)
    with tab3:
        render_github_signal(company_id)
    with tab4:
        render_documents(company_id)


def render_hiring_signal(company_id):
    """Hiring signal visualization."""
    data = fetch_signal_detail(company_id, "hiring_signal")
    
    if not data:
        st.info("No hiring data")
        return
    
    summary_data = data.get('summary', {})
    seniority = data.get('seniority_breakdown', {})
    metadata = data.get('metadata', {})
    
    st.markdown(f"### Score: {data.get('score', 0):.1f}/100")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("TOTAL JOBS", summary_data.get('total_jobs', 0))
    with col2:
        st.metric("AI JOBS", summary_data.get('ai_jobs', 0))
    with col3:
        ratio = summary_data.get('ai_ratio', 0)
        st.metric("AI RATIO", f"{ratio*100:.1f}%")
    with col4:
        phase = metadata.get('phase', 'UNKNOWN')
        st.metric("PHASE", phase)
    
    if seniority and any(seniority.values()):
        col_a, col_b = st.columns(2)
        
        with col_a:
            st.markdown("#### Seniority Distribution")
            st.plotly_chart(render_bar(seniority, "Roles by Level"), width='stretch')
        
        with col_b:
            hiring_trend = {
                'Entry': seniority.get('entry', 0),
                'Mid': seniority.get('mid', 0),
                'Senior': seniority.get('senior', 0),
                'Leadership': seniority.get('leadership', 0)
            }
            st.markdown("#### Hiring Trend")
            line_chart = render_line_chart(hiring_trend, "Distribution Trend")
            if line_chart:
                st.plotly_chart(line_chart, width='stretch')


def render_patent_signal(company_id):
    """Patent signal visualization."""
    data = fetch_signal_detail(company_id, "patent")
    
    if not data:
        st.info("No patent data")
        return
    
    summary_data = data.get('summary', {})
    by_year = data.get('by_year', {})
    patents = data.get('patents', [])
    
    st.markdown(f"### Score: {data.get('score', 0):.1f}/100")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("TOTAL PATENTS", summary_data.get('total_patents', 0))
    with col2:
        st.metric("AI PATENTS", summary_data.get('ai_patents', 0))
    with col3:
        ratio = summary_data.get('ai_ratio', 0)
        st.metric("AI RATIO", f"{ratio*100:.1f}%")
    
    if by_year:
        st.markdown("#### Patent Timeline")
        
        years = sorted(by_year.keys())
        ai_counts = [by_year[y]['ai'] for y in years]
        total_counts = [by_year[y]['total'] for y in years]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=years, y=total_counts, name='Total',
            line=dict(color=COLORS['dim'], width=2), mode='lines+markers'
        ))
        
        fig.add_trace(go.Scatter(
            x=years, y=ai_counts, name='AI Patents',
            line=dict(color=COLORS['primary'], width=3),
            fill='tozeroy', fillcolor='rgba(0, 255, 65, 0.1)',
            mode='lines+markers'
        ))
        
        fig.update_layout(
            height=300, margin=dict(l=20, r=20, t=20, b=40),
            paper_bgcolor=COLORS['bg_dark'], plot_bgcolor=COLORS['bg_dark'],
            font={'family': 'JetBrains Mono', 'color': COLORS['text']},
            xaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
            yaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
            hovermode='x unified',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig, width='stretch')
    
    if patents:
        st.markdown("#### Top AI Patents")
        
        patent_df = pd.DataFrame(patents[:10])
        
        if not patent_df.empty and 'title' in patent_df.columns:
            display_cols = ['title']
            if 'grant_date' in patent_df.columns:
                display_cols.append('grant_date')
            if 'score' in patent_df.columns:
                patent_df['score'] = patent_df['score'].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
                display_cols.append('score')
            
            df_display = patent_df[display_cols].copy()
            df_display.columns = ['TITLE', 'DATE', 'SCORE']
            st.dataframe(df_display, width='stretch', hide_index=True)


def render_github_signal(company_id):
    """GitHub signal visualization."""
    data = fetch_signal_detail(company_id, "github")
    
    if not data:
        st.info("No GitHub data")
        return
    
    summary_data = data.get('summary', {})
    repos = data.get('top_repos', [])
    orgs = data.get('organizations', [])
    
    st.markdown(f"### Score: {data.get('score', 0):.1f}/100")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("REPOS", summary_data.get('total_repos', 0))
    with col2:
        st.metric("AI REPOS", summary_data.get('ai_repos', 0))
    with col3:
        st.metric("STARS", f"{summary_data.get('total_stars', 0):,}")
    
    if orgs:
        st.markdown(f"**Organizations:** `{', '.join(orgs)}`")
    
    if repos:
        st.markdown("#### Top Repositories")
        
        repo_df = pd.DataFrame(repos)
        
        if not repo_df.empty:
            display_cols = []
            if 'name' in repo_df.columns:
                display_cols.append('name')
            if 'stars' in repo_df.columns:
                display_cols.append('stars')
            if 'score' in repo_df.columns:
                display_cols.append('score')
            
            if display_cols:
                df_display = repo_df[display_cols].copy()
                df_display.columns = ['REPOSITORY', 'STARS', 'SCORE']
                st.dataframe(df_display, width='stretch', hide_index=True)


def render_documents(company_id):
    """Document listing with rich metadata and viewer."""
    docs_resp = fetch_documents(company_id)
    
    if not docs_resp or not docs_resp.get('documents'):
        st.info("No documents")
        return
    
    docs = docs_resp['documents']
    
    st.markdown(f"### Documents ({len(docs)} total)")
    
    # Build display data
    doc_data = []
    for doc in docs:
        doc_data.append({
            'TYPE': doc.get('filing_type', '—'),
            'DATE': str(doc.get('filing_date', '—')),
            'STATUS': doc.get('status', '—'),
            'WORDS': f"{doc.get('word_count', 0):,}",
            'CHUNKS': str(doc.get('total_chunks', 0)),
            'SECTIONS': str(doc.get('section_count', 0)),
            'id': doc.get('id')
        })
    
    if doc_data:
        df = pd.DataFrame(doc_data)
        display_df = df.drop(columns=['id'])
        
        # FIXED: Dynamic height based on number of rows
        table_height = min(max(len(display_df) * 35 + 50, 100), 350)
        st.dataframe(display_df, width='stretch', hide_index=True, height=table_height)
    
    # Section breakdown
    if docs and docs[0].get('sections_summary'):
        st.markdown("#### Section Breakdown (Latest Document)")
        sections = json.loads(docs[0]['sections_summary']) if isinstance(docs[0]['sections_summary'], str) else docs[0]['sections_summary']
        
        if sections:
            section_data = []
            for section_id, data in sections.items():
                section_data.append({
                    'SECTION': section_id.upper(),
                    'CHUNKS': str(data.get('chunk_count', 0)),
                    'WORDS': f"{data.get('total_words', 0):,}"
                })
            
            if section_data:
                df_sections = pd.DataFrame(section_data)
                st.dataframe(df_sections, width='stretch', hide_index=True)
    
    # Document Viewer
    st.markdown("---")
    st.markdown("### Document Viewer")
    
    doc_options = {f"{d['TYPE']} - {d['DATE']}": d['id'] for d in doc_data}
    selected_doc_label = st.selectbox("Select document to view", options=list(doc_options.keys()), key="doc_selector")
    
    if selected_doc_label:
        selected_doc_id = doc_options[selected_doc_label]
        render_document_viewer(selected_doc_id)


def render_document_viewer(document_id: str):
    """Expandable section viewer."""
    sections_resp = fetch_document_sections(document_id)
    
    if not sections_resp or not sections_resp.get('sections'):
        st.info("No sections available")
        return
    
    sections = sections_resp['sections']
    ticker = sections_resp.get('ticker', '—')
    filing_type = sections_resp.get('filing_type', '—')
    
    st.markdown(f"**{ticker} {filing_type}** - {len(sections)} sections")
    
    for section in sections:
        section_id = section['section_id']
        section_title = section['section_title']
        chunk_count = section.get('chunk_count', 0)
        word_count = section.get('word_count', 0)
        
        with st.expander(f"**{section_title}** ({chunk_count} chunks, {word_count:,} words)"):
            chunks_resp = fetch_section_chunks(document_id, section_id)
            
            if chunks_resp and chunks_resp.get('chunks'):
                chunks = chunks_resp['chunks']
                
                st.markdown(f"*{len(chunks)} chunks loaded*")
                
                for chunk in chunks:
                    st.markdown(f"**Chunk {chunk.get('chunk_index', 0)}** ({chunk.get('word_count', 0)} words)")
                    
                    content = chunk.get('content', '')
                    if content:
                        display_content = content[:1000] + "..." if len(content) > 1000 else content
                        st.markdown(f"<div style='background:{COLORS['bg_card']};padding:1rem;border-left:2px solid {COLORS['primary']};margin:0.5rem 0;font-size:0.85rem;line-height:1.6'>{display_content}</div>", unsafe_allow_html=True)
            else:
                st.info("No chunks available")


def page_collection():
    """Collection page."""
    st.markdown("# Evidence Collection")
    st.markdown(f"<p style='color:{COLORS['dim']}'>Trigger data pipelines</p>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        selected_tickers = st.multiselect(
            "COMPANIES (EMPTY = ALL)",
            options=['CAT', 'DE', 'UNH', 'HCA', 'ADP', 'PAYX', 'WMT', 'TGT', 'JPM', 'GS'],
            default=None
        )
    
    with col2:
        selected_pipelines = st.multiselect(
            "PIPELINES",
            options=["sec", "job", "patent", "github"],
            default=["job", "patent", "github"]
        )
    
    if st.button("START COLLECTION", type="primary"):
        if not selected_pipelines:
            st.error("Select at least one pipeline")
        else:
            with st.spinner("Triggering..."):
                result = trigger_backfill(
                    tickers=selected_tickers if selected_tickers else None,
                    pipelines=selected_pipelines
                )
                
                if result:
                    st.success("Collection started")
                    st.markdown(f"**Task ID:** `{result['task_id']}`")
                    st.markdown(f"**Tickers:** `{', '.join(result['tickers'])}`")
                    st.markdown(f"**Pipelines:** `{', '.join(result['pipelines'])}`")
                    st.info("Check API logs. Est: 20-30 min")
                else:
                    st.error("Failed")


def main():
    st.sidebar.markdown("# Navigation")
    
    page = st.sidebar.radio(
        "Select Page",
        ["Overview", "Company Detail", "Collection"],
        label_visibility="collapsed"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"<p style='color:{COLORS['dim']};font-size:0.7rem'>PE Org-AI-R v1.0</p>", unsafe_allow_html=True)
    
    if page == "Overview":
        page_overview()
    elif page == "Company Detail":
        page_company_detail()
    else:
        page_collection()


if __name__ == "__main__":
    main()