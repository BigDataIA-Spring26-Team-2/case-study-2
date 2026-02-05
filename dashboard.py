"""
PE Org-AI-R Platform Dashboard
Minimal | Dark | Retro | Professional
"""
import streamlit as st
import httpx
import plotly.graph_objects as go
import pandas as pd

# ============================================================================
# CONFIG
# ============================================================================

API_BASE = "http://localhost:8000/api/v1"

COLORS = {
    'primary': '#00ff41',
    'secondary': '#ff6b35',
    'accent': '#00d9ff',
    'warning': '#ffd23f',
    'bg_dark': '#0d0d0d',      # Pure black
    'bg_card': '#1a1a1a',      # Dark grey
    'text': '#e0e6ed',
    'dim': '#6c757d'
}

# ============================================================================
# STYLING
# ============================================================================

st.set_page_config(
    page_title="PE Org-AI-R",
    layout="wide",
    initial_sidebar_state="collapsed"
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
    
    h1, h2, h3 {{
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
</style>
""", unsafe_allow_html=True)

# ============================================================================
# API
# ============================================================================

@st.cache_data(ttl=300)
def fetch_stats():
    try:
        return httpx.get(f"{API_BASE}/evidence/stats", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=300)
def fetch_company_evidence(company_id):
    try:
        return httpx.get(f"{API_BASE}/evidence/companies/{company_id}/evidence", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=300)
def fetch_signal_detail(company_id, category):
    try:
        return httpx.get(f"{API_BASE}/signals/companies/{company_id}/signals/{category}", timeout=10).json()
    except:
        return None

@st.cache_data(ttl=300)
def fetch_companies():
    try:
        return httpx.get("http://localhost:8000/api/v1/companies", timeout=10).json()
    except:
        return None

# ============================================================================
# COMPONENTS
# ============================================================================

def render_gauge(score, title):
    """Minimal gauge chart."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=score,
        title={'text': title, 'font': {'size': 14, 'color': COLORS['dim']}},
        number={'font': {'size': 48, 'color': COLORS['primary'], 'family': 'JetBrains Mono'}},
        gauge={
            'axis': {'range': [0, 100], 'tickcolor': COLORS['dim']},
            'bar': {'color': COLORS['primary']},
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
        height=250,
        margin=dict(l=20, r=20, t=40, b=20),
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


def render_pie(values, names, title):
    """Donut chart."""
    fig = go.Figure(go.Pie(
        values=values,
        labels=names,
        hole=0.6,
        marker=dict(
            colors=[COLORS['primary'], COLORS['secondary'], COLORS['accent']],
            line=dict(color=COLORS['bg_dark'], width=2)
        )
    ))
    
    fig.update_layout(
        title={'text': title, 'font': {'size': 14, 'color': COLORS['dim']}},
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
        paper_bgcolor=COLORS['bg_dark'],
        plot_bgcolor=COLORS['bg_dark'],
        font={'family': 'JetBrains Mono', 'color': COLORS['text']},
        showlegend=True
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
    
    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("COMPANIES", stats['overall']['companies_processed'])
    
    with col2:
        st.metric("DOCUMENTS", f"{stats['overall']['total_documents']:,}")
    
    with col3:
        st.metric("CHUNKS", f"{stats['overall']['total_chunks']:,}")
    
    with col4:
        avg = stats['overall']['avg_composite_score']
        st.metric("AVG SCORE", f"{avg:.1f}" if avg else "N/A")
    
    st.markdown("---")
    
    # Company rankings
    st.markdown("### Company Rankings")
    
    df = pd.DataFrame(stats['by_company'])
    df = df.sort_values('composite_score', ascending=False, na_position='last')
    
    # Clean up display
    df_display = df[['ticker', 'documents', 'hiring_score', 'patent_score', 'github_score', 'composite_score']].copy()
    df_display.columns = ['TICKER', 'DOCS', 'HIRING', 'PATENT', 'GITHUB', 'COMPOSITE']
    df_display = df_display.fillna('-')
    
    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        height=400
    )
    
    # Chart
    st.markdown("### Score Distribution")
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Hiring',
        x=df['ticker'],
        y=df['hiring_score'].fillna(0),
        marker_color=COLORS['primary']
    ))
    
    fig.add_trace(go.Bar(
        name='Patent',
        x=df['ticker'],
        y=df['patent_score'].fillna(0),
        marker_color=COLORS['secondary']
    ))
    
    fig.add_trace(go.Bar(
        name='GitHub',
        x=df['ticker'],
        y=df['github_score'].fillna(0),
        marker_color=COLORS['accent']
    ))
    
    fig.update_layout(
        barmode='group',
        height=400,
        margin=dict(l=20, r=20, t=20, b=60),
        paper_bgcolor=COLORS['bg_dark'],
        plot_bgcolor=COLORS['bg_dark'],
        font={'family': 'JetBrains Mono', 'color': COLORS['text']},
        xaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
        yaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)', 'range': [0, 100]},
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)


def page_company_detail():
    """Company deep dive."""
    st.markdown("# Company Analysis")
    
    # Get companies
    companies_resp = fetch_companies()
    if not companies_resp:
        st.error("Cannot load companies")
        return
    
    companies = companies_resp.get('items', [])
    ticker_to_id = {c['ticker']: c['id'] for c in companies}
    
    # Select company
    selected_ticker = st.selectbox(
        "SELECT COMPANY",
        options=sorted(ticker_to_id.keys()),
        key="company_select"
    )
    
    if not selected_ticker:
        return
    
    company_id = ticker_to_id[selected_ticker]
    evidence = fetch_company_evidence(company_id)
    
    if not evidence:
        st.warning(f"No data for {selected_ticker}")
        return
    
    summary = evidence['summary']
    
    # Header
    st.markdown(f"## {selected_ticker}")
    st.markdown(f"<p style='color:{COLORS['dim']}'>Last updated: {summary.get('last_updated', 'Never')}</p>", unsafe_allow_html=True)
    
    # Composite score
    col1, col2 = st.columns([1, 3])
    
    with col1:
        composite = summary.get('composite_score')
        if composite:
            st.plotly_chart(render_gauge(composite, "COMPOSITE"), use_container_width=True)
    
    with col2:
        # Component scores
        col2a, col2b, col2c = st.columns(3)
        
        with col2a:
            h = summary.get('hiring_score')
            st.metric("HIRING", f"{h:.1f}" if h else "N/A", delta="30%", delta_color="off")
        
        with col2b:
            p = summary.get('patent_score')
            st.metric("PATENT", f"{p:.1f}" if p else "N/A", delta="25%", delta_color="off")
        
        with col2c:
            g = summary.get('github_score')
            st.metric("GITHUB", f"{g:.1f}" if g else "N/A", delta="20%", delta_color="off")
    
    st.markdown("---")
    
    # Tabs for each signal
    tab1, tab2, tab3, tab4 = st.tabs(["HIRING", "PATENT", "GITHUB", "DOCUMENTS"])
    
    with tab1:
        render_hiring_signal(company_id)
    
    with tab2:
        render_patent_signal(company_id)
    
    with tab3:
        render_github_signal(company_id)
    
    with tab4:
        render_documents(evidence)


def render_hiring_signal(company_id):
    """Hiring signal visualization."""
    data = fetch_signal_detail(company_id, "hiring_signal")
    
    if not data:
        st.info("No hiring data")
        return
    
    summary = data.get('summary', {})
    seniority = data.get('seniority_breakdown', {})
    
    # Score header
    st.markdown(f"### Score: {data['score']:.1f}/100")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("TOTAL JOBS", summary.get('total_jobs', 0))
    
    with col2:
        st.metric("AI JOBS", summary.get('ai_jobs', 0))
    
    with col3:
        ratio = summary.get('ai_ratio', 0)
        st.metric("AI RATIO", f"{ratio*100:.1f}%")
    
    # Seniority breakdown
    if seniority and any(seniority.values()):
        st.markdown("#### Seniority Distribution")
        st.plotly_chart(
            render_bar(seniority, "Roles by Level"),
            use_container_width=True
        )
    
    # Phase
    phase = data.get('metadata', {}).get('phase', 'UNKNOWN')
    st.markdown(f"**Hiring Phase:** `{phase}`")


def render_patent_signal(company_id):
    """Patent signal visualization."""
    data = fetch_signal_detail(company_id, "patent")
    
    if not data:
        st.info("No patent data")
        return
    
    summary = data.get('summary', {})
    by_year = data.get('by_year', {})
    patents = data.get('patents', [])
    
    # Score header
    st.markdown(f"### Score: {data['score']:.1f}/100")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("TOTAL PATENTS", summary.get('total_patents', 0))
    
    with col2:
        st.metric("AI PATENTS", summary.get('ai_patents', 0))
    
    with col3:
        ratio = summary.get('ai_ratio', 0)
        st.metric("AI RATIO", f"{ratio*100:.1f}%")
    
    # Timeline
    if by_year:
        st.markdown("#### Patent Timeline")
        
        years = sorted(by_year.keys())
        ai_counts = [by_year[y]['ai'] for y in years]
        total_counts = [by_year[y]['total'] for y in years]
        
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=years,
            y=total_counts,
            name='Total',
            line=dict(color=COLORS['dim'], width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=years,
            y=ai_counts,
            name='AI Patents',
            line=dict(color=COLORS['primary'], width=3),
            fill='tozeroy'
        ))
        
        fig.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=40),
            paper_bgcolor=COLORS['bg_dark'],
            plot_bgcolor=COLORS['bg_dark'],
            font={'family': 'JetBrains Mono', 'color': COLORS['text']},
            xaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
            yaxis={'gridcolor': 'rgba(108, 117, 125, 0.2)'},
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Top patents
    if patents:
        st.markdown("#### Top AI Patents")
        
        patent_df = pd.DataFrame(patents[:10])
        patent_df = patent_df[['title', 'grant_date', 'score']].copy()
        patent_df.columns = ['TITLE', 'DATE', 'SCORE']
        patent_df['SCORE'] = patent_df['SCORE'].apply(lambda x: f"{x:.3f}")
        
        st.dataframe(patent_df, use_container_width=True, hide_index=True)


def render_github_signal(company_id):
    """GitHub signal visualization."""
    data = fetch_signal_detail(company_id, "github")
    
    if not data:
        st.info("No GitHub data")
        return
    
    summary = data.get('summary', {})
    repos = data.get('top_repos', [])
    orgs = data.get('organizations', [])
    
    # Score header
    st.markdown(f"### Score: {data['score']:.1f}/100")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("REPOS", summary.get('total_repos', 0))
    
    with col2:
        st.metric("AI REPOS", summary.get('ai_repos', 0))
    
    with col3:
        st.metric("STARS", f"{summary.get('total_stars', 0):,}")
    
    # Organizations
    if orgs:
        st.markdown(f"**Organizations:** `{', '.join(orgs)}`")
    
    # Top repos
    if repos:
        st.markdown("#### Top Repositories")
        
        repo_df = pd.DataFrame(repos)
        repo_df = repo_df[['name', 'stars', 'score']].copy()
        repo_df.columns = ['REPOSITORY', 'STARS', 'SCORE']
        
        st.dataframe(repo_df, use_container_width=True, hide_index=True)


def render_documents(evidence):
    """Document listing."""
    docs = evidence.get('recent_documents', [])
    
    if not docs:
        st.info("No documents")
        return
    
    st.markdown(f"### Documents ({evidence['summary']['total_documents']} total)")
    
    doc_df = pd.DataFrame(docs)
    doc_df = doc_df[['filing_type', 'filing_date', 'status', 'word_count']].copy()
    doc_df.columns = ['TYPE', 'DATE', 'STATUS', 'WORDS']
    doc_df['WORDS'] = doc_df['WORDS'].apply(lambda x: f"{x:,}")
    
    st.dataframe(doc_df, use_container_width=True, hide_index=True)

# ============================================================================
# MAIN
# ============================================================================

def main():
    # Navigation
    st.sidebar.markdown("# NAVIGATION")
    
    page = st.sidebar.radio(
        "Navigation",
        ["OVERVIEW", "COMPANY DETAIL"],
        label_visibility="hidden"
    )
    
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"<p style='color:{COLORS['dim']};font-size:0.7rem'>PE Org-AI-R v1.0</p>", unsafe_allow_html=True)
    
    # Render page
    if page == "OVERVIEW":
        page_overview()
    else:
        page_company_detail()


if __name__ == "__main__":
    main()