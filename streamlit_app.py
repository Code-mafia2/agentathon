"""
RetailIQ — Data Intelligence Dashboard
Streamlit frontend for the LangGraph analytics pipeline.
"""

import sys
import os
import tempfile
import time
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from dotenv import load_dotenv

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.loader import load_csv
from utils.state import RetailIQState
from graph.pipeline import build_graph

load_dotenv()

# ════════════════════════════════════════════════════════════
# PAGE CONFIG & THEME
# ════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="RetailIQ — Data Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ════════════════════════════════════════════════════════════
# CUSTOM CSS
# ════════════════════════════════════════════════════════════

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

    /* ── Global ────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    .stApp {
        background: linear-gradient(135deg, #0f0c29 0%, #1a1a2e 40%, #16213e 100%);
    }

    /* ── Hide default streamlit branding ────────── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* ── Hero title ─────────────────────────────── */
    .hero-title {
        font-size: 2.8rem;
        font-weight: 900;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 0;
        letter-spacing: -0.5px;
    }
    .hero-subtitle {
        font-size: 1.1rem;
        color: #8892b0;
        text-align: center;
        margin-top: 0.25rem;
        margin-bottom: 2rem;
        font-weight: 400;
    }

    /* ── Glass card ─────────────────────────────── */
    .glass-card {
        background: rgba(255, 255, 255, 0.04);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 1.5rem;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        margin-bottom: 1rem;
        transition: all 0.3s ease;
    }
    .glass-card:hover {
        background: rgba(255, 255, 255, 0.06);
        border-color: rgba(255, 255, 255, 0.12);
        transform: translateY(-2px);
        box-shadow: 0 8px 32px rgba(102, 126, 234, 0.15);
    }

    /* ── Section header ────────────────────────── */
    .section-header {
        font-size: 1.3rem;
        font-weight: 700;
        color: #e6f1ff;
        margin-bottom: 0.75rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    .section-tag {
        font-size: 0.7rem;
        font-weight: 600;
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        padding: 3px 10px;
        border-radius: 20px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    /* ── Metric card ───────────────────────────── */
    .metric-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
        transition: all 0.3s ease;
    }
    .metric-card:hover {
        background: rgba(102, 126, 234, 0.08);
        border-color: rgba(102, 126, 234, 0.3);
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: 800;
        color: #667eea;
        margin: 0;
    }
    .metric-label {
        font-size: 0.75rem;
        font-weight: 500;
        color: #8892b0;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.25rem;
    }

    /* ── Rank list ─────────────────────────────── */
    .rank-item {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.7rem 1rem;
        margin-bottom: 0.4rem;
        border-radius: 10px;
        transition: all 0.2s ease;
    }
    .rank-item:hover {
        background: rgba(102, 126, 234, 0.08);
    }
    .rank-item.top {
        background: rgba(102, 126, 234, 0.1);
        border-left: 3px solid #667eea;
    }
    .rank-number {
        font-size: 0.85rem;
        font-weight: 700;
        color: #667eea;
        min-width: 28px;
    }
    .rank-name {
        font-size: 0.95rem;
        font-weight: 500;
        color: #ccd6f6;
        flex: 1;
        margin-left: 0.5rem;
    }
    .rank-value {
        font-size: 0.95rem;
        font-weight: 600;
        color: #64ffda;
        text-align: right;
    }

    /* ── Insight block ─────────────────────────── */
    .insight-block {
        background: linear-gradient(135deg, rgba(102,126,234,0.1) 0%, rgba(118,75,162,0.1) 100%);
        border: 1px solid rgba(102, 126, 234, 0.2);
        border-radius: 16px;
        padding: 2rem;
        color: #ccd6f6;
        font-size: 1.05rem;
        line-height: 1.8;
        font-weight: 400;
    }

    /* ── Data quality badge ────────────────────── */
    .dq-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 8px;
        padding: 0.5rem 0.85rem;
        margin: 0.25rem;
        font-size: 0.85rem;
        color: #8892b0;
    }
    .dq-badge .dq-count {
        font-weight: 700;
        color: #ffd166;
        font-size: 1rem;
    }

    /* ── Sidebar ───────────────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
        border-right: 1px solid rgba(255,255,255,0.06);
    }
    [data-testid="stSidebar"] .stMarkdown h1 {
        color: #e6f1ff;
    }

    /* ── Upload box ────────────────────────────── */
    [data-testid="stFileUploader"] {
        border: 2px dashed rgba(102, 126, 234, 0.3);
        border-radius: 16px;
        padding: 1rem;
        transition: border-color 0.3s ease;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: rgba(102, 126, 234, 0.6);
    }

    /* ── Spinner ───────────────────────────────── */
    .stSpinner > div {
        border-top-color: #667eea !important;
    }

    /* ── Status badge ──────────────────────────── */
    .status-pass {
        background: rgba(100, 255, 218, 0.1);
        border: 1px solid rgba(100, 255, 218, 0.3);
        color: #64ffda;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .status-fallback {
        background: rgba(255, 209, 102, 0.1);
        border: 1px solid rgba(255, 209, 102, 0.3);
        color: #ffd166;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 600;
    }

    /* ── Divider ───────────────────────────────── */
    .styled-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(102,126,234,0.3), transparent);
        margin: 2rem 0;
        border: none;
    }

    /* ── Tabs override ─────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0.5rem;
        background: rgba(255,255,255,0.02);
        border-radius: 12px;
        padding: 0.3rem;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        color: #8892b0;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background: rgba(102, 126, 234, 0.15) !important;
        color: #667eea !important;
    }

    /* ── Plotly chart container ──────────────── */
    .js-plotly-plot {
        border-radius: 12px;
        overflow: hidden;
    }
</style>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════

def parse_ranked_list(text: str):
    """Parse a ranked Q result string into list of dicts."""
    items = []
    if not text:
        return items
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or not line[0].isdigit():
            continue
        # "1. Category: ₹1,234.56" or "1. Region: 12.34 days"
        parts = line.split(". ", 1)
        if len(parts) != 2:
            continue
        rank = parts[0].strip()
        rest = parts[1]
        name_val = rest.split(": ", 1)
        if len(name_val) == 2:
            items.append({
                "rank": int(rank),
                "name": name_val[0].strip(),
                "value_str": name_val[1].strip(),
            })
    return items


def parse_revenue_value(val_str: str) -> float:
    """Parse ₹1,234,567.89 → float."""
    cleaned = val_str.replace("₹", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def parse_days_value(val_str: str) -> float:
    """Parse '12.34 days' → float."""
    cleaned = val_str.replace("days", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def parse_pct_value(val_str: str) -> float:
    """Parse '12.34%' → float."""
    cleaned = val_str.replace("%", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def run_pipeline(uploaded_file) -> dict:
    """Run the LangGraph pipeline on an uploaded CSV file."""
    # Write uploaded file to temp location
    with tempfile.NamedTemporaryFile(
        delete=False, suffix=".csv", mode="wb"
    ) as tmp:
        tmp.write(uploaded_file.getvalue())
        tmp_path = tmp.name

    try:
        raw_df = load_csv(tmp_path)

        initial_state: RetailIQState = {
            "raw_df":        raw_df,
            "df":            raw_df.copy(),
            "issues":        {},
            "q1":            None,
            "q2":            None,
            "q4":            None,
            "q5":            None,
            "output":        None,
            "errors":        [],
            "fallback_used": [],
            "audit_status":  None,
        }

        graph = build_graph()
        final_state = graph.invoke(initial_state)
        return final_state
    finally:
        os.unlink(tmp_path)


# ════════════════════════════════════════════════════════════
# CHART BUILDERS
# ════════════════════════════════════════════════════════════

CHART_COLORS = [
    "#667eea", "#764ba2", "#f093fb", "#64ffda",
    "#ffd166", "#ef476f", "#06d6a0", "#118ab2",
]

CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter", color="#8892b0"),
    margin=dict(l=20, r=20, t=40, b=20),
    hoverlabel=dict(
        bgcolor="#1a1a2e",
        font_size=13,
        font_family="Inter",
        bordercolor="rgba(102,126,234,0.3)",
    ),
)


def build_revenue_chart(items):
    """Horizontal bar chart for Q1 revenue by category."""
    names = [i["name"] for i in reversed(items)]
    values = [parse_revenue_value(i["value_str"]) for i in reversed(items)]

    fig = go.Figure(go.Bar(
        x=values, y=names, orientation="h",
        marker=dict(
            color=values,
            colorscale=[[0, "#764ba2"], [0.5, "#667eea"], [1, "#64ffda"]],
            cornerradius=6,
        ),
        hovertemplate="<b>%{y}</b><br>Revenue: ₹%{x:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        height=max(280, len(items) * 48),
        title=dict(text="Revenue by Category", font=dict(size=15, color="#e6f1ff")),
        xaxis=dict(
            showgrid=True, gridcolor="rgba(255,255,255,0.04)",
            tickformat=",.0f", tickprefix="₹",
        ),
        yaxis=dict(showgrid=False),
    )
    return fig


def build_delivery_chart(items):
    """Bar chart for Q2 avg delivery days."""
    names = [i["name"] for i in items]
    values = [parse_days_value(i["value_str"]) for i in items]

    colors = []
    max_val = max(values) if values else 1
    for v in values:
        ratio = v / max_val
        if ratio > 0.8:
            colors.append("#ef476f")
        elif ratio > 0.5:
            colors.append("#ffd166")
        else:
            colors.append("#64ffda")

    fig = go.Figure(go.Bar(
        x=names, y=values,
        marker=dict(color=colors, cornerradius=6),
        hovertemplate="<b>%{x}</b><br>Avg: %{y:.2f} days<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        height=320,
        title=dict(text="Avg Delivery Days by Region", font=dict(size=15, color="#e6f1ff")),
        xaxis=dict(showgrid=False),
        yaxis=dict(
            showgrid=True, gridcolor="rgba(255,255,255,0.04)",
            ticksuffix=" d",
        ),
    )
    return fig


def build_return_rate_chart(items):
    """Donut chart for Q4 return rate by payment method."""
    names = [i["name"] for i in items]
    values = [parse_pct_value(i["value_str"]) for i in items]

    fig = go.Figure(go.Pie(
        labels=names, values=values,
        hole=0.55,
        marker=dict(colors=CHART_COLORS[:len(names)]),
        textinfo="label+percent",
        textfont=dict(size=12, color="#e6f1ff"),
        hovertemplate="<b>%{label}</b><br>Return rate: %{value:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        height=360,
        title=dict(text="Return Rate by Payment Method", font=dict(size=15, color="#e6f1ff")),
        showlegend=True,
        legend=dict(
            font=dict(size=11, color="#8892b0"),
            bgcolor="rgba(0,0,0,0)",
        ),
    )
    return fig


def build_accuracy_gauge(issues, raw_df):
    """Data accuracy score gauge."""
    total_issues = (
        issues.get("duplicate_order_ids", 0)
        + issues.get("quantity_outliers", 0)
        + issues.get("price_format_errors", 0)
        + issues.get("invalid_discounts", 0)
    )
    null_cells = issues.get("total_null_cells", 0)
    
    if raw_df is not None and not raw_df.empty:
        total_cells = len(raw_df) * len(raw_df.columns)
        error_rate = (total_issues + null_cells) / total_cells
        accuracy = max(0.0, 100.0 - (error_rate * 100.0))
    else:
        accuracy = 100.0

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=round(accuracy, 2),
        title=dict(text="Overall Accuracy", font=dict(size=14, color="#e6f1ff")),
        number=dict(suffix="%", font=dict(size=32, color="#e6f1ff")),
        gauge=dict(
            axis=dict(range=[0, 100], tickcolor="rgba(255,255,255,0.1)"),
            bar=dict(color="#667eea"),
            bgcolor="rgba(255,255,255,0.03)",
            bordercolor="rgba(255,255,255,0.06)",
            steps=[
                dict(range=[0, 40], color="rgba(239,71,111,0.15)"),
                dict(range=[40, 70], color="rgba(255,209,102,0.15)"),
                dict(range=[70, 100], color="rgba(100,255,218,0.15)"),
            ],
            threshold=dict(
                line=dict(color="#64ffda", width=3),
                thickness=0.8, value=accuracy,
            ),
        ),
    ))
    fig.update_layout(
        **CHART_LAYOUT,
        height=260,
    )
    return fig


# ════════════════════════════════════════════════════════════
# MAIN UI
# ════════════════════════════════════════════════════════════

# ── Sidebar ────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="text-align:center; padding: 1rem 0;">
        <div style="font-size: 2.5rem;">📊</div>
        <div style="font-size: 1.3rem; font-weight: 700; color: #e6f1ff; margin-top: 0.5rem;">
            RetailIQ
        </div>
        <div style="font-size: 0.8rem; color: #8892b0; margin-top: 0.25rem;">
            Data Intelligence Engine
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Upload Order Data (CSV)",
        type=["csv"],
        help="Upload your e-commerce order dataset. Expected columns: order_id, date, product_category, product_name, quantity, unit_price, discount_percent, customer_region, payment_method, delivery_days, return_status"
    )

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size: 0.8rem; color: #8892b0; padding: 0 0.5rem;">
        <div style="font-weight: 600; color: #ccd6f6; margin-bottom: 0.5rem;">Pipeline Nodes</div>
        <div style="margin-bottom: 0.3rem;">🔍 &nbsp;Validator — Data audit</div>
        <div style="margin-bottom: 0.3rem;">🧹 &nbsp;Cleaner — Data cleaning</div>
        <div style="margin-bottom: 0.3rem;">⚙️ &nbsp;Feature — Revenue compute</div>
        <div style="margin-bottom: 0.3rem;">📈 &nbsp;Analytics — Q1, Q2, Q4</div>
        <div style="margin-bottom: 0.3rem;">✅ &nbsp;Auditor — Validation</div>
        <div style="margin-bottom: 0.3rem;">🤖 &nbsp;Insight — AI summary</div>
        <div style="margin-bottom: 0.3rem;">📝 &nbsp;Formatter — Output</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    st.markdown("""
    <div style="font-size: 0.7rem; color: #4a5568; text-align: center; padding-top: 0.5rem;">
        Powered by LangGraph + Pandas<br>
        Team Heapify — Agentathon 2026
    </div>
    """, unsafe_allow_html=True)


# ── Main Content ───────────────────────────────────────────
st.markdown('<div class="hero-title">RetailIQ Intelligence Dashboard</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="hero-subtitle">Autonomous data pipeline · Real-time analytics · AI-powered insights</div>',
    unsafe_allow_html=True,
)

if not uploaded_file:
    # ── Landing / empty state ──────────────────────────────
    st.markdown("")
    cols = st.columns([1, 2, 1])
    with cols[1]:
        st.markdown("""
        <div class="glass-card" style="text-align: center; padding: 3rem 2rem;">
            <div style="font-size: 4rem; margin-bottom: 1rem;">📂</div>
            <div style="font-size: 1.2rem; font-weight: 600; color: #e6f1ff; margin-bottom: 0.5rem;">
                Upload your dataset to begin
            </div>
            <div style="font-size: 0.9rem; color: #8892b0; line-height: 1.6;">
                Drop a CSV file in the sidebar to run the full<br>
                data intelligence pipeline — cleaning, analytics,<br>
                and AI-generated business insights.
            </div>
            <div style="margin-top: 1.5rem;">
                <span class="dq-badge">📄 CSV format</span>
                <span class="dq-badge">🔄 Auto-cleaning</span>
                <span class="dq-badge">🤖 AI insights</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

else:
    # ── Run pipeline ───────────────────────────────────────
    with st.spinner("🚀 Running RetailIQ pipeline..."):
        start_time = time.time()
        final_state = run_pipeline(uploaded_file)
        elapsed = time.time() - start_time

    issues = final_state.get("issues", {})
    q1_items = parse_ranked_list(final_state.get("q1", ""))
    q2_items = parse_ranked_list(final_state.get("q2", ""))
    q4_items = parse_ranked_list(final_state.get("q4", ""))
    fallbacks = final_state.get("fallback_used", [])
    errors = final_state.get("errors", [])
    df = final_state.get("df")

    # ── Pipeline status bar ────────────────────────────────
    status_cols = st.columns([2, 1, 1, 1])
    with status_cols[0]:
        fb_tag = (
            f'<span class="status-fallback">⚠ {len(fallbacks)} fallback(s)</span>'
            if fallbacks
            else '<span class="status-pass">✓ All nodes passed</span>'
        )
        st.markdown(
            f'<div style="display:flex; align-items:center; gap:0.75rem;">'
            f'<span style="font-size:0.85rem; color:#8892b0;">Pipeline status</span>'
            f'{fb_tag}'
            f'</div>',
            unsafe_allow_html=True,
        )
    with status_cols[1]:
        st.markdown(
            f'<div style="font-size:0.85rem; color:#8892b0;">'
            f'⏱ {elapsed:.1f}s execution</div>',
            unsafe_allow_html=True,
        )
    with status_cols[2]:
        rows_count = len(df) if df is not None else 0
        st.markdown(
            f'<div style="font-size:0.85rem; color:#8892b0;">'
            f'📊 {rows_count:,} clean rows</div>',
            unsafe_allow_html=True,
        )
    with status_cols[3]:
        raw_rows = len(final_state.get("raw_df")) if final_state.get("raw_df") is not None else 0
        st.markdown(
            f'<div style="font-size:0.85rem; color:#8892b0;">'
            f'📋 {raw_rows:,} raw rows</div>',
            unsafe_allow_html=True,
        )

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════
    # Q3 — DATA QUALITY OVERVIEW (top of page)
    # ══════════════════════════════════════════════════════
    st.markdown(
        '<div class="section-header">🔍 Data Quality Audit '
        '<span class="section-tag">Q3</span></div>',
        unsafe_allow_html=True,
    )

    dq_cols = st.columns(6)
    dq_metrics = [
        ("Duplicate IDs", issues.get("duplicate_order_ids", 0), "🔁"),
        ("Quantity Outliers", issues.get("quantity_outliers", 0), "📈"),
        ("Price Errors", issues.get("price_format_errors", 0), "💰"),
        ("Invalid Discounts", issues.get("invalid_discounts", 0), "🏷️"),
        ("Total Nulls", issues.get("total_null_cells", 0), "⚫"),
    ]
    for i, (label, val, icon) in enumerate(dq_metrics):
        with dq_cols[i]:
            st.markdown(f"""
            <div class="metric-card">
                <div style="font-size:1.5rem;">{icon}</div>
                <div class="metric-value">{val:,}</div>
                <div class="metric-label">{label}</div>
            </div>
            """, unsafe_allow_html=True)
    with dq_cols[5]:
        st.plotly_chart(build_accuracy_gauge(issues, final_state.get("raw_df")), use_container_width=True, config={"displayModeBar": False})

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════
    # Q1 & Q2 — REVENUE + DELIVERY (side by side)
    # ══════════════════════════════════════════════════════
    col_q1, col_q2 = st.columns(2)

    with col_q1:
        st.markdown(
            '<div class="section-header">💰 Revenue by Category '
            '<span class="section-tag">Q1</span></div>',
            unsafe_allow_html=True,
        )
        if q1_items:
            st.plotly_chart(
                build_revenue_chart(q1_items),
                use_container_width=True,
                config={"displayModeBar": False},
            )
            # Ranked list below chart
            for item in q1_items:
                top_class = "top" if item["rank"] == 1 else ""
                st.markdown(f"""
                <div class="rank-item {top_class}">
                    <span class="rank-number">#{item['rank']}</span>
                    <span class="rank-name">{item['name']}</span>
                    <span class="rank-value">{item['value_str']}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No revenue data available")

    with col_q2:
        st.markdown(
            '<div class="section-header">🚚 Delivery Performance '
            '<span class="section-tag">Q2</span></div>',
            unsafe_allow_html=True,
        )
        if q2_items:
            st.plotly_chart(
                build_delivery_chart(q2_items),
                use_container_width=True,
                config={"displayModeBar": False},
            )
            for item in q2_items:
                top_class = "top" if item["rank"] == 1 else ""
                st.markdown(f"""
                <div class="rank-item {top_class}">
                    <span class="rank-number">#{item['rank']}</span>
                    <span class="rank-name">{item['name']}</span>
                    <span class="rank-value">{item['value_str']}</span>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No delivery data available")

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════
    # Q4 — RETURN RATES
    # ══════════════════════════════════════════════════════
    col_q4_chart, col_q4_list = st.columns([1, 1])

    with col_q4_chart:
        st.markdown(
            '<div class="section-header">🔄 Return Rate Analysis '
            '<span class="section-tag">Q4</span></div>',
            unsafe_allow_html=True,
        )
        if q4_items:
            st.plotly_chart(
                build_return_rate_chart(q4_items),
                use_container_width=True,
                config={"displayModeBar": False},
            )

    with col_q4_list:
        st.markdown(
            '<div class="section-header" style="visibility:hidden;">spacer</div>',
            unsafe_allow_html=True,
        )
        if q4_items:
            for item in q4_items:
                pct = parse_pct_value(item["value_str"])
                bar_width = min(pct * 6, 100)
                bar_color = "#ef476f" if pct > 11 else ("#ffd166" if pct > 9 else "#64ffda")
                st.markdown(f"""
                <div class="rank-item" style="flex-direction: column; align-items: stretch; padding: 0.8rem 1rem;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.4rem;">
                        <span class="rank-name" style="margin-left:0;">{item['name']}</span>
                        <span class="rank-value" style="color:{bar_color};">{item['value_str']}</span>
                    </div>
                    <div style="height: 6px; background: rgba(255,255,255,0.05); border-radius: 3px; overflow: hidden;">
                        <div style="height: 100%; width: {bar_width}%; background: {bar_color}; border-radius: 3px;
                             transition: width 0.6s ease;"></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════
    # Q5 — AI INSIGHT
    # ══════════════════════════════════════════════════════
    st.markdown(
        '<div class="section-header">🤖 AI Business Insight '
        '<span class="section-tag">Q5</span></div>',
        unsafe_allow_html=True,
    )
    q5_text = final_state.get("q5", "No insight generated.")
    st.markdown(f'<div class="insight-block">{q5_text}</div>', unsafe_allow_html=True)

    st.markdown('<div class="styled-divider"></div>', unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════
    # RAW OUTPUT + PIPELINE LOG (tabs)
    # ══════════════════════════════════════════════════════
    tab_output, tab_log, tab_data = st.tabs(["📄 Raw Output", "🔧 Pipeline Log", "📊 Data Preview"])

    with tab_output:
        output_text = final_state.get("output", "")
        st.code(output_text, language="text")

        # Download button
        st.download_button(
            label="⬇️ Download heapify.txt",
            data=output_text,
            file_name="heapify.txt",
            mime="text/plain",
        )

    with tab_log:
        if fallbacks:
            st.markdown(
                '<div style="font-weight:600; color:#ffd166; margin-bottom:0.5rem;">'
                '⚠️ Fallbacks Triggered</div>',
                unsafe_allow_html=True,
            )
            for fb in fallbacks:
                st.markdown(f"""
                <div class="dq-badge" style="display:block; margin-bottom:0.3rem;">
                    ⚡ {fb}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown(
                '<span class="status-pass" style="display:inline-block; margin-bottom:0.5rem;">'
                '✓ No fallbacks triggered</span>',
                unsafe_allow_html=True,
            )

        if errors:
            st.markdown(
                '<div style="font-weight:600; color:#ef476f; margin-top:1rem; margin-bottom:0.5rem;">'
                '❌ Errors</div>',
                unsafe_allow_html=True,
            )
            for err in errors:
                st.markdown(f"""
                <div class="dq-badge" style="display:block; margin-bottom:0.3rem; border-color: rgba(239,71,111,0.3);">
                    🔴 {err}
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown(
                '<span class="status-pass" style="display:inline-block;">'
                '✓ No errors</span>',
                unsafe_allow_html=True,
            )

    with tab_data:
        if df is not None:
            st.markdown(
                f'<div style="font-size:0.85rem; color:#8892b0; margin-bottom:0.5rem;">'
                f'Showing cleaned data · {len(df):,} rows × {len(df.columns)} columns</div>',
                unsafe_allow_html=True,
            )
            st.dataframe(
                df.head(100),
                use_container_width=True,
                height=400,
            )
        else:
            st.info("No data available")
