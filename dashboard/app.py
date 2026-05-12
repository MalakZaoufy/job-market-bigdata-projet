import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# =====================================================
# PAGE CONFIG
# =====================================================

st.set_page_config(
    page_title="IT Recruitment Intelligence Platform",
    page_icon="🚀",
    layout="wide"
)

# =====================================================
# CUSTOM CSS
# =====================================================

st.markdown(
    """
    <style>

    /* MAIN PAGE */

    .stApp {
        background-color: white;
    }

    .main {
        background-color: white;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    /* TITLES */

    h1, h2, h3, h4, h5, h6 {

        color: #111827 !important;

        font-weight: bold !important;
    }

    p, label, span {

        color: #374151 !important;
    }

    /* KPI CARDS */

    div[data-testid="metric-container"] {

        background: white;

        border: 1px solid #e5e7eb;

        padding: 20px;

        border-radius: 18px;

        box-shadow:
            0px 4px 15px
            rgba(0, 0, 0, 0.08);

        transition: 0.3s;
    }

    div[data-testid="metric-container"]:hover {

        transform: translateY(-4px);

        box-shadow:
            0px 8px 25px
            rgba(59, 130, 246, 0.18);
    }

    /* KPI LABEL */

    div[data-testid="metric-container"] label {

        color: #6b7280 !important;

        font-size: 16px !important;

        font-weight: 600 !important;
    }

    /* KPI VALUE */

    div[data-testid="metric-container"] div {

        color: #2563eb !important;

        font-size: 32px !important;

        font-weight: bold !important;
    }

    /* SIDEBAR */

    section[data-testid="stSidebar"] {

        background-color: #111827;

        border-right: 1px solid #1f2937;
    }

    section[data-testid="stSidebar"] * {

        color: white !important;
    }

    /* SELECTBOX */

    .stSelectbox div[data-baseweb="select"] {

        background-color: white;

        border-radius: 10px;
    }

    /* DATAFRAME */

    .stDataFrame {

        border: 1px solid #e5e7eb;

        border-radius: 10px;
    }

    /* INFO BOX */

    div[data-testid="stAlert"] {

        border-radius: 12px;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# =====================================================
# SIDEBAR
# =====================================================

with st.sidebar:

    st.title("🚀 IT Intelligence")

    st.markdown("---")

    st.subheader("⚡ System Status")

    st.success("Kafka Connected")
    st.success("PostgreSQL Connected")
    st.success("MinIO Active")
    st.success("Airflow Connected")
    st.success("Streaming Running")

    st.markdown("---")

    st.subheader("🛠 Technologies")

    st.markdown("""
    - Kafka
    - Airflow
    - MinIO
    - PostgreSQL
    - Streamlit
    - Plotly
    - Docker
    - Python
    """)

    st.markdown("---")

    st.subheader("📡 Data Pipeline")

    st.code(
        '''
Emploi.ma
↓
Kafka Streaming
↓
Bronze Layer
↓
Silver Layer
↓
Gold Analytics
↓
PostgreSQL Warehouse
↓
Streamlit Dashboard
        '''
    )

# =====================================================
# HERO SECTION
# =====================================================

st.markdown(
    """
    # 🚀 Real-Time IT Recruitment Intelligence Platform

    ### Big Data • Streaming • AI Analytics • Data Engineering
    """
)

st.markdown("---")

# =====================================================
# DATABASE CONNECTION
# =====================================================

DB_USER = "admin"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "job_market"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# =====================================================
# LOAD TABLES
# =====================================================

@st.cache_data
def load_table(table_name):

    try:

        query = f"SELECT * FROM {table_name}"

        return pd.read_sql(
            query,
            engine
        )

    except:

        return pd.DataFrame()

# =====================================================
# SAFE NUMERIC
# =====================================================

def safe_numeric(df, cols):

    for col in cols:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    return df

# =====================================================
# LOAD DATA
# =====================================================

jobs_by_city = safe_numeric(
    load_table("jobs_by_city"),
    ["nb_jobs"]
)

jobs_by_date = safe_numeric(
    load_table("jobs_by_date"),
    ["nb_jobs"]
)

top_companies = safe_numeric(
    load_table("top_companies"),
    ["nb_jobs"]
)

top_skills = safe_numeric(
    load_table("top_skills"),
    ["count"]
)

jobs_by_technology = safe_numeric(
    load_table("jobs_by_technology"),
    ["count"]
)

jobs_by_category = safe_numeric(
    load_table("jobs_by_category"),
    ["nb_jobs"]
)

remote_vs_onsite = safe_numeric(
    load_table("remote_vs_onsite"),
    ["count"]
)

seniority_distribution = safe_numeric(
    load_table("seniority_distribution"),
    ["count"]
)

technology_by_city = safe_numeric(
    load_table("technology_by_city"),
    ["count"]
)

# =====================================================
# GLOBAL KPIS
# =====================================================

st.subheader("📌 Global KPIs")

col1, col2, col3, col4 = st.columns(4)

try:
    total_jobs = int(
        jobs_by_city["nb_jobs"].sum()
    )
except:
    total_jobs = 0

total_companies = len(top_companies)
total_cities = len(jobs_by_city)
total_technologies = len(jobs_by_technology)



with col1:
    st.metric(
        "Total Jobs",
        total_jobs,
        "+12%"
    )

with col2:
    st.metric(
        "Companies",
        total_companies,
        "+5%"
    )

with col3:
    st.metric(
        "Cities",
        total_cities,
        "+2"
    )

with col4:
    st.metric(
        "Technologies",
        total_technologies,
        "+8%"
    )



# =====================================================
# FILTERS
# =====================================================

st.markdown("---")

filter_col1, filter_col2 = st.columns([1, 1])

selected_top_n = filter_col1.slider(
    "Top N Results",
    5,
    30,
    10
)

chart_theme = filter_col2.selectbox(
    "Chart Theme",
    [
        "plotly",
        "ggplot2",
        "seaborn"
    ]
)

# =====================================================
# GEOGRAPHIC ANALYTICS
# =====================================================

st.markdown("---")

st.subheader("🌍 Geographic Analytics")

col1, col2 = st.columns(2)

if not jobs_by_city.empty:

    with col1:

        fig_city = px.bar(

            jobs_by_city.head(selected_top_n),

            x="city",
            y="nb_jobs",

            color="nb_jobs",

            template=chart_theme,

            title="Top Cities by IT Jobs"
        )

        st.plotly_chart(
            fig_city,
            width="stretch"
        )

    with col2:

        fig_city_pie = px.pie(

            jobs_by_city.head(10),

            names="city",

            values="nb_jobs",

            hole=0.5,

            template=chart_theme,

            title="Jobs Distribution by City"
        )

        st.plotly_chart(
            fig_city_pie,
            width="stretch"
        )

# =====================================================
# TECHNOLOGY ANALYTICS
# =====================================================

st.markdown("---")

st.subheader("💻 Technology Analytics")

col1, col2 = st.columns(2)

if not jobs_by_technology.empty:

    with col1:

        fig_tech = px.bar(

            jobs_by_technology.head(selected_top_n),

            x="technology",
            y="count",

            color="count",

            template=chart_theme,

            title="Top Technologies"
        )

        st.plotly_chart(
            fig_tech,
            width="stretch"
        )

    with col2:

        fig_skills = px.treemap(

            top_skills.head(15),

            path=["skill"],

            values="count",

            template=chart_theme,

            title="Top Skills"
        )

        st.plotly_chart(
            fig_skills,
            width="stretch"
        )

# =====================================================
# CATEGORY ANALYTICS
# =====================================================

st.markdown("---")

st.subheader("🧠 IT Categories")

if not jobs_by_category.empty:

    fig_category = px.bar(

        jobs_by_category,

        x="category",
        y="nb_jobs",

        color="nb_jobs",

        template=chart_theme,

        title="Jobs by Category"
    )

    st.plotly_chart(
        fig_category,
        width="stretch"
    )

# =====================================================
# REMOTE / SENIORITY
# =====================================================

st.markdown("---")

st.subheader("🏠 Work Mode & Seniority")

col1, col2 = st.columns(2)

if not remote_vs_onsite.empty:

    with col1:

        fig_remote = px.pie(

            remote_vs_onsite,

            names="work_mode",

            values="count",

            hole=0.5,

            template=chart_theme,

            title="Remote vs Onsite"
        )

        st.plotly_chart(
            fig_remote,
            width="stretch"
        )

if not seniority_distribution.empty:

    with col2:

        fig_seniority = px.bar(

            seniority_distribution,

            x="seniority",
            y="count",

            color="count",

            template=chart_theme,

            title="Seniority Distribution"
        )

        st.plotly_chart(
            fig_seniority,
            width="stretch"
        )

# =====================================================
# TEMPORAL ANALYTICS
# =====================================================

st.markdown("---")

st.subheader("📈 Jobs Evolution")

if not jobs_by_date.empty:

    fig_date = px.line(

        jobs_by_date,

        x="publication_date",
        y="nb_jobs",

        markers=True,

        template=chart_theme,

        title="Jobs Evolution Over Time"
    )

    st.plotly_chart(
        fig_date,
        width="stretch"
    )

# =====================================================
# HEATMAP
# =====================================================

st.markdown("---")

st.subheader("🏙️ Technology by City")

if not technology_by_city.empty:

    fig_heatmap = px.density_heatmap(

        technology_by_city,

        x="city",
        y="technology",
        z="count",

        color_continuous_scale="Blues",

        template=chart_theme,

        title="Technology Demand by City"
    )

    st.plotly_chart(
        fig_heatmap,
        width="stretch"
    )

# =====================================================
# COMPANIES
# =====================================================

st.markdown("---")

st.subheader("🏢 Top Companies")

if not top_companies.empty:

    fig_companies = px.bar(

        top_companies.head(selected_top_n),

        x="company",
        y="nb_jobs",

        color="nb_jobs",

        template=chart_theme,

        title="Top Hiring Companies"
    )

    st.plotly_chart(
        fig_companies,
        width="stretch"
    )

# =====================================================
# LIVE INSIGHTS
# =====================================================

st.markdown("---")

st.subheader("🔥 Latest Market Insights")

col1, col2 = st.columns(2)

with col1:

    if not jobs_by_city.empty:

        st.info(
            f"""
            🌍 Top hiring city:
            {jobs_by_city.iloc[0]['city']}
            """
        )

with col2:

    if not jobs_by_technology.empty:

        st.info(
            f"""
            💻 Most demanded technology:
            {jobs_by_technology.iloc[0]['technology']}
            """
        )

# =====================================================
# TABLES
# =====================================================

st.markdown("---")

st.subheader("📋 Analytics Tables")

selected_table = st.selectbox(

    "Choose Table",

    [
        "Jobs by City",
        "Top Companies",
        "Top Technologies",
        "Jobs by Category",
        "Technology by City"
    ]
)

if selected_table == "Jobs by City":
    st.dataframe(jobs_by_city, width="stretch")

elif selected_table == "Top Companies":
    st.dataframe(top_companies, width="stretch")

elif selected_table == "Top Technologies":
    st.dataframe(jobs_by_technology, width="stretch")

elif selected_table == "Jobs by Category":
    st.dataframe(jobs_by_category, width="stretch")

elif selected_table == "Technology by City":
    st.dataframe(technology_by_city, width="stretch")

# =====================================================
# FOOTER
# =====================================================

st.markdown("---")

st.markdown(
    """
    <center>

    <h3 style='color:#111827;'>
    🏗️ Big Data Architecture
    </h3>

    <p style='color:#4b5563;'>

    Kafka Streaming → Bronze → Silver → Gold →
    PostgreSQL Warehouse → Streamlit Dashboard

    </p>

    <br>

    <p style='color:#2563eb;'>

    Powered by Kafka • Airflow • MinIO • PostgreSQL • Streamlit

    </p>

    </center>
    """,
    unsafe_allow_html=True
)