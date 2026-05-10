import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# =====================================================
# CONFIG PAGE
# =====================================================

st.set_page_config(
    page_title="IT Job Market Analytics",
    page_icon="📊",
    layout="wide"
)

# =====================================================
# CSS
# =====================================================

st.markdown(
    """
    <style>

    .main {
        background-color: #0e1117;
    }

    div[data-testid="metric-container"] {

        background-color: #262730;

        border: 1px solid #444;

        padding: 15px;

        border-radius: 12px;
    }

    div[data-testid="metric-container"] label {

        color: white !important;

        font-size: 16px !important;
    }

    div[data-testid="metric-container"] div {

        color: #00d4ff !important;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# =====================================================
# TITLE
# =====================================================

st.title("📊 IT Job Market Big Data Analytics")

st.markdown(
    "### Real-Time IT Recruitment Intelligence Platform"
)

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
# LOAD TABLE
# =====================================================

@st.cache_data
def load_table(table_name):

    try:

        query = f"SELECT * FROM {table_name}"

        return pd.read_sql(
            query,
            engine
        )

    except Exception as e:

        st.warning(
            f"Table absente : {table_name}"
        )

        return pd.DataFrame()

# =====================================================
# SAFE NUMERIC
# =====================================================

def safe_numeric(df, columns):

    for col in columns:

        if col in df.columns:

            df[col] = pd.to_numeric(
                df[col],
                errors="coerce"
            )

    return df

# =====================================================
# LOAD DATASETS
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

ai_jobs_distribution = safe_numeric(
    load_table("ai_jobs_distribution"),
    ["count"]
)

top_tech_cities = safe_numeric(
    load_table("top_tech_cities"),
    ["nb_jobs"]
)

jobs_last_7_days = safe_numeric(
    load_table("jobs_last_7_days"),
    ["nb_jobs"]
)

company_category = safe_numeric(
    load_table("company_category"),
    ["count"]
)

# =====================================================
# KPIs
# =====================================================

st.markdown("---")
st.subheader("📌 Global KPIs")

col1, col2, col3, col4, col5 = st.columns(5)

# =====================================
# TOTAL JOBS
# =====================================

if (
    not jobs_by_city.empty
    and "nb_jobs" in jobs_by_city.columns
):

    total_jobs = int(
        jobs_by_city["nb_jobs"].sum()
    )

else:

    total_jobs = 0

# =====================================
# TOTAL COMPANIES
# =====================================

total_companies = len(top_companies)

# =====================================
# TOTAL CITIES
# =====================================

total_cities = len(jobs_by_city)

# =====================================
# TOTAL TECHNOLOGIES
# =====================================

total_technologies = len(
    jobs_by_technology
)

# =====================================
# REMOTE JOBS
# =====================================

if (
    not remote_vs_onsite.empty
    and "count" in remote_vs_onsite.columns
):

    remote_jobs = int(

        remote_vs_onsite[
            remote_vs_onsite["work_mode"]
            == "Remote/Hybrid"
        ]["count"].sum()
    )

else:

    remote_jobs = 0

# =====================================
# DISPLAY KPIs
# =====================================

with col1:
    st.metric("Total Jobs", total_jobs)

with col2:
    st.metric("Companies", total_companies)

with col3:
    st.metric("Cities", total_cities)

with col4:
    st.metric(
        "Technologies",
        total_technologies
    )

with col5:
    st.metric(
        "Remote Jobs",
        remote_jobs
    )

# =====================================================
# FILTERS
# =====================================================

st.markdown("---")
st.subheader("🎯 Dashboard Filters")

filter_col1, filter_col2 = st.columns(2)

selected_top_n = filter_col1.slider(
    "Top N Results",
    min_value=5,
    max_value=30,
    value=10
)

chart_theme = filter_col2.selectbox(
    "Chart Theme",
    [
        "plotly_dark",
        "plotly",
        "ggplot2",
        "seaborn"
    ]
)

# =====================================================
# CITY ANALYTICS
# =====================================================

st.markdown("---")
st.subheader("🌍 Geographic Analytics")

if not jobs_by_city.empty:

    col1, col2 = st.columns(2)

    with col1:

        fig_city = px.bar(

            jobs_by_city.head(
                selected_top_n
            ),

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

if not jobs_by_technology.empty:

    col1, col2 = st.columns(2)

    with col1:

        fig_tech = px.bar(

            jobs_by_technology.head(
                selected_top_n
            ),

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

    col1, col2 = st.columns(2)

    with col1:

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

    with col2:

        fig_category_pie = px.pie(

            jobs_by_category,

            names="category",

            values="nb_jobs",

            template=chart_theme,

            title="Category Distribution"
        )

        st.plotly_chart(
            fig_category_pie,
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
# AI ANALYTICS
# =====================================================

st.markdown("---")
st.subheader("🤖 AI Analytics")

if not ai_jobs_distribution.empty:

    fig_ai = px.bar(

        ai_jobs_distribution,

        x="keyword",
        y="count",

        color="count",

        template=chart_theme,

        title="AI Related Jobs"
    )

    st.plotly_chart(
        fig_ai,
        width="stretch"
    )

else:

    st.info(
        "No AI jobs detected."
    )

# =====================================================
# TEMPORAL ANALYTICS
# =====================================================

st.markdown("---")
st.subheader("📈 Temporal Analytics")

if not jobs_by_date.empty:

    col1, col2 = st.columns(2)

    with col1:

        fig_date = px.line(

            jobs_by_date,

            x="publication_date",
            y="nb_jobs",

            markers=True,

            template=chart_theme,

            title="Jobs Evolution"
        )

        st.plotly_chart(
            fig_date,
            width="stretch"
        )

    if not jobs_last_7_days.empty:

        with col2:

            fig_last7 = px.area(

                jobs_last_7_days,

                x="publication_date",
                y="nb_jobs",

                template=chart_theme,

                title="Last 7 Days Trend"
            )

            st.plotly_chart(
                fig_last7,
                width="stretch"
            )

# =====================================================
# TECHNOLOGY BY CITY
# =====================================================

st.markdown("---")
st.subheader("🏙️ Technology by City")

if not technology_by_city.empty:

    fig_heatmap = px.density_heatmap(

        technology_by_city,

        x="city",
        y="technology",
        z="count",

        template=chart_theme,

        title="Technology Demand by City"
    )

    st.plotly_chart(
        fig_heatmap,
        width="stretch"
    )

# =====================================================
# COMPANY ANALYTICS
# =====================================================

st.markdown("---")
st.subheader("🏢 Companies Analytics")

if not top_companies.empty:

    col1, col2 = st.columns(2)

    with col1:

        fig_companies = px.bar(

            top_companies.head(
                selected_top_n
            ),

            x="company",
            y="nb_jobs",

            color="nb_jobs",

            template=chart_theme,

            title="Top Recruiting Companies"
        )

        st.plotly_chart(
            fig_companies,
            width="stretch"
        )

    if not company_category.empty:

        with col2:

            fig_company_category = px.sunburst(

                company_category.head(30),

                path=["company", "category"],

                values="count",

                template=chart_theme,

                title="Company Categories"
            )

            st.plotly_chart(
                fig_company_category,
                width="stretch"
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
    st.dataframe(jobs_by_city)

elif selected_table == "Top Companies":
    st.dataframe(top_companies)

elif selected_table == "Top Technologies":
    st.dataframe(jobs_by_technology)

elif selected_table == "Jobs by Category":
    st.dataframe(jobs_by_category)

elif selected_table == "Technology by City":
    st.dataframe(technology_by_city)

# =====================================================
# FOOTER
# =====================================================

st.markdown("---")

st.markdown(
    """
    ### 🚀 Big Data Architecture

    Kafka Streaming → Bronze → Silver → Gold → PostgreSQL Warehouse → Streamlit Dashboard
    """
)