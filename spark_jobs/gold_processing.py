import boto3
from botocore.client import Config
import json
import pandas as pd
from datetime import datetime
import re
import os

# =========================================
# CONFIG MINIO
# =========================================

if os.path.exists("/opt/airflow"):
    MINIO_ENDPOINT = "http://minio:9000"
else:
    MINIO_ENDPOINT = "http://localhost:9000"

ACCESS_KEY = "admin"
SECRET_KEY = "admin123"
BUCKET_NAME = "job-data-lake"

print("Connexion vers :", MINIO_ENDPOINT)

# =========================================
# CONNEXION MINIO
# =========================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"}
    ),
    region_name="us-east-1"
)

print("Connexion MinIO OK")

# =========================================
# LIRE DERNIER SILVER
# =========================================

objects = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="silver/"
)

files = [
    obj["Key"]
    for obj in objects.get("Contents", [])
    if obj["Key"].endswith(".json")
]

if not files:
    raise Exception("Aucun fichier Silver trouvé")

latest_file = sorted(files)[-1]

print("Fichier Silver utilisé :", latest_file)

# =========================================
# LECTURE JSON
# =========================================

obj = s3.get_object(
    Bucket=BUCKET_NAME,
    Key=latest_file
)

data = json.loads(
    obj["Body"].read().decode("utf-8")
)

df = pd.DataFrame(data)

print("Nombre lignes Silver :", len(df))

# =========================================
# SÉCURITÉ COLONNES
# =========================================

required_columns = [
    "title",
    "title_clean",
    "company",
    "city",
    "publication_date"
]

for col in required_columns:

    if col not in df.columns:
        df[col] = "Unknown"

# =========================================
# GESTION NaN
# =========================================

df = df.fillna("Unknown")

# =========================================
# TYPES
# =========================================

df["title"] = df["title"].astype(str)
df["title_clean"] = df["title_clean"].astype(str)
df["company"] = df["company"].astype(str)
df["city"] = df["city"].astype(str)

# =========================================
# DATE
# =========================================

df["publication_date"] = pd.to_datetime(
    df["publication_date"],
    errors="coerce"
)

# =========================================
# SUPPRESSION DOUBLONS
# =========================================

if "id" in df.columns:

    df = df.drop_duplicates(
        subset=["id"]
    )

elif "link" in df.columns:

    df = df.drop_duplicates(
        subset=["link"]
    )

else:

    df = df.drop_duplicates(
        subset=["title", "company"]
    )

print("Nombre lignes après déduplication :", len(df))

# =========================================
# TECHNOLOGIES
# =========================================

technologies = {

    "Python": r"\bpython\b",

    "Java": r"\bjava\b",

    "SQL": r"\bsql\b",
    
    "C++": r"\bc\+\+\b",

    "C#": r"\bc#\b|\bcsharp\b",

    "DOTNET": r"\bdotnet\b|\.net\b",

    "JavaScript": r"\bjavascript\b|\bjs\b",

    "React": r"\breact\b",

    "Angular": r"\bangular\b",

    "Node.js": r"\bnode\b|\bnodejs\b",

    "Docker": r"\bdocker\b",

    "Kafka": r"\bkafka\b",

    "Spark": r"\bspark\b",

    "Airflow": r"\bairflow\b",

    "AWS": r"\baws\b",

    "Azure": r"\bazure\b",

    "Kubernetes": r"\bkubernetes\b",

    "Power BI": r"\bpower bi\b",

    "Big Data": r"\bbig data\b",

    "Machine Learning": r"\bmachine learning\b",

    "Deep Learning": r"\bdeep learning\b",

    "DevOps": r"\bdevops\b",

    "Cloud": r"\bcloud\b",

    "ETL": r"\betl\b"
}

# =========================================
# GOLD 1 : JOBS BY CITY
# =========================================

jobs_by_city = (
    df.groupby("city")
    .size()
    .reset_index(name="nb_jobs")
    .sort_values(
        by="nb_jobs",
        ascending=False
    )
)

# =========================================
# GOLD 2 : JOBS BY DATE
# =========================================

jobs_by_date = (
    df.groupby(
        df["publication_date"].dt.strftime("%Y-%m-%d")
    )
    .size()
    .reset_index(name="nb_jobs")
    .sort_values(
        by="publication_date"
    )
)

# =========================================
# GOLD 3 : TOP COMPANIES
# =========================================

top_companies = (
    df["company"]
    .value_counts()
    .reset_index()
)

top_companies.columns = [
    "company",
    "nb_jobs"
]

# =========================================
# GOLD 4 : JOBS BY TECHNOLOGY
# =========================================

technology_counts = []

for tech_name, pattern in technologies.items():

    count = df["title_clean"].str.contains(
        pattern,
        case=False,
        na=False,
        regex=True
    ).sum()

    technology_counts.append({

        "technology": tech_name,

        "count": int(count)
    })

jobs_by_technology = pd.DataFrame(
    technology_counts
)

jobs_by_technology = jobs_by_technology[
    jobs_by_technology["count"] > 0
]

jobs_by_technology = jobs_by_technology.sort_values(
    by="count",
    ascending=False
)

# =========================================
# GOLD 5 : TOP SKILLS
# =========================================

top_skills = jobs_by_technology.copy()

top_skills.columns = [
    "skill",
    "count"
]

# =========================================
# GOLD 6 : JOBS BY CATEGORY
# =========================================

def categorize_job(title):

    title = str(title).lower()

    if any(x in title for x in [
        "data engineer",
        "big data",
        "etl"
    ]):
        return "Data Engineering"

    elif any(x in title for x in [
        "data scientist",
        "machine learning",
        "deep learning"
    ]):
        return "AI / Data Science"

    elif any(x in title for x in [
        "data analyst",
        "power bi",
        "bi"
    ]):
        return "Data Analytics"

    elif any(x in title for x in [
        "devops",
        "cloud",
        "aws",
        "azure"
    ]):
        return "DevOps / Cloud"

    elif any(x in title for x in [
        "cybersecurity",
        "security"
    ]):
        return "Cybersecurity"

    elif any(x in title for x in [
        "frontend",
        "react",
        "angular"
    ]):
        return "Frontend Development"

    elif any(x in title for x in [
        "backend",
        "node",
        "java",
        "c#",
        "c++",
        "dotnet"
    ]):
        return "Backend Development"

    elif any(x in title for x in [
        "full stack",
        "fullstack"
    ]):
        return "Full Stack Development"

    elif any(x in title for x in [
        "mobile",
        "android",
        "ios"
    ]):
        return "Mobile Development"

    return "Other IT"

df["category"] = df["title_clean"].apply(
    categorize_job
)

jobs_by_category = (
    df.groupby("category")
    .size()
    .reset_index(name="nb_jobs")
    .sort_values(
        by="nb_jobs",
        ascending=False
    )
)

# =========================================
# GOLD 7 : REMOTE VS ONSITE
# =========================================

def detect_work_mode(title):

    title = str(title).lower()

    remote_keywords = [
        "remote",
        "distance",
        "hybrid",
        "travail à distance",
        "télétravail"
    ]

    if any(k in title for k in remote_keywords):
        return "Remote/Hybrid"

    return "Onsite"

df["work_mode"] = df["title"].apply(
    detect_work_mode
)

remote_vs_onsite = (
    df.groupby("work_mode")
    .size()
    .reset_index(name="count")
)

# =========================================
# GOLD 8 : SENIORITY
# =========================================

def detect_seniority(title):

    title = str(title).lower()

    if any(x in title for x in [
        "senior",
        "lead",
        "expert",
        "architect"
    ]):
        return "Senior"

    elif any(x in title for x in [
        "junior",
        "intern",
        "stage",
        "stagiaire"
    ]):
        return "Junior"

    return "Mid-Level"

df["seniority"] = df["title_clean"].apply(
    detect_seniority
)

seniority_distribution = (
    df.groupby("seniority")
    .size()
    .reset_index(name="count")
)

# =========================================
# GOLD 9 : TECHNOLOGY BY CITY
# =========================================

technology_city_data = []

for tech_name, pattern in technologies.items():

    filtered_df = df[
        df["title_clean"].str.contains(
            pattern,
            case=False,
            na=False,
            regex=True
        )
    ]

    grouped = (
        filtered_df.groupby("city")
        .size()
        .reset_index(name="count")
    )

    grouped = grouped[
        grouped["count"] > 1
    ]

    for _, row in grouped.iterrows():

        technology_city_data.append({

            "city": row["city"],

            "technology": tech_name,

            "count": int(row["count"])
        })

technology_by_city = pd.DataFrame(
    technology_city_data
)

# =========================================
# GOLD 10 : AI JOBS ANALYTICS
# =========================================

ai_patterns = {

    "Machine Learning": r"\bmachine learning\b",

    "Deep Learning": r"\bdeep learning\b",

    "LLM": r"\bllm\b",

    "NLP": r"\bnlp\b",

    "Artificial Intelligence": r"\bartificial intelligence\b"
}

ai_data = []

for name, pattern in ai_patterns.items():

    count = df["title_clean"].str.contains(
        pattern,
        case=False,
        na=False,
        regex=True
    ).sum()

    ai_data.append({

        "keyword": name,

        "count": int(count)
    })

ai_jobs_distribution = pd.DataFrame(
    ai_data
)

ai_jobs_distribution = ai_jobs_distribution[
    ai_jobs_distribution["count"] > 0
]

# =========================================
# GOLD 11 : TOP TECH CITIES
# =========================================

top_tech_cities = jobs_by_city.head(10)

# =========================================
# GOLD 12 : JOBS LAST 7 DAYS
# =========================================

last_7_days_jobs = df[
    df["publication_date"] >= (
        pd.Timestamp.now() - pd.Timedelta(days=7)
    )
]

jobs_last_7_days = (
    last_7_days_jobs
    .groupby(
        last_7_days_jobs["publication_date"]
        .dt.strftime("%Y-%m-%d")
    )
    .size()
    .reset_index(name="nb_jobs")
)

# =========================================
# GOLD 13 : COMPANY CATEGORY
# =========================================

company_category = (
    df.groupby(["company", "category"])
    .size()
    .reset_index(name="count")
    .sort_values(
        by="count",
        ascending=False
    )
)

# =========================================
# EXPORT GOLD
# =========================================

now = datetime.now().strftime(
    "%Y-%m-%d_%H-%M-%S"
)

def upload_to_minio(dataframe, name):

    if dataframe.empty:
        print(f"{name} vide ignoré")
        return

    key = f"gold/{name}_{now}.json"

    s3.put_object(

        Bucket=BUCKET_NAME,

        Key=key,

        Body=json.dumps(
            dataframe.to_dict(
                orient="records"
            ),
            ensure_ascii=False,
            indent=4,
            default=str
        ),

        ContentType="application/json"
    )

    print("Gold créé :", key)

# =========================================
# EXPORTS
# =========================================

gold_datasets = {

    "jobs_by_city": jobs_by_city,

    "jobs_by_date": jobs_by_date,

    "top_companies": top_companies,

    "top_skills": top_skills,

    "jobs_by_technology": jobs_by_technology,

    "jobs_by_category": jobs_by_category,

    "remote_vs_onsite": remote_vs_onsite,

    "seniority_distribution": seniority_distribution,

    "technology_by_city": technology_by_city,

    "ai_jobs_distribution": ai_jobs_distribution,

    "top_tech_cities": top_tech_cities,

    "jobs_last_7_days": jobs_last_7_days,

    "company_category": company_category
}

for name, dataframe in gold_datasets.items():

    upload_to_minio(
        dataframe,
        name
    )

print("\nPipeline GOLD avancé terminé avec succès ✅")