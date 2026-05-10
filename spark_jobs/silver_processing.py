import boto3
from botocore.client import Config
import json
import pandas as pd
from datetime import datetime
import os
import re

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
# LECTURE TOUS FICHIERS BRONZE
# =========================================

all_files = []

# =========================================
# BRONZE BATCH
# =========================================

batch_objects = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="bronze/batch/"
)

batch_files = [
    obj["Key"]
    for obj in batch_objects.get("Contents", [])
    if obj["Key"].endswith(".json")
]

print("Fichiers batch trouvés :", len(batch_files))

all_files.extend(batch_files)

# =========================================
# BRONZE KAFKA
# =========================================

kafka_objects = s3.list_objects_v2(
    Bucket=BUCKET_NAME,
    Prefix="bronze/kafka/"
)

kafka_files = [
    obj["Key"]
    for obj in kafka_objects.get("Contents", [])
    if obj["Key"].endswith(".json")
]

print("Fichiers kafka trouvés :", len(kafka_files))

all_files.extend(kafka_files)

# =========================================
# VÉRIFICATION
# =========================================

if not all_files:
    raise Exception("Aucun fichier Bronze trouvé")

print("Total fichiers Bronze :", len(all_files))

# =========================================
# FUSION JSON
# =========================================

all_data = []

for file_key in all_files:

    try:

        print("Lecture :", file_key)

        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=file_key
        )

        data = json.loads(
            obj["Body"].read()
        )

        if isinstance(data, list):
            all_data.extend(data)

        elif isinstance(data, dict):
            all_data.append(data)

    except Exception as e:

        print(f"Erreur lecture {file_key} :", e)

# =========================================
# DATAFRAME
# =========================================

df = pd.DataFrame(all_data)

print("Nombre lignes avant nettoyage :", len(df))

# =========================================
# NORMALISATION COLONNES
# =========================================

df.columns = [
    str(col).lower()
    for col in df.columns
]

# =========================================
# SÉCURITÉ COLONNES
# =========================================

required_columns = [
    "title",
    "company",
    "publication_date",
    "link"
]

for col in required_columns:

    if col not in df.columns:
        df[col] = "Non spécifié"

# =========================================
# GESTION TYPES
# =========================================

for col in df.columns:

    try:
        df[col] = df[col].astype(str)
    except:
        pass

# =========================================
# SUPPRESSION DOUBLONS ROBUSTE
# =========================================

# priorité id
if "id" in df.columns:

    df = df.drop_duplicates(
        subset=["id"]
    )

# sinon lien
elif "link" in df.columns:

    df = df.drop_duplicates(
        subset=["link"]
    )

# fallback
else:

    df = df.drop_duplicates(
        subset=["title", "company"]
    )

print(
    "Nombre lignes après déduplication :",
    len(df)
)

# =========================================
# CLEAN TITLE
# =========================================

def clean_title(title):

    if not isinstance(title, str):
        return "non spécifié"

    title = title.strip()

    title = title.replace("(H/F)", "")
    title = title.replace("(M/F)", "")

    title = title.split("-")[0]

    return title.strip().lower()

df["title_clean"] = df["title"].apply(
    clean_title
)

# =========================================
# EXTRACTION VILLE
# =========================================

def extract_city(title):

    if not isinstance(title, str):
        return "Non spécifié"

    parts = title.split("-")

    if len(parts) > 1:

        city = parts[-1].strip()

        city = (
            city
            .replace("(H/F)", "")
            .replace("(M/F)", "")
            .strip()
        )

        remote_keywords = [
            "distance",
            "remote",
            "hybrid",
            "travail à distance",
            "télétravail"
        ]

        if any(
            k in city.lower()
            for k in remote_keywords
        ):
            return "Remote"

        if city == "":
            return "Non spécifié"

        return city

    return "Non spécifié"

df["city"] = df["title"].apply(
    extract_city
)

# =========================================
# MOTS CLÉS IT
# =========================================

it_keywords = [

    "développeur",
    "developpeur",
    "developer",

    "software engineer",

    "data engineer",
    "data analyst",
    "data scientist",

    "machine learning",
    "deep learning",

    "devops",
    "cloud",

    "fullstack",
    "full stack",

    "backend",
    "frontend",

    "big data",

    "etl",

    "cybersecurity",

    "informatique",

    "python",
    "java",
    "sql",

    "docker",
    "kafka",
    "spark",
    "airflow",

    "react",
    "angular",
    "node",

    "c++",
    "c#",
    ".net",
    "dotnet"
]

exclude_keywords = [

    "business developer",

    "commercial",
    "sales",

    "rh",
    "ressources humaines",

    "recrutement",

    "finance",

    "comptable",

    "caissier",

    "gestionnaire de paie",

    "catalogage",

    "assistant administratif"
]

# =========================================
# MATCH MOT COMPLET
# =========================================

def contains_keyword(text, keywords):

    text = str(text).lower()

    for keyword in keywords:

        pattern = r"\b" + re.escape(
            keyword.lower()
        ) + r"\b"

        if re.search(pattern, text):
            return True

    return False

# =========================================
# FILTRAGE IT
# =========================================

def is_it_job(title):

    title = str(title).lower()

    is_it = contains_keyword(
        title,
        it_keywords
    )

    is_excluded = contains_keyword(
        title,
        exclude_keywords
    )

    return is_it and not is_excluded

df = df[
    df["title_clean"].apply(
        is_it_job
    )
]

print("Nombre lignes IT :", len(df))

# =========================================
# DATE TRAITEMENT
# =========================================

df["processed_date"] = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)

# =========================================
# TRI DATE
# =========================================

if "publication_date" in df.columns:

    try:

        df["publication_date"] = pd.to_datetime(
            df["publication_date"],
            errors="coerce"
        )

        df = df.sort_values(
            by="publication_date",
            ascending=False
        )

    except:
        pass

# =========================================
# EXPORT SILVER
# =========================================

clean_data = df.to_dict(
    orient="records"
)

now = datetime.now().strftime(
    "%Y-%m-%d_%H-%M-%S"
)

output_key = (
    f"silver/jobs_clean_{now}.json"
)

s3.put_object(

    Bucket=BUCKET_NAME,

    Key=output_key,

    Body=json.dumps(
        clean_data,
        ensure_ascii=False,
        indent=4,
        default=str
    )
)

print()
print("Silver Layer créé :", output_key)
print("Nombre final lignes Silver :", len(df))
print("Pipeline Silver terminé avec succès ✅")