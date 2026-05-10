import boto3
from botocore.client import Config
import json
import pandas as pd
from sqlalchemy import create_engine

# =========================================
# CONFIG MINIO
# =========================================

MINIO_ENDPOINT = "http://localhost:9000"

ACCESS_KEY = "admin"
SECRET_KEY = "admin123"

BUCKET_NAME = "job-data-lake"

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
# CONFIG POSTGRESQL
# =========================================

POSTGRES_USER = "admin"
POSTGRES_PASSWORD = "admin"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "job_market"

# =========================================
# CONNEXION SQLALCHEMY
# =========================================

engine = create_engine(

    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

print("Connexion PostgreSQL OK")

# =========================================
# LISTE TABLES GOLD
# =========================================

gold_files = [

    "gold/jobs_by_city",
    "gold/jobs_by_date",
    "gold/top_companies",
    "gold/top_skills",

    "gold/jobs_by_technology",
    "gold/jobs_by_category",
    "gold/remote_vs_onsite",
    "gold/seniority_distribution",
    "gold/technology_by_city",
    "gold/ai_jobs_distribution",

    "gold/top_tech_cities",
    "gold/jobs_last_7_days",
    "gold/company_category"
]

# =========================================
# CHARGEMENT WAREHOUSE
# =========================================

for prefix in gold_files:

    try:

        # =====================================
        # NOM TABLE SQL
        # =====================================

        table_name = prefix.split("/")[-1]

        # =====================================
        # LISTER FICHIERS GOLD
        # =====================================

        objects = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix
        )

        files = [

            obj["Key"]

            for obj in objects.get(
                "Contents",
                []
            )

            if obj["Key"].endswith(
                ".json"
            )
        ]

        # =====================================
        # SI AUCUN FICHIER
        # =====================================

        if not files:

            print(
                f"Aucun fichier trouvé : {table_name}"
            )

            # créer table vide
            df = pd.DataFrame(
                columns=["empty"]
            )

            df.to_sql(

                table_name,

                engine,

                if_exists="replace",

                index=False
            )

            print(
                f"Table vide créée : {table_name}"
            )

            continue

        # =====================================
        # DERNIER FICHIER
        # =====================================

        latest_file = sorted(files)[-1]

        print(f"\nLecture : {latest_file}")

        # =====================================
        # LECTURE MINIO
        # =====================================

        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=latest_file
        )

        data = json.loads(
            obj["Body"]
            .read()
            .decode("utf-8")
        )

        # =====================================
        # DATAFRAME
        # =====================================

        df = pd.DataFrame(data)

        # =====================================
        # DATASET VIDE
        # =====================================

        if df.empty:

            print(
                f"Dataset vide : {table_name}"
            )

            df = pd.DataFrame(
                columns=["empty"]
            )

        # =====================================
        # CONVERSION TYPES
        # =====================================

        for col in df.columns:

            try:

                df[col] = df[col].astype(str)

            except:
                pass

        # =====================================
        # INSERT SQL
        # =====================================

        df.to_sql(

            table_name,

            engine,

            if_exists="replace",

            index=False
        )

        print(
            f"Table créée : {table_name}"
        )

        print(
            f"Nombre lignes : {len(df)}"
        )

    except Exception as e:

        print(
            f"Erreur table {prefix} : {e}"
        )

print("\nWarehouse PostgreSQL chargé ✅")