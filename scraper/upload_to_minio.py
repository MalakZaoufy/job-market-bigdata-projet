import boto3
from botocore.client import Config
from datetime import datetime
import os
import glob

# =========================================
# CONFIGURATION MINIO
# =========================================

# Détection environnement :
# - Airflow Docker
# - Local Windows

if os.path.exists("/opt/airflow"):
    ENDPOINT_URL = "http://minio:9000"
    FILE_PATH = "/opt/airflow/scraper/jobs_data_*.json"
else:
    ENDPOINT_URL = "http://localhost:9000"
    FILE_PATH = "jobs_data_*.json"

ACCESS_KEY = "admin"
SECRET_KEY = "admin123"
BUCKET_NAME = "job-data-lake"

print("Connexion vers :", ENDPOINT_URL)

# =========================================
# CONNEXION MINIO
# =========================================

s3_client = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
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
# CRÉATION BUCKET SI ABSENT
# =========================================

try:

    buckets = s3_client.list_buckets()

    bucket_names = [
        bucket["Name"]
        for bucket in buckets["Buckets"]
    ]

    if BUCKET_NAME not in bucket_names:

        s3_client.create_bucket(Bucket=BUCKET_NAME)

        print("Bucket créé :", BUCKET_NAME)

except Exception as e:

    print("Erreur connexion MinIO :", e)
    exit()

# =========================================
# RÉCUPÉRATION DERNIER JSON
# =========================================

files = glob.glob(FILE_PATH)

if not files:

    raise Exception(
        f"Aucun fichier trouvé avec : {FILE_PATH}"
    )

# dernier fichier créé
latest_file = max(files, key=os.path.getctime)

LOCAL_FILE = latest_file

print("Fichier utilisé pour upload :", LOCAL_FILE)

# =========================================
# NOM FICHIER BRONZE
# =========================================

now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

OBJECT_NAME = f"bronze/jobs_{now}.json"

# =========================================
# UPLOAD
# =========================================

try:

    s3_client.upload_file(
        LOCAL_FILE,
        BUCKET_NAME,
        OBJECT_NAME
    )

    print("Upload réussi ✅")
    print("Chemin :", OBJECT_NAME)

except Exception as e:

    print("Erreur upload :", e)