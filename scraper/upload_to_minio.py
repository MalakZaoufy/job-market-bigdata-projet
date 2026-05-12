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
    BASE_DIR           = "/opt/airflow/scraper"
    EMPLOIMA_PATTERN   = f"{BASE_DIR}/jobs_data_*.json"
    REKRUTE_PATTERN    = f"{BASE_DIR}/data/raw/rekrute/rekrute_jobs*.json"
else:
    BASE_DIR           = "."
    EMPLOIMA_PATTERN   = "jobs_data_*.json"
    REKRUTE_PATTERN    = "data/raw/rekrute/rekrute_jobs*.json"

ACCESS_KEY  = "admin"
SECRET_KEY  = "admin123"
BUCKET_NAME = "job-data-lake"

ENDPOINT_URL = (
    "http://minio:9000"
    if os.path.exists("/opt/airflow")
    else "http://localhost:9000"
)

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
    buckets      = s3_client.list_buckets()
    bucket_names = [b["Name"] for b in buckets["Buckets"]]

    if BUCKET_NAME not in bucket_names:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print("Bucket créé :", BUCKET_NAME)

except Exception as e:
    print("Erreur connexion MinIO :", e)
    exit()

# =========================================
# RÉSOLUTION DES FICHIERS À UPLOADER
# =========================================
# Structure :
#   {
#     "source_label": "chemin/local/fichier.json"
#   }
# =========================================

now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

files_to_upload = {}

# --- Emploi.ma : dernier fichier créé matching le pattern ---
emploima_files = glob.glob(EMPLOIMA_PATTERN)
if emploima_files:
    latest_emploima = max(emploima_files, key=os.path.getctime)
    files_to_upload["emploima"] = {
        "local_path":   latest_emploima,
        "object_name":  f"bronze/batch/emploima/jobs_{now}.json",
    }
    print(f"[emploi.ma]  Fichier trouvé : {latest_emploima}")
else:
    print(f"[emploi.ma]  Aucun fichier trouvé ({EMPLOIMA_PATTERN}) — ignoré")

# --- Rekrute : dernier fichier créé matching le pattern ---
rekrute_files = glob.glob(REKRUTE_PATTERN)
if rekrute_files:
    latest_rekrute = max(rekrute_files, key=os.path.getctime)
    files_to_upload["rekrute"] = {
        "local_path":  latest_rekrute,
        "object_name": f"bronze/batch/rekrute/rekrute_jobs_{now}.json",
    }
    print(f"[rekrute]    Fichier trouvé : {latest_rekrute}")
else:
    print(f"[rekrute]    Aucun fichier trouvé ({REKRUTE_PATTERN}) — ignoré")

# =========================================
# VÉRIFICATION : au moins 1 fichier
# =========================================

if not files_to_upload:
    raise Exception(
        "Aucun fichier JSON trouvé (ni emploi.ma ni rekrute). "
        "Lancez les scrapers avant l'upload."
    )

# =========================================
# UPLOAD DE CHAQUE FICHIER
# =========================================

success_count = 0
error_count   = 0

for source, info in files_to_upload.items():

    local_path  = info["local_path"]
    object_name = info["object_name"]

    print(f"\n--- Upload [{source}] ---")
    print(f"  Local  : {local_path}")
    print(f"  MinIO  : {object_name}")

    try:
        s3_client.upload_file(local_path, BUCKET_NAME, object_name)
        print(f"  ✅ Upload réussi")
        success_count += 1

    except Exception as e:
        print(f"  ❌ Erreur upload : {e}")
        error_count += 1

# =========================================
# RÉSUMÉ FINAL
# =========================================

print(f"\n{'=' * 40}")
print(f"  Upload terminé")
print(f"  ✅ Succès  : {success_count}")
print(f"  ❌ Erreurs : {error_count}")
print(f"{'=' * 40}")