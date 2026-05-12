from kafka import KafkaConsumer
import json
import boto3
from botocore.client import Config
from datetime import datetime

# =========================================
# CONFIG KAFKA
# =========================================

consumer = KafkaConsumer(
    "jobs_topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="jobs-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer Kafka démarré...\n")

# =========================================
# CONFIG MINIO
# =========================================

MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY     = "admin"
SECRET_KEY     = "admin123"
BUCKET_NAME    = "job-data-lake"

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
# CHARGEMENT HISTORIQUE BRONZE
# =========================================

existing_links = set()

print("\nChargement historique Bronze Kafka...\n")

try:
    objects = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix="bronze/kafka/"
    )

    files = [
        obj["Key"]
        for obj in objects.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]

    print(f"Fichiers trouvés : {len(files)}")

    for file_key in files:
        try:
            obj  = s3.get_object(Bucket=BUCKET_NAME, Key=file_key)
            data = json.loads(obj["Body"].read())

            if isinstance(data, list):
                for job in data:
                    link = job.get("id") or job.get("link")
                    if link:
                        existing_links.add(link)

        except Exception as e:
            print(f"Erreur lecture {file_key} : {e}")

except Exception as e:
    print("Erreur chargement historique :", e)

print(f"Offres déjà connues : {len(existing_links)}\n")

# =========================================
# AFFICHAGE TERMINAL
# =========================================

def print_job(job):
    sep = "─" * 55
    print(sep)
    print(f"  title            : {job.get('title', '')}")
    print(f"  company          : {job.get('company', '')}")
    print(f"  city             : {job.get('city', '')}")
    print(f"  publication_date : {job.get('publication_date', '')}")
    print(f"  link             : {job.get('link', '')}")
    print(f"  source           : {job.get('source', '')}")
    print(f"  scraping_time    : {job.get('scraping_time', '')}")

# =========================================
# UPLOAD MINIO
# =========================================

def upload_to_minio(buffer):
    """
    Envoie le buffer (liste de jobs mixtes rekrute + emploi.ma)
    dans un seul fichier JSON sous bronze/kafka/.
    """
    now         = datetime.now().strftime("%Y-%m-%d_%H-%M-%S-%f")
    object_name = f"bronze/kafka/kafka_jobs_{now}.json"

    body = json.dumps(buffer, ensure_ascii=False, indent=4).encode("utf-8")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=object_name,
        Body=body,
        ContentType="application/json"
    )

    print(f"\n{'=' * 55}")
    print(f"  ✅ Fichier Bronze envoyé vers MinIO")
    print(f"  Chemin  : {object_name}")
    print(f"  Offres  : {len(buffer)}")
    print(f"{'=' * 55}\n")

# =========================================
# BUFFER MICRO-BATCH
# =========================================

jobs_buffer = []
BUFFER_SIZE  = 5

# =========================================
# LECTURE STREAMING
# =========================================

print("En attente de messages...\n")

for message in consumer:

    try:
        job = message.value

        # ---------------------------------
        # ID UNIQUE
        # ---------------------------------
        job_id = job.get("id") or job.get("link")

        # ---------------------------------
        # IGNORER DOUBLONS
        # ---------------------------------
        if job_id in existing_links:
            print(f"  ⏭️  Doublon ignoré : {job.get('title', job_id)}")
            continue

        existing_links.add(job_id)

        # ---------------------------------
        # AFFICHAGE
        # ---------------------------------
        print_job(job)

        # ---------------------------------
        # AJOUT AU BUFFER
        # ---------------------------------
        jobs_buffer.append(job)

        print(f"  Buffer : {len(jobs_buffer)} / {BUFFER_SIZE}")

        # ---------------------------------
        # MICRO-BATCH → MINIO
        # ---------------------------------
        if len(jobs_buffer) >= BUFFER_SIZE:
            upload_to_minio(jobs_buffer)
            jobs_buffer = []           # reset immédiat après upload

    except Exception as e:
        print(f"Erreur Consumer : {e}")