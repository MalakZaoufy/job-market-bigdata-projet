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

    # lire seulement nouveaux messages
    auto_offset_reset="latest",

    enable_auto_commit=True,

    # garder toujours même group_id
    group_id="jobs-group",

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)

print("Consumer Kafka démarré...\n")

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
# CHARGER ANCIENS LINKS BRONZE
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

    print(f"Fichiers Bronze Kafka trouvés : {len(files)}")

    for file_key in files:

        try:

            obj = s3.get_object(
                Bucket=BUCKET_NAME,
                Key=file_key
            )

            data = json.loads(
                obj["Body"].read()
            )

            if isinstance(data, list):

                for job in data:

                    # priorité id
                    if "id" in job:
                        existing_links.add(
                            job["id"]
                        )

                    elif "link" in job:
                        existing_links.add(
                            job["link"]
                        )

        except Exception as e:

            print(
                f"Erreur lecture {file_key} : {e}"
            )

except Exception as e:

    print(
        "Erreur chargement historique :",
        e
    )

print(
    f"Offres déjà connues : {len(existing_links)}"
)

# =========================================
# BUFFER MICRO-BATCH
# =========================================

jobs_buffer = []

BUFFER_SIZE = 5

# =========================================
# LECTURE TEMPS RÉEL
# =========================================

for message in consumer:

    try:

        job = message.value

        # =====================================
        # IDENTIFIANT UNIQUE
        # =====================================

        job_id = job.get(
            "id",
            job.get("link")
        )

        # =====================================
        # IGNORER DOUBLONS HISTORIQUES
        # =====================================

        if job_id in existing_links:

            print(
                "Offre déjà présente ignorée ⏭️"
            )

            print(job.get("title"))

            print("-" * 60)

            continue

        # =====================================
        # AJOUT MÉMOIRE
        # =====================================

        existing_links.add(job_id)

        # =====================================
        # NOUVELLE OFFRE
        # =====================================

        print("Nouvelle offre reçue ✅")

        print(
            json.dumps(
                job,
                indent=4,
                ensure_ascii=False
            )
        )

        print("-" * 60)

        # =====================================
        # BUFFER
        # =====================================

        jobs_buffer.append(job)

        print(
            f"Taille buffer : "
            f"{len(jobs_buffer)} / {BUFFER_SIZE}"
        )

        # =====================================
        # MICRO-BATCH BRONZE
        # =====================================

        if len(jobs_buffer) >= BUFFER_SIZE:

            now = datetime.now().strftime(
                "%Y-%m-%d_%H-%M-%S-%f"
            )

            object_name = (
                f"bronze/kafka/"
                f"kafka_jobs_{now}.json"
            )

            # =================================
            # UPLOAD MINIO
            # =================================

            s3.put_object(

                Bucket=BUCKET_NAME,

                Key=object_name,

                Body=json.dumps(
                    jobs_buffer,
                    ensure_ascii=False,
                    indent=4
                )
            )

            print(
                "\nFichier Bronze Kafka créé ✅"
            )

            print(
                "Chemin :",
                object_name
            )

            print(
                f"Nombre offres sauvegardées : "
                f"{len(jobs_buffer)}"
            )

            print("-" * 60)

            # =================================
            # RESET BUFFER
            # =================================

            jobs_buffer = []

    except Exception as e:

        print(
            "Erreur Consumer :",
            e
        )