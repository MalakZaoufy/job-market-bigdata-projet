import boto3
from botocore.client import Config
import json
import pandas as pd
from datetime import datetime
import os
import re

# =====================================================
# CONFIG MINIO
# =====================================================

if os.path.exists("/opt/airflow"):
    MINIO_ENDPOINT = "http://minio:9000"
else:
    MINIO_ENDPOINT = "http://localhost:9000"

ACCESS_KEY  = "admin"
SECRET_KEY  = "admin123"
BUCKET_NAME = "job-data-lake"

print("Connexion vers :", MINIO_ENDPOINT)

# =====================================================
# CONNEXION MINIO
# =====================================================

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

# =====================================================
# LECTURE BRONZE
# Nouvelle structure MinIO :
#   bronze/batch/emploima/   ← fichiers emploi.ma
#   bronze/batch/rekrute/    ← fichiers rekrute
#   bronze/kafka/            ← fichiers temps réel
# =====================================================

BRONZE_PREFIXES = [
    "bronze/batch/emploima/",
    "bronze/batch/rekrute/",
    "bronze/kafka/",
]

all_files = []

for prefix in BRONZE_PREFIXES:

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    )

    found = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".json")
    ]

    print(
        f"[{prefix}]  {len(found)} fichier(s) trouvé(s)"
    )

    all_files.extend(found)

# =====================================================
# CHECK FILES
# =====================================================

if not all_files:
    raise Exception("Aucun fichier Bronze trouvé")

print(
    "Total fichiers Bronze :",
    len(all_files)
)

# =====================================================
# FUSION DATA
# =====================================================

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

# =====================================================
# DATAFRAME
# =====================================================

df = pd.DataFrame(all_data)

print(
    "Nombre lignes avant nettoyage :",
    len(df)
)

# =====================================================
# CHECK EMPTY
# =====================================================

if df.empty:
    raise Exception("Aucune donnée trouvée")

# =====================================================
# NORMALISATION COLONNES
# =====================================================

df.columns = [
    str(col).lower()
    for col in df.columns
]

# =====================================================
# REQUIRED COLUMNS
# Garantit que toutes les colonnes attendues existent
# même si un scraper ne les produit pas
# =====================================================

required_columns = [
    "title",
    "company",
    "city",
    "publication_date",
    "source",
    "link",
]

for col in required_columns:
    if col not in df.columns:
        df[col] = ""

# =====================================================
# STRING TYPES
# Tout convertir en string pour éviter les erreurs
# de typage mixte (NaN, float, None…)
# =====================================================

for col in df.columns:
    try:
        df[col] = df[col].astype(str)
    except:
        pass

# =====================================================
# NETTOYAGE VALEURS PARASITES
# Remplace toutes les valeurs vides / inconnues
# par une chaîne vide uniforme avant traitement
# =====================================================

EMPTY_VALUES = [
    "nan",
    "none",
    "null",
    "n/a",
    "na",
    "non spécifié",   # injecté par le scraper rekrute
    "non specifie",
    "-",
    "–",
    ".",
]

for col in df.columns:
    df[col] = df[col].str.strip()
    df[col] = df[col].apply(
        lambda x: ""
        if str(x).lower() in EMPTY_VALUES
        else x
    )

# =====================================================
# REMOVE DUPLICATES
# Dédoublonnage sur le lien (clé naturelle unique)
# =====================================================

before_dedup = len(df)

if "link" in df.columns:
    df = df.drop_duplicates(subset=["link"])
else:
    df = df.drop_duplicates()

print(
    f"Déduplication : {before_dedup} → {len(df)} lignes"
    f" ({before_dedup - len(df)} doublons supprimés)"
)

# =====================================================
# CLEAN TITLE
# Supprime les mentions de genre et les espaces
# =====================================================

GENDER_MENTIONS = [
    r"\(H/F\)", r"\(M/F\)", r"\(F/H\)",
    r"\(H\.F\)", r"\(M\.F\)", r"\(F\.H\)",
    r"H/F", r"M/F", r"F/H",
]

def clean_title(title):

    if not isinstance(title, str) or title.strip() == "":
        return "unknown"

    title = title.strip()

    for pattern in GENDER_MENTIONS:
        title = re.sub(pattern, "", title, flags=re.IGNORECASE)

    # supprime les espaces multiples créés après suppression
    title = re.sub(r"\s+", " ", title).strip()

    return title.lower()

df["title_clean"] = df["title"].apply(clean_title)

# =====================================================
# CLEAN COMPANY
# Normalise le nom de l'entreprise
# =====================================================

def clean_company(company):

    if not isinstance(company, str) or company.strip() == "":
        return "Inconnu"

    company = company.strip()

    # supprime les caractères parasites en début/fin
    company = re.sub(r"^[\-\–\.\s]+", "", company)
    company = re.sub(r"[\-\–\.\s]+$", "", company)

    # met en title case si tout en majuscules
    if company.isupper():
        company = company.title()

    return company.strip()

df["company"] = df["company"].apply(clean_company)

# =====================================================
# KNOWN CITIES
# =====================================================

KNOWN_CITIES = [
    "Casablanca",
    "Rabat",
    "Marrakech",
    "Tanger",
    "Agadir",
    "Fès",
    "Meknès",
    "Oujda",
    "Kenitra",
    "Tétouan",
    "Salé",
    "Mohammedia",
    "El Jadida",
    "Nador",
    "Safi",
    "Khouribga",
    "Béni Mellal",
    "Laâyoune",
    "Dakhla",
    "Settat",
    "Berrechid",
    "Khémisset",
    "Ifrane",
    "Larache",
    "Remote",
]

# =====================================================
# INVALID CITY VALUES
# Valeurs qui ressemblent à des villes mais n'en sont pas
# =====================================================

INVALID_CITY_VALUES = [
    "developer", "developpeur", "développeur",
    "engineer",
    "python", "java", "react", "angular", "node",
    "frontend", "backend", "fullstack",
    "c developer", "java developer",
    "data scientist", "data engineer", "data analyst",
    "senior", "junior", "lead", "expert",
    "h/f", "m/f", "f/h",
    "remote work", "hybrid",
    "cdi", "cdd", "stage", "freelance",
    "temps plein", "temps partiel",
]

REMOTE_KEYWORDS = [
    "remote",
    "hybrid",
    "hybride",
    "distance",
    "télétravail",
    "teletravail",
]

# =====================================================
# EXTRACT CITY
# Logique différenciée par source :
#
#   rekrute  → champ "city" déjà propre (extrait par
#              le scraper), on le conserve directement
#
#   emploima → pas de champ city dédié, on l'extrait
#              depuis le titre (pattern: "Titre - Ville")
#
# Dans tous les cas, si aucune ville trouvée → "Remote"
# =====================================================

def extract_city(row):

    source = str(row.get("source", "")).strip().lower()
    city   = str(row.get("city",   "")).strip()

    # ── Rekrute : city déjà propre ───────────────────
    if source == "rekrute" and city not in ("", "nan", "none"):

        # vérifie que ce n'est pas une valeur parasite
        if not any(
            inv in city.lower()
            for inv in INVALID_CITY_VALUES
        ):
            return city

    # ── Recherche dans le titre (emploi.ma + fallback) ─
    title = str(row.get("title", ""))

    # 1) Villes connues dans le titre
    for known in KNOWN_CITIES:
        if known.lower() in title.lower():
            return known

    # 2) Remote / Hybride
    for kw in REMOTE_KEYWORDS:
        if kw in title.lower():
            return "Remote"

    # 3) Dernier segment après " - "
    if " - " in title:

        possible = (
            title.split(" - ")[-1]
            .strip()
        )

        # nettoie les mentions genre
        for pattern in GENDER_MENTIONS:
            possible = re.sub(
                pattern, "", possible,
                flags=re.IGNORECASE
            )

        possible = possible.strip()
        possible_lower = possible.lower()

        is_invalid = any(
            inv in possible_lower
            for inv in INVALID_CITY_VALUES
        )

        is_too_short = len(possible) <= 2

        has_digit = any(
            c.isdigit() for c in possible
        )

        has_slash = "/" in possible

        if (
            not is_invalid
            and not is_too_short
            and not has_digit
            and not has_slash
        ):
            return possible

    # 4) Valeur par défaut
    return "Remote"

df["city"] = df.apply(extract_city, axis=1)

# =====================================================
# IT KEYWORDS
# =====================================================

it_keywords = [
    "développeur", "developpeur", "developer",
    "software engineer",
    "data engineer", "data analyst", "data scientist",
    "machine learning", "deep learning",
    "devops", "cloud",
    "backend", "frontend",
    "fullstack", "full stack",
    "big data",
    "etl",
    "cybersecurity", "cybersécurité",
    "informatique",
    "python", "java", "sql",
    "docker", "kafka", "spark", "airflow",
    "react", "angular", "node",
    "c++", "c#", ".net", "dotnet",
    "réseaux", "telecom", "télécom",
    "consultant", "squad leader",
    "servicenow", "salesforce", "vdi",
    "infrastructure", "business analyst",
    "chargé si", "chef de projet",
    "architecte logiciel",
    "pentest", "devsecops",
    "qa", "test automation",
    "oracle", "azure", "aws", "gcp",
]

exclude_keywords = [
    "commercial", "sales",
    "finance", "comptable",
    "caissier", "catalogage",
    "gestionnaire de paie",
    "assistant administratif",
    "marketing",
    "juridique",
    "infirmier",
    "logistique",
    "achat",
    "rh", "recrutement",
]

# =====================================================
# KEYWORD MATCH
# =====================================================

def contains_keyword(text, keywords):

    text = str(text).lower()

    for keyword in keywords:

        pattern = (
            r"\b"
            + re.escape(keyword.lower())
            + r"\b"
        )

        if re.search(pattern, text):
            return True

    return False

# =====================================================
# FILTER IT
# =====================================================

def is_it_job(title):

    title = str(title).lower()

    is_it = contains_keyword(title, it_keywords)

    is_excluded = contains_keyword(title, exclude_keywords)

    return is_it and not is_excluded

before_it = len(df)

df = df[df["title_clean"].apply(is_it_job)]

print(
    f"Filtre IT : {before_it} → {len(df)} lignes"
    f" ({before_it - len(df)} non-IT supprimées)"
)

# =====================================================
# WORK MODE
# =====================================================

def detect_work_mode(title):

    title = str(title).lower()

    for keyword in REMOTE_KEYWORDS:
        if keyword in title:
            return "Remote/Hybrid"

    return "Onsite"

df["work_mode"] = df["title"].apply(detect_work_mode)

# =====================================================
# SENIORITY
# =====================================================

def detect_seniority(title):

    title = str(title).lower()

    junior_keywords = [
        "junior",
        "stagiaire",
        "intern",
        "débutant",
        "entry level",
    ]

    senior_keywords = [
        "senior",
        "lead",
        "expert",
        "principal",
        "staff",
        "confirmé",
        "confirme",
    ]

    for keyword in junior_keywords:
        if keyword in title:
            return "Junior"

    for keyword in senior_keywords:
        if keyword in title:
            return "Senior"

    return "Mid-Level"

df["seniority"] = df["title"].apply(detect_seniority)

# =====================================================
# NORMALISATION DATE DE PUBLICATION
# Formats possibles :
#   rekrute  → "12/05/2026"  (DD/MM/YYYY)
#   emploima → "2026-05-12"  (YYYY-MM-DD) ou texte libre
# =====================================================

def normalize_date(date_str):

    if not isinstance(date_str, str) or date_str.strip() == "":
        return pd.NaT

    date_str = date_str.strip()

    # Essai format DD/MM/YYYY (rekrute)
    try:
        return pd.to_datetime(date_str, format="%d/%m/%Y")
    except:
        pass

    # Essai format YYYY-MM-DD (emploima)
    try:
        return pd.to_datetime(date_str, format="%Y-%m-%d")
    except:
        pass

    # Essai détection automatique
    try:
        return pd.to_datetime(date_str, infer_datetime_format=True)
    except:
        return pd.NaT

df["publication_date"] = df["publication_date"].apply(
    normalize_date
)

# =====================================================
# SORT DATE
# =====================================================

df = df.sort_values(
    by="publication_date",
    ascending=False,
    na_position="last"
)

# =====================================================
# CONVERSION DATE -> STRING DD/MM/YYYY
# Apres le tri (qui necessite un vrai type datetime),
# on convertit en string lisible pour le JSON.
# NaT -> chaine vide, gere par le nettoyage final.
# =====================================================

df["publication_date"] = df["publication_date"].apply(
    lambda x: x.strftime("%d/%m/%Y")
    if pd.notna(x) and hasattr(x, "strftime")
    else ""
)

# =====================================================
# PROCESS DATE
# =====================================================

df["processed_date"] = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)

# =====================================================
# COLONNES FINALES (ordre propre)
# =====================================================

final_columns = [
    "title",
    "title_clean",
    "company",
    "city",
    "seniority",
    "work_mode",
    "publication_date",
    "processed_date",
    "source",
    "link",
]

# ajoute les colonnes finales manquantes si besoin
for col in final_columns:
    if col not in df.columns:
        df[col] = ""

df = df[final_columns]

# =====================================================
# NETTOYAGE FINAL
# Supprime tous les nan / NaT / None résiduels
# sur toutes les colonnes avant export JSON
# =====================================================

for col in df.columns:
    if df[col].dtype == "object":
        df[col] = df[col].fillna("")
        df[col] = df[col].apply(
            lambda x: ""
            if str(x).strip().lower() in ("nan", "none", "null", "nat", "")
            else x
        )
    else:
        df[col] = df[col].apply(
            lambda x: ""
            if pd.isna(x)
            else x
        )

# =====================================================
# EXPORT SILVER
# =====================================================

clean_data = df.to_dict(orient="records")

now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

output_key = f"silver/jobs_clean_{now}.json"

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
print("Silver créé      :", output_key)
print("Nombre final     :", len(df))
print("Pipeline Silver terminé ✅")