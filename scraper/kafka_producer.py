from kafka import KafkaProducer
import json
import requests
from bs4 import BeautifulSoup
import time
import unicodedata
import re
from datetime import datetime

# =========================================
# CONFIG KAFKA
# =========================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC_NAME = "jobs_topic"

print("Connexion Kafka OK")

# =========================================
# CONFIG SCRAPING
# =========================================

base_url = "https://www.emploi.ma/recherche-jobs-maroc?page={}"

headers = {
    "User-Agent": "Mozilla/5.0"
}

# Nombre pages scraping
pages_to_scrape = 30

# Temps attente entre deux cycles
SCRAPING_INTERVAL = 300  # 5 minutes

# =========================================
# MÉMOIRE OFFRES DÉJÀ ENVOYÉES
# =========================================

sent_jobs = set()

# =========================================
# MOTS-CLÉS IT
# =========================================

it_keywords = [

    "informatique",

    "developer",
    "developpeur",
    "développeur",

    "software engineer",

    "python",
    "java",
    "sql",
    "c++",
    "c#",
    "javascript",
    "react",

    "cloud",
    "devops",

    "full stack",
    "fullstack",

    "backend",
    "frontend",

    "etl",
    "big data",

    "machine learning",
    "deep learning",

    "data engineer",
    "data analyst",
    "data scientist",

    "cybersecurity",

    "docker",
    "kafka",
    "spark",
    "airflow",

    "aws",
    "azure",

    "power bi",

    "kubernetes"
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

    "stagiaire gestion",

    "assistant administratif"
]

# =========================================
# NORMALISATION
# =========================================

def normalize(text):

    return (
        unicodedata
        .normalize("NFKD", text)
        .encode("ASCII", "ignore")
        .decode("utf-8")
        .lower()
    )

# =========================================
# MATCH MOT COMPLET
# =========================================

def contains_keyword(text, keywords):

    for keyword in keywords:

        pattern = r"\b" + re.escape(
            keyword.lower()
        ) + r"\b"

        if re.search(pattern, text):
            return True

    return False

# =========================================
# EXTRACTION DATE
# =========================================

def extract_date(text):

    match = re.search(
        r"\d{2}\.\d{2}\.\d{4}",
        text
    )

    if match:
        return match.group()

    return datetime.now().strftime(
        "%d.%m.%Y"
    )

# =========================================
# FORMAT DATE
# =========================================

def format_date(date_str):

    try:

        return datetime.strptime(
            date_str,
            "%d.%m.%Y"
        ).strftime("%Y-%m-%d")

    except:

        return datetime.now().strftime(
            "%Y-%m-%d"
        )

# =========================================
# SCRAPING FUNCTION
# =========================================

def scrape_jobs():

    global sent_jobs

    total_jobs_sent = 0

    print("\n========== NOUVEAU CYCLE SCRAPING ==========")

    print(
        "Heure :",
        datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    print()

    for page in range(
        1,
        pages_to_scrape + 1
    ):

        print(f"Scraping page {page}...")

        url = base_url.format(page)

        # =====================================
        # REQUÊTE HTTP
        # =====================================

        try:

            response = requests.get(
                url,
                headers=headers,
                timeout=15
            )

            if response.status_code != 200:

                print(
                    f"Erreur HTTP page {page}"
                )

                continue

        except requests.exceptions.RequestException as e:

            print(
                f"Erreur connexion : {e}"
            )

            continue

        # =====================================
        # PARSING HTML
        # =====================================

        soup = BeautifulSoup(
            response.text,
            "lxml"
        )

        job_cards = soup.find_all(
            "div",
            class_="card-job-detail"
        )

        print(
            f"Nombre offres trouvées : {len(job_cards)}"
        )

        # =====================================
        # EXTRACTION OFFRES
        # =====================================

        for job in job_cards:

            try:

                title_tag = job.find("h3")

                company_tag = job.find(
                    "a",
                    class_="company-name"
                )

                link_tag = job.find(
                    "a",
                    href=True
                )

                title = (
                    title_tag.text.strip()
                    if title_tag
                    else "Non trouvé"
                )

                company = (
                    company_tag.text.strip()
                    if company_tag
                    else "Non trouvé"
                )

                job_link = (
                    "https://www.emploi.ma"
                    + link_tag["href"]
                    if link_tag
                    and link_tag.get("href")
                    else "Non trouvé"
                )

                # =================================
                # IGNORER OFFRES DÉJÀ ENVOYÉES
                # =================================

                if job_link in sent_jobs:
                    continue

                job_text = job.get_text(
                    " ",
                    strip=True
                )

                publication_date = format_date(
                    extract_date(job_text)
                )

                # =================================
                # FILTRAGE IT STRICT
                # =================================

                title_clean = normalize(title)

                is_it = contains_keyword(
                    title_clean,
                    it_keywords
                )

                is_excluded = contains_keyword(
                    title_clean,
                    exclude_keywords
                )

                # =================================
                # ENVOI KAFKA
                # =================================

                if is_it and not is_excluded:

                    job_data = {

                        # identifiant unique
                        "id": job_link,

                        "title": title,

                        "company": company,

                        "publication_date": publication_date,

                        "link": job_link,

                        "source": "emploi.ma",

                        "scraping_time": datetime.now().strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                    }

                    producer.send(
                        TOPIC_NAME,
                        key=company.encode("utf-8"),
                        value=job_data
                    )

                    # mémorisation offre
                    sent_jobs.add(job_link)

                    total_jobs_sent += 1

                    print(
                        "Nouvelle offre IT envoyée ✅"
                    )

                    print(job_data)

                    print("-" * 60)

            except Exception as e:

                print(
                    "Erreur extraction offre :",
                    e
                )

        # pause entre pages
        time.sleep(2)

    # =====================================
    # FLUSH KAFKA
    # =====================================

    producer.flush()

    print()

    print(
        f"Total nouvelles offres IT envoyées : {total_jobs_sent}"
    )

    print(
        f"Total offres mémorisées : {len(sent_jobs)}"
    )

    print("Cycle terminé ✅")

# =========================================
# BOUCLE STREAMING CONTINUE
# =========================================

print(
    "\n========== KAFKA STREAMING PRODUCER ==========\n"
)

while True:

    try:

        scrape_jobs()

        print()

        print(
            f"Attente {SCRAPING_INTERVAL // 60} minutes..."
        )

        print("-" * 60)

        time.sleep(
            SCRAPING_INTERVAL
        )

    except KeyboardInterrupt:

        print(
            "\nArrêt manuel du producer."
        )

        break

    except Exception as e:

        print(
            f"Erreur globale : {e}"
        )

        print(
            "Nouvelle tentative dans 30 secondes..."
        )

        time.sleep(30)