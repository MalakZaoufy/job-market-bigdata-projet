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
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

TOPIC_NAME = "jobs_topic"

print("Connexion Kafka OK")

# =========================================
# HEADERS
# =========================================

headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

# =========================================
# PAGINATION
# =========================================

pages_to_scrape = 4

# =========================================
# STREAMING INTERVAL (secondes)
# =========================================

SCRAPING_INTERVAL = 300

# =========================================
# MEMORY (anti-doublon)
# =========================================

sent_jobs = set()

# =========================================
# CONSTANTES
# =========================================

NON_SPECIFIE = "non spécifié"

# =========================================
# IT KEYWORDS
# =========================================

it_keywords = [
    "informatique",
    "developer", "developpeur", "développeur",
    "software engineer",
    "python", "java", "sql", "c++", "c#", "javascript",
    "react", "angular", "node",
    "cloud", "devops",
    "full stack", "fullstack",
    "backend", "frontend",
    "etl", "big data",
    "machine learning", "deep learning",
    "data engineer", "data analyst", "data scientist",
    "cybersecurity", "cybersécurité",
    "docker", "kafka", "spark", "airflow",
    "aws", "azure", "gcp",
    "power bi",
    "kubernetes",
    "security", "sécurité",
    "pentest", "devsecops",
    "sap", "crm dynamics", "salesforce", "servicenow",
    "qa", "test automation",
    "mobile engineer",
    "network engineer", "system engineer",
    "réseaux", "telecom", "télécom",
    "infrastructure",
    "architecte logiciel",
    "business analyst",
    "chef de projet",
    "consultant",
    ".net", "dotnet", "oracle",
]

exclude_keywords = [
    "business developer",
    "commercial", "sales", "commerciaux",
    "rh", "ressources humaines",
    "recrutement",
    "finance", "comptable",
    "caissier",
    "gestionnaire de paie", "gestionnaire", "achat",
    "catalogage",
    "stagiaire gestion",
    "assistant administratif",
    "logistique",
    "marketing", "business officer",
    "juridique",
    "infirmier",
]

# =========================================
# UTILITAIRES
# =========================================

def normalize(text):
    return (
        unicodedata
        .normalize("NFKD", text)
        .encode("ASCII", "ignore")
        .decode("utf-8")
        .lower()
    )

def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

def contains_keyword(text, keywords):
    for keyword in keywords:
        pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
        if re.search(pattern, text):
            return True
    return False

def is_it_job(title):
    title_norm = normalize(str(title))
    return (
        contains_keyword(title_norm, it_keywords)
        and not contains_keyword(title_norm, exclude_keywords)
    )

def get_soup(url, hdrs=None):
    response = requests.get(
        url,
        headers=hdrs or headers,
        timeout=15
    )
    response.raise_for_status()
    return BeautifulSoup(response.text, "lxml")

def send_to_kafka(job_data):
    producer.send(
        TOPIC_NAME,
        key=job_data["company"].encode("utf-8"),
        value=job_data
    )

# =========================================
# AFFICHAGE TERMINAL
# =========================================

def print_job(job):
    sep = "─" * 55
    print(sep)
    print(f"  title            : {job['title']}")
    print(f"  company          : {job['company']}")
    print(f"  city             : {job['city']}")
    print(f"  publication_date : {job['publication_date']}")
    print(f"  link             : {job['link']}")
    print(f"  source           : {job['source']}")
    print(f"  scraping_time    : {job['scraping_time']}")

# =========================================
# EMPLOI.MA CONFIG
# =========================================

EMPLOI_BASE_URL = "https://www.emploi.ma/recherche-jobs-maroc?page={}"

# =========================================
# EMPLOI.MA SCRAPER
# =========================================

def scrape_emploi_jobs():
    global sent_jobs

    total_jobs_sent = 0

    print("\n========== EMPLOI.MA ==========")

    for page in range(1, pages_to_scrape + 1):
        print(f"Page {page}")

        url = EMPLOI_BASE_URL.format(page)

        try:
            soup = get_soup(url)
        except Exception as e:
            print(f"  Erreur Emploi.ma page {page} : {e}")
            continue

        job_cards = soup.find_all("div", class_="card-job-detail")
        print(f"  Offres trouvées : {len(job_cards)}")

        for job in job_cards:
            try:
                title_tag   = job.find("h3")
                company_tag = job.find("a", class_="company-name")
                link_tag    = job.find("a", href=True)

                title   = clean_text(title_tag.text)   if title_tag   else ""
                company = clean_text(company_tag.text) if company_tag else NON_SPECIFIE

                job_link = (
                    "https://www.emploi.ma" + link_tag["href"]
                    if link_tag else ""
                )

                if not job_link or job_link in sent_jobs:
                    continue

                if not is_it_job(title):
                    continue

                location_tag = job.find("span", class_="location")
                city = clean_text(location_tag.text) if location_tag else NON_SPECIFIE

                job_data = {
                    "id":               job_link,
                    "title":            title or NON_SPECIFIE,
                    "company":          company,
                    "city":             city,
                    "publication_date": datetime.now().strftime("%Y-%m-%d"),
                    "link":             job_link,
                    "source":           "emploi.ma",
                    "scraping_time":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                send_to_kafka(job_data)
                sent_jobs.add(job_link)
                total_jobs_sent += 1

                print_job(job_data)

            except Exception as e:
                print(f"  Erreur offre Emploi.ma : {e}")

        time.sleep(2)

    producer.flush()
    print(f"  → Emploi.ma envoyées : {total_jobs_sent}")

# =========================================
# REKRUTE CONFIG
# =========================================

REKRUTE_BASE_URL = (
    "https://www.rekrute.com/offres.html"
    "?s=1&p={}&o=1"
    "&positionId%5B0%5D=13"
    "&positionId%5B1%5D=19"
    "&positionId%5B2%5D=23"
)

# =========================================
# REKRUTE CARD PARSER
#
# Structure réelle d'une card Rekrute :
#
#   <li class="post-id" id="182712">
#     <div class="col-sm-2">
#       <img alt="Nom Entreprise" class="photo">   ← company
#     </div>
#     <div class="col-sm-10">
#       <h2>
#         <a class="titreJob" href="/offre-...html">
#           Titre du Poste | Casablanca (Maroc)    ← title | city
#         </a>
#       </h2>
#       <em class="date">
#         Publication : du <span>12/05/2026</span> au ...  ← date
#       </em>
#     </div>
#   </li>
# =========================================

def parse_rekrute_card(card):
    """
    Parse un <li class="post-id"> et retourne un dict job ou None.
    Aucune requête HTTP supplémentaire — tout est dans la card.
    """
    # --- lien + titre brut ---
    a_tag = card.select_one("a.titreJob")
    if not a_tag:
        return None

    href = a_tag.get("href", "")
    link = "https://www.rekrute.com" + href if href.startswith("/") else href

    # Format : "Titre | Ville (Pays)"
    titre_brut = clean_text(a_tag.get_text())
    if "|" in titre_brut:
        parts     = titre_brut.split("|", 1)
        title     = clean_text(parts[0])
        ville_raw = clean_text(parts[1])
        city      = clean_text(ville_raw.split("(")[0])
    else:
        title = titre_brut
        city  = NON_SPECIFIE

    # --- filtre IT ---
    if not is_it_job(title):
        return None

    # --- entreprise : alt de l'img logo ---
    company = NON_SPECIFIE
    img_tag = card.select_one(".col-sm-2 img.photo")
    if img_tag:
        alt = clean_text(img_tag.get("alt", ""))
        if alt:
            company = alt
        else:
            title_attr = clean_text(img_tag.get("title", ""))
            if title_attr:
                company = title_attr

    # --- date : premier <span> dans <em class="date"> ---
    publication_date = datetime.now().strftime("%Y-%m-%d")
    em_date = card.select_one("em.date")
    if em_date:
        spans = em_date.find_all("span")
        if spans:
            publication_date = clean_text(spans[0].get_text())

    return {
        "id":               link,
        "title":            title or NON_SPECIFIE,
        "company":          company,
        "city":             city or NON_SPECIFIE,
        "publication_date": publication_date,
        "link":             link,
        "source":           "rekrute",
        "scraping_time":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

# =========================================
# REKRUTE SCRAPER
# =========================================

def scrape_rekrute_jobs():
    global sent_jobs

    total_jobs_sent = 0

    print("\n========== REKRUTE ==========")

    for page in range(1, pages_to_scrape + 1):
        print(f"Page {page}")

        url = REKRUTE_BASE_URL.format(page)

        try:
            soup  = get_soup(url)
            cards = soup.select("li.post-id")
            print(f"  Offres trouvées : {len(cards)}")
        except Exception as e:
            print(f"  Erreur Rekrute page {page} : {e}")
            continue

        for card in cards:
            try:
                job_data = parse_rekrute_card(card)

                if job_data is None:
                    continue

                if job_data["link"] in sent_jobs:
                    continue

                send_to_kafka(job_data)
                sent_jobs.add(job_data["link"])
                total_jobs_sent += 1

                print_job(job_data)

            except Exception as e:
                print(f"  Erreur offre Rekrute : {e}")

        time.sleep(2)

    producer.flush()
    print(f"  → Rekrute envoyées : {total_jobs_sent}")

# =========================================
# STREAMING LOOP
# =========================================

print("\n========== KAFKA STREAMING ==========\n")

while True:
    try:
        scrape_emploi_jobs()
        scrape_rekrute_jobs()

        print()
        print(f"Attente {SCRAPING_INTERVAL // 60} minutes...")
        print("-" * 60)
        time.sleep(SCRAPING_INTERVAL)

    except KeyboardInterrupt:
        print("\nArrêt manuel.")
        break

    except Exception as e:
        print(f"Erreur globale : {e}")
        time.sleep(30)