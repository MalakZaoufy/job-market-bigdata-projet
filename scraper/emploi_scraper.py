import requests
from bs4 import BeautifulSoup
import time
import json
import re
from datetime import datetime
import unicodedata

# ================================
# CONFIGURATION
# ================================

base_url = "https://www.emploi.ma/recherche-jobs-maroc?page={}"
headers = {"User-Agent": "Mozilla/5.0"}

pages_to_scrape = 30

it_keywords = [
    "informatique",
    "data",
    "developer",
    "developpeur",
    "software",
    "python",
    "java",
    "cloud",
    "devops",
    "full stack",
    "backend",
    "frontend",
    "etl",
    "big data",
    "machine learning"
]

exclude_keywords = [
    "business developer",
    "commercial",
    "sales",
    "rh",
    "ressources humaines",
    "recrutement",
    "paie",
    "finance",
    "comptable"
]

# ================================
# NORMALISATION TEXTE (IMPORTANT)
# ================================

def normalize(text):
    return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8').lower()

# ================================
# FONCTIONS DATE
# ================================

def extract_date(text):
    match = re.search(r"\d{2}\.\d{2}\.\d{4}", text)
    return match.group() if match else "Non trouvé"

def format_date(date_str):
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
    except:
        return date_str

# ================================
# SCRAPING
# ================================

jobs_data = []

print("========== SCRAPING EMPLOI.MA ==========\n")

for page in range(1, pages_to_scrape + 1):

    print(f"Scraping page {page}...")

    url = base_url.format(page)

    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=15
        )

        if response.status_code != 200:
            print(f"Erreur sur la page {page}")
            continue

    except requests.exceptions.RequestException as e:
        print(f"Erreur connexion page {page} : {e}")
        continue
    
    soup = BeautifulSoup(response.text, "lxml")
    job_cards = soup.find_all("div", class_="card-job-detail")

    print(f"Nombre d'offres trouvées : {len(job_cards)}\n")

    for job in job_cards:

        title_tag = job.find("h3")
        company_tag = job.find("a", class_="company-name")
        link_tag = job.find("a", href=True)

        title = title_tag.text.strip() if title_tag else "Non trouvé"
        company = company_tag.text.strip() if company_tag else "Non trouvé"

        job_link = (
            "https://www.emploi.ma" + link_tag["href"]
            if link_tag and link_tag.get("href")
            else "Non trouvé"
        )

        job_text = job.get_text(" ", strip=True)
        publication_date = format_date(extract_date(job_text))

        # ================================
        # FILTRAGE IT CORRIGÉ
        # ================================

        title_clean = normalize(title)

        is_it = any(k in title_clean for k in it_keywords)
        is_excluded = any(e in title_clean for e in exclude_keywords)

        if is_it and not is_excluded:

            job_info = {
                "title": title,
                "company": company,
                "publication_date": publication_date,
                "link": job_link,
                "source": "emploi.ma"
            }

            jobs_data.append(job_info)

            # affichage (debug propre)
            print("Titre :", title)
            print("Entreprise :", company)
            print("Date :", publication_date)
            print("Lien :", job_link)
            print("-" * 60)

    time.sleep(2)

# ================================
# SAUVEGARDE JSON
# ================================

print(f"\nNombre total d'offres IT récupérées : {len(jobs_data)}")

# 🔥 vérification critique
print("DEBUG SAVED ITEMS:", len(jobs_data))

now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
filename = f"jobs_data_{now}.json"

with open(filename, "w", encoding="utf-8") as file:
    json.dump(jobs_data, file, ensure_ascii=False, indent=4)

print("\nFichier créé :", filename)