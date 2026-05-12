import requests
from bs4 import BeautifulSoup
import json
import re
from pathlib import Path
import time

# =====================================================
# CONFIG
# =====================================================

BASE_URL = "https://www.rekrute.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

OUTPUT_DIR = Path("data/raw/rekrute")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MAX_PAGES = 30

NON_SPECIFIE = "non spécifié"

# =====================================================
# CLEAN TEXT
# =====================================================

def clean_text(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()

# =====================================================
# IT KEYWORDS
# =====================================================

it_keywords = [
    "développeur", "developpeur", "developer",
    "software engineer",
    "data", "data engineer", "data analyst", "data scientist",
    "machine learning", "deep learning",
    "devops", "cloud",
    "backend", "frontend",
    "fullstack", "full stack",
    "big data", "etl",
    "cybersecurity", "cybersécurité",
    "informatique",
    "python", "java", "sql",
    "docker", "kafka", "spark", "airflow",
    "react", "angular", "node",
    "consultant sap",
    "c++", "c#", ".net", "dotnet",
    "crm dynamics",
    "mobile core", "mobile engineer",
    "sécurité", "security",
    "pentest", "devsecops",
    "architecte logiciel",
    "qa", "test automation",
    "system engineer", "network engineer",
    "oracle", "azure", "aws", "gcp",
    "réseaux", "telecom", "télécom",
    "si", "chargé si", "chef de projet",
    "consultant", "squad leader",
    "servicenow", "salesforce", "vdi",
    "infrastructure", "business analyst",
]

exclude_keywords = [
    "commercial", "sales",
    "finance", "comptable",
    "assistant administratif",
    "rh", "recrutement",
    "caissier", "logistique",
    "marketing", "business officer",
    "commerciaux",
    "gestionnaire", "achat",
    "infirmier", "juridique",
]

# =====================================================
# KEYWORD MATCH
# =====================================================

def contains_keyword(text, keywords):
    text = str(text).lower()
    for keyword in keywords:
        pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
        if re.search(pattern, text):
            return True
    return False

def is_it_job(title):
    title = str(title).lower()
    return (
        contains_keyword(title, it_keywords)
        and not contains_keyword(title, exclude_keywords)
    )

# =====================================================
# GET HTML
# =====================================================

def get_soup(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

# =====================================================
# PARSE UNE CARD  (li.post-id)
#
# Structure réelle Rekrute (vérifiée sur le HTML source) :
#
#   <li class="post-id" id="182712">
#     <div class="col-sm-2">
#       <img alt="Labs networks maghrib" class="photo">   ← ENTREPRISE
#     </div>
#     <div class="col-sm-10">
#       <h2>
#         <a class="titreJob" href="/offre-...html">
#           Consultant Radio Télécom | Casablanca (Maroc)  ← TITRE | VILLE
#         </a>
#       </h2>
#       <em class="date">
#         Publication : du <span>12/05/2026</span> au ... ← DATE
#       </em>
#     </div>
#   </li>
# =====================================================

def parse_card(card):
    """
    Extrait titre, company, ville, date, lien depuis un <li class="post-id">.
    Retourne un dict ou None si non IT / doublon.
    """

    # --------------------------------------------------
    # LIEN + TITRE BRUT
    # --------------------------------------------------
    a_tag = card.select_one("a.titreJob")
    if not a_tag:
        return None

    href = a_tag.get("href", "")
    link = BASE_URL + href if href.startswith("/") else href

    # Format Rekrute : "Titre du poste | Ville (Pays)"
    titre_brut = clean_text(a_tag.get_text())
    if "|" in titre_brut:
        parts     = titre_brut.split("|", 1)
        title     = clean_text(parts[0])
        ville_raw = clean_text(parts[1])       # ex: "Casablanca (Maroc)"
        city      = clean_text(ville_raw.split("(")[0])
    else:
        title = titre_brut
        city  = NON_SPECIFIE

    # --------------------------------------------------
    # FILTRE IT
    # --------------------------------------------------
    if not is_it_job(title):
        return None

    # --------------------------------------------------
    # ENTREPRISE
    # L'image logo (col-sm-2) a son alt = nom de l'entreprise.
    # C'est la donnée la plus fiable sur Rekrute.
    # --------------------------------------------------
    company = NON_SPECIFIE

    img_tag = card.select_one(".col-sm-2 img.photo")
    if img_tag:
        alt = clean_text(img_tag.get("alt", ""))
        if alt:
            company = alt

    # Fallback : attribut title du même <img>
    if company == NON_SPECIFIE and img_tag:
        title_attr = clean_text(img_tag.get("title", ""))
        if title_attr:
            company = title_attr

    # --------------------------------------------------
    # DATE DE PUBLICATION
    # <em class="date">Publication : du <span>12/05/2026</span> au ...</em>
    # --------------------------------------------------
    publication_date = NON_SPECIFIE

    em_date = card.select_one("em.date")
    if em_date:
        spans = em_date.find_all("span")
        if spans:
            publication_date = clean_text(spans[0].get_text())

    # --------------------------------------------------
    # RÉSULTAT
    # --------------------------------------------------
    return {
        "title":            title            or NON_SPECIFIE,
        "company":          company,
        "city":             city             or NON_SPECIFIE,
        "publication_date": publication_date,
        "source":           "rekrute",
        "link":             link             or NON_SPECIFIE,
    }

# =====================================================
# AFFICHAGE TERMINAL (formaté)
# =====================================================

def print_job(job):
    sep = "─" * 45
    print(sep)
    print(f"title   : {job['title']}")
    print(f"company : {job['company']}")
    print(f"link    : {job['link']}")
    print(f"source  : {job['source']}")
    print(f"date    : {job['publication_date']}")
    print(f"city    : {job['city']}")

# =====================================================
# MAIN
# =====================================================

def main():
    print("=" * 45)
    print("         REKRUTE SCRAPER")
    print("=" * 45)

    jobs = []
    seen = set()

    # --------------------------------------------------
    # PAGINATION — extraction directe depuis la liste
    # --------------------------------------------------
    for page_num in range(1, MAX_PAGES + 1):
        page_url = (
            f"{BASE_URL}/offres.html"
            f"?s=1&p={page_num}&o=1"
            f"&positionId%5B0%5D=13"
            f"&positionId%5B1%5D=19"
            f"&positionId%5B2%5D=23"
        )

        print(f"\n>>> PAGE {page_num}")

        try:
            soup  = get_soup(page_url)
            cards = soup.select("li.post-id")
            print(f"    {len(cards)} offres sur cette page")

            for card in cards:
                job = parse_card(card)
                if job is None:
                    continue
                if job["link"] in seen:
                    continue
                seen.add(job["link"])
                jobs.append(job)
                print_job(job)

        except Exception as e:
            print(f"    [ERREUR page {page_num}] {e}")

        time.sleep(2)

    # --------------------------------------------------
    # SAUVEGARDE JSON
    # --------------------------------------------------
    output_file = OUTPUT_DIR / "rekrute_jobs.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=4)

    print(f"\n{'=' * 45}")
    print(f"  {len(jobs)} offres IT sauvegardées")
    print(f"  Fichier : {output_file}")
    print(f"{'=' * 45}")

# =====================================================
# RUN
# =====================================================

if __name__ == "__main__":
    main()