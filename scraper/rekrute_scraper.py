import requests
from bs4 import BeautifulSoup
import time

url = "https://www.rekrute.com/offres.html?s=1&p=1&o=1"

headers = {
    "User-Agent": "Mozilla/5.0"
}

# vrais mots-clés IT sur le titre réel
it_keywords = [
    "data engineer",
    "data analyst",
    "business intelligence",
    "bi developer",
    "python developer",
    "java developer",
    "software developer",
    "software engineer",
    "devops engineer",
    "cloud engineer",
    "machine learning",
    "data scientist",
    "cybersecurity",
    "security engineer",
    "full stack developer",
    "backend developer",
    "frontend developer",
    "etl developer",
    "big data",
    "spark developer",
    "database administrator",
    "dba",
    "system engineer",
    "network engineer"
]

response = requests.get(url, headers=headers)

print("Status Code :", response.status_code)

if response.status_code == 200:
    print("Connexion réussie à Rekrute\n")

    soup = BeautifulSoup(response.text, "lxml")
    links = soup.find_all("a", href=True)

    job_links = []

    # récupérer les vrais liens d'offres
    for link in links:
        href = link["href"]

        if "/offre-emploi-" in href:
            clean_link = href.split("?")[0]
            full_link = "https://www.rekrute.com" + clean_link

            if full_link not in job_links:
                job_links.append(full_link)

    print(f"Nombre total de liens trouvés : {len(job_links)}\n")

    it_jobs = []

    # ouvrir chaque offre individuellement
    for job_url in job_links[:100]:  # test sur 10 offres seulement
        try:
            job_response = requests.get(job_url, headers=headers)

            if job_response.status_code == 200:
                job_soup = BeautifulSoup(job_response.text, "lxml")

                # récupérer le vrai titre de l'offre
                page_title = job_soup.title.text.lower()

                if any(keyword in page_title for keyword in it_keywords):
                    it_jobs.append(job_url)
                    print("Offre IT trouvée :", job_url)

            time.sleep(1)  # éviter blocage du site

        except Exception as e:
            print("Erreur :", e)

    print(f"\nNombre final d'offres IT : {len(it_jobs)}")

else:
    print("Erreur de connexion")