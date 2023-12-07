import requests
import os
import json
from bs4 import BeautifulSoup

BASE_URL = 'https://en.wikipedia.org'
START_URL = 'https://en.wikipedia.org/wiki/FIFA_World_Cup'
DATA_LIMIT = 1000 * 1024 * 1024
MAX_RETRIES = 3

TO_VISIT = [START_URL]
VISITED = set()
DATA_DOWNLOADED = 0

if not os.path.exists("downloaded_pages"):
    os.makedirs("downloaded_pages")


def download_page(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


while TO_VISIT and DATA_DOWNLOADED < DATA_LIMIT:
    current_url = TO_VISIT.pop(0)
    if current_url in VISITED:
        continue

    page_content = download_page(current_url)

    if not page_content:
        print(f"Failed to download {current_url}")
        continue

    DATA_DOWNLOADED += len(page_content)
    VISITED.add(current_url)

    # Save to JSON
    data = {
        "url": current_url,
        "content": page_content
    }
    filename = os.path.join("downloaded_pages", f"{hash(current_url)}.json")
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    EXCLUDED_PREFIXES = ["Special:", "Talk:", "File:", "Category:", "Template_talk:", "User:", "User_talk:", "Help:",
                         "Portal:", "Draft:", "Template:", ]

    soup = BeautifulSoup(page_content, 'html.parser')
    for link in soup.find_all('a', href=True):
        url_part = link.get('href')
        if any(prefix in url_part for prefix in EXCLUDED_PREFIXES):
            continue
        if "/wiki/" in url_part and ("FIFA_World_Cup" in url_part or "national_football_team" in url_part):
            next_url = BASE_URL + url_part
            if next_url not in VISITED and next_url not in TO_VISIT:
                TO_VISIT.append(next_url)

print(f"Downloaded {len(VISITED)} pages.")
