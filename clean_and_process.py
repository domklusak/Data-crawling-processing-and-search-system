from bs4 import BeautifulSoup
import os
import json
import re

def extract_text_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    paragraphs = soup.find_all('p')
    extracted_text = " ".join(para.get_text(" ", strip=True) for para in paragraphs)
    cleaned_text = re.sub(r'\s+', ' ', extracted_text)
    return cleaned_text


def process_and_save_data():
    all_data = []

    for filename in os.listdir(INPUT_DIRECTORY):
        filepath = os.path.join(INPUT_DIRECTORY, filename)
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)
            cleaned_content = extract_text_from_html(data["content"])
            all_data.append({
                "url": data["url"],
                "content": cleaned_content
            })

    # ukladanie dat
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as output_file:
        json.dump(all_data, output_file, ensure_ascii=False, indent=4)


INPUT_DIRECTORY = "downloaded_pages"
OUTPUT_FILE = "cleaned_data.json"
process_and_save_data()
print("Data extraction completed")
