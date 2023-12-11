from bs4 import BeautifulSoup
import os
import json
import re

def extract_text_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    paragraphs = soup.find_all('p')
    extracted_text = " ".join(para.get_text(" ", strip=True) for para in paragraphs)
    cleaned_text = re.sub(r'\s+', ' ', extracted_text)
    print(cleaned_text)
    return cleaned_text


def process_and_save_data(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as file:
        data_list = json.load(file)

    all_data = []

    for data in data_list:
        cleaned_content = extract_text_from_html(data["content"])
        all_data.append({
            "url": data["url"],
            "content": cleaned_content
        })

    with open(output_file, 'w', encoding='utf-8') as output_file:
        json.dump(all_data, output_file, ensure_ascii=False, indent=4)
    print("Data extraction completed")

