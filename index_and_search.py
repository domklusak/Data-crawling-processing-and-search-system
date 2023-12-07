import os
import json
import lucene
import html
import re
lucenevm = lucene.initVM(vmargs=['-Djava.awt.headless=true'])

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, MapType

from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.search import IndexSearcher

from org.apache.lucene.index import IndexWriter, IndexWriterConfig,DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser

from org.apache.lucene.document import Document, Field, StringField, TextField
from org.apache.lucene.store import FSDirectory

from java.io import File


print(lucenevm)

class LuceneApp:
    def __init__(self, index_dir="index"):
        if not os.path.exists(index_dir):
            os.mkdir(index_dir)

        self.store = FSDirectory.open(File(index_dir).toPath())

        analyzer = EnglishAnalyzer()
        config = IndexWriterConfig(analyzer)
        self.writer = IndexWriter(self.store, config)
        self.searcher = None

    def index_documents_from_file(self, file_name="cleaned_data.json"):
        with open(file_name, 'r') as f:
            data = json.load(f)
            for item in data:
                url = item.get("url", "")  # Retrieving URL from your JSON data
                content = item.get("content", "")  # Retrieving content from your JSON data
                self.index_document(url, content)

    def index_document(self, url, content, additional_fields=None):
        doc = Document()
        doc.add(StringField("URL", url, Field.Store.YES))
        doc.add(TextField("content", content, Field.Store.YES))

        if additional_fields:
            for field, values in additional_fields.items():
                if isinstance(values, list):
                    concatenated_values = ', '.join(values)  # Combine all values into a single string
                    doc.add(TextField(field, concatenated_values, Field.Store.YES))
                else:
                    doc.add(TextField(field, values, Field.Store.YES))

        self.writer.addDocument(doc)

    def extract_snippet(content, sentence_count=2):
        # Use a simple method to split the content into sentences and return the first few
        sentences = content.split('. ')[:sentence_count]
        return '. '.join(sentences) + ('.' if sentences else '')

    def search(self, query_string, max_results=5):
        if self.searcher is None:
            self.searcher = IndexSearcher(DirectoryReader.open(self.store))

        analyzer = EnglishAnalyzer()
        query = QueryParser("content", analyzer).parse(query_string)
        hits = self.searcher.search(query, max_results).scoreDocs

        results = []
        for hit in hits:
            hit_doc = self.searcher.doc(hit.doc)
            url = hit_doc.get("URL")
            score = hit.score
            content_snippet = hit_doc.get("content")[:400]  # Retrieves a snippet of the content

            # Include URL, score, and content snippet in your results
            result = {
                "URL": url,
                "Score": score,
                "Content Snippet": content_snippet
            }
            results.append(result)

        return results


    def close(self):
        self.writer.close(
        )

def init_spark_session(app_name="Wikipedia Processing"):

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.9.0") \
        .getOrCreate()

def read_xml(spark, file_path, row_tag="page"):

    return spark.read \
        .format("xml") \
        .option("rowTag", row_tag) \
        .load(file_path)

def remove_markup(text):
    text = re.sub(r'\{\{.*?\}\}', '', text)
    text = re.sub(r'\[\[File:.*?\]\]', '', text)
    text = re.sub(r'\[\[Image:.*?\]\]', '', text)
    text = re.sub(r'<ref[^>]*>.*?</ref>', '', text)
    text = re.sub(r'\[http[^\]]*\]', '', text)
    text = re.sub(r'\[\[(?:[^\]|]*\|)?([^\]]+)\]\]', r'\1', text)
    text = re.sub(r'<!--.*?-->', '', text)
    text = re.sub(r'<style.*?>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script.*?>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r"\'{2,}", '', text)
    text = re.sub(r'[\r\n\t]+', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'\n', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text)
    text = re.sub(r'==.*?==', '', text)
    text = re.sub(r'\{\|.*?\|\}', '', text, flags=re.DOTALL)
    text = re.sub(r'\[\[:?[^]|]+\|', '', text)
    text = re.sub(r'\[\[:?[^]]+\]\]', '', text)
    text = re.sub(r'\(\s*\)', '', text)
    text = re.sub(r'\[[^\]]*\]', '', text)
    text = re.sub(r'{{lang[^}]*}}', '', text)
    text = re.sub(r'{{IPA[^}]*}}', '', text)
    text = re.sub(r'{{coord[^}]*}}', '', text)
    text = re.sub(r'{{cite[^}]*}}', '', text)
    text = re.sub(r'{{convert[^}]*}}', '', text)
    text = re.sub(r'{{main[^}]*}}', '', text)
    text = re.sub(r'{{clarify[^}]*}}', '', text)
    text = re.sub(r'{{Infobox[^}]*}}', '', text, flags=re.DOTALL)
    text = re.sub(r'{{[^}]*}}', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&ndash;', '-', text)
    text = re.sub(r'&mdash;', '—', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&hellip;', '…', text)
    text = re.sub(r'&rsquo;', '’', text)
    text = re.sub(r'&lsquo;', '‘', text)
    text = re.sub(r'&ldquo;', '“', text)
    text = re.sub(r'&rdquo;', '”', text)
    text = re.sub(r'&bull;', '•', text)
    text = re.sub(r'&trade;', '™', text)
    text = re.sub(r'&reg;', '®', text)
    text = re.sub(r'&copy;', '©', text)
    text = re.sub(r'&[a-zA-Z]+;', '', text) 
    return text.strip()


def extract_details(text):
    match_details = re.findall(r'(\d{4} FIFA World Cup)', text)
    founding_dates = re.findall(r'founded in (\d{4})', text, re.IGNORECASE)
    top_scorers = re.findall(r'top scorer[s]?[:|=]?\s*([\w\s]+)', text, re.IGNORECASE)
    team_captains = re.findall(r'captain[:|=]?\s*([\w\s]+)', text, re.IGNORECASE)
    championship_wins = re.findall(r'championship[s]?[:|=]?\s*([\w\s]+)', text, re.IGNORECASE)
    famous_players = re.findall(r'([\w\s]+)\s*\([fF]amous [pP]layer\)', text)
    stadiums = re.findall(r'([\w\s]+) [sS]tadium', text)

    return {
        'match_details': match_details,
        'founding_dates': founding_dates,
        'top_scorers': top_scorers,
        'team_captains': team_captains,
        'championship_wins': championship_wins,
        'famous_players': famous_players,
        'stadiums': stadiums
    }

def title_to_url_suffix(title):
    return title.replace(" ", "_")


remove_markup_udf = udf(remove_markup, StringType())
extract_details_udf = udf(extract_details, MapType(StringType(), StringType()))
title_to_url_suffix_udf = udf(title_to_url_suffix, StringType())

def filter_and_process(df, indexed_urls_suffix):
    # Extract necessary columns from the XML structure
    df = df.withColumn('title', col('title'))
    df = df.withColumn('text', regexp_extract('revision.text._VALUE', r'<text.*?>(.*?)</text>', 1))

    # Filtering based on the title
    url_pattern = '|'.join([re.escape(suffix) for suffix in indexed_urls_suffix])
    filtered_df = df.filter(df['title'].rlike(url_pattern))

    # Apply cleaning and extracting details
    processed_df = filtered_df.withColumn("clean_content", remove_markup_udf(col("text")))
    extracted_df = processed_df.withColumn("extracted_details", extract_details_udf(col("clean_content")))
    print(extracted_df)

    return extracted_df



def convert_df_to_json(df, output_path):
    df.write.json(output_path)

def index_processed_data(app, directory="processed_data"):
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            with open(file_path, 'r') as f:
                for line in f:
                    json_object = json.loads(line)
                    url = json_object.get("url", "empty")  
                    content = json_object.get("content", "")
                    additional_fields = json_object.get("extracted_details", {})
                    app.index_document(url, content, additional_fields)

def retrieve_urls_from_index(index_dir):
    directory = FSDirectory.open(File(index_dir).toPath())
    reader = DirectoryReader.open(directory)
    urls = []

    for i in range(reader.maxDoc()):
        doc = reader.document(i)
        url = doc.get("URL") 
        if url:
            urls.append(url)

    reader.close()
    return urls

def retrieve_urls_suffix_from_index(index_directory):
    full_urls = retrieve_urls_from_index(index_directory)
    urls_suffix = [url.split('/')[-1] for url in full_urls]

    return urls_suffix




def user_confirmation(prompt):
    response = input(prompt + " (y/n): ").lower()
    return response == 'y'


def run_unit_tests(app):
    test_queries = ["England national football team", "Lionel Messi"]
    for query in test_queries:
        print(f"\nTesting query: '{query}'")
        results = app.search(query)
        for result in results:
            print(f"URL: {result['URL']}\nScore: {result['Score']}")
            print(f"Relevant Information: {result['Content Snippet']}\n")

if __name__ == "__main__":
    index_directory = "index"
    process_data = user_confirmation("Do you want to process new data?")
    if process_data:
        spark = init_spark_session()
        xml_file_path = "enwiki-latest-pages-articles1.xml-p1p41242"
        df = read_xml(spark, xml_file_path)
        indexed_urls_suffix = retrieve_urls_suffix_from_index(index_directory)
        processed_df = filter_and_process(df, indexed_urls_suffix)
        json_output_path = "processed_data.json"
        convert_df_to_json(processed_df, json_output_path)

    app = LuceneApp()

    index_original_data = user_confirmation("Do you want to index original documents?")
    if index_original_data:
        print("Indexing original documents...")
        app.index_documents_from_file("cleaned_data.json")
        print("Indexing complete!")

    index_new_data = user_confirmation("Do you want to index new processed data?")
    if index_new_data:
        print("Indexing processed documents...")
        index_processed_data(app, "processed_data.json")
        print("Indexing complete!")

    app.close()
    continue_running = True
    while continue_running:
        print("\nOptions:\n1. Search\n2. Run Unit Tests\n3. Exit")
        choice = input("Enter your choice (1/2/3): ")

        if choice == '1':
            query_string = input("Enter search query: ")
            search_results = app.search(query_string)
            for result in search_results:
                print(f"URL: {result['URL']}\nScore: {result['Score']}")
                print(f"Relevant Information: {result['Content Snippet']}\n")
        elif choice == '2':
            run_unit_tests(app)
        elif choice == '3':
            continue_running = False
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")