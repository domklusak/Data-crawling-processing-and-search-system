import os
import json
import lucene
import html
import re
lucenevm = lucene.initVM(vmargs=['-Djava.awt.headless=true'])

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
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
            match_details = hit_doc.get("match_details")  # Retrieves a single string with all values
            founding_dates = hit_doc.get("founding_dates")
            top_scorers = hit_doc.get("top_scorers")

            # Include these in your results
            result = {
                "URL": url,
                "Score": score,
                "Match Details": match_details,
                "Founding Dates": founding_dates,
                "Top Scorers": top_scorers
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



remove_markup_udf = udf(remove_markup, StringType())
extract_details_udf = udf(extract_details, MapType(StringType(), StringType()))

def filter_and_process(df):

    keywords = ["FIFA World Cup", "national football team", "national soccer team"]
    filtered_df = df.filter(df['title'].rlike('|'.join(keywords)))

    # Apply the UDFs to clean the content and extract details
    processed_df = filtered_df.withColumn("clean_content", remove_markup_udf(col("revision.text._VALUE")))
    extracted_df = processed_df.withColumn("extracted_details", extract_details_udf(col("clean_content")))

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
                    url = json_object.get("url", "empty")  # Extract URL if available
                    content = json_object.get("content", "")  # Extract content
                    # Extract additional fields if needed
                    additional_fields = json_object.get("extracted_details", {})
                    app.index_document(url, content, additional_fields)




def user_confirmation(prompt):
    response = input(prompt + " (y/n): ").lower()
    return response == 'y'

if __name__ == "__main__":
    process_data = input("Do you want to process new data? (y/n): ").lower()
    if process_data == "y":
        spark = init_spark_session()
        xml_file_path = "enwiki-latest-pages-articles1.xml-p1p41242"
        df = read_xml(spark, xml_file_path)
        processed_df = filter_and_process(df)
        json_output_path = "processed_data.json"
        convert_df_to_json(processed_df, json_output_path)

    app = LuceneApp()
    print("Indexing original documents...")
    app.index_documents_from_file("cleaned_data.json")

    index_new_data = input("Do you want to index new processed data? (y/n): ").lower()
    if index_new_data == "y":
        print("Indexing processed documents...")
        index_processed_data(app, "processed_data.json")

    print("Indexing complete!")
    app.close()

    while True:
        action = input("Do you want to 'search'? (s) Or 'exit' to quit: ")
        if action == "s":
            query_string = input("Enter search query: ")
            search_results = app.search(query_string)
            for result in search_results:
                print(f"URL: {result['URL']}\nScore: {result['Score']}")
                print(f"Match Details: {result['Match Details']}")
                print(f"Founding Dates: {result['Founding Dates']}")
                print(f"Top Scorers: {result['Top Scorers']}\n")
        elif action == "exit":
            break
