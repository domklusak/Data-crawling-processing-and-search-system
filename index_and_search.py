import os
import json
import lucene
lucenevm = lucene.initVM(vmargs=['-Djava.awt.headless=true'])

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
        
        self.close()

    def index_document(self, url, content):
        doc = Document()
        if not url:
            url = "empty"

        doc.add(StringField("URL", url, Field.Store.YES))  # Not analyzing the URL
        doc.add(TextField("content", content, Field.Store.YES))
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
            results.append((hit_doc.get("URL"), hit.score))

        return results

    def close(self):
        self.writer.close()


if __name__ == "__main__":
    app = LuceneApp()
    print("Indexing documents...")
    app.index_documents_from_file()  # Index documents from cleaned_data.json at the start
    print("Indexing complete!")
    while True:
        action = input("Do you want to 'search'? (s) Or 'exit' to quit: ")
        if action == "s":
            query_string = input("Enter search query: ")
            results = app.search(query_string)
            for url, score in results:
                print(f"URL: {url}\nScore: {score}\n")
        elif action == "exit":
            break
