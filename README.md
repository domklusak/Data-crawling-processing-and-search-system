# Football data crawling, processing and search system

## Description
This project is focused on processing and indexing football-related data, particularly from Wikipedia and HTML sources. It involves extracting, cleaning, and indexing data for efficient searching and retrieval.

## Features
- **Data Processing**: Parses XML and HTML data, extracting relevant information such as team details, match histories, and player statistics.
- **Data Cleaning**: Utilizes regular expressions and BeautifulSoup for removing unwanted markup and formatting.
- **Search Functionality**: Implemented using PyLucene, enabling efficient search queries over indexed data.
- **Unit Testing**: Includes tests to validate the correctness of data extraction and search functionalities.

## Technologies
- Python
- PySpark for data processing
- BeautifulSoup for HTML parsing
- PyLucene for indexing and search
- Regular Expressions for data cleaning

## Setup
Detailed instructions on setting up the environment, dependencies, and running the scripts.

## Usage

### Setting Up the Environment
1. **Docker Setup**: Ensure Docker is installed on your system.
2. **Devcontainer Extension**: Use Visual Studio Code with the Devcontainer extension to manage and run the Docker environment.
3. **Image Preparation**: Use a Docker image configured for PyLucene. 

### Running the Application
1. **Starting the Application**: Run the script using the command `/usr/local/bin/python3 /<yourworkspace>/index_and_search.py` from the terminal within the Devcontainer.
2. **Processing Options**: The script prompts for various options:
   - Processing new data.
   - Indexing processed data.
   - Performing a search query.
3. **Search Functionality**: Enter a search query as instructed to retrieve relevant football data. The system returns URLs, scores, and content snippets related to the query.
4. **Unit Testing**: Run predefined unit tests to validate the correctness of data extraction and searching functionalities.

### Exiting the Application
- To exit, follow the prompt instructions in the script. 

