# Extract-Transform Mini App

This is a simple ETL (Extract-Transform) mini app built using Python. The app focuses on the first two stages of the ETL pipeline: **Extract** and **Transform**. 

The app performs the following tasks:
1. **Extract**: Downloads data from a given source (parquet files) for a specific year.
2. **Transform**: Processes the downloaded data, cleaning it, filtering, and performing transformations.

## Features
- **Extract**: Downloads the data for a given year from a source URL.
- **Transform**: Transforms the extracted data, including data cleaning, handling missing values, and aggregation.
- **Multithreading**: Uses Python’s `threading` library to download files concurrently, speeding up the extraction process.

## Requirements
- Python 3.x
- `requests` (for downloading files)
- `pandas` (for data manipulation)

To install the necessary dependencies, you can run:

```bash
pip install -r requirements.txt
