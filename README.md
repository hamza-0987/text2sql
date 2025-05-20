# Text2SQL

A Python project that converts natural language questions to SQL queries using DuckDB and LLMs.

## Features
- Natural language to SQL conversion
- DuckDB database integration
- CSV data loading
- LLM-powered query generation

## Requirements
- Python 3.8+
- DuckDB
- Groq API key

## Installation
1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up your `.env` file with GROQ_API_KEY

## Usage
Run `python text2sql.py` and ask questions about the data in `employees.csv` and `purchases.csv`.