import os
import json
import duckdb
import sqlparse
import pandas as pd
from dotenv import load_dotenv
from groq import Groq

# Load environment variables from .env file
load_dotenv()

# Get the Groq API key and create a Groq client
groq_api_key = os.getenv('GROQ_API_KEY')
if not groq_api_key:
    raise ValueError("GROQ_API_KEY environment variable is not set. Please set it in your .env file.")

client = Groq(api_key=groq_api_key)

def chat_with_groq(client, prompt, model, response_format=None):
    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": prompt}
        ],
        response_format=response_format
    )
    return completion.choices[0].message.content

import os
import sys
import platform
import duckdb
import pandas as pd

def execute_duckdb_query(query):
    """
    Execute a SQL query on DuckDB with comprehensive error handling
    and detailed diagnostic output.
    """
    try:
        # Get absolute paths to data files
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, 'data')
        
        employees_path = os.path.abspath(os.path.join(data_dir, 'employees.csv')).replace(os.sep, '/')
        purchases_path = os.path.abspath(os.path.join(data_dir, 'purchases.csv')).replace(os.sep, '/')

        # Print diagnostic information
        print("\n=== FILE ACCESS DIAGNOSTICS ===")
        print(f"Script directory: {script_dir}")
        print(f"Data directory: {data_dir}")
        print(f"Employees path: {employees_path} (Exists: {os.path.exists(employees_path)})")
        print(f"Purchases path: {purchases_path} (Exists: {os.path.exists(purchases_path)})")
        print(f"Current working directory: {os.getcwd()}")
        print("Directory contents:", os.listdir(data_dir if os.path.exists(data_dir) else script_dir))
        print("==============================\n")

        # Verify files exist
        if not os.path.exists(employees_path):
            raise FileNotFoundError(f"Employees file not found at: {employees_path}")
        if not os.path.exists(purchases_path):
            raise FileNotFoundError(f"Purchases file not found at: {purchases_path}")

        # Connect to DuckDB
        conn = duckdb.connect(database=':memory:', read_only=False)
        
        # Load data - IMPORTANT: We're creating TABLES, not querying CSVs directly
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS employees AS 
            SELECT * FROM read_csv('{employees_path}', header=true, auto_detect=true);
        """)
        
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS purchases AS 
            SELECT * FROM read_csv('{purchases_path}', header=true, auto_detect=true);
        """)

        # Fix common query mistakes
        query = query.strip()
        if not query.endswith(';'):
            query += ';'
            
        # Correct table references in query
        query = query.replace('purchases.csv', 'purchases').replace('employees.csv', 'employees')

        try:
            result = conn.execute(query).fetchdf()
            return result.reset_index(drop=True)
        except Exception as e:
            print(f"\n=== QUERY EXECUTION ERROR ===")
            print(f"Original query: {query}")
            # Show available tables and columns if table error
            if "Catalog Error" in str(e):
                tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
                print("\nAvailable tables:", [t[0] for t in tables])
                for table in [t[0] for t in tables]:
                    columns = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';").fetchall()
                    print(f"Columns in {table}:", [c[0] for c in columns])
            print(f"Error details: {str(e)}")
            print("=============================\n")
            raise

    except Exception as e:
        print("\n=== COMPLETE ERROR REPORT ===")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        print("\nSystem information:")
        print(f"Python version: {sys.version}")
        print(f"DuckDB version: {duckdb.__version__}")
        print(f"Operating system: {platform.system()} {platform.release()}")
        print("===========================\n")
        raise RuntimeError(f"Failed to execute query: {str(e)}") from e

    finally:
        if 'conn' in locals():
            conn.close()

def get_summarization(client, user_question, df, model):
    prompt = f'''
A user asked the following question pertaining to local database tables:

{user_question}

To answer the question, a dataframe was returned:

Dataframe:
{df}

In a few sentences, summarize the data in the table as it pertains to the original user question. 
Avoid qualifiers like "based on the data" and do not comment on the structure or metadata of the table itself.
'''
    return chat_with_groq(client, prompt, model, None)

# Use the Llama3 70b model
model = "llama3-70b-8192"

# Print welcome message
print("Welcome to the DuckDB Query Generator!")
print("You can ask questions about the data in the 'employees.csv' and 'purchases.csv' files.")

# Load the base prompt template
script_dir = os.path.dirname(os.path.abspath(__file__))
prompt_path = os.path.join(script_dir, 'prompts', 'base_prompt.txt')
with open(prompt_path, 'r') as file:
    base_prompt = file.read()

# Enhance the base prompt with schema inspection instructions
base_prompt += """
For database schema questions:
- To count tables: SELECT count(*) as table_count FROM information_schema.tables WHERE table_schema = 'main'
- To describe tables: SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main'
"""

# Main loop
while True:
    user_question = input("\nAsk a question (or 'quit' to exit): ").strip()
    
    if user_question.lower() == 'quit':
        break
        
    if not user_question:
        continue

    full_prompt = base_prompt.format(user_question=user_question)

    try:
        llm_response = chat_with_groq(client, full_prompt, model, {"type": "json_object"})
        result_json = json.loads(llm_response)

        if 'sql' in result_json:
            sql_query = result_json['sql']
            results_df = execute_duckdb_query(sql_query)

            formatted_sql_query = sqlparse.format(sql_query, reindent=True, keyword_case='upper')

            print("\nGenerated SQL Query:")
            print("```sql\n" + formatted_sql_query + "\n```")

            print("\nQuery Results:")
            print(results_df.to_markdown(index=False))

            summarization = get_summarization(client, user_question, results_df, model)
            print("\nSummarization:")
            print(summarization)

        elif 'error' in result_json:
            print("ERROR: Could not generate valid SQL for this question")
            print(result_json['error'])

    except json.JSONDecodeError:
        print("Error: Could not parse the AI response as JSON")
    except Exception as e:
        print(f"An error occurred: {str(e)}")