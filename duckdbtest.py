import duckdb
import pandas as pd

# Connect to DuckDB
conn = duckdb.connect(database=':memory:', read_only=False)

# Load data
conn.execute("""
    CREATE TABLE IF NOT EXISTS employees AS 
    SELECT * FROM read_csv('data/employees.csv', header=true, auto_detect=true);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS purchases AS 
    SELECT * FROM read_csv('data/purchases.csv', header=true, auto_detect=true);
""")

# Example query
result = conn.execute("SELECT * FROM employees").fetchdf()
print(result)