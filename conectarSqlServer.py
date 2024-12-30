import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import duckdb

# # Conexión a SQL Server
# conn_str = (
#     'DRIVER={ODBC Driver 17 for SQL Server};'
#     'SERVER=192.168.8.3;'
#     'DATABASE=encvulne;'
#     'UID=etl-agent;'
#     'PWD=etl-agentdesa2024'
# )

conn= create_engine(f"mssql+pyodbc://etl-agent:etl-agentdesa2024@192.168.8.3/encvulne?driver=ODBC Driver 17 for SQL Server")

# conn = pyodbc.connect(conn_str)
query = "SELECT * FROM avances_encuestas2"

# Leer datos de la base de datos
df = pd.read_sql(query, conn)



df.to_parquet('encuestas.parquet', index=False)

con = duckdb.connect(database=':memory:') # Usar una base de datos en memoria
  
con.execute("copy(select * from  'encuestas.parquet') to 'AvanceEncuestas.json'")



    