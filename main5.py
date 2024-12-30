import duckdb

# Crear una conexión a DuckDB
con = duckdb.connect(database=':memory:')
con.execute("CREATE TABLE pg_table AS SELECT * FROM read_csv_auto('tabla_pg_corrected.csv');")
nuevos ="select * from pg_table";
df_nuevos = con.execute(nuevos).fetchdf()


