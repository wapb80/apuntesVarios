from sqlalchemy import create_engine, text
import duckdb

# Configuración de la conexión a MySQL
mysql_engine = create_engine('mysql+pymysql://lm_desa:lm_desa_Sead2023@192.168.8.2:3306/lm')

# Configuración de la conexión a DuckDB
duckdb_con = duckdb.connect(database=':memory:')  # Usar una base de datos en memoria

# Consulta para seleccionar todos los datos de la tabla MySQL
query_mysql = "SELECT token FROM lime_tokens_845448_prueba"

# Ejecutar la consulta y obtener los resultados
with mysql_engine.connect() as connection:
    result = connection.execute(text(query_mysql))
    rows = result.fetchall()
    columns = result.keys()

# Crear la tabla en DuckDB
create_table_query = f"CREATE TABLE nombre_tabla_duckdb ({', '.join([f'{col} VARCHAR' for col in columns])})"
duckdb_con.execute(create_table_query)

# Preparar la inserción de datos
insert_query = f"INSERT INTO nombre_tabla_duckdb VALUES ({', '.join(['?' for _ in columns])})"

# Insertar cada fila de datos en la tabla de DuckDB
# Insertar cada fila de datos en la tabla de DuckDB
for row in rows:
    # Convertir row a una lista
    row_as_list = list(row)
    duckdb_con.execute(insert_query, row_as_list)

# Verificar los datos en DuckDB
print(duckdb_con.execute("SELECT * FROM nombre_tabla_duckdb limit 10").fetchall())
print(duckdb_con.execute("SELECT count(*) FROM nombre_tabla_duckdb limit 10").fetchall())
