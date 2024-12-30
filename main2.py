import duckdb
import pandas as pd
from sqlalchemy import create_engine

# Conexión a PostgreSQL
pg_engine = create_engine('postgresql://usuario:contraseña@host:puerto/nombre_base_datos')
# Conexión a MySQL
mysql_engine = create_engine('mysql+pymysql://usuario:contraseña@host:puerto/nombre_base_datos')

# Leer tabla de PostgreSQL
query_pg = "select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 from dw.convenios.sige_liberado where id_mes = 202405 and id_tipo_dependencia != 4  and ((id_ensenanza = 10	and id_grado = 4)or (id_ensenanza = 10	and id_grado = 5) or (id_ensenanza = 211	and id_grado = 24) or (id_ensenanza = 211 and id_grado = 25)or (id_ensenanza = 212 and id_grado = 24)or (id_ensenanza = 212 and id_grado = 25)or (id_ensenanza = 213 and id_grado = 24) or (id_ensenanza = 213 and id_grado = 25)or (id_ensenanza = 214 and id_grado = 24)or (id_ensenanza = 214 and id_grado = 25)or (id_ensenanza = 215 and id_grado = 24)or (id_ensenanza = 215 and id_grado = 25)or (id_ensenanza = 216 and id_grado = 24)or (id_ensenanza = 216 and id_grado = 25)or (id_ensenanza = 217 and id_grado = 24)or (id_ensenanza = 217 and id_grado = 25)or (id_ensenanza = 218 and id_grado = 24)or (id_ensenanza = 218 and id_grado = 25)or (id_ensenanza = 299 and id_grado = 24)or (id_ensenanza = 299 and id_grado = 25))  ;"
df_pg = pd.read_sql(query_pg, pg_engine)

# Leer tabla de MySQL
query_mysql = "SELECT * FROM lime_tokens_845448_prueba"
df_mysql = pd.read_sql(query_mysql, mysql_engine)


# Guardar DataFrames como CSV
df_pg.to_csv('tabla_pg.csv', index=False)
df_mysql.to_csv('tabla_mysql.csv', index=False)

# Crear una instancia de DuckDB
con = duckdb.connect()

# Leer los CSV en DuckDB
con.execute("CREATE TABLE pg_table AS SELECT * FROM read_csv_auto('tabla_pg.csv')")
con.execute("CREATE TABLE mysql_table AS SELECT * FROM read_csv_auto('tabla_mysql.csv')")

# Identificar datos en MySQL que no están en PostgreSQL
query_mysql_not_in_pg = """
SELECT mysql_table.*
FROM mysql_table
LEFT JOIN pg_table
ON mysql_table.id = pg_table.id
WHERE pg_table.id IS NULL
"""
df_mysql_not_in_pg = con.execute(query_mysql_not_in_pg).fetchdf()
df_mysql_not_in_pg.to_csv('mysql_not_in_pg.csv', index=False)

# Identificar datos en PostgreSQL que no están en MySQL
query_pg_not_in_mysql = """
SELECT pg_table.*
FROM pg_table
LEFT JOIN mysql_table
ON pg_table.id = mysql_table.id
WHERE mysql_table.id IS NULL
"""
df_pg_not_in_mysql = con.execute(query_pg_not_in_mysql).fetchdf()
df_pg_not_in_mysql.to_csv('pg_not_in_mysql.csv', index=False)

print("Proceso completado con éxito.")
print("Datos en MySQL que no están en PostgreSQL guardados en 'mysql_not_in_pg.csv'.")
print("Datos en PostgreSQL que no están en MySQL guardados en 'pg_not_in_mysql.csv'.")
