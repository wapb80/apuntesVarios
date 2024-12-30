import pandas as pd
from sqlalchemy import create_engine, text
import duckdb
import csv

# # Conexión a PostgreSQL
pg_engine = create_engine('postgresql://convenios_read:convenios_read@192.168.8.2:5432/dw')
# Conexión a MySQL
mysql_engine = create_engine('mysql+pymysql://lm_desa:lm_desa_Sead2023@192.168.8.2:3306/lm')

# Leer tabla de PostgreSQL
query_pg = "select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 from dw.convenios.sige_liberado where id_mes = 202406 and id_tipo_dependencia != 4  and ((id_ensenanza = 10	and id_grado = 4)or (id_ensenanza = 10	and id_grado = 5) or (id_ensenanza = 211	and id_grado = 24) or (id_ensenanza = 211 and id_grado = 25)or (id_ensenanza = 212 and id_grado = 24)or (id_ensenanza = 212 and id_grado = 25)or (id_ensenanza = 213 and id_grado = 24) or (id_ensenanza = 213 and id_grado = 25)or (id_ensenanza = 214 and id_grado = 24)or (id_ensenanza = 214 and id_grado = 25)or (id_ensenanza = 215 and id_grado = 24)or (id_ensenanza = 215 and id_grado = 25)or (id_ensenanza = 216 and id_grado = 24)or (id_ensenanza = 216 and id_grado = 25)or (id_ensenanza = 217 and id_grado = 24)or (id_ensenanza = 217 and id_grado = 25)or (id_ensenanza = 218 and id_grado = 24)or (id_ensenanza = 218 and id_grado = 25)or (id_ensenanza = 299 and id_grado = 24)or (id_ensenanza = 299 and id_grado = 25))"
df_pg = pd.read_sql(query_pg, pg_engine)

# Leer tabla de MySQL completa
query_mysql = "SELECT token FROM lime_tokens_845448_prueba"
df_mysql = pd.read_sql(query_mysql, mysql_engine)


# Leer tabla de MySQL los contestados  
query_mysql_contestados = "SELECT token,submitdate FROM  lime_survey_845448 where submitdate is not null"
df_mysql_contestados = pd.read_sql(query_mysql_contestados, mysql_engine)


# # Guardar DataFrames como CSV
df_pg.to_csv('tabla_pg.csv', index=False)
df_mysql.to_csv('tabla_mysql.csv', index=False)
df_mysql_contestados.to_csv('tabla_mysql_contestados.csv', index=False)

# # Corregir formato de archivo CSV de PostgreSQL
input_file = 'tabla_pg.csv'
output_file = 'tabla_pg_corrected.csv'

with open(input_file, mode='r', newline='', encoding='utf-8') as infile, open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
    
    for row in reader:
        writer.writerow(row)

print(f"Archivo corregido guardado como {output_file}.")

#  desde aca para arriba esta todo bien




# Crear una instancia de DuckDB
con = duckdb.connect()

# Leer los CSV en DuckDB
con.execute("CREATE TABLE pg_table AS SELECT * FROM read_csv_auto('tabla_pg_corrected.csv')")
con.execute("CREATE TABLE mysql_table AS SELECT * FROM read_csv_auto('tabla_mysql.csv')")
con.execute("CREATE TABLE mysql_table_contestados AS SELECT * FROM read_csv_auto('tabla_mysql_contestados.csv')")


# Identificar datos en PostgreSQL que no están en MySQL -- Los nuevos
query_pg_not_in_mysql = """
SELECT pg_table.*
FROM pg_table
LEFT JOIN mysql_table
ON pg_table.token = mysql_table.token
WHERE mysql_table.token IS NULL
"""
df_pg_not_in_mysql = con.execute(query_pg_not_in_mysql).fetchdf()
df_pg_not_in_mysql.to_csv('pg_not_in_mysql.csv', index=False)

print("Datos en PostgreSQL que no están en MySQL guardados en 'pg_not_in_mysql.csv'.")


# Identificar datos en MySQL que no están en PostgreSQL -- los con movimiento
query_mysql_not_in_pg = """
SELECT mysql_table.*
FROM mysql_table
LEFT JOIN pg_table
ON mysql_table.token = pg_table.token
WHERE pg_table.token IS NULL
"""
df_mysql_not_in_pg = con.execute(query_mysql_not_in_pg).fetchdf()
df_mysql_not_in_pg.to_csv('mysql_not_in_pg.csv', index=False)


con.execute("CREATE TABLE movimientos AS SELECT * FROM read_csv_auto('mysql_not_in_pg.csv')")

# Saber los que hay que eliminar
query_eliminar = """
SELECT movimientos.token from movimientos left join mysql_table_contestados on movimientos.token=mysql_table_contestados.token where mysql_table_contestados.token is null;
"""
df_query_eliminar = con.execute(query_eliminar).fetchdf()
df_query_eliminar.to_csv('eliminar.csv', index=False)



# Insertar datos en MySQL
# Leer el CSV con los datos a insertar en MySQL
df_pg_not_in_mysql = pd.read_csv('pg_not_in_mysql.csv')

# # Insertar los datos en la tabla de MySQL
df_pg_not_in_mysql.to_sql('lime_tokens_845448_prueba', mysql_engine, if_exists='append', index=False)

print("Datos insertados en la tabla de MySQL correctamente.")

# Leer el archivo CSV con los registros a eliminar
csv_file_path = 'eliminar.csv'
df_to_delete = pd.read_csv(csv_file_path)
# Verificar que el DataFrame no está vacío
if not df_to_delete.empty:
    # Verificar que la columna 'token' existe en el DataFrame
    if 'token' in df_to_delete.columns:
        # Convertir la columna 'token' a una lista, eliminando valores nulos y vacíos
        tokens_to_delete = df_to_delete['token'].dropna().tolist()
        
        # Filtrar tokens no vacíos
        tokens_to_delete = [token for token in tokens_to_delete if token]

        # Verificar si la lista de tokens no está vacía
        if tokens_to_delete:
            # Crear la consulta de eliminación
            delete_query = f"""
            DELETE FROM nombre_tabla_mysql
            WHERE token IN ({', '.join(map(lambda x: f"'{x}'", tokens_to_delete))})
            """

            # Ejecutar la consulta de eliminación
            with mysql_engine.connect() as connection:
                connection.execute(text(delete_query))

            print(f"Se eliminaron los registros con token: {', '.join(tokens_to_delete)}")
        else:
            print("La lista de tokens a eliminar está vacía después de filtrar valores nulos o vacíos.")
    else:
        print("La columna 'token' no existe en el archivo CSV.")
else:
    print("El archivo CSV está vacío.")


# # Convertir DataFrame a una lista de IDs o valores únicos
# ids_to_delete = df_to_delete['token'].tolist()  # Ajusta el nombre de la columna según tu archivo CSV


# # Crear la consulta de eliminación
# delete_query = f"""
# DELETE FROM lime_tokens_845448_prueba
# WHERE token IN ({', '.join(map(str, ids_to_delete))})
# """

# # Ejecutar la consulta de eliminación
# with mysql_engine.connect() as connection:
#     connection.execute(text(delete_query))


