# funciona bien , 
# Identificar datos en PostgreSQL que no están en MySQL -- Los nuevos y agregarlos 
# Identificar datos en MySQL que no están en PostgreSQL (los con movimiento), estos sedeben eliminar 
# import sys
# from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import duckdb
#from sqlalchemy import text
#from sqlalchemy.exc import SQLAlchemyError


def main():

    # Conexión a PostgreSQL
    pg_engine = create_engine('postgresql://convenios_read:convenios_read@192.168.8.2:5432/dw')

    # Conexión a MySQL DSISTEMA EV
    #mysql_engine = create_engine('mysql+pymysql://sistemaev:sistemaev@192.168.8.2:3306/sistema-ev')

    #  Conexión a MySQL Produccion
    # mysql_engine = create_engine('mysql+pymysql://lm:lm_prod_SEAD2024@34.176.0.228:3306/lm')
        # Conexión a MySQL Desarrollo
    mysql_engine = create_engine('mysql+pymysql://root:Sead_2023#@192.168.8.2:3306/lm')
    # Conexión a MySQL Produccion SIEV2
    # mysql_engine = create_engine('mysql+pymysql://root:ha_K*jlJtHS6bK3q@34.176.0.228:3306/siev2')

   
    print("Leyendo Datos")
    # Leer desde limesurvey
    query_mysql = f"""SELECT 
    encuesta,
    tipo AS accion,
    m.token,
    antiguo_id_RBD AS id_RBD,
    antiguo_RBD AS RBD,
    antiguo_Ensenanza AS Ensenanza,
    antiguo_Curso AS Curso,
    antigua_Region AS Region,
    antigua_Comuna AS Comuna,
    submitdate 
FROM (
    SELECT m.*
    FROM test.movimientos m 
    JOIN (
        SELECT token, MAX(fechaIngreso) AS fecha_maxima
        FROM test.movimientos
        GROUP BY token 
    ) AS t2 ON m.token = t2.token AND m.fechaIngreso = t2.fecha_maxima
) m 
JOIN lime_survey_956762 l ON m.token = l.token 
WHERE (m.fechaIngreso DIV 10000) > CAST(DATE(l.submitdate) AS UNSIGNED)
AND m.encuesta = 'pesoTalla'
AND m.tipo != 'Nuevos';
                """                
    df_mysql = pd.read_sql(query_mysql, mysql_engine)
    
    # Leer tabla de PostgreSQL
    query_pg = f"""select id_rut_alumno,nombres,apellido_paterno,apellido_materno from (
select id_rut_alumno,nombres,apellido_paterno,apellido_materno from dw.encvulne.universo_enc_vulnerabilidad
union all
select id_rut_alumno,nombres,apellido_paterno,apellido_materno from dw.encvulne.universo_enc_vulnerabilidad_202407 uev 
union all
select id_rut_alumno,nombres,apellido_paterno,apellido_materno from dw.encvulne.universo_enc_vulnerabilidad_202408 uev 
union all
select id_rut_alumno,nombres,apellido_paterno,apellido_materno from dw.encvulne.universo_enc_vulnerabilidad_202409 uev 
union all
select id_rut_alumno,nombres,apellido_paterno,apellido_materno from dw.encvulne.universo_enc_vulnerabilidad_202410 uev  )
group by id_rut_alumno,nombres,apellido_paterno,apellido_materno

                """
                
                
    df_pg = pd.read_sql(query_pg, pg_engine)
   
  

    # ######################################################################################
    # Guardar DataFrames como Parquet
    df_pg.to_parquet('tabla_Universo.parquet', index=False)
    df_mysql.to_parquet('tabla_Paso.parquet', index=False)
    # df_mysql_contestados.to_parquet('tabla_contestados.parquet', index=False)
    print("Comparando Datos")
    ######################################################################################
    # Crear una instancia de DuckDB
    con = duckdb.connect(database=':memory:') # Usar una base de datos en memoria
    
    # Podria haber creado las tablas en memoria pero lo deje haciendo la query directa de los archivos parquet.
    ################################ Leer los Parquet en DuckDB
    # con.execute("CREATE TABLE pg_table AS SELECT * FROM read_parquet('tabla_pg.parquet')")
    # con.execute("CREATE TABLE mysql_table AS SELECT * FROM read_parquet('tabla_mysql.parquet');")

    # con.execute("select u.*,r.ds_tipo_dependencia,r.ds_region_estable,r.ds_provincia_estable,r.ds_comuna_estable from mysql_table u inner join pg_table r on r.id_rbd=u.rbd;")
    con.execute("copy(select * from 'tabla_Paso.parquet' t1 join 'tabla_Universo.parquet' t2 ON t1.token=t2.id_rut_alumno ) to 'realRbdEncuesta.csv'  (HEADER, DELIMITER ',', ENCODING 'UTF-8');")
    
if __name__ == "__main__":
       main()
