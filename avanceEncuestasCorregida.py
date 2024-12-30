import pandas as pd
from sqlalchemy import create_engine, text
import duckdb


def main():

    # Conexión a PostgreSQL
    # pg_engine = create_engine('postgresql://convenios_read:convenios_read@192.168.8.2:5432/dw')

    # Conexión a MySQL DSISTEMA EV
    #mysql_engine = create_engine('mysql+pymysql://sistemaev:sistemaev@192.168.8.2:3306/sistema-ev')

    #  Conexión a MySQL Produccion Lime
    mysql_lime = create_engine('mysql+pymysql://lm:lm_prod_SEAD2024@34.176.0.228:3306/lm')
    
    # Conexión a MySQL Produccion datamart
    mysql_encuesta = create_engine('mysql+pymysql://root:ha_K*jlJtHS6bK3q@34.176.0.228:3306/datamart')

   
    print("Leyendo Datos")
    # Leer tabla de Produccion datamart encuestas
    query_encuesta = f"select * from datamart.avances_encuestas"
    df_encuesta = pd.read_sql(query_encuesta, mysql_encuesta)
  
    query_lime = f"""select DISTINCT attribute_4, "845448" as encuesta  from lime_tokens_845448 
                        union all
                        select DISTINCT attribute_4, "216195" as encuesta  from lime_tokens_216195 
                        union all
                        select DISTINCT attribute_4, "627416" as encuesta  from lime_tokens_627416 
                        union all
                        select DISTINCT attribute_4, "632748" as encuesta  from lime_tokens_632748 
                        union all
                        select DISTINCT attribute_4, "843277" as encuesta  from lime_tokens_843277 
                        union all
                        select DISTINCT attribute_4, "317298" as encuesta  from lime_tokens_317298 
                        union all
                        select DISTINCT attribute_4, "956762" as encuesta  from lime_tokens_956762
                        """                
    df_lime = pd.read_sql(query_lime, mysql_lime)

  

    # ######################################################################################
    # Guardar DataFrames como Parquet
    df_encuesta.to_parquet('encuestasAvance.parquet', index=False)
    df_lime.to_parquet('rbdLime.parquet', index=False)
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
    con.execute("copy(select * from 'encuestasAvance.parquet' where concat_ws('-', rbd,encuesta_id) in (select concat_ws('-',attribute_4 ,encuesta) from 'rbdLime.parquet')) to 'avanceEncuestaCorregida.csv'  (HEADER, DELIMITER ',', ENCODING 'UTF-8');")
    con.execute("copy(select * from 'encuestasAvance.parquet' where concat_ws('-', rbd,encuesta_id) not in (select concat_ws('-',attribute_4 ,encuesta) from 'rbdLime.parquet')) to 'avanceEncuestaCorregida-Malas.csv'  (HEADER, DELIMITER ',', ENCODING 'UTF-8');")

if __name__ == "__main__":
       main()
