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
    
    # Conexión a MySQL Produccion SIEV2
    mysql_engine = create_engine('mysql+pymysql://root:ha_K*jlJtHS6bK3q@34.176.0.228:3306/siev2')

   
    print("Leyendo Datos")
    # Leer tabla de PostgreSQL
    query_pg = f"""select id_region_estable,id_rbd, id_tipo_dependencia, ds_tipo_dependencia, ds_region_estable, ds_provincia_estable, ds_comuna_estable 
                from dw.interno.sige_liberado 
                where id_mes = (select max(id_mes) from interno.sige_liberado) 
                group by id_region_estable,id_rbd, id_tipo_dependencia, ds_tipo_dependencia, ds_region_estable, ds_provincia_estable, ds_comuna_estable 
                """
    df_pg = pd.read_sql(query_pg, pg_engine)
   
    # Leer tabla de MySQL completa
    # query_mysql = f"SELECT token,attribute_4,attribute_24,attribute_7,attribute_8,attribute_9,attribute_11,attribute_12,attribute_13 FROM lime_tokens_{encuesta}_prueba "
    # query_mysql = f"""select u.nombre_usuario rbd,u.nombres,u.apellido_paterno,u.apellido_materno,u.correo_electronico,u.correo_electronico_dir,u.telefono1,u.telefono2 
    #             from `sistema-ev`.usuarios u 
    #             inner join usuarios_roles ur on u.usuario_id = ur.usuario_id 
    #             inner join roles r on ur.rol_id=r.rol_id  
    #             where r.rol_id =1
    #             and u.activo =1"""
    query_mysql = f"""select u.nombre_usuario rbd,u.nombres,u.apellido_paterno,u.apellido_materno,u.correo_electronico,u.correo_electronico_dir,u.telefono1,u.telefono2 
                from  usuarios u 
                inner join usuarios_roles ur on u.usuario_id = ur.usuario_id 
                inner join roles r on ur.rol_id=r.rol_id  
                where r.rol_id =1
                and u.activo =1"""                
    df_mysql = pd.read_sql(query_mysql, mysql_engine)

  

    # ######################################################################################
    # Guardar DataFrames como Parquet
    df_pg.to_parquet('tabla_rbd.parquet', index=False)
    df_mysql.to_parquet('tabla_usuario.parquet', index=False)
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
    con.execute("copy(select r.id_region_estable region,r.ds_region_estable regionEstablecimiento,r.ds_provincia_estable Provincia,r.ds_comuna_estable Comuna ,r.id_rbd Rbd,r.ds_tipo_dependencia Dependencia,u.* EXCLUDE (rbd) from  'tabla_usuario.parquet' u  inner join 'tabla_rbd.parquet' r on r.id_rbd=u.rbd) to 'contactosRbd.csv'  (HEADER, DELIMITER ',', ENCODING 'UTF-8');")
    
if __name__ == "__main__":
       main()
