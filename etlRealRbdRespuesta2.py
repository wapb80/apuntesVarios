# Programa para sacar los token que tuvieron movimientos y rescatar el real RBD de la encuesta 
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
    # Leer desde limesurvey para sacar el listado de los que se eliminaron y buscarlos en el sige
    query_mysql = f"""select f.token from lime_tokens_317298 l right join   (		
                        select   m.token, antiguo_id_RBD id_RBD 
                        from (SELECT m.*
                        FROM test.movimientos m 
                        JOIN (
                            SELECT token, MAX(fechaIngreso) AS fecha_maxima
                            FROM test.movimientos where  encuesta ='Rpme'
                            GROUP BY token 
                        ) AS t2 ON m.token = t2.token where m.fechaIngreso = t2.fecha_maxima) m 
                            join lime_survey_317298 l ON m.token = l.token 
                            WHERE (m.fechaIngreso DIV 100) > cast( DATE_FORMAT(l.submitdate, '%%Y%%m%%d%%H') as UNSIGNED)
                                and m.tipo !='Nuevos'
                ) as f  on f.token = l.token 
                where l.token is null
                """                
    df_mysql = pd.read_sql(query_mysql, mysql_engine)

    # Formatear las claves en un string separado por comas
    claves = ','.join(map(str, df_mysql['token'].tolist()))
    
    
    # Construir la segunda consulta en el sige con el listado anterior
    # Leer tabla de PostgreSQL
    query_pg = f"""SELECT  
	  nombres as firstname
	  ,apellido_paterno || ' ' || apellido_materno as lastname
	  ,'sincorreo@sincorreo.cl' as email
	  ,'OK' as emailstatus
	  ,'es' as language
	  ,'N' as sent
	  ,'N' as remindersent
	  ,0 as remindercount
	  ,'N' as usesleft
	  ,'N' as completed
	  ,id_region_estable as attribute_1
	  ,id_provincia_estable as attribute_2
	  ,id_comuna_estable as attribute_3
	  ,id_rbd as attribute_4
	  ,ds_tipo_dependencia as attribute_5  
	  ,case when id_ensenanza = 10 then 1
	   when id_ensenanza = 110 then 3
	   when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4
	   when id_ensenanza > 200 and id_ensenanza < 300 then 5
	   when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6
	   end as attribute_6
	  ,case when id_ensenanza = 10 then 'PARVULARIA'
	   when id_ensenanza = 110 then 'BÁSICA'
	   when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA'
	   when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'
	   when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO'
	   end as attribute_7	   
	  ,id_grado as attribute_8
	  ,ds_grado as attribute_9
	  ,id_ensenanza as attribute_10
	  ,ds_region_estable as attribute_11
	  ,ds_provincia_estable as attribute_12
	  ,ds_comuna_estable as attribute_13
	  ,id_rut_alumno as attribute_14
	  ,dgv_alumno as attribute_15
	  ,ds_sexo as attribute_16
	  ,id_rut_alumno || '-' || dgv_alumno as attribute_17
	  ,nombres as attribute_18
	  ,apellido_paterno as attribute_19
	  ,apellido_materno as attribute_20
	  ,id_mes as attribute_21
	  ,fecha_nacimiento as attribute_22
	  ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 -- se calcula la edad en base a la fecha de nacimiento SIGE
	  ,ds_nom_estable as attribute_24
		FROM (
		    SELECT *, ROW_NUMBER() OVER (PARTITION BY id_rut_alumno ORDER BY id_mes DESC) AS rn
		    FROM (
		        select * from dw.interno.sige_liberado where id_mes = 202411
		        UNION ALL
		        select * from dw.interno.sige_liberado where id_mes = 202410
		        UNION ALL
		        select * from dw.interno.sige_liberado where id_mes = 202409
		        UNION ALL
		        select * from dw.interno.sige_liberado where id_mes = 202408
		        UNION ALL
		        select * from dw.interno.sige_liberado where id_mes = 202407
		    ) sub
		) deduplicated
		WHERE rn = 1
		and id_rut_alumno   IN ({claves})
                """
                
                
    df_pg = pd.read_sql(query_pg, pg_engine)
    
    # 	Consulta que Entrega los registros que tuvieron movimiento y tengo en lime_token informacion para modificarla con la tabla de movimientos 		
    # query_mysql_movimientos = f"""
    query_mysql_movimientos= f""" 
    	
                       SELECT 
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
        FROM test.movimientos  where  encuesta ='Rpme'
        GROUP BY token 
    ) AS t2 ON m.token = t2.token where m.fechaIngreso = t2.fecha_maxima
) m 
JOIN lime_survey_317298 l ON m.token = l.token 
WHERE (m.fechaIngreso DIV 100) > cast( DATE_FORMAT(l.submitdate, '%%Y%%m%%d%%H') as UNSIGNED)
AND m.tipo != 'Nuevos';
                """       
               
    # """
    # print(query_pg)
    df_movimientos = pd.read_sql(query_mysql_movimientos, mysql_engine)

    query_mysql_survey= f""" select * from lime_tokens_317298"""
    df_survey = pd.read_sql(query_mysql_survey, mysql_engine)
    
    
    # ######################################## Sacar listado de RBD existentes 
    # ##############################################
    query_mysql_rbd=f"""select  id_region_estable
	  ,id_provincia_estable 
	  ,id_comuna_estable 
	  ,id_rbd 
	  ,ds_tipo_dependencia 
	  ,ds_region_estable 
	  ,ds_provincia_estable 
	  ,ds_comuna_estable 
      ,ds_nom_estable
            from interno.sige_liberado
            where id_mes = (select max(id_mes) from interno.sige_liberado)
            group by id_region_estable
	  ,id_provincia_estable 
	  ,id_comuna_estable
	  ,id_rbd
	  ,ds_tipo_dependencia 
	  ,ds_region_estable 
	  ,ds_provincia_estable 
	  ,ds_comuna_estable 
      ,ds_nom_estable
            order by id_rbd asc;
    """
    
    df_rbd = pd.read_sql(query_mysql_rbd, pg_engine)
    # ######################################################################################
    # Guardar DataFrames como Parquet
    df_pg.to_parquet('tabla_Universo.parquet', index=False)
    df_movimientos.to_parquet('tabla_movimientos.parquet', index=False)
    df_survey.to_parquet('tabla_survey.parquet', index=False)
    df_rbd.to_parquet('tabla_rbd.parquet', index=False)
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
    # con.execute("copy(select * from 'tabla_Paso.parquet' t1 join 'tabla_Universo.parquet' t2 ON t1.token=t2.id_rut_alumno ) to 'realRbdEncuesta.csv'  (HEADER, DELIMITER ',', ENCODING 'UTF-8');")
    
if __name__ == "__main__":
       main()
