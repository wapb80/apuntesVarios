# funciona bien , 
# Identificar datos en PostgreSQL que no están en MySQL -- Los nuevos y agregarlos 
# Identificar datos en MySQL que no están en PostgreSQL (los con movimiento), estos sedeben eliminar 
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import duckdb
# from sqlalchemy import text
# from sqlalchemy.exc import SQLAlchemyError


def format_digit(number):
    number_str = str(number)
    if len(number_str) == 1:
        return '0' + number_str
    return number_str

def validate_params(year, month):
    try:
        if len(month)< 2:
            return
        current_date = datetime.now()
        current_month = current_date.month
        current_year = current_date.year
        
        formatted_month = int(format_digit(month))
        formatted_year = int(format_digit(year))
        
        if formatted_year > current_year:
            return False
        elif formatted_year == current_year and formatted_month > current_month:
            return False
        return True
    except ValueError:
        return False
    
def validate_encuesta(encuesta):
    try:
        encuesta = int(encuesta)
        if encuesta < 8:
            return True
        else:
            return False
    except ValueError:
        return False
    

def main2(encuesta,year, month,nomencuesta):
    yearmonth=f"{year}{month:02}"
    # Conexión a PostgreSQL
    pg_engine = create_engine('postgresql://convenios_read:convenios_read@192.168.8.2:5432/dw')

    # Conexión a MySQL Desarrollo
    mysql_engine = create_engine('mysql+pymysql://root:Sead_2023#@192.168.8.2:3306/lm')

    #  Conexión a MySQL Produccion
    # mysql_engine = create_engine('mysql+pymysql://lm:lm_prod_SEAD2024@34.176.0.228:3306/lm')

    #  Conexión para sacar los id de enseñanza y grado 
    mysql_engine_siev = create_engine('mysql+pymysql://root:ha_K*jlJtHS6bK3q@34.176.0.228:3306/siev2')   
    query = f"select id_ensenanza,id_grado from siev2.ensenanzas_grados_encuestas ege  where encuesta_id ={encuesta}"
    df = pd.read_sql(query, mysql_engine_siev)

    # Crear la expresión lógica a partir del DataFrame
    condiciones = []
    for index, row in df.iterrows():
        condicion = f"(id_ensenanza = {row['id_ensenanza']} and id_grado = {row['id_grado']})"
        condiciones.append(condicion)

    # Unir las condiciones con 'or'
    expresion_logica = " or ".join(condiciones)

    # Mostrar la expresión final
    expresion_final = f"({expresion_logica})"
    
     

    # # Creo los select en postgres (SIGE) para cada encuesta
    Parvularia= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado where id_mes = {yearmonth} and id_tipo_dependencia != 4  and ({expresion_logica})"
    PrimeroBasico= f"select 'OK' as emailstatus ,'es' as language ,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3 ,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5  ,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12 ,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21 ,fecha_nacimiento as attribute_22 ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 ,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ({expresion_logica})"
    QuintoBasico= f"select 'OK' as emailstatus ,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount ,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6 ,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8 ,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17 ,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22 ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ({expresion_logica})"
    PrimeroMedio= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5  ,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7 ,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17 ,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth}  and id_tipo_dependencia != 4 and ({expresion_logica})"
    EPJA = f"select 'OK' as emailstatus  ,'es' as language,'N' as sent ,'N' as remindersent ,0 as remindercount ,id_rut_alumno as token  ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname ,'sincorreo@sincorreo.cl' as email ,id_region_estable as attribute_1,id_provincia_estable as attribute_2 ,id_comuna_estable as attribute_3 ,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ({expresion_logica})"
    RPME= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and  ({expresion_logica})"
    TALLAPESO = f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1  when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ({expresion_logica})"

    #Parvularia= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 10	and id_grado = 4)or (id_ensenanza = 10	and id_grado = 5) or (id_ensenanza = 211	and id_grado = 24) or (id_ensenanza = 211 and id_grado = 25)or (id_ensenanza = 212 and id_grado = 24)or (id_ensenanza = 212 and id_grado = 25)or (id_ensenanza = 213 and id_grado = 24) or (id_ensenanza = 213 and id_grado = 25)or (id_ensenanza = 214 and id_grado = 24)or (id_ensenanza = 214 and id_grado = 25)or (id_ensenanza = 215 and id_grado = 24)or (id_ensenanza = 215 and id_grado = 25)or (id_ensenanza = 216 and id_grado = 24)or (id_ensenanza = 216 and id_grado = 25)or (id_ensenanza = 217 and id_grado = 24)or (id_ensenanza = 217 and id_grado = 25)or (id_ensenanza = 218 and id_grado = 24)or (id_ensenanza = 218 and id_grado = 25)or (id_ensenanza = 299 and id_grado = 24)or (id_ensenanza = 299 and id_grado = 25)or (id_ensenanza = 10 and id_grado = 9))"    
    # PrimeroBasico= f"select 'OK' as emailstatus ,'es' as language ,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3 ,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5  ,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12 ,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21 ,fecha_nacimiento as attribute_22 ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 ,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 110	and id_grado = 1)or (id_ensenanza = 211	and id_grado = 1)or (id_ensenanza = 212	and id_grado = 1)or (id_ensenanza = 213	and id_grado = 1)or (id_ensenanza = 215	and id_grado = 1)or (id_ensenanza = 216	and id_grado = 1)or (id_ensenanza = 217	and id_grado = 1)or (id_ensenanza = 218	and id_grado = 1)or (id_ensenanza = 299	and id_grado = 1));"
    # QuintoBasico= f"select 'OK' as emailstatus ,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount ,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6 ,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8 ,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17 ,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22 ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 110	and id_grado = 5) or (id_ensenanza = 211	and id_grado = 5)	OR (id_ensenanza = 212	and id_grado = 5) OR (id_ensenanza = 213	and id_grado = 5)OR (id_ensenanza = 215	and id_grado = 5) OR (id_ensenanza = 216	and id_grado = 5)OR (id_ensenanza = 217	and id_grado = 5) OR (id_ensenanza = 218	and id_grado = 5)OR (id_ensenanza = 299	and id_grado = 5));"
    # PrimeroMedio= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5  ,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7 ,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17 ,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth}  and id_tipo_dependencia != 4 and ((id_ensenanza = 211 and id_grado =  31)or (id_ensenanza = 211 and id_grado = 32)or (id_ensenanza = 211 and id_grado = 33)or (id_ensenanza = 212 and id_grado = 31)or (id_ensenanza = 212 and id_grado = 32)or (id_ensenanza = 212 and id_grado = 33)or (id_ensenanza = 213 and id_grado = 31)or (id_ensenanza = 213 and id_grado = 32) or (id_ensenanza = 213 and id_grado = 33)or (id_ensenanza = 213 and id_grado = 34)or (id_ensenanza = 215 and id_grado = 31)or (id_ensenanza = 215 and id_grado = 32)or (id_ensenanza = 215 and id_grado = 33)or (id_ensenanza = 216 and id_grado = 31)or (id_ensenanza = 216 and id_grado = 32)or (id_ensenanza = 216 and id_grado = 33)or (id_ensenanza = 217 and id_grado = 31)or (id_ensenanza = 217 and id_grado = 32)or (id_ensenanza = 217 and id_grado = 33)or (id_ensenanza = 218 and id_grado = 31)or (id_ensenanza = 218 and id_grado = 32) or (id_ensenanza = 218 and id_grado = 33)or (id_ensenanza = 219 and id_grado = 31)or (id_ensenanza = 219 and id_grado = 32)or (id_ensenanza = 219 and id_grado = 33)or (id_ensenanza = 299 and id_grado = 31)or (id_ensenanza = 299 and id_grado = 32)or (id_ensenanza = 299 and id_grado = 33)or (id_ensenanza = 310 and id_grado = 1)or (id_ensenanza = 410 and id_grado = 1)or (id_ensenanza = 510 and id_grado = 1)or (id_ensenanza = 610 and id_grado = 1)or (id_ensenanza = 710 and id_grado = 1)or (id_ensenanza = 810 and id_grado = 1)or (id_ensenanza = 910	and id_grado = 	1));"
    # EPJA = f"select 'OK' as emailstatus  ,'es' as language,'N' as sent ,'N' as remindersent ,0 as remindercount ,id_rut_alumno as token  ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname ,'sincorreo@sincorreo.cl' as email ,id_region_estable as attribute_1,id_provincia_estable as attribute_2 ,id_comuna_estable as attribute_3 ,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 165 and id_grado = 1) or (id_ensenanza = 165 and id_grado = 2) or (id_ensenanza = 165 and id_grado = 3)or (id_ensenanza = 167 and id_grado = 2)or (id_ensenanza = 167 and id_grado = 3)or (id_ensenanza = 363 and id_grado = 1)or (id_ensenanza = 363 and id_grado = 3) or (id_ensenanza = 463 and id_grado = 1)or (id_ensenanza = 463 and id_grado = 3)or (id_ensenanza = 463 and id_grado = 4)or (id_ensenanza = 563 and id_grado = 1)or (id_ensenanza = 563 and id_grado = 3)or (id_ensenanza = 563 and id_grado = 4)or (id_ensenanza = 663 and id_grado = 1)or (id_ensenanza = 663 and id_grado = 3)or (id_ensenanza = 663 and id_grado = 4)or (id_ensenanza = 763 and id_grado = 1)or (id_ensenanza = 763 and id_grado = 3)or (id_ensenanza = 763 and id_grado = 4)or (id_ensenanza = 863 and id_grado = 1)or (id_ensenanza = 863 and id_grado = 3)or (id_ensenanza = 863 and id_grado = 4));"
    # RPME= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 110 and id_grado = 5) or (id_ensenanza = 110 and id_grado = 6)or (id_ensenanza = 110 and id_grado = 7)or (id_ensenanza = 110 and id_grado = 8)or (id_ensenanza = 165 and id_grado = 1)or (id_ensenanza = 165 and id_grado = 2)or (id_ensenanza = 165 and id_grado = 3)or (id_ensenanza = 167 and id_grado = 2)or (id_ensenanza = 167 and id_grado = 3)or (id_ensenanza = 310 and id_grado = 1)or (id_ensenanza = 310 and id_grado = 2)or (id_ensenanza = 310 and id_grado = 3)or (id_ensenanza = 310 and id_grado = 4)or (id_ensenanza = 363 and id_grado = 1)or (id_ensenanza = 363 and id_grado = 3)or (id_ensenanza = 410 and id_grado = 1)or (id_ensenanza = 410 and id_grado = 2)or (id_ensenanza = 410 and id_grado = 3)or (id_ensenanza = 410 and id_grado = 4)or (id_ensenanza = 463 and id_grado = 1)or (id_ensenanza = 463 and id_grado = 3)or (id_ensenanza = 463 and id_grado = 4)or (id_ensenanza = 510 and id_grado = 1)or (id_ensenanza = 510 and id_grado = 2)or (id_ensenanza = 510 and id_grado = 3)or (id_ensenanza = 510 and id_grado = 4)or (id_ensenanza = 563 and id_grado = 1)or (id_ensenanza = 563 and id_grado = 3)or (id_ensenanza = 563 and id_grado = 4)or (id_ensenanza = 610 and id_grado = 1)or (id_ensenanza = 610 and id_grado = 2)or (id_ensenanza = 610 and id_grado = 3)or (id_ensenanza = 610 and id_grado = 4)or (id_ensenanza = 663 and id_grado = 1)or (id_ensenanza = 663 and id_grado = 3)or (id_ensenanza = 663 and id_grado = 4)or (id_ensenanza = 710 and id_grado = 1)or (id_ensenanza = 710 and id_grado = 2)or (id_ensenanza = 710 and id_grado = 3) or (id_ensenanza = 710 and id_grado = 4)or (id_ensenanza = 763 and id_grado = 1)or (id_ensenanza = 763 and id_grado = 3)or (id_ensenanza = 763 and id_grado = 4)or (id_ensenanza = 810 and id_grado = 1)or (id_ensenanza = 810 and id_grado = 2)or (id_ensenanza = 810 and id_grado = 3)or (id_ensenanza = 810 and id_grado = 4) or (id_ensenanza = 863 and id_grado = 1) or (id_ensenanza = 863 and id_grado = 3)or (id_ensenanza = 863 and id_grado = 4)or (id_ensenanza = 910 and id_grado = 1)or (id_ensenanza = 910 and id_grado = 2)or (id_ensenanza = 910 and id_grado = 3)or (id_ensenanza = 910 and id_grado = 4) or (id_ensenanza = 910 and id_grado = 5)or (id_ensenanza = 910 and id_grado = 6)or (id_ensenanza = 910 and id_grado = 7)or (id_ensenanza = 910 and id_grado = 8)or (id_ensenanza = 299 and id_grado = 5)or (id_ensenanza = 299 and id_grado = 6)or (id_ensenanza = 299 and id_grado = 7)or (id_ensenanza = 299 and id_grado = 8));"
    # TALLAPESO = f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1  when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.interno.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 10 and id_grado = 4)or (id_ensenanza = 10 and id_grado = 5)or (id_ensenanza = 10 and id_grado = 9)or (id_ensenanza = 110 and id_grado = 1)or (id_ensenanza = 110 and id_grado = 5)or (id_ensenanza = 214 and id_grado = 24)or (id_ensenanza = 214 and id_grado = 25)or (id_ensenanza = 299 and id_grado = 1)or (id_ensenanza = 299 and id_grado = 5)or (id_ensenanza = 299 and id_grado = 24)or (id_ensenanza = 299 and id_grado = 25)or (id_ensenanza = 310 and id_grado = 1)or (id_ensenanza = 410 and id_grado = 1)or (id_ensenanza = 510 and id_grado = 1)or (id_ensenanza = 610 and id_grado = 1)or (id_ensenanza = 710 and id_grado = 1)or (id_ensenanza = 810 and id_grado = 1)or (id_ensenanza = 910 and id_grado = 1));" 

    queries = {
    f"query_pg_845448":Parvularia,
    f"query_pg_216195":PrimeroBasico,
    f"query_pg_627416":QuintoBasico,
    f"query_pg_632748":PrimeroMedio,
    f"query_pg_843277":EPJA,
    f"query_pg_317298":RPME,
    f"query_pg_956762":TALLAPESO
    }
    print("Leyendo Datos")
    # Leer tabla de PostgreSQL
    # print(queries[f"query_pg_{encuesta}"])  /// para imprimir la query 
    df_pg = pd.read_sql(queries[f"query_pg_{encuesta}"], pg_engine)

    # Leer tabla de MySQL completa
    # query_mysql = f"SELECT token,attribute_4,attribute_24,attribute_7,attribute_8,attribute_9,attribute_11,attribute_12,attribute_13 FROM lime_tokens_{encuesta}_prueba "
    query_mysql = f"SELECT token,attribute_4,attribute_24,attribute_7,attribute_8,attribute_9,attribute_11,attribute_12,attribute_13,completed FROM lime_tokens_{encuesta}"
    df_mysql = pd.read_sql(query_mysql, mysql_engine)

  

    ######################################################################################
    # Guardar DataFrames como Parquet
    df_pg.to_parquet('tabla_pg.parquet', index=False)
    df_mysql.to_parquet('tabla_mysql.parquet', index=False)
    # df_mysql_contestados.to_parquet('tabla_contestados.parquet', index=False)
    print("Comparando Datos")
    ######################################################################################
    # Crear una instancia de DuckDB
    con = duckdb.connect(database=':memory:') # Usar una base de datos en memoria
    ################################ Leer los Parquet en DuckDB
    con.execute("CREATE TABLE pg_table AS SELECT * FROM read_parquet('tabla_pg.parquet')")
    con.execute("CREATE TABLE mysql_table AS SELECT * FROM read_parquet('tabla_mysql.parquet');")

    # Leer la tabla desde DuckDB Los que se deben eliminar
    df_to_delete = con.execute("SELECT mysql_table.token FROM mysql_table LEFT JOIN pg_table ON mysql_table.token = pg_table.token WHERE pg_table.token IS NULL union select  m.token  from mysql_table as m  left Join pg_table as p ON p.token=m.token and p.attribute_4!=m.attribute_4 where p.token is not null;").fetchdf()

    ################################ Verificar que el DataFrame no está vacío
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
                # delete_query = f"""
                # DELETE FROM lime_tokens_{encuesta}_prueba
                # WHERE token IN ({', '.join(map(lambda x: f"'{x}'", tokens_to_delete))})
                # """
                delete_query = f"""
                DELETE FROM lime_tokens_{encuesta}
                WHERE token IN ({', '.join(map(lambda x: f"'{x}'", tokens_to_delete))})
                """
                # Ejecutar la consulta de eliminación
                with mysql_engine.connect() as connection:
                    # print(text(delete_query))
                    connection.execute(text(delete_query))
                    connection.commit()

                print(f"Datos Eliminados")
            else:
                print("La lista de tokens a eliminar está vacía después de filtrar valores nulos o vacíos.")
        else:
            print("La columna 'token' no existe en la tabla DuckDB.")
    else:
        print("No hay tokens a eliminar.")




    # Identificar datos en PostgreSQL que no están en MySQL -- (Los nuevos) 
    # Define la consulta que quieres ejecutar en DuckDB

    print("Grabando nuevos datos")
    nuevos = """
    SELECT p.* FROM pg_table as p LEFT JOIN mysql_table as m  ON p.token = m.token WHERE m.token IS NULL
    union select  p.*  from mysql_table as m  left Join pg_table as p   ON p.token=m.token and p.attribute_4!=m.attribute_4 where p.token is not null;
    """
    
    # Ejecutar la consulta y guardar el resultado en un DataFrame
    df_nuevos = con.execute(nuevos).fetchdf()
    # Insertar los datos en la tabla de MySQL
    # tabla=f"lime_tokens_{encuesta}_prueba"
    tabla=f"lime_tokens_{encuesta}"
    # tabla=f"lime_prueba"
    df_nuevos.to_sql(tabla, mysql_engine, if_exists='append', index=False)




    # ################################ VER CUALES SE MOVIERON Y ESTABAN CON LA ENCUESTA CONTESTADA
    df_to_update = con.execute("select  m.token,m.completed  from mysql_table as m left Join pg_table as p ON p.token=m.token and p.attribute_4!=m.attribute_4 where p.token is not null and m.completed!='N' ").fetchdf()

    
    #################################  Verificar que el DataFrame no está vacío y actualizar 
    if not df_to_update.empty:
                  for index, row in df_to_update.iterrows():
                    sql = f"""
                    UPDATE lime_tokens_{encuesta} SET completed = '{row['completed']}' WHERE token = '{row['token']}';
                    """
                    # print(sql)
                    with mysql_engine.connect() as connection:
                        connection.execute(text(sql))
                        connection.commit()
                        
    else:
        print("No hay tokens a actualizar.")

    
if __name__ == "__main__":

    if len(sys.argv) != 4:

        print("1 Parvularia")
        print("2 Primero Basico")
        print("3 Quinto Basico")
        print("4 Primero Medio")
        print("5 Epja")
        print("6 Peso-Talla")
        print("7 Rpme")
        print("------------------Uso: python main.py <opcion Encuesta> <año> <mes> --------------------")        
        sys.exit(1)
    
    numencuesta = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    
    if not validate_params(year, month):
        print("Error: El año o mes proporcionados no son válidos. EJ: 2024 07 ")
        sys.exit(1)
    if not validate_encuesta(numencuesta):
        print("Error: El numero de la encuesta no esválido.")
        sys.exit(1)
    encuesta = ['845448', '216195', '627416', '632748','843277','956762','317298']
    nomEncuesta = ['parvularia', 'primeroBasico', 'quintoBasico', 'primeroMedio','Epja','PesoTalla','Rpme']
    main2(encuesta[int(numencuesta)-1],year,month,nomEncuesta[int(numencuesta)-1])
