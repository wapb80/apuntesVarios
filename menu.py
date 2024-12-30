# funciona bien , 
# Identificar datos en PostgreSQL que no están en MySQL -- Los nuevos y filtrados ya con los contestados
# deja solo los que no han contestado que son nuevos , si es asi no lo agrega, 
# Identificar datos en MySQL que no están en PostgreSQL (los con movimiento), estos sedeben eliminar 
# deja solo los que no han contestado y los elimina
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import duckdb
import csv

def validate_params(year, month):
    try:
        year = int(year)
        month = int(month)
        datetime(year=year, month=month, day=1)
        if year > 2023:
            return True
        else:
            return False
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
    

def main2(encuesta,year, month):
    yearmonth=f"{year}{month:02}"
    
    # # Conexión a PostgreSQL
    pg_engine = create_engine('postgresql://convenios_read:convenios_read@192.168.8.2:5432/dw')
    # Conexión a MySQL
    mysql_engine = create_engine('mysql+pymysql://lm_desa:lm_desa_Sead2023@192.168.8.2:3306/lm')

    # Creo los select en postgres (SIGE) para cada encuesta
 
    Parvularia= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 from dw.convenios.sige_liberado where id_mes = {yearmonth} and id_tipo_dependencia != 4  and ((id_ensenanza = 10	and id_grado = 4)or (id_ensenanza = 10	and id_grado = 5) or (id_ensenanza = 211	and id_grado = 24) or (id_ensenanza = 211 and id_grado = 25)or (id_ensenanza = 212 and id_grado = 24)or (id_ensenanza = 212 and id_grado = 25)or (id_ensenanza = 213 and id_grado = 24) or (id_ensenanza = 213 and id_grado = 25)or (id_ensenanza = 214 and id_grado = 24)or (id_ensenanza = 214 and id_grado = 25)or (id_ensenanza = 215 and id_grado = 24)or (id_ensenanza = 215 and id_grado = 25)or (id_ensenanza = 216 and id_grado = 24)or (id_ensenanza = 216 and id_grado = 25)or (id_ensenanza = 217 and id_grado = 24)or (id_ensenanza = 217 and id_grado = 25)or (id_ensenanza = 218 and id_grado = 24)or (id_ensenanza = 218 and id_grado = 25)or (id_ensenanza = 299 and id_grado = 24)or (id_ensenanza = 299 and id_grado = 25))"
    PrimeroBasico= f"select 'OK' as emailstatus ,'es' as language ,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3 ,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5  ,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12 ,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21 ,fecha_nacimiento as attribute_22 ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23 ,ds_nom_estable as attribute_24 from dw.convenios.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 110	and id_grado = 1)or (id_ensenanza = 211	and id_grado = 1)or (id_ensenanza = 212	and id_grado = 1)or (id_ensenanza = 213	and id_grado = 1)or (id_ensenanza = 215	and id_grado = 1)or (id_ensenanza = 216	and id_grado = 1)or (id_ensenanza = 217	and id_grado = 1)or (id_ensenanza = 218	and id_grado = 1)or (id_ensenanza = 299	and id_grado = 1));"
    QuintoBasico= f"select 'OK' as emailstatus ,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount ,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6 ,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL'  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8 ,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17 ,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22 ,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.convenios.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 110	and id_grado = 5) or (id_ensenanza = 211	and id_grado = 5)	OR (id_ensenanza = 212	and id_grado = 5) OR (id_ensenanza = 213	and id_grado = 5)OR (id_ensenanza = 215	and id_grado = 5) OR (id_ensenanza = 216	and id_grado = 5)OR (id_ensenanza = 217	and id_grado = 5) OR (id_ensenanza = 218	and id_grado = 5)OR (id_ensenanza = 299	and id_grado = 5));"
    PrimeroMedio= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5  ,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5  when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7 ,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17 ,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.convenios.sige_liberado sl where id_mes = {yearmonth}  and id_tipo_dependencia != 4 and ((id_ensenanza = 211 and id_grado =  31)or (id_ensenanza = 211 and id_grado = 32)or (id_ensenanza = 211 and id_grado = 33)or (id_ensenanza = 212 and id_grado = 31)or (id_ensenanza = 212 and id_grado = 32)or (id_ensenanza = 212 and id_grado = 33)or (id_ensenanza = 213 and id_grado = 31)or (id_ensenanza = 213 and id_grado = 32) or (id_ensenanza = 213 and id_grado = 33)or (id_ensenanza = 213 and id_grado = 34)or (id_ensenanza = 215 and id_grado = 31)or (id_ensenanza = 215 and id_grado = 32)or (id_ensenanza = 215 and id_grado = 33)or (id_ensenanza = 216 and id_grado = 31)or (id_ensenanza = 216 and id_grado = 32)or (id_ensenanza = 216 and id_grado = 33)or (id_ensenanza = 217 and id_grado = 31)or (id_ensenanza = 217 and id_grado = 32)or (id_ensenanza = 217 and id_grado = 33)or (id_ensenanza = 218 and id_grado = 31)or (id_ensenanza = 218 and id_grado = 32) or (id_ensenanza = 218 and id_grado = 33)or (id_ensenanza = 219 and id_grado = 31)or (id_ensenanza = 219 and id_grado = 32)or (id_ensenanza = 219 and id_grado = 33)or (id_ensenanza = 299 and id_grado = 31)or (id_ensenanza = 299 and id_grado = 32)or (id_ensenanza = 299 and id_grado = 33)or (id_ensenanza = 310 and id_grado = 1)or (id_ensenanza = 410 and id_grado = 1)or (id_ensenanza = 510 and id_grado = 1)or (id_ensenanza = 610 and id_grado = 1)or (id_ensenanza = 710 and id_grado = 1)or (id_ensenanza = 810 and id_grado = 1));"
    EPJA = f"select 'OK' as emailstatus  ,'es' as language,'N' as sent ,'N' as remindersent ,0 as remindercount ,id_rut_alumno as token  ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname ,'sincorreo@sincorreo.cl' as email ,id_region_estable as attribute_1,id_provincia_estable as attribute_2 ,id_comuna_estable as attribute_3 ,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.convenios.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 165 and id_grado = 1) or (id_ensenanza = 165 and id_grado = 2) or (id_ensenanza = 165 and id_grado = 3)or (id_ensenanza = 167 and id_grado = 2)or (id_ensenanza = 167 and id_grado = 3)or (id_ensenanza = 363 and id_grado = 1)or (id_ensenanza = 363 and id_grado = 3) or (id_ensenanza = 463 and id_grado = 1)or (id_ensenanza = 463 and id_grado = 3)or (id_ensenanza = 463 and id_grado = 4)or (id_ensenanza = 563 and id_grado = 1)or (id_ensenanza = 563 and id_grado = 3)or (id_ensenanza = 563 and id_grado = 4)or (id_ensenanza = 663 and id_grado = 1)or (id_ensenanza = 663 and id_grado = 3)or (id_ensenanza = 663 and id_grado = 4)or (id_ensenanza = 763 and id_grado = 1)or (id_ensenanza = 763 and id_grado = 3)or (id_ensenanza = 763 and id_grado = 4)or (id_ensenanza = 863 and id_grado = 3)or (id_ensenanza = 863 and id_grado = 4));"
    RPME= f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token ,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1 when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.convenios.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 110 and id_grado = 5) or (id_ensenanza = 110 and id_grado = 6)or (id_ensenanza = 110 and id_grado = 7)or (id_ensenanza = 110 and id_grado = 8)or (id_ensenanza = 165 and id_grado = 1)or (id_ensenanza = 165 and id_grado = 2)or (id_ensenanza = 165 and id_grado = 3)or (id_ensenanza = 167 and id_grado = 2)or (id_ensenanza = 167 and id_grado = 3)or (id_ensenanza = 310 and id_grado = 1)or (id_ensenanza = 310 and id_grado = 2)or (id_ensenanza = 310 and id_grado = 3)or (id_ensenanza = 310 and id_grado = 4)or (id_ensenanza = 363 and id_grado = 1)or (id_ensenanza = 363 and id_grado = 3)or (id_ensenanza = 410 and id_grado = 1)or (id_ensenanza = 410 and id_grado = 2)or (id_ensenanza = 410 and id_grado = 3)or (id_ensenanza = 410 and id_grado = 4)or (id_ensenanza = 463 and id_grado = 1)or (id_ensenanza = 463 and id_grado = 3)or (id_ensenanza = 463 and id_grado = 4)or (id_ensenanza = 510 and id_grado = 1)or (id_ensenanza = 510 and id_grado = 2)or (id_ensenanza = 510 and id_grado = 3)or (id_ensenanza = 510 and id_grado = 4)or (id_ensenanza = 563 and id_grado = 1)or (id_ensenanza = 563 and id_grado = 3)or (id_ensenanza = 563 and id_grado = 4)or (id_ensenanza = 610 and id_grado = 1)or (id_ensenanza = 610 and id_grado = 2)or (id_ensenanza = 610 and id_grado = 3)or (id_ensenanza = 610 and id_grado = 4)or (id_ensenanza = 663 and id_grado = 1)or (id_ensenanza = 663 and id_grado = 3)or (id_ensenanza = 663 and id_grado = 4)or (id_ensenanza = 710 and id_grado = 1)or (id_ensenanza = 710 and id_grado = 2)or (id_ensenanza = 710 and id_grado = 3) or (id_ensenanza = 710 and id_grado = 4)or (id_ensenanza = 763 and id_grado = 1)or (id_ensenanza = 763 and id_grado = 3)or (id_ensenanza = 763 and id_grado = 4)or (id_ensenanza = 810 and id_grado = 1)or (id_ensenanza = 810 and id_grado = 2)or (id_ensenanza = 810 and id_grado = 3)or (id_ensenanza = 810 and id_grado = 4)or (id_ensenanza = 863 and id_grado = 3)or (id_ensenanza = 863 and id_grado = 4)or (id_ensenanza = 910 and id_grado = 1)or (id_ensenanza = 910 and id_grado = 2)or (id_ensenanza = 910 and id_grado = 3)or (id_ensenanza = 910 and id_grado = 4));"    
    TALLAPESO = f"select 'OK' as emailstatus,'es' as language,'N' as sent,'N' as remindersent,0 as remindercount,id_rut_alumno as token,nombres as firstname,apellido_paterno || ' ' || apellido_materno as lastname,'sincorreo@sincorreo.cl' as email,id_region_estable as attribute_1,id_provincia_estable as attribute_2,id_comuna_estable as attribute_3,id_rbd as attribute_4,ds_tipo_dependencia as attribute_5,case when id_ensenanza = 10 then 1  when id_ensenanza = 110 then 3 when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 4 when id_ensenanza > 200 and id_ensenanza < 300 then 5 when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 6 end as attribute_6,case when id_ensenanza = 10 then 'PARVULARIA' when id_ensenanza = 110 then 'BÁSICA' when id_ensenanza in (310, 410, 510, 610, 710, 810, 910) then 'MEDIA' when id_ensenanza > 200 and id_ensenanza < 300 then 'ESPECIAL' when id_ensenanza in (165, 167, 363, 463, 563, 663, 763, 863, 963) then 'ADULTO' end as attribute_7,id_grado as attribute_8,ds_grado as attribute_9,id_ensenanza as attribute_10,ds_region_estable as attribute_11,ds_provincia_estable as attribute_12,ds_comuna_estable as attribute_13,id_rut_alumno as attribute_14,dgv_alumno as attribute_15,ds_sexo as attribute_16,id_rut_alumno || '-' || dgv_alumno as attribute_17,nombres as attribute_18,apellido_paterno as attribute_19,apellido_materno as attribute_20,id_mes as attribute_21,fecha_nacimiento as attribute_22,DATE_PART('YEAR', AGE(TO_DATE(fecha_nacimiento, 'YYYYMMDD')))::int as attribute_23,ds_nom_estable as attribute_24 from dw.convenios.sige_liberado sl where id_mes = {yearmonth} and id_tipo_dependencia != 4 and ((id_ensenanza = 10 and id_grado = 4)or (id_ensenanza = 10 and id_grado = 5)or (id_ensenanza = 10 and id_grado = 10)or (id_ensenanza = 110 and id_grado = 1)or (id_ensenanza = 110 and id_grado = 5)or (id_ensenanza = 211 and id_grado = 1)or (id_ensenanza = 211 and id_grado = 5)or (id_ensenanza = 211 and id_grado = 24)or (id_ensenanza = 211 and id_grado = 25)or (id_ensenanza = 211 and id_grado = 31)or (id_ensenanza = 211 and id_grado = 32)or (id_ensenanza = 211 and id_grado = 33)or (id_ensenanza = 212 and id_grado = 1)or (id_ensenanza = 212 and id_grado = 5)or (id_ensenanza = 212 and id_grado = 24)or (id_ensenanza = 212 and id_grado = 25)or (id_ensenanza = 212 and id_grado = 31)or (id_ensenanza = 212 and id_grado = 32)or (id_ensenanza = 212 and id_grado = 33)or (id_ensenanza = 213 and id_grado = 1)or (id_ensenanza = 213 and id_grado = 5)or (id_ensenanza = 213 and id_grado = 24)or (id_ensenanza = 213 and id_grado = 25)or (id_ensenanza = 213 and id_grado = 31)or (id_ensenanza = 213 and id_grado = 32)or (id_ensenanza = 213 and id_grado = 33)or (id_ensenanza = 213 and id_grado = 34)or (id_ensenanza = 214 and id_grado = 24)or (id_ensenanza = 214 and id_grado = 25)or (id_ensenanza = 215 and id_grado = 1)or (id_ensenanza = 215 and id_grado = 5)or (id_ensenanza = 215 and id_grado = 24)or (id_ensenanza = 215 and id_grado = 25)or (id_ensenanza = 215 and id_grado = 31)or (id_ensenanza = 215 and id_grado = 32)or (id_ensenanza = 215 and id_grado = 33)or (id_ensenanza = 216 and id_grado = 1)or (id_ensenanza = 216 and id_grado = 5)or (id_ensenanza = 216 and id_grado = 24)or (id_ensenanza = 216 and id_grado = 25)or (id_ensenanza = 216 and id_grado = 31)or (id_ensenanza = 216 and id_grado = 32)or (id_ensenanza = 216 and id_grado = 33)or (id_ensenanza = 217 and id_grado = 1)or (id_ensenanza = 217 and id_grado = 5)or (id_ensenanza = 217 and id_grado = 24)or (id_ensenanza = 217 and id_grado = 25)or (id_ensenanza = 217 and id_grado = 31)or (id_ensenanza = 217 and id_grado = 32)or (id_ensenanza = 217 and id_grado = 33)or (id_ensenanza = 218 and id_grado = 1)or (id_ensenanza = 218 and id_grado = 5)or (id_ensenanza = 218 and id_grado = 24)or (id_ensenanza = 218 and id_grado = 25)or (id_ensenanza = 218 and id_grado = 31)or (id_ensenanza = 218 and id_grado = 32)or (id_ensenanza = 218 and id_grado = 33)or (id_ensenanza = 219 and id_grado = 31)or (id_ensenanza = 219 and id_grado = 32)or (id_ensenanza = 219 and id_grado = 33)or (id_ensenanza = 299 and id_grado = 1)or (id_ensenanza = 299 and id_grado = 5)or (id_ensenanza = 299 and id_grado = 24)or (id_ensenanza = 299 and id_grado = 25)or (id_ensenanza = 299 and id_grado = 31)or (id_ensenanza = 299 and id_grado = 32)or (id_ensenanza = 299 and id_grado = 33)or (id_ensenanza = 310 and id_grado = 1)or (id_ensenanza = 410 and id_grado = 1)or (id_ensenanza = 510 and id_grado = 1)or (id_ensenanza = 610 and id_grado = 1)or (id_ensenanza = 710 and id_grado = 1)or (id_ensenanza = 810 and id_grado = 1)or (id_ensenanza = 910 and id_grado = 1));"

    queries = {
    f"query_pg_845448":Parvularia,
    f"query_pg_216195":PrimeroBasico,
    f"query_pg_627416":QuintoBasico,
    f"query_pg_632748":PrimeroMedio,
    f"query_pg_843277":EPJA,
    f"query_pg_317298":RPME,
    f"query_pg_956762":TALLAPESO
    }
   
    # Leer tabla de PostgreSQL
    # print(queries[f"query_pg_{encuesta}"])
    df_pg = pd.read_sql(queries[f"query_pg_{encuesta}"], pg_engine)

    # Leer tabla de MySQL completa
    query_mysql = f"SELECT token FROM lime_tokens_{encuesta}_prueba"
    df_mysql = pd.read_sql(query_mysql, mysql_engine)


    # Leer tabla de MySQL de todos los contestados de todas las encuestas ,
    # para luego conpararlos con los nuevos y los que se movieron
    
    query_mysql_contestados = f"""
    SELECT token FROM  lime_survey_845448 where submitdate is not null  and token is not null
    union
    SELECT token FROM  lime_survey_216195 where submitdate is not null  and token is not null
    union
    SELECT token FROM  lime_survey_627416 where submitdate is not null  and token is not null
    union
    SELECT token FROM  lime_survey_632748 where submitdate is not null  and token is not null"""

    #  Las saque por que no corresponde que una persona que 
    # contesta estas encuestas influye en las de la enseñanza normal
    # union
    # SELECT token FROM  lime_survey_843277 where submitdate is not null  and token is not null
    # union
    # SELECT token FROM  lime_survey_317298 where submitdate is not null and token is not null
    # union
    # SELECT token FROM  lime_survey_956762 where submitdate is not null and token is not null"""

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

    #  desde aca para arriba esta todo bien #####################################################################################
    # Crear una instancia de DuckDB
    con = duckdb.connect(database=':memory:')
    # Configuración de la conexión a DuckDB
    # duckdb_con = duckdb.connect(database=':memory:')  # Usar una base de datos en memoria

    # Leer los CSV en DuckDB
    con.execute("CREATE TABLE pg_table AS SELECT * FROM read_csv_auto('tabla_pg_corrected.csv');")
    con.execute("CREATE TABLE mysql_table AS SELECT * FROM read_csv_auto('tabla_mysql.csv');")
    con.execute("CREATE TABLE contestados AS SELECT * FROM read_csv_auto('tabla_mysql_contestados.csv');")

    # Identificar datos en PostgreSQL que no están en MySQL -- (Los nuevos) Filtrados ya con los contestados
    # con.execute("CREATE TABLE Nuevos AS SELECT p.* FROM pg_table as p LEFT JOIN mysql_table as m  ON p.token = m.token WHERE m.token IS NULL   AND p.token NOT IN (SELECT token FROM contestados );")
    # Define la consulta que quieres ejecutar en DuckDB
    nuevos = """
    SELECT p.* FROM pg_table as p LEFT JOIN mysql_table as m  ON p.token = m.token WHERE m.token IS NULL   AND p.token NOT IN (SELECT token FROM contestados );
    """
    # Ejecutar la consulta y guardar el resultado en un DataFrame
    # df_nuevos = duckdb.query(nuevos).to_df()
    df_nuevos = con.execute(nuevos).fetchdf()
    # Insertar los datos en la tabla de MySQL
    tabla=f"lime_tokens_{encuesta}_prueba"
    df_nuevos.to_sql(tabla, mysql_engine, if_exists='append', index=False)

    con.execute("CREATE TABLE eliminar AS SELECT mysql_table.token FROM mysql_table LEFT JOIN pg_table ON mysql_table.token = pg_table.token WHERE pg_table.token IS NULL AND mysql_table.token NOT IN (SELECT token FROM contestados );")
    # Leer la tabla desde DuckDB
    df_to_delete = con.execute("SELECT * FROM eliminar").fetchdf()

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
                DELETE FROM lime_tokens_{encuesta}_prueba
                WHERE token IN ({', '.join(map(lambda x: f"'{x}'", tokens_to_delete))})
                """

                # Ejecutar la consulta de eliminación
                with mysql_engine.connect() as connection:
                    connection.execute(text(delete_query))

                print(f"Fin")
            else:
                print("La lista de tokens a eliminar está vacía después de filtrar valores nulos o vacíos.")
        else:
            print("La columna 'token' no existe en la tabla DuckDB.")
    else:
        print("No hay tokens a eliminar.")


if __name__ == "__main__":

    if len(sys.argv) != 4:

        print("1 Parvularia")
        print("2 Primero Basico")
        print("3 Quinto Basico")
        print("4 Primero Medio")
        print("5 Epja")
        print("6 Talla - Peso")
        print("7 Rpme")
        print("------------------Uso: python main.py <opcion Encuesta> <año> <mes> --------------------")        
        sys.exit(1)
    
    numencuesta = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    
    if not validate_params(year, month):
        print("Error: El año o mes proporcionados no son válidos.")
        sys.exit(1)
    if not validate_encuesta(numencuesta):
        print("Error: El numero de la encuesta no esválido.")
        sys.exit(1)


    # print(year+month)
    # print(f"{year}{month.zfill(2)}")

    encuesta = ['845448', '216195', '627416', '632748','843277','956762','317298']
    main2(encuesta[int(numencuesta)-1],year,month)
