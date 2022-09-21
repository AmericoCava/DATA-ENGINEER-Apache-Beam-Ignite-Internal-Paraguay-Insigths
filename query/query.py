import configure.configure as C

tabla_info_negativa = f"{C.project_id}.{C.dataset_id}.{C.tb_info_negativa}"
tabla_faja = f"{C.project_id}.{C.dataset_id}.{C.tb_score_faja}"
tabla_faja_plus = f"{C.project_id}.{C.dataset_id}.{C.tb_score_faja_plus}"
fecha_observacion = f"{C.fecha_observacion}"
fecha_archive = f"{C.fecha_archive}"
tn = f"{C.tn}"
faja_tn = f"{C.faja_tn}"
recuperabilidad = None

def sql_info_negativa():
        
    qry = """SELECT 
             informconf_id ,
             pers_id ,
             tipo_doc_id ,
             pers_tipo ,
             PERSONA ,
             CONTROL_CARTERA ,
             DEMANDAS ,
             CONVOCATORIAS ,
             QUIEBRAS ,
             REMATES ,
             INHIBICIONES ,
             INHABILITACIONES ,
             MOROSIDADES ,
             MOROSIDADES_CE ,
             MOROSIDADES_COM ,
             '{fecha_observacion}' as t0,
             '{tn}' as tn,
             '{faja_tn}' as faja_tn,
             '{recuperabilidad}' as recuperabilidad
             FROM {tabla_info_negativa}
             WHERE PERSONA.primer_nombre IS NOT NULL
          """.format(tabla_info_negativa = tabla_info_negativa, fecha_observacion = fecha_observacion, tn = tn, faja_tn = faja_tn, recuperabilidad = recuperabilidad)

    return qry

def sql_fajas():
    
    qry = """SELECT DISTINCT 
             t1.informconf_id, 
             t1.faja_f1, 
             t1.faja_gral, 
             t2.faja_pos 
             FROM {tabla_faja} t1 
             LEFT JOIN {tabla_faja_plus} t2 
             ON t1.pers_id = t2.pers_id 
             AND t1.archv = t2.archv 
             AND t1.archv = {fecha_archive}
          """.format(tabla_faja = tabla_faja, tabla_faja_plus = tabla_faja_plus, fecha_archive = fecha_archive)
    
    return qry