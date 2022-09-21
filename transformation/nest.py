import transformation.transformation as T
import transformation.sum as S
import transformation.concatenar as CT
import transformation.record as R

def nest_antecedentes(row):
    record = {}
    record = S.antecedentes_sum(row)
    record = CT.antecedentes_conca(record)
    record = R.antecedentes_record(record)
    
    return record

def nest_morosidades_ot(row):
    record = {}
    record = S.morosidades_ot_sum(row)
    record = CT.morosidades_ot_conca(record)
    record = R.morosidades_ot_record(record)
    
    return record

def nest_morosidades_af(row):
    record = {}
    record = S.morosidades_af_sum(row)
    record = CT.morosidades_af_conca(record)
    record = R.morosidades_af_record(record)
    
    return record

def nest_info_negativa(row):
    record = {}
    record = T.sexo(row)
#    record = T.edad_rango(record)
    record = T.none_sexo(record)
    record = T.none_edad_rango(record)
    
    return record

def nest_info_negativa_fajas(row):
    record = {}
    record = T.append_fajas(row)
    record = T.transform_fajas(record)
    record = CT.info_negativa_fajas_conca(record)
    record = R.info_negativa_record(record)
    
    return record

def nest_info_negativa_join_antecedentes(row):
    record = {}
    record = T.append_morisades_af(row)
    record = T.append_morisades_ot(record)
    record = T.append_antecedentes(record)
    record = T.recuperabilidad(record)
    record = CT.info_negativa_join_antecedentes_conca(record)
    record = R.cartera_db_record(record)
    
    return record

def nest_primera_linea(row):
    record = {}
    record = R.primera_linea_record_1(row)
    record = CT.primera_linea_conca(record)
    record = R.primera_linea_record_2(record)
    
    return record

def nest_segunda_linea(row):
    record = {}
    record = CT.segunda_linea_conca(row)
    record = R.segunda_linea_record(record)
    
    return record

def nest_cc_lista(row):
    record = {}
    record = CT.cc_lista_conca(row)
    record = R.cc_lista_record(record)
    record = T.sexo_cc_lista(record)
    
    return record

def nest_rango_edad(row):
    record = {}
    record = T.append_edades(row)
    record = T.change_edad_rango(record)
    record = CT.rango_edad_conca(record)
    
    return record

def nest_faja_edad(row):
    record = {}
    record = S.faja_edad_sum(row)
    record = T.transformation_edad_rango(record)
    
    return record

def nest_rango_sexo(row):
    record = {}
    record = T.append_sexos(row)
    record = T.change_sexos(record)
    record = CT.rango_sexo_conca(record)
    
    return record

def nest_faja_sexo(row):
    record = {}
    record = S.faja_sexo_sum(row)
    record = T.transformation_sexo(record)
    
    return record

def nest_cartera_db(row):
    record = CT.info_negativa_join_antecedentes_conca(row)
    record = R.big_query_record(record)
    record = T.time_stamp(record)
    
    return record