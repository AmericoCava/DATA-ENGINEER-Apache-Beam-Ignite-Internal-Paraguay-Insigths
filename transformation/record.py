def antecedentes_record(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['antecedentes'] = row[0].split(',')[1]
    
    return record

def morosidades_ot_record(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['morosidades_ot'] = row[0].split(',')[1]
    
    return record

def morosidades_af_record(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['morosidades_af'] = row[0].split(',')[1]
    
    return record

def info_negativa_record(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['persona'] = row[0].split(',')[1]
    record['afiliado_principal'] = row[0].split(',')[2]
    record['documento'] = row[0].split(',')[3]
    record['tipo_doc_id'] = row[0].split(',')[4]
    record['primer_nombre'] = row[0].split(',')[5]
    record['primer_apellido'] = row[0].split(',')[6]
    record['sexo'] = row[0].split(',')[7]
    record['fecha_nacimiento'] = row[0].split(',')[8]
    record['edad'] = row[0].split(',')[9]
    record['edad_rango'] = row[0].split(',')[10]
    record['t0'] = row[0].split(',')[11]
    record['faja_t0'] = row[0].split(',')[12]
    record['tn'] = row[0].split(',')[13]
    record['faja_tn'] = row[0].split(',')[14]
    record['recuperabilidad'] = row[0].split(',')[15]
    
    return record

def cartera_db_record(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['persona'] = row[0].split(',')[1]
    record['afiliado_principal'] = row[0].split(',')[2]
    record['documento'] = row[0].split(',')[3]
    record['tipo_doc_id'] = row[0].split(',')[4]
    record['primer_nombre'] = row[0].split(',')[5]
    record['primer_apellido'] = row[0].split(',')[6]
    record['sexo'] = row[0].split(',')[7]
    record['fecha_nacimiento'] = row[0].split(',')[8]
    record['edad'] = row[0].split(',')[9]
    record['edad_rango'] = row[0].split(',')[10]
    record['t0'] = row[0].split(',')[11]
    record['faja_t0'] = row[0].split(',')[12]
    record['tn'] = row[0].split(',')[13]
    record['faja_tn'] = row[0].split(',')[14]
    record['recuperabilidad'] = row[0].split(',')[15]
    
    return record

def primera_linea_record_1(row):
    record = {}
    resultado_1 = sum(row[1]['primera_linea']+row[1]['aux'])
    resultado_2 = str(resultado_1).zfill(8)
    record['resultado'] = resultado_2
    record['t0'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    record['orden'] = 1
    
    return record

def primera_linea_record_2(row):
    record = {}
    record['resultado'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    record['orden'] = row[0].split(',')[2]
    
    return record

def segunda_linea_record(row):
    record = {}
    record['resultado'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    record['orden'] = 2
    
    return record

def cc_lista_record(row):
    record = {}
    record['documento'] = row[0].split(',')[0]
    record['primer_nombre'] = row[0].split(',')[1]
    record['primer_apellido'] = row[0].split(',')[2]
    record['sexo'] = row[0].split(',')[3]
    record['edad'] = row[0].split(',')[4]
    record['edad_rango'] = row[0].split(',')[5]
    record['t0'] = row[0].split(',')[6]
    record['faja'] = row[0].split(',')[7]
    record['recuperabilidad'] = row[0].split(',')[8]
    record['afiliado_principal'] = row[0].split(',')[9]
    
    return record

def big_query_record(row):
    record = {}
    record['informconf_id'] = row[0].split(';')[0]
    record['persona'] = row[0].split(',')[1]
    record['afiliado_principal'] = row[0].split(',')[2]
    record['documento'] = row[0].split(',')[3]
    record['tipo_doc_id'] = row[0].split(',')[4]
    record['primer_nombre'] = row[0].split(',')[5]
    record['primer_apellido'] = row[0].split(',')[6]
    record['sexo'] = row[0].split(',')[7]
    record['fecha_nacimiento'] = row[0].split(',')[8]
    record['edad'] = row[0].split(',')[9]
    record['edad_rango'] = row[0].split(',')[10]
    record['t0'] = row[0].split(',')[11]
    record['faja_t0'] = row[0].split(',')[12]
    record['tn'] = row[0].split(',')[13]
    record['faja_tn'] = row[0].split(',')[14]
    record['recuperabilidad'] = row[0].split(',')[15]
    
    return record