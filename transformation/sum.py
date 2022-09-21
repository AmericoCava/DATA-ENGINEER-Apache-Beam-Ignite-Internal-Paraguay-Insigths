def antecedentes_sum(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    antecedentes = sum(row[1]['demandas']
                     + row[1]['convocatorias']
                     + row[1]['quiebras']
                     + row[1]['remates']
                     + row[1]['inhibiciones']
                     + row[1]['inhabilitaciones'])
    record['antecedentes'] = antecedentes
    
    return record

def morosidades_ot_sum(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    morosidades_ot = sum(row[1]['morosidades_ot_pub']
                         + row[1]['morosidades_ot_ce30']
                         + row[1]['morosidades_ot_ce30com'])
    record['morosidades_ot'] = morosidades_ot
    
    return record

def morosidades_af_sum(row):
    record = {}
    record['informconf_id'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    morosidades_af = sum(row[1]['morosidades_af_pub']
                         + row[1]['morosidades_af_ce30']
                         + row[1]['morosidades_af_ce30com'])
    record['morosidades_af'] = morosidades_af
    
    return record

def faja_edad_sum(row):
    record = {}
    record['faja'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    record['sin_datos'] = row[0].split(',')[2]
    record['20_o_menos'] = row[0].split(',')[3]
    record['21_a_25'] = row[0].split(',')[4]
    record['26_a_30'] = row[0].split(',')[5]
    record['31_a_40'] = row[0].split(',')[6]
    record['41_a_50'] = row[0].split(',')[7]
    record['51_a_60'] = row[0].split(',')[8]
    record['61_o_más'] = row[0].split(',')[9]
    record['total'] = str(sum(row[1]['rango_edad']
                          + row[1]['aux']))
    
    return record

def faja_sexo_sum(row):
    record = {}
    record['faja'] = row[0].split(',')[0]
    record['afiliado_principal'] = row[0].split(',')[1]
    record['sin_datos'] = row[0].split(',')[2]
    record['femenino'] = row[0].split(',')[3]
    record['masculino'] = row[0].split(',')[4]
    record['total'] = str(sum(row[1]['rango_sexo']
                          + row[1]['aux']))
    
    return record