def modulos_conca(row):
    record = []
    record.append(row['informconf_id']+','+
                  str(row['afiliado_principal']))
    record.append(1)
    record = (record[0],record[1])

    return record

def antecedentes_conca(row):
    record = []
    record.append(row['informconf_id']+';'+
                  str(row['afiliado_principal'])+','+
                  str(row['antecedentes']))
    record.append(1)
    record = (record[0],record[1])

    return record

def morosidades_ot_conca(row):
    record = []
    record.append(row['informconf_id']+';'+
                  str(row['afiliado_principal'])+','+
                  str(row['morosidades_ot']))
    record.append(1)
    record = (record[0],record[1])
    
    return record

def morosidades_af_conca(row):
    record = []
    record.append(row['informconf_id']+';'+
                  str(row['afiliado_principal'])+','+
                  str(row['morosidades_af']))
    record.append(1)
    record = (record[0],record[1])
    
    return record

def info_negativa_fajas_conca(row):
    record = []
    record.append(row['informconf_id']+';'+
                  str(row['afiliado_principal'])+','+
                  str(row['persona'])+','+
                  str(row['afiliado_principal'])+','+
                  str(row['documento'])+','+
                  str(row['tipo_doc_id'])+','+
                  str(row['primer_nombre'])+','+
                  str(row['primer_apellido'])+','+
                  str(row['sexo'])+','+
                  str(row['fecha_nacimiento'])+','+
                  str(row['edad'])+','+
                  str(row['edad_rango'])+','+
                  str(row['t0'])+','+
                  str(row['faja_t0'])+','+
                  str(row['tn'])+','+
                  str(row['faja_tn'])+','+
                  str(row['recuperabilidad']))
    record.append(1)
    record = (record[0],record[1])

    return record

def info_negativa_join_antecedentes_conca(row):
    record = []
    record.append(row['informconf_id']+','+
                  str(row['persona'])+','+
                  str(row['afiliado_principal'])+','+
                  str(row['documento'])+','+
                  str(row['tipo_doc_id'])+','+
                  str(row['primer_nombre'])+','+
                  str(row['primer_apellido'])+','+
                  str(row['sexo'])+','+
                  str(row['fecha_nacimiento'])+','+
                  str(row['edad'])+','+
                  str(row['edad_rango'])+','+
                  str(row['t0'])+','+
                  str(row['faja_t0'])+','+
                  str(row['tn'])+','+
                  str(row['faja_tn'])+','+
                  str(row['recuperabilidad']))
    record.append(1)
    record = (record[0],record[1])

    return record

def aux_2_conca(row):
    record = []
    record.append(row['t0']+','+
                  str(row['afiliado_principal']))
    record.append(1)
    record = (record[0],record[1])

    return record

def primera_linea_conca(row):
    record = []
    record.append(row['resultado']+
                  str(row['t0'])+','+
                  str(row['afiliado_principal'])+','+
                  str(row['orden']))
    record.append(1)
    record = (record[0],record[1])
    
    return record

def segunda_linea_conca(row):
    record = []
    record.append(row['documento']+'/'+
                  str(row['t0'])+'/'+
                  str(row['faja_t0'])+'/'+
                  str(row['sexo'])+'/'+
                  str(row['edad_rango'])+'/'+
                  str(row['recuperabilidad'])+','+
                  str(row['afiliado_principal']))
    record.append(1)
    record = (record[0],record[1])

    return record

def cc_lista_conca(row):
    record = []
    record.append(row['documento']+','+
                  str(row['primer_nombre'])+','+
                  str(row['primer_apellido'])+','+
                  str(row['sexo'])+','+
                  str(row['edad'])+','+
                  str(row['edad_rango'])+','+
                  str(row['t0'])+','+
                  str(row['faja_t0'])+','+
                  str(row['recuperabilidad'])+','+
                  str(row['afiliado_principal']))
    record.append(1)
    record = (record[0],record[1])
    
    return record

def rango_edad_conca(row):
    record = []
    record.append(row['faja_t0']+','+
                  str(row['afiliado_principal'])+','+
                  str(row['sin_datos'])+','+
                  str(row['20_o_menos'])+','+
                  str(row['21_a_25'])+','+
                  str(row['26_a_30'])+','+
                  str(row['31_a_40'])+','+
                  str(row['41_a_50'])+','+
                  str(row['51_a_60'])+','+
                  str(row['61_o_más'])+','+
                  str(row['total']))
    record.append(1)
    record = (record[0],record[1])

    return record

def rango_sexo_conca(row):
    record = []
    record.append(row['faja_t0']+','+
                  str(row['afiliado_principal'])+','+
                  str(row['sin_datos'])+','+
                  str(row['femenino'])+','+
                  str(row['masculino'])+','+
                  str(row['total']))
    record.append(1)
    record = (record[0],record[1])

    return record