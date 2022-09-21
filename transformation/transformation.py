from datetime import time, datetime
fecha = int(datetime.now().strftime("%Y"))
fecha_archive = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def edad_rango(row):
    if ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 0 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 20:
        row['edad_rango'] = '20 o menos'
    elif ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 21 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 25:
        row['edad_rango'] = '21 a 25'
    elif ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 26 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 30:
        row['edad_rango'] = '26 a 30'
    elif ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 31 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 40:
        row['edad_rango'] = '31 a 40'
    elif ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 41 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 50:
        row['edad_rango'] = '41 a 50'
    elif ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 51 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 60:
        row['edad_rango'] = '51 a 60'
    elif ((fecha - int(row['fecha_nacimiento'][0:4]))) >= 61 and ((fecha - int(row['fecha_nacimiento'][0:4]))) <= 130:
        row['edad_rango'] = '61 o más'
    else: row['edad_rango'] = None
    
    return row

def sexo(row):
    if row['sexo'] == 'M':
        row['sexo'] = 'Masculino'
    elif row['sexo'] == 'F':
        row['sexo'] = 'Femenino'
    else: row['sexo'] = None
        
    return row


def append_fajas(row):
    if ('faja_gral' in row) == False and ('faja_pos' in row) == False and ('faja_f1' in row) == False:
        row['faja_gral'] = None
        row['faja_pos'] = None
        row['faja_f1'] = None
    
    return row

def transform_fajas(row):
    if (row['faja_t0']) == '204':
            row['faja_t0'] = row['faja_gral']
    elif (row['faja_t0']) == '205':
            row['faja_t0'] = row['faja_pos']
    elif (row['faja_t0']) == '200':
            row['faja_t0'] = row['faja_f1'] 
    else: row['faja_t0'] = None
    
    return row

def append_morisades_af(row):
    if ('morosidades_af' in row) == False:
        row['morosidades_af'] = 0
        
    return row

def append_morisades_ot(row):
    if ('morosidades_ot' in row) == False:
        row['morosidades_ot'] = 0
        
    return row

def append_antecedentes(row):
    if ('antecedentes' in row) == False:
        row['antecedentes'] = 0
    
    return row

def recuperabilidad(row):
    if int(row['morosidades_af']) == 0 and int(row['morosidades_ot']) == 0 and int(row['antecedentes']) == 0:
        row['recuperabilidad'] = 1
    elif int(row['morosidades_af']) >= 0 and int(row['morosidades_ot']) == 0 and int(row['antecedentes']) == 0:
        row['recuperabilidad'] = 2
    elif int(row['morosidades_af']) >= 0 and int(row['morosidades_ot']) >= 0 and int(row['antecedentes']) == 0:
        row['recuperabilidad'] = 3
    elif int(row['morosidades_af']) >= 0 and int(row['morosidades_ot']) == 0 and int(row['antecedentes']) >= 0:
        row['recuperabilidad'] = 4
    elif int(row['morosidades_af']) >= 0 and int(row['morosidades_ot']) >= 0 and int(row['antecedentes']) >= 0:
        row['recuperabilidad'] = 5
    elif int(row['morosidades_af']) == 0 and int(row['morosidades_ot']) >= 0 and int(row['antecedentes']) == 0:
        row['recuperabilidad'] = 6
    elif int(row['morosidades_af']) == 0 and int(row['morosidades_ot']) == 0 and int(row['antecedentes']) >= 0:
        row['recuperabilidad'] = 7
    elif int(row['morosidades_af']) == 0 and int(row['morosidades_ot']) >= 0 and int(row['antecedentes']) >= 0:
        row['recuperabilidad'] = 8
    else: row['recuperabilidad'] = 0
        
    return row

def sexo_cc_lista(row):
    if row['sexo'] == 'Masculino':
        row['sexo'] = 'M'
    elif row['sexo'] == 'Femenino':
         row['sexo'] = 'F'
    else: row['sexo'] = 'sin_datos'
        
    return row

def none_edad_rango(row):
    if (row['edad_rango']) == None:
        row['fecha_nacimiento'] = 'sin_datos'
        row['edad'] = 'sin_datos'
        row['edad_rango'] = 'sin_datos'
        
    return row

def none_sexo(row):
    if (row['sexo']) == None:
        row['sexo'] = 'sin_datos'
        
    return row

def time_stamp(row):
    if ('time_stamp' in row) == False:
        row['time_stamp'] = fecha_archive
        
    return row

def append_edades(row):
    if ('value' in row) == False:
        row['sin_datos'] = '0'
        row['20_o_menos'] = '0'
        row['21_a_25'] = '0'
        row['26_a_30'] = '0'
        row['31_a_40'] = '0'
        row['41_a_50'] = '0'
        row['51_a_60'] = '0'
        row['61_o_más'] = '0'
        row['total'] = '0'
        
    return row

def change_edad_rango(row):
    if (row['edad_rango']) == None:
        row['sin_datos'] = '1'
    elif (row['edad_rango']) == '20 o menos':
          row['20_o_menos'] = '1'
    elif (row['edad_rango']) == '21 a 25':
          row['21_a_25'] = '1'
    elif (row['edad_rango']) == '26 a 30':
          row['26_a_30'] = '1'
    elif (row['edad_rango']) == '31 a 40':
          row['31_a_40'] = '1'
    elif (row['edad_rango']) == '41 a 50':
          row['41_a_50'] = '1'
    elif (row['edad_rango']) == '51 a 60':
          row['51_a_60'] = '1'
    elif (row['edad_rango']) == '61 o más':
          row['61_o_más'] = '1'
        
    return row

def transformation_edad_rango(row):
    if (row['sin_datos']) == '1':
        row['sin_datos'] = row['total']
    elif (row['20_o_menos']) == '1':
          row['20_o_menos'] = row['total']
    elif (row['21_a_25']) == '1':
          row['21_a_25'] = row['total']
    elif (row['26_a_30']) == '1':
          row['26_a_30'] = row['total']
    elif (row['31_a_40']) == '1':
          row['31_a_40'] = row['total']
    elif (row['41_a_50']) == '1':
          row['41_a_50'] = row['total']
    elif (row['51_a_60']) == '1':
          row['51_a_60'] = row['total']
    elif (row['61_o_más']) == '1':
          row['61_o_más'] = row['total']
        
    return row

def append_sexos(row):
    if ('values' in row) == False:
        row['sin_datos'] = '0'
        row['femenino'] = '0'
        row['masculino'] = '0'
        row['total'] = '0'
        
    return row

def change_sexos(row):
    if (row['sexo']) == None:
        row['sin_datos'] = '1'
    elif (row['sexo']) == 'Femenino':
          row['femenino'] = '1'
    elif (row['sexo']) == 'Masculino':
          row['masculino'] = '1'
        
    return row

def transformation_sexo(row):
    if (row['sin_datos']) == '1':
        row['sin_datos'] = row['total']
    elif (row['femenino']) == '1':
          row['femenino'] = row['total']
    elif (row['masculino']) == '1':
          row['masculino'] = row['total']
        
    return row