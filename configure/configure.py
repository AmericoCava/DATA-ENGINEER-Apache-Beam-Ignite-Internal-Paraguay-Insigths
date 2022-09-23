from datetime import time, datetime
from apache_beam.io.gcp.internal.clients import bigquery as dfr
fecha = datetime.now().strftime("%Y-%m-%d")

##NAME WORKSPACE
project_id = 'dfa-dna-ws0020-la-prd-a1b1'
dataset_id = 'dfa_dna_ws0020_la_prd_sandbox'
dataset_landing_id = 'dfa-dna-ws0020-la-prd-landing-zone'

##NAME BUCKET
Ignite_Portfolio_Insights = "Ignite_Portfolio_Insights"
tmp = "tmp"
txt = "txt"
cc_afiliados_all = "cc_afiliados_all"
cc_faja_edad = "cc_faja_edad"
cc_faja_sexo = "cc_faja_sexo"
cc_lista = "cc_lista"

##TABLE NAME WRITTE GCP
table_id = 'py_efx_ignite_portfolio_insigth_cartera_db'

##NAME TABLES QUERY
tb_info_negativa = 'py_efx_info_negativa_weekly'
tb_score_faja = 'py_efx_score_faja'
tb_score_faja_plus = 'py_efx_score_faja_plus'

##FILTER QUERY
fecha_observacion = 20220808
fecha_archive = 20220723

##ATRIBUTES FOR DESIGN
tn = None
faja_tn = None

##FILTER ANTECEDENTES
tipo_control = 2
pers_tipo = 'P'
fecha_fniquito = None
fecha_ai = None
fecha_sd = None
situacion = 'F'
servicio_o_producto_ce30 = 54
servicio_o_producto_ce30com = 83
estado = 'A'

#NAME AFILIADOS
n_itau = 'itau'
n_familiar = 'familiar'
n_atlas = 'atlas'
n_fielco = 'fielco'
n_ueno = 'ueno'
n_regional = 'regional'
n_rio = 'rio'
n_vision = 'vision'
n_interfisa = 'interfisa'
n_bnf = 'bnf'
n_cefisa = 'cefisa'
n_basa = 'basa'
n_bbva = 'bbva'
n_continental = 'continental'

##FILTER AFILIADOS
itau = '12968'
familiar = '11157'
atlas = '13542'
fielco = '458'
ueno = '25869'
regional = '175'
rio = '24038'
vision = '10179'
interfisa = '19935'
bnf = '203'
cefisa = '18719'
basa = '22622'
bbva = '5457'
continental = '152'

##HEADER OUTPUT FILES
header_cc_afiliados_all_output = 'resultado,afiliado_principal,orden'
header_cc_lista_output = 'documento,primer_nombre,primer_apellido,sexo,edad,edad_rango,t0,faja,recuperabilidad,afiliado_principal'
header_cc_faja_edad_output = 'faja,afiliado_principal,sin_datos,20_o_menos,21_a_25,26_a_30,31_a_40,41_a_50,51_a_60,61_o_mas,total'
header_cc_faja_sexo_output = 'faja,afiliado_principal,sin_datos,femenino,masculino,total'

#VARIOS
dbase_temp = 'dfa_dna_ws0020_ignite_portfolio_insigth_beam_temp'
dataset_reference = dfr.DatasetReference(projectId=project_id, datasetId=dbase_temp)