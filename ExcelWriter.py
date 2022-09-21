from google.cloud import storage
import pandas as pd
import io
from io import BytesIO
from datetime import time, datetime
import configure.configure as C

#TIME_START
time_start = datetime.now().strftime("%H:%M:%S")
print(f"Start {time_start}")

#CONFIGURE_GCP
client = storage.Client()
bucket_name = client.get_bucket(C.dataset_landing_id)
bucket = client.get_bucket(bucket_name)
filename = list(bucket.list_blobs(prefix=''))

print(f"Configure GCP Loaded")

#PATH_BLOB_CC_AFILIADOS_ALL
path_blob_cc_afiliados_all_atlas = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/atlas.csv")
path_blob_cc_afiliados_all_basa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/basa.csv")
path_blob_cc_afiliados_all_bbva = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/bbva.csv")
path_blob_cc_afiliados_all_bnf = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/bnf.csv")
path_blob_cc_afiliados_all_cefisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/cefisa.csv")
path_blob_cc_afiliados_all_continental = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/continental.csv")
path_blob_cc_afiliados_all_familiar = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/familiar.csv")
path_blob_cc_afiliados_all_fielco = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/fielco.csv")
path_blob_cc_afiliados_all_interfisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/interfisa.csv")
path_blob_cc_afiliados_all_itau = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/itau.csv")
path_blob_cc_afiliados_all_regional = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/regional.csv")
path_blob_cc_afiliados_all_rio = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/rio.csv")
path_blob_cc_afiliados_all_ueno = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/ueno.csv")
path_blob_cc_afiliados_all_vision = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_afiliados_all/vision.csv")

print(f"Path´s cc_afiliados_all Loaded")

#PATH_BLOB_CC_LISTA
path_blob_cc_lista_atlas = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/atlas.csv")
path_blob_cc_lista_basa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/basa.csv")
path_blob_cc_lista_bbva = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/bbva.csv")
path_blob_cc_lista_bnf = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/bnf.csv")
path_blob_cc_lista_cefisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/cefisa.csv")
path_blob_cc_lista_continental = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/continental.csv")
path_blob_cc_lista_fielco = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/fielco.csv")
path_blob_cc_lista_interfisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/interfisa.csv")
path_blob_cc_lista_regional = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/regional.csv")
path_blob_cc_lista_rio = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/rio.csv")
path_blob_cc_lista_ueno = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/ueno.csv")
path_blob_cc_lista_vision = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_lista/vision.csv")

print(f"Path´s cc_lista Loaded")

#PATH_BLOB_CC_FAJA_SEXO
path_blob_cc_faja_sexo_atlas = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/atlas.csv")
path_blob_cc_faja_sexo_basa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/basa.csv")
path_blob_cc_faja_sexo_bbva = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/bbva.csv")
path_blob_cc_faja_sexo_bnf = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/bnf.csv")
path_blob_cc_faja_sexo_cefisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/cefisa.csv")
path_blob_cc_faja_sexo_continental = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/continental.csv")
path_blob_cc_faja_sexo_familiar = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/familiar.csv")
path_blob_cc_faja_sexo_fielco = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/fielco.csv")
path_blob_cc_faja_sexo_interfisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/interfisa.csv")
path_blob_cc_faja_sexo_itau = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/itau.csv")
path_blob_cc_faja_sexo_regional = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/regional.csv")
path_blob_cc_faja_sexo_rio = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/rio.csv")
path_blob_cc_faja_sexo_ueno = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/ueno.csv")
path_blob_cc_faja_sexo_vision = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_sexo/vision.csv")

print(f"Path´s cc_faja_sexo Loaded")

#PATH_BLOB_CC_FAJA_EDAD
path_blob_cc_faja_edad_atlas = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/atlas.csv")
path_blob_cc_faja_edad_basa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/basa.csv")
path_blob_cc_faja_edad_bbva = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/bbva.csv")
path_blob_cc_faja_edad_bnf = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/bnf.csv")
path_blob_cc_faja_edad_cefisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/cefisa.csv")
path_blob_cc_faja_edad_continental = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/continental.csv")
path_blob_cc_faja_edad_familiar = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/familiar.csv")
path_blob_cc_faja_edad_fielco = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/fielco.csv")
path_blob_cc_faja_edad_interfisa = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/interfisa.csv")
path_blob_cc_faja_edad_itau = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/itau.csv")
path_blob_cc_faja_edad_regional = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/regional.csv")
path_blob_cc_faja_edad_rio = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/rio.csv")
path_blob_cc_faja_edad_ueno = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/ueno.csv")
path_blob_cc_faja_edad_vision = bucket.blob(f"Ignite_Portfolio_Insights/tmp/txt_{C.fecha}/cc_faja_edad/vision.csv")

print(f"Path´s cc_faja_edad Loaded")

#DATA_CC_AFILIADOS_ALL
data_cc_afiliados_all_atlas = path_blob_cc_afiliados_all_atlas.download_as_string()
data_cc_afiliados_all_basa = path_blob_cc_afiliados_all_basa.download_as_string()
data_cc_afiliados_all_bbva = path_blob_cc_afiliados_all_bbva.download_as_string()
data_cc_afiliados_all_bnf = path_blob_cc_afiliados_all_bnf.download_as_string()
data_cc_afiliados_all_cefisa = path_blob_cc_afiliados_all_cefisa.download_as_string()
data_cc_afiliados_all_continental = path_blob_cc_afiliados_all_continental.download_as_string()
data_cc_afiliados_all_familiar = path_blob_cc_afiliados_all_familiar.download_as_string()
data_cc_afiliados_all_fielco = path_blob_cc_afiliados_all_fielco.download_as_string()
data_cc_afiliados_all_interfisa = path_blob_cc_afiliados_all_interfisa.download_as_string()
data_cc_afiliados_all_itau = path_blob_cc_afiliados_all_itau.download_as_string()
data_cc_afiliados_all_regional = path_blob_cc_afiliados_all_regional.download_as_string()
data_cc_afiliados_all_rio = path_blob_cc_afiliados_all_rio.download_as_string()
data_cc_afiliados_all_ueno = path_blob_cc_afiliados_all_ueno.download_as_string()
data_cc_afiliados_all_vision = path_blob_cc_afiliados_all_vision.download_as_string()

print(f"data cc_afiliados_all Loaded")

#DATA_CC_LISTA
data_cc_lista_atlas = path_blob_cc_lista_atlas.download_as_string()
data_cc_lista_basa = path_blob_cc_lista_basa.download_as_string()
data_cc_lista_bbva = path_blob_cc_lista_bbva.download_as_string()
data_cc_lista_bnf = path_blob_cc_lista_bnf.download_as_string()
data_cc_lista_cefisa = path_blob_cc_lista_cefisa.download_as_string()
data_cc_lista_continental = path_blob_cc_lista_continental.download_as_string()
data_cc_lista_fielco = path_blob_cc_lista_fielco.download_as_string()
data_cc_lista_interfisa = path_blob_cc_lista_interfisa.download_as_string()
data_cc_lista_regional = path_blob_cc_lista_regional.download_as_string()
data_cc_lista_rio = path_blob_cc_lista_rio.download_as_string()
data_cc_lista_ueno = path_blob_cc_lista_ueno.download_as_string()
data_cc_lista_vision = path_blob_cc_lista_vision.download_as_string()

print(f"data cc_lista Loaded")

#DATA_CC_FAJA_SEXO
data_cc_faja_sexo_atlas = path_blob_cc_faja_sexo_atlas.download_as_string()
data_cc_faja_sexo_basa = path_blob_cc_faja_sexo_basa.download_as_string()
data_cc_faja_sexo_bbva = path_blob_cc_faja_sexo_bbva.download_as_string()
data_cc_faja_sexo_bnf = path_blob_cc_faja_sexo_bnf.download_as_string()
data_cc_faja_sexo_cefisa = path_blob_cc_faja_sexo_cefisa.download_as_string()
data_cc_faja_sexo_continental = path_blob_cc_faja_sexo_continental.download_as_string()
data_cc_faja_sexo_familiar = path_blob_cc_faja_sexo_familiar.download_as_string()
data_cc_faja_sexo_fielco = path_blob_cc_faja_sexo_fielco.download_as_string()
data_cc_faja_sexo_interfisa = path_blob_cc_faja_sexo_interfisa.download_as_string()
data_cc_faja_sexo_itau = path_blob_cc_faja_sexo_itau.download_as_string()
data_cc_faja_sexo_regional = path_blob_cc_faja_sexo_regional.download_as_string()
data_cc_faja_sexo_rio = path_blob_cc_faja_sexo_rio.download_as_string()
data_cc_faja_sexo_ueno = path_blob_cc_faja_sexo_ueno.download_as_string()
data_cc_faja_sexo_vision = path_blob_cc_faja_sexo_vision.download_as_string()

print(f"data cc_faja_sexo Loaded")

#DATA_CC_FAJA_EDAD
data_cc_faja_edad_atlas = path_blob_cc_faja_edad_atlas.download_as_string()
data_cc_faja_edad_basa = path_blob_cc_faja_edad_basa.download_as_string()
data_cc_faja_edad_bbva = path_blob_cc_faja_edad_bbva.download_as_string()
data_cc_faja_edad_bnf = path_blob_cc_faja_edad_bnf.download_as_string()
data_cc_faja_edad_cefisa = path_blob_cc_faja_edad_cefisa.download_as_string()
data_cc_faja_edad_continental = path_blob_cc_faja_edad_continental.download_as_string()
data_cc_faja_edad_familiar = path_blob_cc_faja_edad_familiar.download_as_string()
data_cc_faja_edad_fielco = path_blob_cc_faja_edad_fielco.download_as_string()
data_cc_faja_edad_interfisa = path_blob_cc_faja_edad_interfisa.download_as_string()
data_cc_faja_edad_itau = path_blob_cc_faja_edad_itau.download_as_string()
data_cc_faja_edad_regional = path_blob_cc_faja_edad_regional.download_as_string()
data_cc_faja_edad_rio = path_blob_cc_faja_edad_rio.download_as_string()
data_cc_faja_edad_ueno = path_blob_cc_faja_edad_ueno.download_as_string()
data_cc_faja_edad_vision = path_blob_cc_faja_edad_vision.download_as_string()

print(f"data cc_faja_edad Loaded")

#DF_CC_AFILIADOS_ALL
df_cc_afiliados_all_atlas = pd.read_csv(io.BytesIO(data_cc_afiliados_all_atlas), encoding='utf-8')
df_cc_afiliados_all_basa = pd.read_csv(io.BytesIO(data_cc_afiliados_all_basa), encoding='utf-8')
df_cc_afiliados_all_bbva = pd.read_csv(io.BytesIO(data_cc_afiliados_all_bbva), encoding='utf-8')
df_cc_afiliados_all_bnf = pd.read_csv(io.BytesIO(data_cc_afiliados_all_bnf), encoding='utf-8')
df_cc_afiliados_all_cefisa = pd.read_csv(io.BytesIO(data_cc_afiliados_all_cefisa), encoding='utf-8')
df_cc_afiliados_all_continental = pd.read_csv(io.BytesIO(data_cc_afiliados_all_continental), encoding='utf-8')
df_cc_afiliados_all_familiar = pd.read_csv(io.BytesIO(data_cc_afiliados_all_familiar), encoding='utf-8')
df_cc_afiliados_all_fielco = pd.read_csv(io.BytesIO(data_cc_afiliados_all_fielco), encoding='utf-8')
df_cc_afiliados_all_interfisa = pd.read_csv(io.BytesIO(data_cc_afiliados_all_interfisa), encoding='utf-8')
df_cc_afiliados_all_itau = pd.read_csv(io.BytesIO(data_cc_afiliados_all_itau), encoding='utf-8')
df_cc_afiliados_all_regional = pd.read_csv(io.BytesIO(data_cc_afiliados_all_regional), encoding='utf-8')
df_cc_afiliados_all_rio = pd.read_csv(io.BytesIO(data_cc_afiliados_all_rio), encoding='utf-8')
df_cc_afiliados_all_ueno = pd.read_csv(io.BytesIO(data_cc_afiliados_all_ueno), encoding='utf-8')
df_cc_afiliados_all_vision = pd.read_csv(io.BytesIO(data_cc_afiliados_all_vision), encoding='utf-8')

print(f"Data Frame cc_afiliados_all Created")

#DF_CC_LISTA
df_cc_lista_atlas = pd.read_csv(io.BytesIO(data_cc_lista_atlas), encoding='utf-8')
df_cc_lista_basa = pd.read_csv(io.BytesIO(data_cc_lista_basa), encoding='utf-8')
df_cc_lista_bbva = pd.read_csv(io.BytesIO(data_cc_lista_bbva), encoding='utf-8')
df_cc_lista_bnf = pd.read_csv(io.BytesIO(data_cc_lista_bnf), encoding='utf-8')
df_cc_lista_cefisa = pd.read_csv(io.BytesIO(data_cc_lista_cefisa), encoding='utf-8')
df_cc_lista_continental = pd.read_csv(io.BytesIO(data_cc_lista_continental), encoding='utf-8')
df_cc_lista_fielco = pd.read_csv(io.BytesIO(data_cc_lista_fielco), encoding='utf-8')
df_cc_lista_interfisa = pd.read_csv(io.BytesIO(data_cc_lista_interfisa), encoding='utf-8')
df_cc_lista_regional = pd.read_csv(io.BytesIO(data_cc_lista_regional), encoding='utf-8')
df_cc_lista_rio = pd.read_csv(io.BytesIO(data_cc_lista_rio), encoding='utf-8')
df_cc_lista_ueno = pd.read_csv(io.BytesIO(data_cc_lista_ueno), encoding='utf-8')
df_cc_lista_vision = pd.read_csv(io.BytesIO(data_cc_lista_vision), encoding='utf-8')

print(f"Data Frame cc_lista Created")

#DF_CC_FAJA_SEXO
df_cc_faja_sexo_atlas = pd.read_csv(io.BytesIO(data_cc_faja_sexo_atlas), encoding='utf-8')
df_cc_faja_sexo_basa = pd.read_csv(io.BytesIO(data_cc_faja_sexo_basa), encoding='utf-8')
df_cc_faja_sexo_bbva = pd.read_csv(io.BytesIO(data_cc_faja_sexo_bbva), encoding='utf-8')
df_cc_faja_sexo_bnf = pd.read_csv(io.BytesIO(data_cc_faja_sexo_bnf), encoding='utf-8')
df_cc_faja_sexo_cefisa = pd.read_csv(io.BytesIO(data_cc_faja_sexo_cefisa), encoding='utf-8')
df_cc_faja_sexo_continental = pd.read_csv(io.BytesIO(data_cc_faja_sexo_continental), encoding='utf-8')
df_cc_faja_sexo_familiar = pd.read_csv(io.BytesIO(data_cc_faja_sexo_familiar), encoding='utf-8')
df_cc_faja_sexo_fielco = pd.read_csv(io.BytesIO(data_cc_faja_sexo_fielco), encoding='utf-8')
df_cc_faja_sexo_interfisa = pd.read_csv(io.BytesIO(data_cc_faja_sexo_interfisa), encoding='utf-8')
df_cc_faja_sexo_itau = pd.read_csv(io.BytesIO(data_cc_faja_sexo_itau), encoding='utf-8')
df_cc_faja_sexo_regional = pd.read_csv(io.BytesIO(data_cc_faja_sexo_regional), encoding='utf-8')
df_cc_faja_sexo_rio = pd.read_csv(io.BytesIO(data_cc_faja_sexo_rio), encoding='utf-8')
df_cc_faja_sexo_ueno = pd.read_csv(io.BytesIO(data_cc_faja_sexo_ueno), encoding='utf-8')
df_cc_faja_sexo_vision = pd.read_csv(io.BytesIO(data_cc_faja_sexo_vision), encoding='utf-8')

print(f"Data Frame cc_faja_sexo Created")

#DF_CC_FAJA_EDAD
df_cc_faja_edad_atlas = pd.read_csv(io.BytesIO(data_cc_faja_edad_atlas), encoding='utf-8')
df_cc_faja_edad_basa = pd.read_csv(io.BytesIO(data_cc_faja_edad_basa), encoding='utf-8')
df_cc_faja_edad_bbva = pd.read_csv(io.BytesIO(data_cc_faja_edad_bbva), encoding='utf-8')
df_cc_faja_edad_bnf = pd.read_csv(io.BytesIO(data_cc_faja_edad_bnf), encoding='utf-8')
df_cc_faja_edad_cefisa = pd.read_csv(io.BytesIO(data_cc_faja_edad_cefisa), encoding='utf-8')
df_cc_faja_edad_continental = pd.read_csv(io.BytesIO(data_cc_faja_edad_continental), encoding='utf-8')
df_cc_faja_edad_familiar = pd.read_csv(io.BytesIO(data_cc_faja_edad_familiar), encoding='utf-8')
df_cc_faja_edad_fielco = pd.read_csv(io.BytesIO(data_cc_faja_edad_fielco), encoding='utf-8')
df_cc_faja_edad_interfisa = pd.read_csv(io.BytesIO(data_cc_faja_edad_interfisa), encoding='utf-8')
df_cc_faja_edad_itau = pd.read_csv(io.BytesIO(data_cc_faja_edad_itau), encoding='utf-8')
df_cc_faja_edad_regional = pd.read_csv(io.BytesIO(data_cc_faja_edad_regional), encoding='utf-8')
df_cc_faja_edad_rio = pd.read_csv(io.BytesIO(data_cc_faja_edad_rio), encoding='utf-8')
df_cc_faja_edad_ueno = pd.read_csv(io.BytesIO(data_cc_faja_edad_ueno), encoding='utf-8')
df_cc_faja_edad_vision = pd.read_csv(io.BytesIO(data_cc_faja_edad_vision), encoding='utf-8')

print(f"Data Frame cc_faja_edad Created")

#EXCEL_WRITTER
write_itau = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000052_ITAU_RSCC_{C.fecha}.xlsx')
write_familiar = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000044_FAMILIAR_RSCC_{C.fecha}.xlsx')
write_atlas = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000054_ATLAS_RSCC_{C.fecha}.xlsx')
write_fielco = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000026_FIELCO_RSCC_{C.fecha}.xlsx')
write_regional = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000046_REGIONAL_RSCC_{C.fecha}.xlsx')
write_rio = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000042_RIO_RSCC_{C.fecha}.xlsx')
write_vision = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000036_VISION_RSCC_{C.fecha}.xlsx')
write_interfisa = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000028_INTERFISA_RSCC_{C.fecha}.xlsx')
write_bnf = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000004_BNF_RSCC_{C.fecha}.xlsx')
write_cefisa = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000035_CEFISA_RSCC_{C.fecha}.xlsx')
write_basa = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000016_BASA_RSCC_{C.fecha}.xlsx')
write_bbva = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/006212_BBVA_RSCC_{C.fecha}.xlsx')
write_continental = pd.ExcelWriter(f'gs://dfa-dna-ws0020-la-prd-landing-zone/Ignite_Portfolio_Insights/out/xlxs_{C.fecha}/000002_CONTINENTAL_RSCC_{C.fecha}.xlsx')

print(f"ExcelWriter Created")

#EXCEL_ITAU
df_cc_afiliados_all_itau.to_excel(write_itau, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_itau.to_excel(write_itau, 'cc_faja_sexo', index=False)
df_cc_faja_edad_itau.to_excel(write_itau, 'cc_faja_edad', index=False)

print(f"Shetts Loaded To The ExcelWriter ITAU")

#EXCEL_FAMILIAR
df_cc_afiliados_all_familiar.to_excel(write_familiar, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_familiar.to_excel(write_familiar, 'cc_faja_sexo', index=False)
df_cc_faja_edad_familiar.to_excel(write_familiar, 'cc_faja_edad', index=False)

print(f"Shetts Loaded To The ExcelWriter FAMILIAR")

#EXCEL_ATLAS
df_cc_afiliados_all_atlas.to_excel(write_atlas, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_atlas.to_excel(write_atlas, 'cc_faja_sexo', index=False)
df_cc_faja_edad_atlas.to_excel(write_atlas, 'cc_faja_edad', index=False)
df_cc_lista_atlas.to_excel(write_atlas, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter ATLAS")

#EXCEL_FIELCO
df_cc_afiliados_all_fielco.to_excel(write_fielco, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_fielco.to_excel(write_fielco, 'cc_faja_sexo', index=False)
df_cc_faja_edad_fielco.to_excel(write_fielco, 'cc_faja_edad', index=False)
df_cc_lista_fielco.to_excel(write_fielco, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter FIELCO")

#EXCEL_REGIONAL
df_cc_afiliados_all_regional.to_excel(write_regional, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_regional.to_excel(write_regional, 'cc_faja_sexo', index=False)
df_cc_faja_edad_regional.to_excel(write_regional, 'cc_faja_edad', index=False)
df_cc_lista_regional.to_excel(write_regional, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter REGIONAL")

#EXCEL_RIO
df_cc_afiliados_all_rio.to_excel(write_rio, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_rio.to_excel(write_rio, 'cc_faja_sexo', index=False)
df_cc_faja_edad_rio.to_excel(write_rio, 'cc_faja_edad', index=False)
df_cc_lista_rio.to_excel(write_rio, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter RIO")

#EXCEL_VISION
df_cc_afiliados_all_vision.to_excel(write_vision, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_vision.to_excel(write_vision, 'cc_faja_sexo', index=False)
df_cc_faja_edad_vision.to_excel(write_vision, 'cc_faja_edad', index=False)
df_cc_lista_vision.to_excel(write_vision, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter VISION")

#EXCEL_INTERFISA
df_cc_afiliados_all_interfisa.to_excel(write_interfisa, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_interfisa.to_excel(write_interfisa, 'cc_faja_sexo', index=False)
df_cc_faja_edad_interfisa.to_excel(write_interfisa, 'cc_faja_edad', index=False)
df_cc_lista_interfisa.to_excel(write_interfisa, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter INTERFISA")

#EXCEL_BNF
df_cc_afiliados_all_bnf.to_excel(write_bnf, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_bnf.to_excel(write_bnf, 'cc_faja_sexo', index=False)
df_cc_faja_edad_bnf.to_excel(write_bnf, 'cc_faja_edad', index=False)
df_cc_lista_bnf.to_excel(write_bnf, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter BNF")

#EXCEL_CEFISA
df_cc_afiliados_all_cefisa.to_excel(write_cefisa, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_cefisa.to_excel(write_cefisa, 'cc_faja_sexo', index=False)
df_cc_faja_edad_cefisa.to_excel(write_cefisa, 'cc_faja_edad', index=False)
df_cc_lista_cefisa.to_excel(write_cefisa, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter CEFISA")

#EXCEL_BASA
df_cc_afiliados_all_basa.to_excel(write_basa, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_basa.to_excel(write_basa, 'cc_faja_sexo', index=False)
df_cc_faja_edad_basa.to_excel(write_basa, 'cc_faja_edad', index=False)
df_cc_lista_basa.to_excel(write_basa, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter BASA")

#EXCEL_BBVA
df_cc_afiliados_all_bbva.to_excel(write_bbva, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_bbva.to_excel(write_bbva, 'cc_faja_sexo', index=False)
df_cc_faja_edad_bbva.to_excel(write_bbva, 'cc_faja_edad', index=False)
df_cc_lista_bbva.to_excel(write_bbva, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter BBVA")

#EXCEL_CONTINENTAL
df_cc_afiliados_all_continental.to_excel(write_continental, 'cc_afiliados_all', index=False)
df_cc_faja_sexo_continental.to_excel(write_continental, 'cc_faja_sexo', index=False)
df_cc_faja_edad_continental.to_excel(write_continental, 'cc_faja_edad', index=False)
df_cc_lista_continental.to_excel(write_continental, 'cc_lista', index=False)

print(f"Shetts Loaded To The ExcelWriter CONTINENTAL")

#SAVE_CLOSE
write_itau.save(), write_itau.close()
print(f"Process Completed ITAU: Review Out Bucket")
write_familiar.save(), write_familiar.close()
print(f"Process Completed FAMILIAR: Review Out Bucket")
write_atlas.save(), write_atlas.close()
print(f"Process Completed ATLAS: Review Out Bucket")
write_fielco.save(), write_fielco.close()
print(f"Process Completed FIELCO: Review Out Bucket")
write_regional.save(), write_regional.close()
print(f"Process Completed REGIONAL: Review Out Bucket")
write_rio.save(), write_rio.close()
print(f"Process Completed RIO: Review Out Bucket")
write_vision.save(), write_vision.close()
print(f"Process Completed VISION: Review Out Bucket")
write_interfisa.save(), write_interfisa.close()
print(f"Process Completed INTERFISA: Review Out Bucket")
write_bnf.save(), write_bnf.close()
print(f"Process Completed BNF: Review Out Bucket")
write_cefisa.save(), write_cefisa.close()
print(f"Process Completed CEFISA: Review Out Bucket")
write_basa.save(), write_basa.close()
print(f"Process Completed BASA: Review Out Bucket")
write_bbva.save(), write_bbva.close()
print(f"Process Completed BBVA: Review Out Bucket")
write_continental.save(), write_continental.close()
print(f"Process Completed CONTINENTAL: Review Out Bucket")

#TIME_FINAL
time_final = datetime.now().strftime("%H:%M:%S")
print(f"Process Completed: Review Out Bucket {time_final}")