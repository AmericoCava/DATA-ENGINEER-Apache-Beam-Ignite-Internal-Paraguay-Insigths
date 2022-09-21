import apache_beam as beam
import configure.configure as C
import modules.cartera_db as A
import transformation.sum as S
import transformation.concatenar as CT
import transformation.nest as N
import transformation.transformation as T
import query.schema as SC
import util.util as U
        
class txt_file(beam.PTransform):
    
    def __init__(self, pipeline):
        self.p = pipeline
    
    def expand(self, pcolls):
        
        table = SC.table_spec
        
        read_antecedentes = (self.p
        | 'read_antecedentes' >> A.cartera_db(self.p))
        
        cc_lista = (read_antecedentes
        | "nest_cc_lista" >>  beam.Map(N.nest_cc_lista))
        
        aux_1 = (read_antecedentes
        | "filter_aux_1" >> beam.Filter(lambda x: x['informconf_id'] == 0))
        
        aux_2 = (read_antecedentes
        | "filter_aux_primera_linea" >> beam.Map(CT.aux_2_conca)
        | "Sum_primera_linea" >> beam.CombinePerKey(sum))
        
        primera_linea = ({'primera_linea': aux_2, 'aux': aux_1}
        | "union_primera_linea" >> beam.CoGroupByKey()
        | "nest_primera_linea" >>  beam.Map(N.nest_primera_linea))
        
        segunda_linea = (read_antecedentes
        | "nest_segunda_linea" >>  beam.Map(N.nest_segunda_linea))
        
        cc_afiliados_all = ([primera_linea, segunda_linea]
        | "flatten" >> beam.Flatten())
        
        rango_edad = (read_antecedentes
        | "nest_rango_edad" >>  beam.Map(N.nest_rango_edad))
        
        faja_edad = ({'rango_edad': rango_edad, 'aux': aux_1}
        | "union_faja_edad" >> beam.CoGroupByKey()
        | "nest_faja_edad" >>  beam.Map(N.nest_faja_edad))
        
        rango_sexo = (read_antecedentes
        | "nest_rango_sexo" >>  beam.Map(N.nest_rango_sexo))
        
        faja_sexo = ({'rango_sexo': rango_sexo, 'aux': aux_1}
        | "union_faja_sexo" >> beam.CoGroupByKey()
        | "nest_faja_sexo" >>  beam.Map(N.nest_faja_sexo))
        
        cartera_db = (read_antecedentes
        | "nest_cartera_db" >>  beam.Map(N.nest_cartera_db)
        | 'write_table_GCP' >> beam.io.WriteToBigQuery(
            table,
            schema=SC.table_schema,
            additional_bq_parameters={'timePartitioning': {'type': 'MONTH', 'field':'time_stamp'}}))
        
        itau_cc_afiliados_all = (cc_afiliados_all
        | "filter_itau_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.itau)
        | 'get_values_itau_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_itau_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_itau_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_itau}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        familiar_cc_afiliados_all = (cc_afiliados_all
        | "filter_familiar_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.familiar)
        | 'get_values_familiar_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_familiar_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_familiar_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_familiar}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        atlas_cc_afiliados_all = (cc_afiliados_all
        | "filter_atlas_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.atlas)
        | 'get_values_atlas_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_atlas_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_atlas_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_atlas}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        fielco_cc_afiliados_all = (cc_afiliados_all
        | "filter_fielco _cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.fielco)
        | 'get_values_fielco_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_fielco_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_fielco_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_fielco}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        ueno_cc_afiliados_all = (cc_afiliados_all
        | "filter_ueno_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.ueno)
        | 'get_values_ueno_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_ueno_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_ueno_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_ueno}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        regional_cc_afiliados_all = (cc_afiliados_all
        | "filter_regional_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.regional)
        | 'get_values_regional_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_regional_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_regional_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_regional}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        rio_cc_afiliados_all = (cc_afiliados_all
        | "filter_rio_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.rio)
        | 'get_values_rio_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_rio_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_rio_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_rio}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        vision_cc_afiliados_all = (cc_afiliados_all
        | "filter_vision_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.vision)
        | 'get_values_vision_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_vision_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_vision_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_vision}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        interfisa_cc_afiliados_all = (cc_afiliados_all
        | "filter_interfisa_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.interfisa)
        | 'get_values_interfisa_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_interfisa_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_interfisa_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_interfisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        bnf_cc_afiliados_all = (cc_afiliados_all
        | "filter_bnf_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bnf)
        | 'get_values_bnf_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bnf_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bnf_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_bnf}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        cefisa_cc_afiliados_all = (cc_afiliados_all
        | "filter_cefisa_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.cefisa)
        | 'get_values_cefisa_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_cefisa_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_cefisa_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_cefisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        basa_cc_afiliados_all = (cc_afiliados_all
        | "filter_basa_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.basa)
        | 'get_values_basa_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_basa_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_basa_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_basa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        bbva_cc_afiliados_all = (cc_afiliados_all
        | "filter_bbva_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bbva)
        | 'get_values_bbva_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bbva_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bbva_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_bbva}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        continental_cc_afiliados_all = (cc_afiliados_all
        | "filter_continental_cc_afiliados_all" >> beam.Filter(lambda element: element['afiliado_principal'] == C.continental)
        | 'get_values_continental_cc_afiliados_all' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_continental_cc_afiliados_all' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_continental_cc_afiliados_all' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_afiliados_all}/{C.n_continental}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_afiliados_all_output)))
        
        atlas_cc_lista = (cc_lista
        | "filter_atlas_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.atlas)
        | 'get_values_atlas_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_atlas_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_atlas_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_atlas}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        fielco_cc_lista = (cc_lista
        | "filter_fielco_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.fielco)
        | 'get_values_fielco_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_fielco_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_fielco_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_fielco}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        ueno_cc_lista = (cc_lista
        | "filter_ueno_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.ueno)
        | 'get_values_ueno_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_ueno_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_ueno_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_ueno}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        regional_cc_lista = (cc_lista
        | "filter_regional_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.regional)
        | 'get_values_regional_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_regional_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_regional_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_regional}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        rio_cc_lista = (cc_lista
        | "filter_rio_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.rio)
        | 'get_values_rio_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_rio_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_rio_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_rio}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        vision_cc_lista = (cc_lista
        | "filter_vision_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.vision)
        | 'get_values_vision_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_vision_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_vision_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_vision}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        interfisa_cc_lista = (cc_lista
        | "filter_interfisa_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.interfisa)
        | 'get_values_interfisa_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_interfisa_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_interfisa_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_interfisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        bnf_cc_lista = (cc_lista
        | "filter_bnf_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bnf)
        | 'get_values_bnf_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bnf_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bnf_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_bnf}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        cefisa_cc_lista = (cc_lista
        | "filter_cefisa_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.cefisa)
        | 'get_values_cefisa_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_cefisa_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_cefisa_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_cefisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        basa_cc_lista = (cc_lista
        | "filter_basa_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.basa)
        | 'get_values_basa_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_basa_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_basa_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_basa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        bbva_cc_lista = (cc_lista
        | "filter_bbva_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bbva)
        | 'get_values_bbva_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bbva_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bbva_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_bbva}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        continental_cc_lista = (cc_lista
        | "filter_continental_cc_lista" >> beam.Filter(lambda element: element['afiliado_principal'] == C.continental)
        | 'get_values_continental_cc_lista' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_continental_cc_lista' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_continental_cc_lista' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_lista}/{C.n_continental}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_lista_output)))
        
        itau_cc_faja_edad = (faja_edad
        | "filter_itau_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.itau)
        | 'get_values_itau_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_itau_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_itau_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_itau}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        familiar_cc_faja_edad = (faja_edad
        | "filter_familiar_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.familiar)
        | 'get_values_familiar_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_familiar_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_familiar_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_familiar}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        atlas_cc_faja_edad = (faja_edad
        | "filter_atlas_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.atlas)
        | 'get_values_atlas_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_atlas_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_atlas_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_atlas}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        fielco_cc_faja_edad = (faja_edad
        | "filter_fielco_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.fielco)
        | 'get_values_fielco_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_fielco_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_fielco_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_fielco}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        ueno_cc_faja_edad = (faja_edad
        | "filter_ueno_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.ueno)
        | 'get_values_ueno_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_ueno_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_ueno_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_ueno}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        regional_cc_faja_edad = (faja_edad
        | "filter_regional_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.regional)
        | 'get_values_regional_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_regional_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_regional_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_regional}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        rio_cc_faja_edad = (faja_edad
        | "filter_rio_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.rio)
        | 'get_values_rio_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_rio_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_rio_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_rio}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        vision_cc_faja_edad = (faja_edad
        | "filter_vision_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.vision)
        | 'get_values_vision_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_vision_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_vision_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_vision}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        interfisa_cc_faja_edad = (faja_edad
        | "filter_interfisa_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.interfisa)
        | 'get_values_interfisa_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_interfisa_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_interfisa_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_interfisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        bnf_cc_faja_edad = (faja_edad
        | "filter_bnf_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bnf)
        | 'get_values_bnf_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bnf_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bnf_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_bnf}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        cefisa_cc_faja_edad = (faja_edad
        | "filter_cefisa_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.cefisa)
        | 'get_values_cefisa_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_cefisa_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_cefisa_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_cefisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        basa_cc_faja_edad = (faja_edad
        | "filter_basa_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.basa)
        | 'get_values_basa_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_basa_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_basa_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_basa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        bbva_cc_faja_edad = (faja_edad
        | "filter_bbva_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bbva)
        | 'get_values_bbva_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bbva_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bbva_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_bbva}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        continental_cc_faja_edad = (faja_edad
        | "filter_continental_cc_faja_edad" >> beam.Filter(lambda element: element['afiliado_principal'] == C.continental)
        | 'get_values_continental_cc_faja_edad' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_continental_cc_faja_edad' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_continental_cc_faja_edad' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_edad}/{C.n_continental}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_edad_output)))
        
        itau_cc_faja_sexo = (faja_sexo
        | "filter_itau_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.itau)
        | 'get_values_itau_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_itau_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_itau_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_itau}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        familiar_cc_faja_sexo = (faja_sexo
        | "filter_familiar_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.familiar)
        | 'get_values_familiar_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_familiar_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_familiar_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_familiar}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        atlas_cc_faja_sexo = (faja_sexo
        | "filter_atlas_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.atlas)
        | 'get_values_atlas_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_atlas_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_atlas_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_atlas}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        fielco_cc_faja_sexo = (faja_sexo
        | "filter_fielco_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.fielco)
        | 'get_values_fielco_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_fielco_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_fielco_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_fielco}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        ueno_cc_faja_sexo = (faja_sexo
        | "filter_ueno_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.ueno)
        | 'get_values_ueno_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_ueno_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_ueno_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_ueno}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        regional_cc_faja_sexo = (faja_sexo
        | "filter_regional_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.regional)
        | 'get_values_regional_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_regional_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_regional_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_regional}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        rio_cc_faja_sexo = (faja_sexo
        | "filter_rio_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.rio)
        | 'get_values_rio_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_rio_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_rio_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_rio}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        vision_cc_faja_sexo = (faja_sexo
        | "filter_vision_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.vision)
        | 'get_values_vision_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_vision_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_vision_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_vision}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        interfisa_cc_faja_sexo = (faja_sexo
        | "filter_interfisa_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.interfisa)
        | 'get_values_interfisa_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_interfisa_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_interfisa_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_interfisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        bnf_cc_faja_sexo = (faja_sexo
        | "filter_bnf_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bnf)
        | 'get_values_bnf_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bnf_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bnf_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_bnf}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        cefisa_cc_faja_sexo = (faja_sexo
        | "filter_cefisa_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.cefisa)
        | 'get_values_cefisa_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_cefisa_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_cefisa_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_cefisa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        basa_cc_faja_sexo = (faja_sexo
        | "filter_basa_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.basa)
        | 'get_values_basa_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_basa_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_basa_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_basa}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        bbva_cc_faja_sexo = (faja_sexo
        | "filter_bbva_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.bbva)
        | 'get_values_bbva_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_bbva_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_bbva_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_bbva}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
        
        continental_cc_faja_sexo = (faja_sexo
        | "filter_continental_cc_faja_sexo" >> beam.Filter(lambda element: element['afiliado_principal'] == C.continental)
        | 'get_values_continental_cc_faja_sexo' >> beam.Map(lambda x: list(x.values()))
        | 'CSV_format_continental_cc_faja_sexo' >> beam.Map(lambda row: ', '.join(['"'+ str(column) +'"' for column in row]))
        | 'Write_to_CSV_continental_cc_faja_sexo' >> beam.io.WriteToText(
            f"gs://{C.dataset_landing_id}/{C.Ignite_Portfolio_Insights}/{C.tmp}/{C.txt}_{C.fecha}/{C.cc_faja_sexo}/{C.n_continental}".format(C.dataset_landing_id),
            file_name_suffix='.csv',
            num_shards=1, 
            shard_name_template='',
            header=(C.header_cc_faja_sexo_output)))
            
        return continental_cc_faja_sexo