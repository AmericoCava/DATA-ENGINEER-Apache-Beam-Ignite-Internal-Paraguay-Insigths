import apache_beam as beam
import query.query as Q
import configure.configure as C
import transformation.array as A
import transformation.concatenar as CT
import transformation.sum as S
import transformation.transformation as T
import transformation.nest as N
import util.util as U
import util.join as J
        
class cartera_db(beam.PTransform):
    
    def __init__(self, pipeline):
        self.p = pipeline
    
    def expand(self, pcolls):
    
        pcol_info_negativa = (self.p
        | "read_table" >> beam.io.ReadFromBigQuery(query=Q.sql_info_negativa(), use_standard_sql=True, temp_dataset=C.dataset_reference))
        
        pcol_cartera = (pcol_info_negativa
        | "extraction_cartera" >> beam.ParDo(A.extraccion_control_cartera())
        | "filter_cartera" >> beam.Filter(lambda element: element['tipo_control'] == C.tipo_control)
        | "key_value_cartera" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_demandas = (pcol_info_negativa
        | "extraction_convocatorias" >> beam.ParDo(A.extraccion_demandas())
        | "key_value_convocatorias" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_convocatorias = (pcol_info_negativa
        | "extraction_demandas" >> beam.ParDo(A.extraccion_convocatorias())
        | "key_value_demandas" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_quiebras = (pcol_info_negativa
        | "extraction_quiebras" >> beam.ParDo(A.extraccion_quiebras())
        | "key_value_quiebras" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_remates = (pcol_info_negativa
        | "extraction_remates" >> beam.ParDo(A.extraccion_remates())
        | "key_value_remates" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_inhibiciones = (pcol_info_negativa
        | "extraction_inhibiciones" >> beam.ParDo(A.extraccion_inhibiciones())
        | "key_value_inhibiciones" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_inhabilitaciones = (pcol_info_negativa
        | "extraction_inhabilitaciones" >> beam.ParDo(A.extraccion_inhabilitaciones())
        | "key_value_inhabilitaciones" >> beam.Map(U.map_to_pair, 'informconf_id'))
 
        join_demandas = ({'demandas': pcol_demandas, 'cartera': pcol_cartera}
        | "Inner_Join_Demandas-Cartera" >> J.InnerJoin('demandas', 'cartera', 'informconf_id'))
        
        join_convocatorias = ({'convocatorias': pcol_convocatorias, 'cartera': pcol_cartera}
        | "Inner_Join_convocatorias-Cartera" >> J.InnerJoin('convocatorias', 'cartera', 'informconf_id'))
        
        join_quiebras = ({'quiebras': pcol_quiebras, 'cartera': pcol_cartera}
        | "Inner_Join_quiebras-Cartera" >> J.InnerJoin('quiebras', 'cartera', 'informconf_id'))
        
        join_remates = ({'remates ': pcol_remates , 'cartera': pcol_cartera}
        | "Inner_Join_remates-Cartera" >> J.InnerJoin('remates ', 'cartera', 'informconf_id'))
        
        join_inhibiciones = ({'inhibiciones ': pcol_inhibiciones , 'cartera': pcol_cartera}
        | "Inner_Join_inhibiciones-Cartera" >> J.InnerJoin('inhibiciones ', 'cartera', 'informconf_id'))
        
        join_inhabilitaciones = ({'inhabilitaciones ': pcol_inhabilitaciones , 'cartera': pcol_cartera}
        | "Inner_Join_inhabilitaciones-Cartera" >> J.InnerJoin('inhabilitaciones ', 'cartera', 'informconf_id'))
        
        demandas = (join_demandas
        | "filter_demandas" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo 
                                           and element['fecha_fniquito'] is C.fecha_fniquito 
                                           and element['fecha_ai'] is C.fecha_ai 
                                           and element['situacion'] != C.situacion)
        | "transformation_demandas" >> beam.Map(CT.modulos_conca)
        | "Sum_demandas" >> beam.CombinePerKey(sum))
        
        convocatorias = (join_convocatorias
        | "filter_convocatorias" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo 
                                                and element['fecha_sd'] is C.fecha_sd 
                                                and element['situacion'] != C.situacion)
        | "transformation_convocatorias" >> beam.Map(CT.modulos_conca)
        | "Sum_convocatorias" >> beam.CombinePerKey(sum))
        
        quiebras = (join_quiebras
        | "filter_quiebras" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo 
                                           and element['fecha_sd'] is C.fecha_sd 
                                           and element['situacion'] != C.situacion)
        | "transformation_quiebras" >> beam.Map(CT.modulos_conca) 
        | "Sum quiebras" >> beam.CombinePerKey(sum))
        
        remates = (join_remates
        | "filter_remates" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo 
                                          and element['fecha_finiquito'] is C.fecha_fniquito 
                                          and element['situacion'] != C.situacion)
        | "transformation_remates" >> beam.Map(CT.modulos_conca)
        | "Sum_remates" >> beam.CombinePerKey(sum))
        
        inhibiciones = (join_inhibiciones
        | "filter_inhibiciones" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo 
                                               and element['fecha_ai'] is C.fecha_ai 
                                               and element['fecha_fniquito'] is C.fecha_fniquito 
                                               and element['situacion'] != C.situacion)
        | "transformation_inhibiciones" >> beam.Map(CT.modulos_conca)
        | "Sum_inhibiciones" >> beam.CombinePerKey(sum))
        
        inhabilitaciones = (join_inhabilitaciones
        | "filter_inhabilitaciones" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo)
        | "transformation_inhabilitaciones" >> beam.Map(CT.modulos_conca)
        | "Sum_inhabilitaciones" >> beam.CombinePerKey(sum))
        
        antecedentes = ( 
        {'demandas': demandas, 'convocatorias': convocatorias, 'quiebras': quiebras, 'remates': remates, 'inhibiciones': inhibiciones, 'inhabilitaciones': inhabilitaciones}
        | "antecedentes_union" >> beam.CoGroupByKey()
        | "nest_antecedentes" >>  beam.Map(N.nest_antecedentes)
        | "key_value_map_antecedentes" >> beam.Map(U.map_to_pair,'informconf_id'))
        
        pcol_morosidades_ot_pub = (pcol_info_negativa
        | "extraction_morosidades_ot_pub" >> beam.ParDo(A.extraccion_morosidades_ot_pub())
        | "filter_pcol_morosidades_ot_pub" >> beam.Filter(lambda element: element['afiliado_principal_m'] != element['afiliado_principal_c'])                           
        | "key_value_morosidades_ot_pub" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_morosidades_ot_ce30 = (pcol_info_negativa
        | "extraction_morosidades_ot_ce30" >> beam.ParDo(A.extraccion_morosidades_ot_ce30())
        | "filter_morosidades_ot_ce30" >> beam.Filter(lambda element: element['servicio_o_producto'] == C.servicio_o_producto_ce30
                                                      and element['estado'] is C.estado
                                                      and element['afiliado_principal_m'] != element['afiliado_principal_c'])
        | "key_value_morosidades_ce30" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_morosidades_ot_ce30com = (pcol_info_negativa
        | "extraction_morosidades_ot_ce30com" >> beam.ParDo(A.extraccion_morosidades_ot_ce30com())
        | "filter_morosidades_ot_ce30com" >> beam.Filter(lambda element: element['servicio_o_producto'] == C.servicio_o_producto_ce30com
                                                         and element['estado'] is C.estado
                                                         and element['afiliado_principal_m'] != element['afiliado_principal_c'])
        | "key_value_morosidades_ot_ce30com" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        join_morosidades_ot_pub = ({'morosidades_ot_pub': pcol_morosidades_ot_pub, 'cartera': pcol_cartera}
        | "Inner_Join_morosidades_ot_pub-Cartera" >> J.InnerJoin('morosidades_ot_pub', 'cartera', 'informconf_id')
        | "transformation_morosidades_ot_pub" >> beam.Map(CT.modulos_conca)
        | "Sum_morosidades_ot_pub" >> beam.CombinePerKey(sum))
        
        join_morosidades_ot_ce30 = ({'morosidades_ot_ce30': pcol_morosidades_ot_ce30, 'cartera': pcol_cartera}
        | "Inner_Join_morosidades_ot_ce30-Cartera" >> J.InnerJoin('morosidades_ot_ce30', 'cartera', 'informconf_id')
        | "transformation_morosidades_ot_ce30" >> beam.Map(CT.modulos_conca)
        | "Sum_morosidades_ot_ce30" >> beam.CombinePerKey(sum))
        
        join_morosidades_ot_ce30com = ({'morosidades_ot_ce30com': pcol_morosidades_ot_ce30com, 'cartera': pcol_cartera}
        | "Inner_Join_morosidades_ot_ce30com-Cartera" >> J.InnerJoin('morosidades_ot_ce30com', 'cartera', 'informconf_id')
        | "transformation_morosidades_ot_ce30com" >> beam.Map(CT.modulos_conca)
        | "Sum_morosidades_ot_ce30com" >> beam.CombinePerKey(sum))
        
        morosidades_ot = ( 
        {'morosidades_ot_pub': join_morosidades_ot_pub, 'morosidades_ot_ce30': join_morosidades_ot_ce30, 'morosidades_ot_ce30com': join_morosidades_ot_ce30com}
        | "morosidades_ot_union" >> beam.CoGroupByKey()
        | "nest_morosidades_ot" >>  beam.Map(N.nest_morosidades_ot)
        | "key_value_map_morosidades_ot" >> beam.Map(U.map_to_pair,'informconf_id'))
        
        pcol_morosidades_af_pub = (pcol_info_negativa
        | "extraction_morosidades_af_pub" >> beam.ParDo(A.extraccion_morosidades_af_pub())
        | "filter_pcol_morosidades_af_pub" >> beam.Filter(lambda element: element['afiliado_principal_m'] == element['afiliado_principal_c'])
        | "key_morosidades_af_pub" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_morosidades_af_ce30 = (pcol_info_negativa
        | "extraction_morosidades_af_ce30" >> beam.ParDo(A.extraccion_morosidades_af_ce30())
        | "filter_morosidades_af_ce30" >> beam.Filter(lambda element: element['servicio_o_producto'] == C.servicio_o_producto_ce30
                                                      and element['estado'] is C.estado
                                                      and element['afiliado_principal_m'] == element['afiliado_principal_c'])
        | "key_value_morosidades_af_ce30" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        pcol_morosidades_af_ce30com = (pcol_info_negativa
        | "extraction_morosidades_af_ce30com" >> beam.ParDo(A.extraccion_morosidades_af_ce30com())
        | "filter_morosidades_af_ce30com" >> beam.Filter(lambda element: element['servicio_o_producto'] == C.servicio_o_producto_ce30com
                                                         and element['estado'] is C.estado
                                                         and element['afiliado_principal_m'] == element['afiliado_principal_c'])
        | "key_value_morosidades_af_ce30com" >> beam.Map(U.map_to_pair, 'informconf_id'))
        
        join_morosidades_af_pub = ({'morosidades_af_pub': pcol_morosidades_af_pub, 'cartera': pcol_cartera}
        | "Inner_Join_morosidades_af_pub-Cartera" >> J.InnerJoin('morosidades_af_pub', 'cartera', 'informconf_id')
        | "transformation_morosidades_af_pub" >> beam.Map(CT.modulos_conca)
        | "Sum_morosidades_af_pub" >> beam.CombinePerKey(sum))
        
        join_morosidades_af_ce30 = ({'morosidades_af_ce30': pcol_morosidades_af_ce30, 'cartera': pcol_cartera}
        | "Inner_Join_morosidades_af_ce30-Cartera" >> J.InnerJoin('morosidades_af_ce30', 'cartera', 'informconf_id')
        | "transformation_morosidades_af_ce30" >> beam.Map(CT.modulos_conca)
        | "Sum_morosidades_af_ce30" >> beam.CombinePerKey(sum))
        
        join_morosidades_af_ce30com = ({'morosidades_af_ce30com': pcol_morosidades_af_ce30com, 'cartera': pcol_cartera}
        | "Inner_Join_morosidades_af_ce30com-Cartera" >> J.InnerJoin('morosidades_af_ce30com', 'cartera', 'informconf_id')
        | "transformation_morosidades_af_ce30com" >> beam.Map(CT.modulos_conca)
        | "Sum_morosidades_af_ce30com" >> beam.CombinePerKey(sum))
        
        morosidades_af = ( 
        {'morosidades_af_pub': join_morosidades_af_pub, 'morosidades_af_ce30': join_morosidades_af_ce30, 'morosidades_af_ce30com': join_morosidades_af_ce30com}
        | "morosidades_af_union" >> beam.CoGroupByKey()
        | "nest_morosidades_af" >>  beam.Map(N.nest_morosidades_af)
        | "key_value_map_morosidades_af" >> beam.Map(U.map_to_pair,'informconf_id'))
        
        info_negativa = (pcol_info_negativa
        | "extraction_info_negativa" >> beam.ParDo(A.info_negativa())
        | "filter_info_negativa" >> beam.Filter(lambda element: element['tipo_control'] == C.tipo_control)
        | "nest_info_negativa" >>  beam.Map(N.nest_info_negativa)
        | "key_value_map_info_negativa" >> beam.Map(U.map_to_pair,'informconf_id'))               
        
        fajas = (self.p
        | "read_fajas" >> beam.io.ReadFromBigQuery(query=Q.sql_fajas(), use_standard_sql=True, temp_dataset=C.dataset_reference)
        | "fajas_key_value_map" >> beam.Map(U.map_to_pair,'informconf_id'))
        
        info_negativa_fajas = ({'info_negativa': info_negativa, 'fajas': fajas} 
        | "info_negativa_fajas_Join" >> J.LeftJoin('info_negativa', 'fajas', 'informconf_id')
        | "filter_info_negativa_fajas" >> beam.Filter(lambda element: element['pers_tipo'] == C.pers_tipo)
        | "nest_info_negativa_fajas" >>  beam.Map(N.nest_info_negativa_fajas)
        | "info_negativa_fajas_key_value_map" >> beam.Map(U.map_to_pair,'informconf_id'))
        
        info_negativa_join_morosidades_af = ({'info_negativa': info_negativa_fajas, 'morosidades_af': morosidades_af} 
        | "Join_1" >> J.LeftJoin('info_negativa', 'morosidades_af', 'informconf_id')
        | "join_1_key_value_map" >> beam.Map(U.map_to_pair,'informconf_id'))
        
        info_negativa_join_morosidades_ot = ({'info_negativa_join_morosidades_af': info_negativa_join_morosidades_af, 'morosidades_ot': morosidades_ot} 
        | "Join_2" >> J.LeftJoin('info_negativa_join_morosidades_af', 'morosidades_ot', 'informconf_id')
        | "join_2_key_value_map" >> beam.Map(U.map_to_pair,'informconf_id')) 
        
        info_negativa_join_antecedentes = ({'info_negativa_join_morosidades_ot': info_negativa_join_morosidades_ot, 'antecedentes': antecedentes} 
        | "Join_3" >> J.LeftJoin('info_negativa_join_morosidades_ot', 'antecedentes', 'informconf_id')
        | "nest_info_negativa_join_antecedentes" >>  beam.Map(N.nest_info_negativa_join_antecedentes))
        
        return info_negativa_join_antecedentes