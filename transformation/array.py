import apache_beam as beam

class extraccion_control_cartera(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['CONTROL_CARTERA']) > 0:
            for items in elements['CONTROL_CARTERA']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'scoring' : None if items['AFILIADO'] is None else items['AFILIADO']['scoring'],
                       'tipo_control': items['tipo_control']
                      }

class extraccion_demandas(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['DEMANDAS']) > 0:
            for items in elements['DEMANDAS']:
                yield {'informconf_id': elements['informconf_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_fniquito': items['fecha_fniquito'],
                       'fecha_ai' : items['fecha_ai'],
                       'situacion': items['situacion']
                      }
                
class extraccion_convocatorias(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['CONVOCATORIAS']) > 0:
            for items in elements['CONVOCATORIAS']:
                yield {'informconf_id': elements['informconf_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_sd': items['fecha_sd'],
                       'situacion': items['situacion']
                      }
                
class extraccion_quiebras(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['QUIEBRAS']) > 0:
            for items in elements['QUIEBRAS']:
                yield {'informconf_id': elements['informconf_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_sd': items['fecha_sd'],
                       'situacion': items['situacion']
                      }
                
class extraccion_remates(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['REMATES']) > 0:
            for items in elements['REMATES']:
                yield {'informconf_id': elements['informconf_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_finiquito': items['fecha_finiquito'],
                       'situacion': items['situacion']
                      }

class extraccion_inhibiciones(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['INHIBICIONES']) > 0:
            for items in elements['INHIBICIONES']:
                yield {'informconf_id': elements['informconf_id'],
                       'pers_tipo': elements['pers_tipo'],
                       'fecha_ai': items['fecha_ai'],
                       'fecha_fniquito': items['fecha_fniquito'],
                       'situacion': items['situacion']
                      }
                
class extraccion_inhabilitaciones(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['INHABILITACIONES']) > 0:
            for items in elements['INHABILITACIONES']:
                yield {'informconf_id': elements['informconf_id'],
                       'pers_tipo': elements['pers_tipo']
                      }
                
class extraccion_morosidades_ot_pub(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES']) > 0:
            for items in elements['MOROSIDADES']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal_m': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'afiliado_principal_c': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal_c_cartera'],
                       'pers_tipo': elements['pers_tipo']
                      }
class extraccion_morosidades_ot_ce30(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES_CE']) > 0:
            for items in elements['MOROSIDADES_CE']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal_m': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'afiliado_principal_c': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal_c_cartera'],
                       'pers_tipo': elements['pers_tipo'],
                       'servicio_o_producto': items['AFILIADO']['PRECIOS']['servicio_o_producto'],
                       'estado': items['AFILIADO']['PRECIOS']['estado']
                      }
                
class extraccion_morosidades_ot_ce30com(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES_COM']) > 0:
            for items in elements['MOROSIDADES_COM']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal_m': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'afiliado_principal_c': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal_c_cartera'],
                       'pers_tipo': elements['pers_tipo'],
                       'servicio_o_producto': items['AFILIADO']['PRECIOS']['servicio_o_producto'],
                       'estado': items['AFILIADO']['PRECIOS']['estado']
                      }

class extraccion_morosidades_af_pub(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES']) > 0:
            for items in elements['MOROSIDADES']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal_m': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'afiliado_principal_c': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal_c_cartera'],
                       'pers_tipo': elements['pers_tipo']
                      }
class extraccion_morosidades_af_ce30(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES_CE']) > 0:
            for items in elements['MOROSIDADES_CE']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal_m': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'afiliado_principal_c': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal_c_cartera'],
                       'pers_tipo': elements['pers_tipo'],
                       'servicio_o_producto': items['AFILIADO']['PRECIOS']['servicio_o_producto'],
                       'estado': items['AFILIADO']['PRECIOS']['estado']
                      }
                
class extraccion_morosidades_af_ce30com(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['MOROSIDADES_COM']) > 0:
            for items in elements['MOROSIDADES_COM']:
                yield {'informconf_id': elements['informconf_id'],
                       'afiliado_principal_m': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'afiliado_principal_c': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal_c_cartera'],
                       'pers_tipo': elements['pers_tipo'],
                       'servicio_o_producto': items['AFILIADO']['PRECIOS']['servicio_o_producto'],
                       'estado': items['AFILIADO']['PRECIOS']['estado']
                      }
                
class info_negativa(beam.DoFn):
    
    def process(self, elements):
        
        if len(elements['CONTROL_CARTERA']) > 0:
            for items in elements['CONTROL_CARTERA']:
                yield {'informconf_id': elements['informconf_id'],
                       'persona': elements['informconf_id'],
                       'afiliado_principal': None if items['AFILIADO'] is None else items['AFILIADO']['afiliado_principal'],
                       'documento': elements['pers_id'],
                       'tipo_doc_id': elements['tipo_doc_id'],
                       'primer_nombre': elements['PERSONA']['primer_nombre'],
                       'primer_apellido': elements['PERSONA']['primer_apellido'],
                       'sexo': elements['PERSONA']['sexo'],
                       'fecha_nacimiento': elements['PERSONA']['fecha_nacimiento'],
                       'edad': elements['PERSONA']['fecha_nacimiento'],
                       'edad_rango': elements['PERSONA']['fecha_nacimiento'],
                       't0': elements['t0'],
                       'faja_t0' : None if items['AFILIADO'] is None else items['AFILIADO']['scoring'],
                       'tn': elements['tn'],
                       'faja_tn': elements['faja_tn'],
                       'recuperabilidad': elements['recuperabilidad'],
                       'pers_tipo': elements['pers_tipo'],
                       'tipo_control': items['tipo_control']
                      }