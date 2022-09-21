import configure.configure as C

table_spec = f'{C.project_id}:{C.dataset_id}.{C.table_id}'
    
table_schema = 'informconf_id:STRING, \
                persona:STRING, \
                afiliado_principal:STRING, \
                documento:STRING, \
                tipo_doc_id:STRING, \
                primer_nombre:STRING, \
                primer_apellido:STRING, \
                sexo:STRING, \
                fecha_nacimiento:STRING, \
                edad:STRING, \
                edad_rango:STRING, \
                t0:STRING, \
                faja_t0:STRING, \
                tn:STRING, \
                faja_tn:STRING, \
                recuperabilidad:STRING, \
                time_stamp:TIMESTAMP'