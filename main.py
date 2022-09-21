import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import sys
import modules.txt as X
from datetime import datetime

class MisOpciones(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--fecha', help='Fecha de ejecucion', default=None, required=False)

def entrada(fecha):
    if fecha is None:
        return str(datetime.today().strftime('%Y-%m-%d'))
    return fecha
    
def proceso(pipeline_options, fecha):

    with beam.Pipeline(options=pipeline_options) as p:
            
        read_txt_writter = (p
        | 'read_txt_writter' >> X.txt_file(p))

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(sys.argv)
    pipeline_options = PipelineOptions(pipeline_args)
    
    args = MisOpciones()
    fecha = entrada(args.fecha)
    proceso(pipeline_options, fecha)