from pyspark.sql import types
from pyspark.sql.functions import col

from etl.refiners import Refiner

print('Refining table user')
users = Refiner()

users.read_csv('../data/usuarios.csv')

users.cast_columns({
    'user_id':             types.StringType,
    'dados_pessoais':      types.StringType,
    'nivel_de_risco':      types.IntegerType,
    'objetivo':            types.StringType,
    'perfil_de_risco':     types.StringType,
    'fez_adicional':       types.BooleanType,
    'fez_resgate_parcial': types.BooleanType,
    'fez_resgate_total':   types.BooleanType,
    'poupanca':            (types.DecimalType, {'precision': 10, 'scale': 2}),
    'renda_fixa':          (types.DecimalType, {'precision': 10, 'scale': 2}),
    'renda_variavel':      (types.DecimalType, {'precision': 10, 'scale': 2}),
})

users.parse_json_columns({
    'dados_pessoais': types.StructType([
        types.StructField('genero', types.StringType(), True),
        types.StructField('estado_civil', types.StringType(), True),
        types.StructField('idade', types.StringType(), True),
    ]),
})

users.cast_columns({'idade': (types.IntegerType, {})})

users.remap_columns({
    'estado_civil': {
        'CASADO(A) COM BRASILEIRO(A) NATO(A)':         'CASADO(A)',
        'CASADO(A) COM BRASILEIRO(A) NATURALIZADO(A)': 'CASADO(A)',
        'UNIAO ESTAVEL':                               'CASADO(A)',
    }
})

print('Fill na')
users.df = users.df.fillna(0, ['poupanca', 'renda_fixa', 'renda_variavel'])

users.with_column(
        'flag_investidor_recorrente',
        col('fez_adicional') & ~ (col('fez_resgate_parcial') | col('fez_resgate_total'))
)

users.with_column(
        'investimentos_externos',
        col('poupanca') + col('renda_fixa') + col('renda_variavel')
)

users.write_csv('../data/refined_zone/users.csv')
print('refined table user')
