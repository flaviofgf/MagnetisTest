from pyspark.sql import Window
from pyspark.sql import types
from pyspark.sql.functions import *

from etl.refiners import Refiner

print('Refining table funnel')
funnel = Refiner()

funnel.read_csv('../data/funil.csv')

funnel.cast_columns({
    'user_id':        types.IntegerType,
    'timestamp':      types.TimestampType,
    'evento':         types.StringType,
    'valor_simulado': (types.DecimalType, {'precision': 10, 'scale': 2}),
})

funnel.with_column(
        'ordem_evento',
        rank().over(Window.partitionBy(col('user_id')).orderBy(col('timestamp')))
)

funnel.with_column(
        'primeiro_evento',
        1 == rank().over(Window.partitionBy(col('user_id')).orderBy(col('timestamp')))
)

funnel.with_column(
        'ordem_valor_simulado',
        rank().over(Window.partitionBy('user_id', col('valor_simulado').isNull()).orderBy(desc('timestamp')))
)

funnel.with_column(
        'primeiro_evento',
        col('ordem_evento') == 1
)

funnel.with_column(
        'ultimo_valor_simulado',
        col('valor_simulado').isNotNull() & (col('ordem_valor_simulado') == 1)
)

funnel.write_csv('../data/refined_zone/funnel.csv')
print('refined table funnel')
