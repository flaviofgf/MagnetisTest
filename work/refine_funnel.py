from pyspark.sql import types

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

funnel.write_csv('../data/refined_zone/funnel.csv')
print('refined table funnel')
