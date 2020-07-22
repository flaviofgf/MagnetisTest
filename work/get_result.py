from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, first, last

from etl.refiners import Refiner

print('Getting result')
users, funnel = Refiner(), Refiner()

users.read_csv('../data/refined_zone/users.csv')

funnel.read_csv('../data/refined_zone/funnel.csv')

users.join(funnel.df.where('primeiro_evento is True').alias('f1'), ['user_id'], 'left')
users.join(funnel.df.where('ultimo_valor_simulado is True').alias('f2'), ['user_id'], 'left')

print('Select result')
users.df = users.df.select(
        col('user_id'), col('genero'), col('estado_civil'), col('idade'), col('nivel_de_risco'),
        col('objetivo'), col('perfil_de_risco'),
        date_format(col('f1.timestamp'), 'yyyy-MM').alias('homepage'),
        col('f2.valor_simulado').alias('valor_simulado'),
        col('flag_investidor_recorrente'), col('investimentos_externos')
).orderBy(col('user_id'))

users.write_csv('../data/result.csv')
print('Got result')
