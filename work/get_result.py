from pyspark.sql import Window
from pyspark.sql.functions import col, date_format, first, last

from etl.refiners import Refiner

print('Getting result')
users, funnel = Refiner(), Refiner()

users.read_csv('../data/refined_zone/users.csv')

funnel.read_csv('../data/refined_zone/funnel.csv')

users.join(funnel.df, ['user_id'], 'left')

window = Window \
    .partitionBy(col('user_id')) \
    .orderBy(col('timestamp')) \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

print('Select result')
users.df = users.df.select(
        col('user_id'), col('genero'), col('estado_civil'), col('idade'), col('nivel_de_risco'),
        col('objetivo'), col('perfil_de_risco'),
        date_format(first(col('timestamp'), True).over(window), 'yyyy-MM').alias('homepage'),
        last(col('valor_simulado'), True).over(window).alias('valor_simulado'),
        col('flag_investidor_recorrente'), col('investimentos_externos')
).distinct()

users.write_csv('../data/result.csv')
print('Got result')
