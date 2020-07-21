from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql import DataFrame


class Refiner(object):
    spark = SparkSession.builder.appName('etl').getOrCreate()
    df: DataFrame = None
    
    def read_csv(self, path, header=True, infer_schema=True, encoding='utf-8', sep=';'):
        print('Read csv:', path)
        
        self.df = self.spark.read.csv(path, header=header, inferSchema=infer_schema, encoding=encoding, sep=sep)
    
    def write_csv(self, path, header=True, sep=';'):
        print('Write csv:', path)
        
        self.df.toPandas().to_csv(path, header=header, sep=sep)
    
    def cast_columns(self, columns: dict):
        print(f'Cast {len(columns)} column(s)')
        
        for name, schema in columns.items():
            schema_kwargs = {}
            
            if isinstance(schema, tuple):
                schema_kwargs = schema[1]
                schema = schema[0]
            
            self.df = self.df.withColumn(name, self.df[name].cast(schema(**schema_kwargs)))
    
    def parse_json_columns(self, columns: dict):
        print(f'Parse {len(columns)} json columns(s)')
        
        for name, schema in columns.items():
            self.df = self.df.withColumn(name, from_json(name, schema)) \
                .select(
                    *[col(col_name) for col_name in self.df.columns if not col_name == name],
                    col(f'{name}.*')
            )
    
    def remap_columns(self, columns):
        print(f'Remap {len(columns)} columns(s)')
        
        for name, mapping in columns.items():
            self.df = self.df.replace(to_replace=mapping, subset=[name])
    
    def join(self, other: DataFrame, on: list, how: str = 'left'):
        print('Join tables')
        
        self.df = self.df.join(other, on=on, how=how)

    def with_column(self, name, value):
        print(f'Add or update {name} column')
        
        self.df = self.df.withColumn(name, value)
