from pyspark.sql import functions as f
from pyspark.sql.window import Window as w
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
                    .appName('Demo Spark') \
                    .getOrCreate()

demo = spark.read.format("csv").option("header",True).load('/home/eduardo/Downloads/demo.csv')

windowSpec = w.orderBy("country")
catalog = demo.select('country').distinct().orderBy(f.asc('country'))\
			  .select(f.row_number().over(windowSpec).alias('country_id'),'country')
catalog.repartition(1).write.mode('overwrite').csv("catalog_country.csv", sep='|',header=True)

read_catalog = spark.read.option('header',True).option('sep','|').csv('catalog_country.csv')

left_join = demo.join(read_catalog,['country'],how='left')

print('---------- Generacion y Union de catalogo  -----------\n')
print('------------------------------------------------------\n')
print('\tUnion del catalogo y el archivo demo: {} registros totales\n\tArchivo demo original: {} registros'.format(left_join.count(), demo.count()))
print('------------------------------------------------------\n')
print('------------------------------------------------------\n')
