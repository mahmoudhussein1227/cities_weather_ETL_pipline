from pyspark.sql import SparkSession 
import pyspark.sql.functions as f
from pyspark.sql.types import StructType , StructField , StringType , DateType , FloatType
import os ,sys
from pyspark.sql import Window
import logging

# tables to gather -> df_min_max , hot_cold_df

spark = SparkSession.builder.appName('transf') \
    .master('local[*]') \
    .getOrCreate()

os.environ['SPARK_HOME'] = 'C:/spark'
os.environ['HADOOP_HOME'] = 'C:/hadoop'
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


country  = sys.argv[1]
path = f"dags/results/{country}_cities.csv"

schema = StructType([
        StructField('city' , StringType() , False),
        StructField('country' , StringType() , True),
        StructField('time' , DateType() , False),
        StructField('max_temp' , FloatType() , False),
        StructField('min_temp' , FloatType() , False)
])

df = spark.read.csv(path=path , schema=schema , sep=',' , header=True)


weather_info = df.agg(f.max("max_temp").alias("max_temp"), 
                        f.min("min_temp").alias("min_temp")).collect()

# first analysis getting a dataframe with max overall max_temp and min overall min_temp
df_min_max = df.filter((f.col('max_temp') == weather_info[0]['max_temp'])|
                       (f.col('min_temp') == weather_info[0]['min_temp']))

mean_temp =  (weather_info[0]['max_temp'] +  weather_info[0]['min_temp'])/2

# categrorizing the cities [relative hot and relatively cold ] based on the mean value of avg_temp
mean_temp_df = df.withColumn('mean_temp' , f.round((f.col('max_temp') + f.col('min_temp'))/2 , 2))

hot_cold_df = mean_temp_df.withColumn('Weather_condtion',
                                       f.when(f.col('mean_temp') > mean_temp ,'hot')
                                        .when(f.col('mean_temp') < mean_temp ,'soft')
                                        .otherwise('moderate'))
                                      

des_file_weather_summary =f'dags/results/{country}_weather_summary.csv'
des_file_min_max = f'dags/results/{country}_weather_info.csv'


logging.info("starting the loading")
hot_cold_df.write.mode('overwrite').csv(des_file_weather_summary , header =True)

df_min_max.write.mode('overwrite').csv(des_file_min_max , header = True)

logging.info("ending the loading")