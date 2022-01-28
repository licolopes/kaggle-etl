from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Abre a 
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.3.1.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/airflow_db") \
    .option("dbtable", "canada_covid") \
    .option("user", "airflow_user") \
    .option("password", "airflow_pass") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# Groupby e Sort do dataframe
tot = df.count()

df_ByAge = df.groupBy("age") \
    .count() \
    .withColumnRenamed('count', 'cnt_per_age') \
    .withColumn('perc_of_count_total', (F.col('cnt_per_age') / tot) * 100 ) \
    .orderBy("age", ascending=False) \
    .where("age == 50 or age == 75 or age == 90")

# Salvar o resultado
df_ByAge.toPandas().to_csv('/opt/airflow/output/result_percentByAge.csv', index=False)