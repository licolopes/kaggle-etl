from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

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
df2 = df.groupBy("reason_for_admission").count()
df2 = df2.orderBy("count", ascending=False)

# Salvar o resultado
df2.limit(2).toPandas().to_csv('/opt/airflow/output/result_qtdByReason.csv', index=False)