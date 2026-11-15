from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

# Пакет, необхідний для читання та запису Kafka зі Spark
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    f'--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0 pyspark-shell'
)
# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreamingAlerts")
         .master("local[*]")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

MY_NAME = "roman.ianivskyi"
input_topic = f"{MY_NAME}_building_sensors"
output_topic = f"{MY_NAME}_alerts"

# Спільні налаштування підключення до Kafka
jaas_config = (
    f'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_config["username"]}" '
    f'password="{kafka_config["password"]}";'
)

# 1. Зчитування параметрів алертів із CSV-файлу
alerts_conditions_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("alerts_conditions.csv")

# 2. Читання потоку даних із Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Визначення схеми для JSON із даними датчиків
json_schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", IntegerType(), True),
    StructField("humidity", IntegerType(), True)
])

# Десеріалізація та перетворення типів
clean_df = df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*") \
    .drop('key', 'value') \
    .withColumnRenamed("key_deserialized", "key") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("sensor_id", col("value_json.sensor_id")) \
    .withColumn("temperature", col("value_json.temperature")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .drop("value_json", "value_deserialized")

# 3. Агрегація даних: watermark 10 секунд та sliding window (довжина 1 хв, крок 30 сек)
aggregated_df = clean_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(
        avg("temperature").alias("t_avg"),
        avg("humidity").alias("h_avg")
    )

# 4. Cross join із правилами алертів та фільтрація (ігноруючи межі зі значенням -999)
humidity_condition = (
    (col("humidity_min") == -999) | (col("h_avg") >= col("humidity_min"))
        ) & (
    (col("humidity_max") == -999) | (col("h_avg") <= col("humidity_max"))
)

temperature_condition = (
    (col("temperature_min") == -999) | (col("t_avg") >= col("temperature_min"))
        ) & (
    (col("temperature_max") == -999) | (col("t_avg") <= col("temperature_max"))
)

alerts_df = aggregated_df.crossJoin(alerts_conditions_df) \
    .filter(humidity_condition & temperature_condition) \
    .select(
        struct(
            col("window.start").alias("start"),
            col("window.end").alias("end")
        ).alias("window"),
        round(col("t_avg"), 2).alias("t_avg"),
        round(col("h_avg"), 2).alias("h_avg"),
        col("code").cast("string").alias("code"),
        col("message"),
        current_timestamp().alias("timestamp")
    )

# Виведення алертів у консоль для контролю
console_query = alerts_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoints-alerts-console") \
    .start()

# 5. Підготовка та запис отриманих алертів у вихідний Kafka-топік
prepare_to_kafka_df = alerts_df.select(
    col("code").alias("key"),
    to_json(struct("*")).alias("value")
)

kafka_query = prepare_to_kafka_df.writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("topic", output_topic) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("checkpointLocation", "/tmp/checkpoints-alerts-kafka") \
    .start()

spark.streams.awaitAnyTermination()