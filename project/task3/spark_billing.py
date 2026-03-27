from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum


def main():
    # Инициализация Spark Session
    spark = SparkSession.builder \
        .appName("SmartGridBilling") \
        .getOrCreate()

    # Путь к файлу внутри Apache Spark контейнера
    input_file = "/opt/spark/work-dir/task1/telemetry.csv"

    print(f"Reading data from {input_file}...")

    # Чтение CSV
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Расчет мощности (Voltage * Current) и агрегация по sensor_id
    # Энергопотребление (Вт*с)
    billing_df = df.withColumn("power_ws", col("voltage") * col("current")) \
        .groupBy("sensor_id") \
        .agg(_sum("power_ws").alias("total_power_ws")) \
        .orderBy(col("total_power_ws").desc())

    # Вывод топ-5
    print("\nТоп-5 датчиков по энергопотреблению (Spark):")
    billing_df.show(5)

    # Остановка сессии
    spark.stop()


if __name__ == "__main__":
    main()
