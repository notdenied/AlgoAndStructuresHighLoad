import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum


def main():
    # Инициализация Spark Session
    spark = SparkSession.builder \
        .appName("SmartGridBilling") \
        .getOrCreate()

    # Путь к файлу внутри Apache Spark контейнера
    container_path = "/opt/spark/work-dir/task1/telemetry.csv"
    # Относительный путь для локального запуска (на хосте)
    host_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../task1/telemetry.csv"))

    if os.path.exists(container_path):
        input_file = container_path
    elif os.path.exists(host_path):
        input_file = host_path
    else:
        # If neither exists locally, fallback to the expected container path or let Spark handle it (perhaps it's a URI)
        input_file = container_path

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
