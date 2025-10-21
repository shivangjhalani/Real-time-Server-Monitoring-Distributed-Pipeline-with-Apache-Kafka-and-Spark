import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, window, date_format, lit, current_date, concat, to_timestamp, when, format_number
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def process_cpu_mem_data(spark: SparkSession, config: dict):
    team_number = config["team_number"]
    cpu_threshold = config["alert_thresholds"]["cpu_pct"]
    mem_threshold = config["alert_thresholds"]["mem_pct"]
    window_duration = config["spark_jobs"]["window_duration"]
    slide_duration = config["spark_jobs"]["slide_duration"]
    output_dir = config["paths"]["output_dir"]

    spark.sparkContext.setLogLevel("ERROR")

    cpu_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("cpu_pct", FloatType(), True)
    ])

    mem_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("mem_pct", FloatType(), True)
    ])

    # Assuming consumers write to the output directory
    cpu_data_path = f"{output_dir}cpu_data.csv"
    mem_data_path = f"{output_dir}mem_data.csv"

    cpu_df = spark.read.csv(cpu_data_path, header=True, schema=cpu_schema)
    mem_df = spark.read.csv(mem_data_path, header=True, schema=mem_schema)

    # Add current date to timestamp and convert to timestamp type
    current_day = date_format(current_date(), "yyyy-MM-dd")

    cpu_df = cpu_df.withColumn("timestamp", to_timestamp(concat(lit(current_day), lit(" "), col("ts"))))
    mem_df = mem_df.withColumn("timestamp", to_timestamp(concat(lit(current_day), lit(" "), col("ts"))))

    df = cpu_df.join(mem_df, ["timestamp", "server_id"])

    windowed_df = df.groupBy(
        window(col("timestamp"), window_duration, slide_duration),
        col("server_id")
    ).agg(
        avg("cpu_pct").alias("avg_cpu"),
        avg("mem_pct").alias("avg_mem")
    )

    alert_df = windowed_df.withColumn(
        "alert",
        when((col("avg_cpu") > cpu_threshold) & (col("avg_mem") > mem_threshold), "High CPU + Memory stress")
        .when((col("avg_cpu") > cpu_threshold) & (col("avg_mem") <= mem_threshold), "CPU spike suspected")
        .when((col("avg_mem") > mem_threshold) & (col("avg_cpu") <= cpu_threshold), "Memory saturation suspected")
        .otherwise("Normal")
    )

    final_df = alert_df.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        format_number(col("avg_cpu"), 2).alias("avg_cpu"),
        format_number(col("avg_mem"), 2).alias("avg_mem"),
        col("alert")
    ).filter(col("alert") != "Normal")

    output_path = f"{output_dir}team_{team_number}_CPU_MEM"
    final_df.write.csv(output_path, header=True, mode="overwrite")

    print(f"Spark Job 1 finished. Output written to {output_path}")

if __name__ == "__main__":
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    spark = SparkSession.builder \
        .appName(f"Team{config['team_number']}-CPU-Memory-Analysis") \
        .getOrCreate()

    process_cpu_mem_data(spark, config)
    spark.stop()
