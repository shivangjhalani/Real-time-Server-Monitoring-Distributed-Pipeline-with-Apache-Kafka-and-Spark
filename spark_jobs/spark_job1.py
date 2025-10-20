import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, format_number, date_format, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_data(spark, file_path, schema):
    return spark.read.csv(file_path, header=True, schema=schema)

def process_cpu_mem_data(spark, config):
    output_dir = config['paths']['output_dir']
    cpu_file = os.path.join(output_dir, 'cpu_data.csv')
    mem_file = os.path.join(output_dir, 'mem_data.csv')

    cpu_schema = StructType([
        StructField("ts", TimestampType(), True),
        StructField("server_id", StringType(), True),
        StructField("cpu_pct", FloatType(), True)
    ])

    mem_schema = StructType([
        StructField("ts", TimestampType(), True),
        StructField("server_id", StringType(), True),
        StructField("mem_pct", FloatType(), True)
    ])

    cpu_df = read_data(spark, cpu_file, cpu_schema)
    mem_df = read_data(spark, mem_file, mem_schema)

    window_duration = config['spark_jobs']['window_duration']
    slide_duration = config['spark_jobs']['slide_duration']

    cpu_windowed = cpu_df.groupBy(window("ts", window_duration, slide_duration), "server_id") \
                         .agg(avg("cpu_pct").alias("avg_cpu"))

    mem_windowed = mem_df.groupBy(window("ts", window_duration, slide_duration), "server_id") \
                         .agg(avg("mem_pct").alias("avg_mem"))

    results = cpu_windowed.join(mem_windowed, ["window", "server_id"])

    cpu_threshold = config['alert_thresholds']['cpu_pct']
    mem_threshold = config['alert_thresholds']['mem_pct']

    results = results.withColumn("alert",
        when((col("avg_cpu") > cpu_threshold) & (col("avg_mem") > mem_threshold), "High CPU + Memory stress")
        .when((col("avg_cpu") > cpu_threshold) & (col("avg_mem") <= mem_threshold), "CPU spike suspected")
        .when((col("avg_mem") > mem_threshold) & (col("avg_cpu") <= cpu_threshold), "Memory saturation suspected")
        .otherwise("Normal")
    )

    results = results.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        format_number(col("avg_cpu"), 2).alias("avg_cpu"),
        format_number(col("avg_mem"), 2).alias("avg_mem"),
        col("alert")
    ).filter(col("alert") != "Normal")

    team_no = config['team_number']
    final_csv_path = os.path.join(output_dir, f"team_{team_no}_CPU_MEM.csv")
    temp_dir = os.path.join(output_dir, f"tmp_team_{team_no}_CPU_MEM")
    results.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
    # Move the single part file to final CSV and cleanup temp dir
    part_file = None
    for name in os.listdir(temp_dir):
        if name.startswith("part-") and name.endswith(".csv"):
            part_file = name
            break
    if part_file is not None:
        os.replace(os.path.join(temp_dir, part_file), final_csv_path)
    # remove remaining files and temp directory
    for name in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, name))
    os.rmdir(temp_dir)

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    spark = create_spark_session("CPU_Memory_Analysis")
    process_cpu_mem_data(spark, config)
    spark.stop()
