import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, format_string, date_format, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def create_spark_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_data(spark, file_path, schema):
    return spark.read.csv(file_path, header=True, schema=schema)

def process_net_disk_data(spark, config):
    output_dir = config['paths']['output_dir']
    net_file = os.path.join(output_dir, 'net_data.csv')
    disk_file = os.path.join(output_dir, 'disk_data.csv')

    net_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("net_in", FloatType(), True),
        StructField("net_out", FloatType(), True)
    ])

    disk_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("disk_io", FloatType(), True)
    ])

    net_df = read_data(spark, net_file, net_schema) \
        .withColumn("ts", to_timestamp(col("ts"), "HH:mm:ss")) \
        .dropna(subset=["ts"])  # ensure valid timestamps
    disk_df = read_data(spark, disk_file, disk_schema) \
        .withColumn("ts", to_timestamp(col("ts"), "HH:mm:ss")) \
        .dropna(subset=["ts"])  # ensure valid timestamps

    window_duration = config['spark_jobs']['window_duration']
    slide_duration = config['spark_jobs']['slide_duration']

    net_windowed = net_df.groupBy(window("ts", window_duration, slide_duration), "server_id") \
                         .agg(max("net_in").alias("max_net_in"))

    disk_windowed = disk_df.groupBy(window("ts", window_duration, slide_duration), "server_id") \
                           .agg(max("disk_io").alias("max_disk_io"))

    results = net_windowed.join(disk_windowed, ["window", "server_id"], "full_outer") \
                         .na.fill(0.0, subset=["max_net_in", "max_disk_io"])

    net_threshold = config['alert_thresholds']['net_in']
    disk_threshold = config['alert_thresholds']['disk_io']

    results = results.withColumn("alert",
        when((col("max_net_in") > net_threshold) & (col("max_disk_io") > disk_threshold), "Network flood + Disk thrash suspected")
        .when((col("max_net_in") > net_threshold) & (col("max_disk_io") <= disk_threshold), "Possible DDoS")
        .when((col("max_disk_io") > disk_threshold) & (col("max_net_in") <= net_threshold), "Disk thrash suspected")
        .otherwise("Normal")
    )

    results = results.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        format_string("%.2f", col("max_net_in")).alias("max_net_in"),
        format_string("%.2f", col("max_disk_io")).alias("max_disk_io"),
        col("alert")
    ).filter(col("alert") != "Normal")

    team_no = config['team_number']
    final_csv_path = os.path.join(output_dir, f"team_{team_no}_NET_DISK.csv")
    temp_dir = os.path.join(output_dir, f"tmp_team_{team_no}_NET_DISK")
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
    spark = create_spark_session("Network_Disk_Analysis")
    process_net_disk_data(spark, config)
    spark.stop()
