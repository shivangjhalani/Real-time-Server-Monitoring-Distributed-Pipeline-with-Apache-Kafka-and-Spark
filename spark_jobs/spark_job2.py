from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, when, date_format, round
import yaml
import os

def process_net_disk_data(spark, config):
    """
    Analyzes Network and Disk data to generate alerts.
    """
    # Spark session is now passed as an argument

    # --- 1. Data Loading and Preparation ---
    output_dir = config['paths']['output_dir']
    net_df = spark.read.csv(os.path.join(output_dir, 'net_data.csv'), header=True, inferSchema=True)
    disk_df = spark.read.csv(os.path.join(output_dir, 'disk_data.csv'), header=True, inferSchema=True)

    # Join and convert data types
    combined_df = net_df.join(disk_df, on=["ts", "server_id"], how="inner") \
        .withColumn("ts", col("ts").cast("timestamp")) \
        .withColumn("net_in", col("net_in").cast("float")) \
        .withColumn("disk_io", col("disk_io").cast("float"))

    # --- 2. Windowed Aggregation ---
    window_duration = config['spark_jobs']['window_duration']
    slide_duration = config['spark_jobs']['slide_duration']

    aggregated_df = combined_df.groupBy(
        "server_id",
        window("ts", window_duration, slide_duration)
    ).agg(
        max("net_in").alias("max_net_in"),
        max("disk_io").alias("max_disk_io")
    )

    # --- 3. Applying the Alerting Logic ---
    net_threshold = config['alert_thresholds']['net_in']
    disk_threshold = config['alert_thresholds']['disk_io']

    alerts_df = aggregated_df.withColumn("alert",
        when((col("max_net_in") > net_threshold) & (col("max_disk_io") > disk_threshold), "Network flood + Disk thrash suspected")
        .when(col("max_net_in") > net_threshold, "Possible DDoS")
        .when(col("max_disk_io") > disk_threshold, "Disk thrash suspected")
        .otherwise("OK")
    )

    # --- 4. Formatting and Saving the Output ---
    final_df = alerts_df.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        round(col("max_net_in"), 2).alias("max_net_in"),
        round(col("max_disk_io"), 2).alias("max_disk_io"),
        col("alert")
    )

    output_filename = f"team_{config['team_number']}_NET_DISK.csv"
    final_df.coalesce(1).write.csv(os.path.join(output_dir, output_filename), header=True, mode="overwrite")

    print(f"Spark Job 2 completed. Output saved to {output_filename}")
    # spark.stop() should be handled by the caller (main.py)

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)

    spark = SparkSession.builder.appName(f"Team{config['team_number']}SparkJob2").getOrCreate()
    process_net_disk_data(spark, config)
    spark.stop()
