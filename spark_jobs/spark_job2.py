"""
Spark Job 2: Network and Disk Metrics Analysis
Performs window-based aggregation on Network and Disk data with anomaly detection.
"""

import yaml
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as spark_max, window, date_format,
    to_timestamp, concat, lit, when, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def load_config(config_path='../config/config.yaml'):
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session():
    """Create and return a Spark session."""
    return SparkSession.builder \
        .appName("ServerMonitoring-Job2-Network-Disk") \
        .master("local[*]") \
        .getOrCreate()


def read_csv_data(spark, file_path, schema):
    """Read CSV data with specified schema."""
    return spark.read.csv(file_path, header=True, schema=schema)


def apply_alert_logic(df, net_threshold, disk_threshold):
    """
    Apply alerting logic for Network and Disk metrics.

    Alert Conditions:
    1. max(net_in) > threshold AND max(disk_io) > threshold -> "Network flood + Disk thrash suspected"
    2. max(net_in) > threshold AND max(disk_io) <= threshold -> "Possible DDoS"
    3. max(disk_io) > threshold AND max(net_in) <= threshold -> "Disk thrash suspected"
    4. Otherwise -> "Normal"
    """
    return df.withColumn(
        "alert",
        when(
            (col("max_net_in") > net_threshold) & (col("max_disk_io") > disk_threshold),
            "Network flood + Disk thrash suspected"
        ).when(
            (col("max_net_in") > net_threshold) & (col("max_disk_io") <= disk_threshold),
            "Possible DDoS"
        ).when(
            (col("max_disk_io") > disk_threshold) & (col("max_net_in") <= net_threshold),
            "Disk thrash suspected"
        ).otherwise("Normal")
    )


def run_spark_job2(config):
    """
    Main function to execute Spark Job 2.
    Processes Network and Disk data with window-based aggregation.
    """
    print("="*70)
    print("Starting Spark Job 2: Network and Disk Analysis")
    print("="*70)

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Load configuration
    output_dir = config['paths']['output_dir']
    team_number = config['team_number']
    net_threshold = config['alert_thresholds']['net_in']
    disk_threshold = config['alert_thresholds']['disk_io']

    print(f"\nConfiguration:")
    print(f"  Team Number: {team_number}")
    print(f"  Network In Threshold: {net_threshold}")
    print(f"  Disk I/O Threshold: {disk_threshold}")
    print(f"  Output Directory: {output_dir}")

    # Define file paths
    net_file = os.path.join(output_dir, 'net_data.csv')
    disk_file = os.path.join(output_dir, 'disk_data.csv')
    output_file = os.path.join(output_dir, f'team_{team_number}_NET_DISK.csv')

    # Define schemas
    net_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("net_in", DoubleType(), True),
        StructField("net_out", DoubleType(), True)
    ])

    disk_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("disk_io", DoubleType(), True)
    ])

    # Read data
    print("\nReading data files...")
    net_df = read_csv_data(spark, net_file, net_schema)
    disk_df = read_csv_data(spark, disk_file, disk_schema)

    print(f"  Network records: {net_df.count()}")
    print(f"  Disk records: {disk_df.count()}")

    # Convert timestamp strings (HH:MM:SS) to timestamp type
    # We need to add a date component for proper timestamp conversion
    # Using a fixed date as we only care about time-based windows
    net_df = net_df.withColumn(
        "timestamp",
        to_timestamp(concat(lit("1970-01-01 "), col("ts")), "yyyy-MM-dd HH:mm:ss")
    )

    disk_df = disk_df.withColumn(
        "timestamp",
        to_timestamp(concat(lit("1970-01-01 "), col("ts")), "yyyy-MM-dd HH:mm:ss")
    )

    # Join Network and Disk data on timestamp and server_id
    print("\nJoining Network and Disk data...")
    joined_df = net_df.join(
        disk_df,
        (net_df.timestamp == disk_df.timestamp) & (net_df.server_id == disk_df.server_id),
        "inner"
    ).select(
        net_df.timestamp,
        net_df.server_id,
        net_df.net_in,
        net_df.net_out,
        disk_df.disk_io
    )

    print(f"  Joined records: {joined_df.count()}")

    # Apply window-based aggregation (30 seconds window, 10 seconds slide)
    print("\nApplying window-based aggregation...")
    print("  Window: 30 seconds, Slide: 10 seconds")

    windowed_df = joined_df.groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("server_id")
    ).agg(
        spark_max("net_in").alias("max_net_in_raw"),
        spark_max("disk_io").alias("max_disk_io_raw")
    )

    # Round values to 2 decimal places
    windowed_df = windowed_df.withColumn("max_net_in", spark_round(col("max_net_in_raw"), 2))
    windowed_df = windowed_df.withColumn("max_disk_io", spark_round(col("max_disk_io_raw"), 2))

    # Extract window start and end times
    windowed_df = windowed_df.withColumn(
        "window_start",
        date_format(col("window.start"), "HH:mm:ss")
    ).withColumn(
        "window_end",
        date_format(col("window.end"), "HH:mm:ss")
    )

    # Apply alert logic
    print("\nApplying alert logic...")
    result_df = apply_alert_logic(windowed_df, net_threshold, disk_threshold)

    # Select final columns in the required order
    final_df = result_df.select(
        "server_id",
        "window_start",
        "window_end",
        "max_net_in",
        "max_disk_io",
        "alert"
    ).orderBy("server_id", "window_start")

    # Show sample results
    print("\nSample Results:")
    final_df.show(10, truncate=False)

    # Count alerts by type
    print("\nAlert Summary:")
    alert_summary = final_df.groupBy("alert").count().orderBy("count", ascending=False)
    alert_summary.show(truncate=False)

    # Write to CSV
    print(f"\nWriting results to: {output_file}")
    final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file + "_temp")

    # Rename the output file to remove partition directory
    import glob
    import shutil

    csv_file = glob.glob(os.path.join(output_file + "_temp", "part-*.csv"))[0]
    shutil.move(csv_file, output_file)
    shutil.rmtree(output_file + "_temp")

    print(f"✅ Results successfully written to: {output_file}")
    print(f"   Total windowed records: {final_df.count()}")

    # Stop Spark session
    spark.stop()
    print("\n" + "="*70)
    print("Spark Job 2 Completed Successfully")
    print("="*70)


if __name__ == "__main__":
    # Load configuration
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')
    config = load_config(config_path)

    # Run the job
    run_spark_job2(config)


