"""
Spark Job 1: CPU and Memory Metrics Analysis
Performs window-based aggregation on CPU and Memory data with anomaly detection.
"""

import os
import glob
import shutil
from pyspark.sql.functions import (
    col, avg, window, date_format,
    to_timestamp, concat, lit, when, round as spark_round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


def read_csv_data(spark, file_path, schema):
    """Read CSV data with specified schema."""
    return spark.read.csv(file_path, header=True, schema=schema)


def apply_alert_logic(df, cpu_threshold, mem_threshold):
    """
    Apply alerting logic for CPU and Memory metrics.

    Alert Conditions:
    1. avg(cpu_pct) > threshold AND avg(mem_pct) > threshold -> "High CPU + Memory stress"
    2. avg(cpu_pct) > threshold AND avg(mem_pct) <= threshold -> "CPU spike suspected"
    3. avg(mem_pct) > threshold AND avg(cpu_pct) <= threshold -> "Memory saturation suspected"
    4. Otherwise -> "Normal"
    """
    return df.withColumn(
        "alert",
        when(
            (col("avg_cpu_raw") > cpu_threshold) & (col("avg_mem_raw") > mem_threshold),
            "High CPU + Memory stress"
        ).when(
            (col("avg_cpu_raw") > cpu_threshold) & (col("avg_mem_raw") <= mem_threshold),
            "CPU spike suspected"
        ).when(
            (col("avg_mem_raw") > mem_threshold) & (col("avg_cpu_raw") <= cpu_threshold),
            "Memory saturation suspected"
        ).otherwise("Normal")
    )


def process_cpu_mem_data(spark, config):
    """
    Main function to execute Spark Job 1.
    Processes CPU and Memory data with window-based aggregation.

    Args:
        spark: SparkSession instance
        config: Configuration dictionary loaded from config.yaml
    """
    # Set log level
    spark.sparkContext.setLogLevel("ERROR")

    # Load configuration
    output_dir = config['paths']['output_dir']
    team_number = config['team_number']
    cpu_threshold = config['alert_thresholds']['cpu_pct']
    mem_threshold = config['alert_thresholds']['mem_pct']

    # Define file paths
    cpu_file = os.path.join(output_dir, 'cpu_data.csv')
    mem_file = os.path.join(output_dir, 'mem_data.csv')
    output_file = os.path.join(output_dir, f'team_{team_number}_CPU_MEM.csv')

    # Define schemas
    cpu_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("cpu_pct", DoubleType(), True)
    ])

    mem_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("server_id", StringType(), True),
        StructField("mem_pct", DoubleType(), True)
    ])

    # Read data
    cpu_df = read_csv_data(spark, cpu_file, cpu_schema)
    mem_df = read_csv_data(spark, mem_file, mem_schema)

    # Convert timestamp strings (HH:MM:SS) to timestamp type
    # We need to add a date component for proper timestamp conversion
    # Using a fixed date as we only care about time-based windows
    cpu_df = cpu_df.withColumn(
        "timestamp",
        to_timestamp(concat(lit("1970-01-01 "), col("ts")), "yyyy-MM-dd HH:mm:ss")
    )

    mem_df = mem_df.withColumn(
        "timestamp",
        to_timestamp(concat(lit("1970-01-01 "), col("ts")), "yyyy-MM-dd HH:mm:ss")
    )

    # Aggregate per metric separately, then join on window bounds
    window_duration = config['spark_jobs'].get('window_duration', '30 seconds')
    slide_duration = config['spark_jobs'].get('slide_duration', '10 seconds')

    cpu_windowed = cpu_df.groupBy(
        window(col("timestamp"), window_duration, slide_duration),
        col("server_id")
    ).agg(
        avg("cpu_pct").alias("avg_cpu_raw")
    ).withColumn(
        "window_start", date_format(col("window.start"), "HH:mm:ss")
    ).withColumn(
        "window_end", date_format(col("window.end"), "HH:mm:ss")
    ).select("server_id", "window_start", "window_end", "avg_cpu_raw")

    mem_windowed = mem_df.groupBy(
        window(col("timestamp"), window_duration, slide_duration),
        col("server_id")
    ).agg(
        avg("mem_pct").alias("avg_mem_raw")
    ).withColumn(
        "window_start", date_format(col("window.start"), "HH:mm:ss")
    ).withColumn(
        "window_end", date_format(col("window.end"), "HH:mm:ss")
    ).select("server_id", "window_start", "window_end", "avg_mem_raw")

    windowed_df = cpu_windowed.join(
        mem_windowed,
        on=["server_id", "window_start", "window_end"],
        how="inner"
    )

    # Round values to 2 decimal places
    windowed_df = windowed_df.withColumn("avg_cpu", spark_round(col("avg_cpu_raw"), 2))
    windowed_df = windowed_df.withColumn("avg_mem", spark_round(col("avg_mem_raw"), 2))

    # Apply alert logic
    result_df = apply_alert_logic(windowed_df, cpu_threshold, mem_threshold)

    # Select final columns in the required order
    final_df = result_df.select(
        "server_id",
        "window_start",
        "window_end",
        "avg_cpu",
        "avg_mem",
        "alert"
    ).orderBy("window_start", "server_id")

    # Write to CSV
    final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file + "_temp")

    # Rename the output file to remove partition directory
    csv_file = glob.glob(os.path.join(output_file + "_temp", "part-*.csv"))[0]
    shutil.move(csv_file, output_file)
    shutil.rmtree(output_file + "_temp")


