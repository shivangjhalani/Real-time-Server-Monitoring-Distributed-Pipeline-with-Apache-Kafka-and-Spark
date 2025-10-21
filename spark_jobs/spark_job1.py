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
            (col("avg_cpu") > cpu_threshold) & (col("avg_mem") > mem_threshold),
            "High CPU + Memory stress"
        ).when(
            (col("avg_cpu") > cpu_threshold) & (col("avg_mem") <= mem_threshold),
            "CPU spike suspected"
        ).when(
            (col("avg_mem") > mem_threshold) & (col("avg_cpu") <= cpu_threshold),
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
    print("="*70)
    print("Starting Spark Job 1: CPU and Memory Analysis")
    print("="*70)

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    # Load configuration
    output_dir = config['paths']['output_dir']
    team_number = config['team_number']
    cpu_threshold = config['alert_thresholds']['cpu_pct']
    mem_threshold = config['alert_thresholds']['mem_pct']

    print(f"\nConfiguration:")
    print(f"  Team Number: {team_number}")
    print(f"  CPU Threshold: {cpu_threshold}")
    print(f"  Memory Threshold: {mem_threshold}")
    print(f"  Output Directory: {output_dir}")

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
    print("\nReading data files...")
    cpu_df = read_csv_data(spark, cpu_file, cpu_schema)
    mem_df = read_csv_data(spark, mem_file, mem_schema)

    print(f"  CPU records: {cpu_df.count()}")
    print(f"  Memory records: {mem_df.count()}")

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

    # Join CPU and Memory data on timestamp and server_id
    print("\nJoining CPU and Memory data...")
    joined_df = cpu_df.join(
        mem_df,
        (cpu_df.timestamp == mem_df.timestamp) & (cpu_df.server_id == mem_df.server_id),
        "inner"
    ).select(
        cpu_df.timestamp,
        cpu_df.server_id,
        cpu_df.cpu_pct,
        mem_df.mem_pct
    )

    print(f"  Joined records: {joined_df.count()}")

    # Apply window-based aggregation (30 seconds window, 10 seconds slide)
    print("\nApplying window-based aggregation...")
    print("  Window: 30 seconds, Slide: 10 seconds")

    windowed_df = joined_df.groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("server_id")
    ).agg(
        avg("cpu_pct").alias("avg_cpu_raw"),
        avg("mem_pct").alias("avg_mem_raw")
    )

    # Round values to 2 decimal places
    windowed_df = windowed_df.withColumn("avg_cpu", spark_round(col("avg_cpu_raw"), 2))
    windowed_df = windowed_df.withColumn("avg_mem", spark_round(col("avg_mem_raw"), 2))

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
    result_df = apply_alert_logic(windowed_df, cpu_threshold, mem_threshold)

    # Select final columns in the required order
    final_df = result_df.select(
        "server_id",
        "window_start",
        "window_end",
        "avg_cpu",
        "avg_mem",
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
    csv_file = glob.glob(os.path.join(output_file + "_temp", "part-*.csv"))[0]
    shutil.move(csv_file, output_file)
    shutil.rmtree(output_file + "_temp")

    print(f"✅ Results successfully written to: {output_file}")
    print(f"   Total windowed records: {final_df.count()}")

    print("\n" + "="*70)
    print("Spark Job 1 Completed Successfully")
    print("="*70)


