from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, when, date_format, round
import yaml
import os

def run_spark_job_1(config):
    """
    Analyzes CPU and Memory data to generate alerts.
    """
    spark = SparkSession.builder.appName(f"Team{config['team_number']}SparkJob1").getOrCreate()

    # --- 1. Data Loading and Preparation ---
    output_dir = config['paths']['output_dir']
    cpu_df = spark.read.csv(os.path.join(output_dir, 'cpu_data.csv'), header=True, inferSchema=True)
    mem_df = spark.read.csv(os.path.join(output_dir, 'mem_data.csv'), header=True, inferSchema=True)

    # Join and convert data types
    combined_df = cpu_df.join(mem_df, on=["ts", "server_id"], how="inner") \
        .withColumn("ts", col("ts").cast("timestamp")) \
        .withColumn("cpu_pct", col("cpu_pct").cast("float")) \
        .withColumn("mem_pct", col("mem_pct").cast("float"))

    # --- 2. Windowed Aggregation ---
    window_duration = config['spark_jobs']['window_duration']
    slide_duration = config['spark_jobs']['slide_duration']

    aggregated_df = combined_df.groupBy(
        "server_id",
        window("ts", window_duration, slide_duration)
    ).agg(
        avg("cpu_pct").alias("avg_cpu"),
        avg("mem_pct").alias("avg_mem")
    )

    # --- 3. Applying the Alerting Logic ---
    cpu_threshold = config['alert_thresholds']['cpu_pct']
    mem_threshold = config['alert_thresholds']['mem_pct']

    alerts_df = aggregated_df.withColumn("alert",
        when((col("avg_cpu") > cpu_threshold) & (col("avg_mem") > mem_threshold), "High CPU + Memory stress")
        .when(col("avg_cpu") > cpu_threshold, "CPU spike suspected")
        .when(col("avg_mem") > mem_threshold, "Memory saturation suspected")
        .otherwise("OK")
    )

    # --- 4. Formatting and Saving the Output ---
    final_df = alerts_df.select(
        col("server_id"),
        date_format(col("window.start"), "HH:mm:ss").alias("window_start"),
        date_format(col("window.end"), "HH:mm:ss").alias("window_end"),
        round(col("avg_cpu"), 2).alias("avg_cpu"),
        round(col("avg_mem"), 2).alias("avg_mem"),
        col("alert")
    )

    output_filename = f"team_{config['team_number']}_CPU_MEM.csv"
    final_df.coalesce(1).write.csv(os.path.join(output_dir, output_filename), header=True, mode="overwrite")

    print(f"Spark Job 1 completed. Output saved to {output_filename}")
    spark.stop()

if __name__ == "__main__":
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    run_spark_job_1(config)
